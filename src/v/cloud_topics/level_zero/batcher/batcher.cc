/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/batcher/batcher.h"

#include "cloud_io/remote.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/batcher/aggregator.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "ssx/sformat.h"
#include "utils/human.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <variant>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

template<class Clock>
batcher<Clock>::batcher(
  typename write_pipeline<Clock>::stage stage,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote_api,
  cloud_topics::cluster_services* cluster_services)
  : write_pipeline_actor<Clock>(std::move(stage))
  , _cluster_services(cluster_services)
  , _remote(remote_api)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_backoff_interval(
      config::shard_local_cfg()
        .cloud_storage_upload_loop_initial_backoff_ms.bind())
  , _rtc(_as)
  , _logger(cd_log, _rtc)
  , _probe(config::shard_local_cfg().disable_metrics()) {}

template<class Clock>
ss::future<> batcher<Clock>::start() {
    vlog(cd_log.debug, "Batcher start");
    // Start the actor's message processing loop
    co_await write_pipeline_actor<Clock>::start();
}

template<class Clock>
ss::future<> batcher<Clock>::stop() {
    vlog(cd_log.debug, "Batcher stop");
    _as.request_abort();
    co_await _gate.close();
    // Stop the actor's message processing loop
    co_await write_pipeline_actor<Clock>::stop();
}

template<class Clock>
ss::future<> batcher<Clock>::process(pipeline_notification) {
    // Pull all available write requests from the pipeline
    auto list = this->stage().pull_write_requests(
      max_buffer_size, max_cardinality);

    if (list.requests.empty()) {
        co_return;
    }

    vlog(_logger.debug, "Processing {} write requests", list.requests.size());

    // Spawn the upload work in the background
    // We can spawn work without worrying about memory usage because the
    // pipeline tracks memory for us and will stop accepting new write
    // requests if we go over the limit.
    ssx::spawn_with_gate(_gate, [this, list = std::move(list)]() mutable {
        return run_once(std::move(list))
          .then([this](std::expected<std::monostate, errc> res) {
              if (!res.has_value()) {
                  if (res.error() == errc::shutting_down) {
                      vlog(_logger.info, "Batcher upload shutting down");
                  } else {
                      vlog(
                        _logger.info, "Batcher upload error: {}", res.error());
                  }
              }
          });
    });
}

template<class Clock>
void batcher<Clock>::on_error(std::exception_ptr e) noexcept {
    vlog(_logger.error, "Batcher actor error: {}", e);
}

template<class Clock>
ss::future<std::expected<size_t, errc>>
batcher<Clock>::upload_object(object_id id, iobuf payload) {
    auto content_length = payload.size_bytes();
    vlog(
      _logger.trace,
      "upload_object is called, upload size: {} ({} bytes)",
      human::bytes(content_length),
      content_length);

    auto err = errc::success;
    try {
        // Clock type is not parametrized further down the call chain.
        basic_retry_chain_node<Clock> local_rtc(
          Clock::now() + _upload_timeout(),
          _upload_backoff_interval(),
          retry_strategy::backoff,
          &_rtc);

        auto path = object_path_factory::level_zero_path(id);

        micro_probe probe;

        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = path,
          .parent_rtc = local_rtc,
          .success_cb =
            [&probe, sz = payload.size_bytes()] {
                probe.num_cloud_writes++;
                probe.cloud_write_bytes += sz;
            },
          .backoff_cb = [&probe] { probe.num_cloud_writes++; }};

        auto upl_result = co_await _remote.upload_object({
          .transfer_details = std::move(td),
          .display_str = "L0_object",
          .payload = std::move(payload),
        });

        this->stage().register_micro_probe(probe);

        switch (upl_result) {
        case cloud_io::upload_result::success:
            break;
        case cloud_io::upload_result::cancelled:
            err = errc::shutting_down;
            break;
        case cloud_io::upload_result::timedout:
            err = errc::timeout;
            break;
        case cloud_io::upload_result::failed:
            err = errc::upload_failure;
        }
    } catch (...) {
        auto e = std::current_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return std::unexpected{errc::shutting_down};
        } else {
            vlog(_logger.error, "Unexpected L0 upload error {}", e);
            // Return early to prevent the double logging below
            co_return std::unexpected{errc::unexpected_failure};
        }
    }

    if (err != errc::success) {
        vlog(_logger.warn, "L0 upload error: {}", err);
        co_return std::unexpected{err};
    }

    co_return content_length;
}

template<class Clock>
ss::future<std::expected<std::monostate, errc>> batcher<Clock>::run_once(
  typename write_pipeline<Clock>::write_requests_list list) noexcept {
    try {
        // NOTE: the main workflow looks like this:
        // - remove expired write requests
        // - collect write requests which can be aggregated/uploaded as L0
        //   object
        // - create 'aggregator' and fill it with write requests (the
        //   requests which are added to the aggregator shouldn't be removed
        //   from _pending list)
        // - the 'aggregator' is used to generate L0 object and upload it
        // - the 'aggregator' is used to acknowledge (either success or
        //   failure) all aggregated write requests
        //
        // The invariants here are:
        // 1. expired write requests shouldn't be added to the 'aggregator'
        // 2. if the request is added to the 'aggregator' its promise
        //    shouldn't be set
        //
        // The first invariant is enforced by calling
        // 'remote_timed_out_write_requests' in the same time slice as
        // collecting the write requests. The second invariant is enforced
        // by the strict order in which the ack() method is called
        // explicitly after the operation is either committed or failed.

        if (list.requests.empty()) {
            vlog(_logger.trace, "No write requests to process");
            co_return std::monostate{};
        }

        auto epoch_fut = co_await ss::coroutine::as_future<cluster_epoch>(
          _cluster_services->current_epoch(&_as));

        if (epoch_fut.failed()) {
            vlog(
              _logger.warn,
              "Failed to get cluster epoch: {}",
              epoch_fut.get_exception());
            while (!list.requests.empty()) {
                auto& wr = list.requests.back();
                wr.set_value(errc::failed_to_get_epoch);
                list.requests.pop_back();
            }
            _probe.register_epoch_error();
            co_return std::unexpected(errc::failed_to_get_epoch);
        }

        aggregator<Clock> aggregator;
        while (!list.requests.empty()) {
            auto& wr = list.requests.back();
            wr._hook.unlink();
            aggregator.add(wr);
        }

        /*
         * L0 object is named using an epoch calculated as:
         *
         *    start_epoch = max(p.highest_topic_start_epoch() for all p in L0)
         *    epoch = max(start_epoch, cached_cluster_epoch)
         */
        auto object_epoch = std::max(
          aggregator.highest_topic_start_epoch(), epoch_fut.get());

        // TODO: skip waiting if list.completed is not true
        auto object = aggregator.prepare(object_id::create(object_epoch));
        auto size_bytes = object.payload.size_bytes();
        auto result = co_await upload_object(
          object.id, std::move(object.payload));
        if (!result) {
            // TODO: fix the error
            // NOTE: it should be possible to translate the
            // error to kafka error at the call site but I
            // don't want to depend on kafka layer directly.
            // Timeout should work well at this point.
            aggregator.ack_error(errc::timeout);
            _probe.register_error();
            co_return std::unexpected{result.error()};
        }
        aggregator.ack();
        _probe.register_upload(size_bytes);
        co_return std::monostate{};
    } catch (...) {
        auto err = std::current_exception();
        if (ssx::is_shutdown_exception(err)) {
            vlog(_logger.debug, "Batcher shutdown error: {}", err);
            co_return std::unexpected{errc::shutting_down};
        }
        vlog(_logger.error, "Unexpected batcher error: {}", err);
        co_return std::unexpected{errc::unexpected_failure};
    }
}

template class batcher<ss::lowres_clock>;
template class batcher<ss::manual_clock>;

} // namespace cloud_topics::l0
