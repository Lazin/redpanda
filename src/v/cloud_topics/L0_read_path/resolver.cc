/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/L0_read_path/resolver.h"

#include "base/unreachable.h"
#include "cloud_topics/L0_read_path/placeholder_extent_reader.h"
#include "cloud_topics/core/event_filter.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/logger.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/internal/timers.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace experimental::cloud_topics {

resolver::resolver(
  core::read_pipeline<>* pipeline,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* remote,
  cloud_io::basic_cache_service_api<>* cache,
  std::unique_ptr<cluster_partition_manager_api> pm)
  : _pipeline(pipeline)
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _cache(cache)
  , _pm(std::move(pm))
  , _rtc(&_pipeline->get_root_rtc())
  , _logger(cd_log, _rtc, "ct:resolver")
  , _my_stage(_pipeline->register_pipeline_stage()) {}

ss::future<> resolver::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_resolve_pipeline(); });
    return ss::now();
}

ss::future<> resolver::stop() { co_await _gate.close(); }

ss::future<> resolver::bg_resolve_pipeline() {
    while (true) {
        core::event_filter<> filter(
          core::event_type::new_read_request, _my_stage);
        auto event = co_await _pipeline->subscribe(
          filter, _rtc.root_abort_source());
        switch (event.type) {
        case core::event_type::shutting_down:
            co_return;
        case core::event_type::err_timedout:
        case core::event_type::new_write_request:
        case core::event_type::none:
            unreachable();
        case core::event_type::new_read_request:
            break;
        }
        auto res = co_await process_requests();
        if (res.has_error()) {
            if (res.error() == errc::shutting_down) {
                vlog(_logger.debug, "Shutting down");
                co_return;
            } else {
                // Other types of errors are logged inside
                // the 'process_request'
                _pipeline->register_pipeline_error(res.error());
            }
        }
    }
}

struct memory_limiting_consumer {
    struct result_t {
        chunked_vector<model::record_batch> batches;
        size_t total_size_bytes{0};
        size_t max_bytes{0};
        model::offset base_offset{model::offset::max()};
        model::offset last_offset;
    };

    explicit memory_limiting_consumer(result_t* result)
      : _result(result) {}

    static dl_placeholder decode_placeholder(model::record_batch batch) {
        iobuf payload = std::move(batch).release_data();
        iobuf_parser parser(std::move(payload));
        auto record = model::parse_one_record_from_buffer(parser);
        iobuf value = std::move(record).release_value();
        auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
        return placeholder;
    }

    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        if (rb.header().type == model::record_batch_type::dl_placeholder) {
            // NOTE: OK to copy because placeholders are smol
            _result->base_offset = std::min(
              _result->base_offset, rb.base_offset());
            _result->last_offset = std::max(
              _result->last_offset, rb.last_offset());
            auto placeholder = decode_placeholder(rb.copy());
            _result->total_size_bytes += placeholder.size_bytes;
            _result->batches.push_back(std::move(rb));
            if (_result->total_size_bytes > _result->max_bytes) {
                co_return ss::stop_iteration::yes;
            }
        }
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    result_t* _result;
};

/// Get original log reader obtained from Raft. Consume it and store every
/// placeholder batch in memory. Stop consuming once memory limit is
/// reached.
/// This method solves two problems:
/// 1. The underlying reader returns placeholder batches but the memory
///    limit should be applied only to materialized batches. The
///    placeholders are much smaller compared to the original 'raft_data'
///    batches that they represent.
/// 2. We need to know the offset range which is going to be consumed by the
///    read request to query aborted transactions. We can't do this based on
///    offsets in the request because the clients are specifying max offset
///    plus some memory limit. So we can't query aborted transactions before
///    the underlying reader is consumed and the actual sizes of
///    materialized batches is known.
/// We also don't want to consume too much from the reader and then discard
/// some data. This will prevent reuse of the reader.
static ss::future<memory_limiting_consumer::result_t>
prepare_log_reader(model::record_batch_reader underlying, size_t max_bytes) {
    // Current implementation expects to see placeholder batches
    // and configuration batches. It doesn't expect 'raft_data' batches
    // to there. This has to be fixed if we want to have hybrid
    // partitions that interleave placeholders and data batches.
    memory_limiting_consumer::result_t consume_result{
      .max_bytes = max_bytes,
    };
    memory_limiting_consumer cons(&consume_result);
    co_await underlying.consume(cons, model::no_timeout);
    co_return std::move(consume_result);
}

ss::future<checked<bool, errc>> resolver::process_requests() {
    // TODO: use proper limit
    auto to_process = _pipeline->get_read_requests(100_MiB, _my_stage);
    for (auto& req : to_process.ready) {
        if (req.is_timequery()) {
            vlog(
              _logger.error,
              "Timequery not supported, request ntp: {}",
              req.ntp);
            // Shortcut for timequery until timequery functionality is
            // implemented
            req.set_value(errc::timeout);
            continue;
        }
        auto partition = _pm->get_partition(req.ntp);
        if (partition == nullptr) {
            // Partition was moved
            vlog(_logger.error, "Partition {} moved", req.ntp);
            // TODO: add and use different error code
            req.set_value(errc::unexpected_failure);
            continue;
        }

        std::optional<model::record_batch_reader> prepared;
        std::optional<fragmented_vector<model::tx_range>> aborted_tx;
        try {
            auto cfg = req.get_log_reader_config();
            // Translate offsets, the cloud topics subsystem doesn't "know"
            // anything about non-data batches
            cfg.translate_offsets = storage::translate_offsets::yes;
            auto underlying = co_await partition->make_reader(
              cfg, req.expiration_time);

            auto prep_result = co_await prepare_log_reader(
              std::move(underlying), cfg.max_bytes);

            aborted_tx = co_await partition->aborted_transactions(
              prep_result.base_offset, prep_result.last_offset);
            prepared = model::make_fragmented_memory_record_batch_reader(
              std::move(prep_result.batches));
        } catch (...) {
            vlog(
              _logger.error,
              "Failed to fetch from {} due to exception: {}",
              req.ntp,
              std::current_exception());
            req.set_value(errc::unexpected_failure);
            continue;
        }

        retry_chain_node op_rtc(
          req.expiration_time,
          std::chrono::seconds(1),
          retry_strategy::disallow,
          &_rtc);

        auto reader = make_placeholder_extent_reader(
          req.get_log_reader_config(),
          _bucket,
          std::move(prepared.value()),
          *_remote,
          *_cache,
          _rtc);
        req.set_value(core::read_request_fetch_result{
          .reader = std::move(reader),
          .tx = std::move(aborted_tx.value()),
        });
    }
    co_return to_process.complete;
}

} // namespace experimental::cloud_topics
