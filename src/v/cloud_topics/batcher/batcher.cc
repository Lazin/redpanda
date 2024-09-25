/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/batcher.h"

#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "storage/record_batch_utils.h"
#include "utils/human.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <exception>
#include <iterator>

using namespace std::chrono_literals;

namespace cloud_topics {

inline size_t serialized_size(model::reader_data_layout dl) {
    return dl.num_headers * model::packed_record_batch_header_size
           + dl.total_payload_size;
}

template<class Clock>
batcher<Clock>::batcher(
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<Clock>& remote)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _upload_timeout(
      config::shard_local_cfg().cloud_storage_segment_upload_timeout_ms.bind())
  , _upload_interval(config::shard_local_cfg()
                       .cloud_storage_upload_loop_initial_backoff_ms
                       .bind()) // TODO: use different config
  , _rtc(_as)
  // TODO: fixme
  , _logger(cd_log, _rtc, "NEEDLE") {}

template<class Clock>
ss::future<> batcher<Clock>::start() {
    if (ss::this_shard_id() == 0) {
        // Shard 0 aggregates information about all other shards
        ssx::spawn_with_gate(_gate, [this] { return bg_controller_loop(); });
    }
    co_return;
}

template<class Clock>
ss::future<> batcher<Clock>::stop() {
    _as.request_abort();
    co_await _gate.close();
    co_return;
}

template<class Clock>
typename batcher<Clock>::size_limited_write_req_list
batcher<Clock>::get_write_requests(size_t max_bytes) {
    vlog(
      _logger.trace,
      "get_write_requests called with max_bytes = {}",
      max_bytes);
    size_limited_write_req_list result;
    size_t acc_size = 0;
    // The elements in the list are in the insertion order.
    auto it = _pending.begin();
    for (; it != _pending.end(); it++) {
        auto dl = model::maybe_get_data_layout(it->reader);
        vassert(
          dl.has_value(),
          "Unexpected record_batch_reader type, can't get data layout");
        auto sz = serialized_size(dl.value());
        acc_size += sz;
        if (acc_size >= max_bytes) {
            // Include last element
            it++;
            break;
        }
    }
    result.ready.splice(result.ready.end(), _pending, _pending.begin(), it);
    result.size_bytes = acc_size;
    result.complete = _pending.empty();
    vlog(
      _logger.trace,
      "get_write_requests returned {} elements, containing {}",
      result.ready.size(),
      human::bytes(result.size_bytes));
    return result;
}

namespace details {


struct one_shot_provider : stream_provider {
    explicit one_shot_provider(ss::input_stream<char> s)
      : stream(std::move(s)) {}
    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(stream, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (stream) {
            co_await stream.value().close();
        }
    }
    std::optional<ss::input_stream<char>> stream;
};

} // namespace details

template<class Clock>
ss::future<result<size_t>> batcher<Clock>::upload_object(
  typename batcher<Clock>::aggregated_write_request_ptr p) {
    vlog(
      _logger.trace,
      "upload_object is called, {} NTP's, upload size: {}",
      p->to_upload.size(),
      human::bytes(p->size_bytes));
    auto id = uuid_t::create();
    auto name = ssx::sformat("{}", id);
    details::data_layout layout;

    // Send error response to all waiters included into the upload
    auto ack_error = [&](errc e) {
        for (auto& [ntp, vec] : p->to_upload) {
            for (auto& ph : vec) {
                ph->set_value(e);
            }
        }
    };

    // Send successful response to all waiters included into the upload.
    // The response includes placeholder batches.
    auto ack_result = [&]() mutable {
        for (auto& [ntp, vec] : p->to_upload) {
            for (auto& ph : vec) {
                auto ntp = ph->ntp;
                auto& ref = layout.get_batch_ref(ntp);
                // TODO: add some defenses from missing 'set_value' calls
                // and from double 'set_value' calls.
                ph->set_value(std::move(ref.placeholders));
            }
        }
        // Invariant: we should set every promise in the request
    };

    // The failure model is all or nothing. In case of success the entire
    // content of the 'p' is uploaded. We need to notify every participant
    // about the success. Same is true in case of failure. This code can't
    // upload the input partially.
    auto err = errc::success;
    try {
        // Clock type is not parametrized further down the call chain.
        basic_retry_chain_node<Clock> local_rtc(
          Clock::now() + _upload_timeout(),
          // Backoff doesn't matter, the operation never retries
          100ms,
          retry_strategy::disallow,
          &_rtc);

        auto path = cloud_storage_clients::object_key(name);

        // make input stream by joining different record_batch_reader outputs
        auto input_stream = ss::input_stream<char>(ss::data_source(
          std::make_unique<details::concatenating_stream_data_source<Clock>>(
            object_id{id}, p, layout)));

        // reset functor can be used only once
        // no retries are attempted
        auto reset_str = [s = std::move(input_stream)]() mutable {
            using provider_t = std::unique_ptr<stream_provider>;
            return ss::make_ready_future<provider_t>(
              std::make_unique<details::one_shot_provider>(std::move(s)));
        };
        // without retries we don't need lazy abort source
        lazy_abort_source noop{[]() { return std::nullopt; }};

        cloud_io::basic_transfer_details<Clock> td{
          .bucket = _bucket,
          .key = path,
          .parent_rtc = local_rtc,
        };

        auto upl_result = co_await _remote.upload_stream(
          std::move(td),
          p->size_bytes,
          std::move(reset_str),
          noop,
          std::string_view("L0"),
          0);

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
            err = errc::shutting_down;
        } else {
            vlog(_logger.error, "Unexpected L0 upload error {}", e);
            err = errc::unexpected_failure;
        }
    }

    if (err != errc::success) {
        vlog(_logger.error, "L0 upload error: {}", err);
        ack_error(err);
        co_return err;
    }

    ack_result();
    co_return p->size_bytes;
}

template<class Clock>
ss::future<> batcher<Clock>::wait_for_next_upload() {
    try {
        co_await _cv.wait<Clock>(Clock::now() + _upload_interval(), [this] {
            return _current_size >= 10_MiB; // TODO: use configuration parameter
        });
    } catch (const ss::condition_variable_timed_out&) {
    }
}

template<class Clock>
ss::future<> batcher<Clock>::bg_controller_loop() {
    auto h = _gate.hold();
    try {
        while (!_as.abort_requested()) {
            co_await wait_for_next_upload();

            auto list = get_write_requests(10_MiB); // TODO: use configuration parameter
            // Here it's important to upload an object
            // and set every promise in every write_request.
            // We shouldn't abandon the write requests
            // in any case because it will stall the produce
            // request.

            // TODO: invoke upload_object
        }
    } catch (...) {
        auto ptr = std::current_exception();
        if (ssx::is_shutdown_exception(ptr)) {
            vlog(_logger.debug, "bg_controller_loop is shutting down");
        } else {
            vlog(_logger.error, "bg_controller_loop has failed: {}", ptr);
        }
    }
}

template<class Clock>
ss::future<result<model::record_batch_reader>>
batcher<Clock>::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader&& r,
  std::chrono::milliseconds timeout) {
    auto h = _gate.hold();
    auto index = _index++;
    auto layout = maybe_get_data_layout(r);
    if (!layout.has_value()) {
        // We expect to get in-memory record batch reader here so
        // we will be able to estimate the size.
        co_return errc::timeout;
    }
    auto size = serialized_size(layout.value());
    // The write request is stored on the stack of the
    // fiber until the 'response' promise is set. The
    // promise can be set by any fiber that uploaded the
    // data from the write request.
    write_request request(std::move(ntp), index, std::move(r), timeout);
    // TODO: The MT-version of this could be implemented as an external
    // load balancer that submits request to chosen shard
    // and awaits the result. This method will be an entry point into the
    // load balancer.
    _current_size += size;
    auto fut = request.response.get_future();
    _pending.push_back(request);
    auto res = co_await std::move(fut);
    if (res.has_error()) {
        co_return res.error();
    }
    // At this point the request is no longer referenced
    // by any other shard
    auto rdr = model::make_memory_record_batch_reader(std::move(res.value()));
    co_return std::move(rdr);
}

template class batcher<ss::lowres_clock>;
template class batcher<ss::manual_clock>;

} // namespace cloud_topics
