/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/read_debounce/read_debounce.h"

#include "base/outcome.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "container/chunked_vector.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/future-util.h"

#include <exception>

namespace cloud_topics::l0 {

constexpr size_t max_bytes_per_iter = 10_MiB;

template<class Clock>
read_debounce<Clock>::read_debounce(read_pipeline<Clock>::stage s)
  : _pipeline_stage(s) {}

template<class Clock>
ss::future<> read_debounce<Clock>::start() {
    ssx::spawn_with_gate(_gate, [this] { return bg_loop(); });
    return ss::now();
}

template<class Clock>
ss::future<> read_debounce<Clock>::stop() {
    for (auto& state : _in_flight) {
        state.lock.broken();
    }
    co_await _gate.close();
}

template<class Clock>
ss::future<> read_debounce<Clock>::bg_loop() {
    auto holder = _gate.hold();
    while (!_pipeline_stage.stopped()) {
        // Pick up new requests as fast as possible.
        // Proxy them forward.
        auto fut = co_await ss::coroutine::as_future(
          _pipeline_stage.pull_fetch_requests(max_bytes_per_iter));

        if (fut.failed()) {
            auto e = fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read debounce stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read debounce failed to pull requests: {}",
              e);
            continue;
        }
        auto fut_res = std::move(fut).get();
        if (fut_res.has_error()) {
            auto err = fut_res.error();
            if (err == errc::shutting_down) {
                vlog(
                  _pipeline_stage.logger().debug,
                  "Read debounce stopping due to shutdown");
                co_return;
            }
            vlog(
              _pipeline_stage.logger().error,
              "Read debounce received error pulling requests: {}",
              fut_res.error());
            continue;
        }
        auto to_process = std::move(fut_res.value());
        auto queue = std::move(to_process.requests);
        while (!queue.empty()) {
            auto req = &queue.front();
            queue.pop_front();
            // Safe because the requests are capped by memory use.
            ssx::spawn_with_gate(_gate, [this, req]() mutable {
                return process_single_request(req);
            });
        }
    }
}

template<class Clock>
ss::future<>
read_debounce<Clock>::process_single_request(read_request<Clock>* req) {
    auto fallback = ss::defer([req] {
        // We need to guarantee that the request is always resolved
        // otherwise the caller will hang.
        vlog(req->rtc_logger.error, "The request is dropped");
        req->set_value(errc::unexpected_failure);
    });
    // The request is expected to target a single L0 object.
    try {
        // The metadata is supposed to refer to a single L0 object.
        // If it doesn't there will be no error but the debouncing
        // could be inefficient. Because of that read_debounce should
        // always be paired with the read_fanout. The req->query.meta
        // may still have more than one extent.
        vassert(
          req->query.meta.size() > 0, "Empty read queries are not allowed");

        // Add L0 object UUID to the map of in-flight requests so the subsequent
        // requests could be debounced
        auto id = req->query.meta.front().id.name;
        auto hash = absl::Hash<uuid_t>{}(id);
        auto ix = hash % debounce_hash_size;
        auto u = co_await _in_flight.at(ix).lock.get_units(
          req->expiration_time);

        // Here it's guaranteed that this is there is no other in-flight request
        // that targets the L0 object. It is important to proxy the request and
        // not just propagate it so this fiber would know when to release the
        // units.
        dataplane_query query{
          .output_size_estimate = req->query.output_size_estimate,
          .meta = req->query.meta.copy(),
        };

        read_request<Clock> proxy(
          req->ntp,
          std::move(query),
          req->expiration_time,
          &_pipeline_stage.get_root_rtc(),
          req->stage);

        if (_pipeline_stage.stopped()) {
            co_return;
        }

        auto fut = proxy.response.get_future();
        _pipeline_stage.push_next_stage(proxy);
        auto fut_res = co_await ss::coroutine::as_future(std::move(fut));

        if (fut_res.failed()) {
            auto err = fut_res.get_exception();
            std::rethrow_exception(err);
        }
        auto result = std::move(fut_res).get();
        if (!result.has_value()) {
            vlog(
              req->rtc_logger.warn,
              "Materialize operation failed: {}",
              result.error());
            fallback.cancel();
            req->set_value(result.error());
            co_return;
        }
        // Pipe the result back into the original request
        fallback.cancel();
        req->set_value(std::move(result.value()));
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(req->rtc_logger.debug, "Read debounce shutting down");
            fallback.cancel();
            req->set_value(errc::shutting_down);
            co_return;
        }
        vlog(
          req->rtc_logger.error,
          "Unexpected exception in read debounce: {}",
          ex);
        fallback.cancel();
        req->set_value(errc::unexpected_failure);
    }
}

template class read_debounce<ss::lowres_clock>;
template class read_debounce<ss::manual_clock>;

} // namespace cloud_topics::l0
