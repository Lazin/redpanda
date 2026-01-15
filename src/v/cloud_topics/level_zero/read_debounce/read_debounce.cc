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

#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "cloud_topics/logger.h"
#include "container/chunked_vector.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/future-util.h"

#include <chrono>
#include <exception>

namespace cloud_topics::l0 {

constexpr size_t max_bytes_per_iter = 10_MiB;
constexpr auto debounce_interval = std::chrono::milliseconds(250);

template<class Clock>
read_debounce<Clock>::read_debounce(typename read_pipeline<Clock>::stage s)
  : read_pipeline_actor<Clock>(std::move(s)) {}

template<class Clock>
ss::future<> read_debounce<Clock>::start() {
    co_await read_pipeline_actor<Clock>::start();
}

template<class Clock>
ss::future<> read_debounce<Clock>::stop() {
    for (auto& state : _in_flight) {
        state.lock.broken();
    }
    co_await _gate.close();
    co_await read_pipeline_actor<Clock>::stop();
}

template<class Clock>
ss::future<> read_debounce<Clock>::process(pipeline_notification) {
    // Pick up new requests as fast as possible.
    // Proxy them forward.
    auto to_process = this->stage().pull_fetch_requests_nowait(
      max_bytes_per_iter);
    auto queue = std::move(to_process.requests);
    while (!queue.empty()) {
        auto req = &queue.front();
        queue.pop_front();
        // Safe because the requests are capped by memory use.
        ssx::spawn_with_gate(
          _gate, [this, req]() mutable { return process_single_request(req); });
    }

    // Notify next actor that requests have been processed
    this->notify_next();
    co_return;
}

template<class Clock>
void read_debounce<Clock>::on_error(std::exception_ptr e) noexcept {
    vlog(cd_log.error, "Read debounce error: {}", e);
    this->stage().register_pipeline_error(errc::unexpected_failure);
}

template<class Clock>
ss::future<>
read_debounce<Clock>::process_single_request(read_request<Clock>* req) {
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
        auto u = _in_flight.at(ix).lock.try_get_units();
        if (!u.has_value() && !this->stage().stopped()) {
            try {
                u = co_await _in_flight.at(ix).lock.get_units(
                  debounce_interval);
            } catch (const ss::semaphore_timed_out&) {
                vlog(
                  req->rtc_logger.debug,
                  "Lock timed out, id: {}, proceeding anyway",
                  id);
            }
        } else if (this->stage().stopped()) {
            co_return;
        }

        // Here, it's not guaranteed that 'u' will actually have
        // units. It is possible for the 'get_units' call to time out
        // leaving 'u' uninitialized. This is intentional because we don't
        // want to block requests indefinitely. The 'get_units' call is
        // supposed to debounce requests for limited amount of time.
        // In case if 'u' is nullopt we may trigger same download twice
        // which is not a problem for correctness.
        // It's expected that the majority of GetObject requests will be
        // fulfilled within the debounce_interval.
        dataplane_query query{
          .output_size_estimate = req->query.output_size_estimate,
          .meta = req->query.meta.copy(),
        };

        auto proxy = ss::make_lw_shared<read_request<Clock>>(
          req->ntp,
          std::move(query),
          req->expiration_time,
          &this->stage().get_root_rtc(),
          req->stage);

        this->stage().push_next_stage(*proxy);

        auto holder = _gate.hold();
        proxy->response.get_future()
          .finally([proxy, u = std::move(u), h = std::move(holder)] mutable {
              // finally is used to capture 'u' and 'proxy'
              // while the request is fulfilled.
              // This call exits shortly but 'proxy' should
              // live until the request is running. The value
              // of 'u' could be 'nullopt'.
              u.reset();
          })
          .forward_to(std::move(req->response));
        // At this point it's guaranteed that the req->response
        // promise will be set.
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            vlog(req->rtc_logger.debug, "Read debounce shutting down");
            req->set_value(errc::shutting_down);
            co_return;
        }
        vlog(
          req->rtc_logger.error,
          "Unexpected exception in read debounce: {}",
          ex);
        req->set_value(errc::unexpected_failure);
    }
}

template class read_debounce<ss::lowres_clock>;
template class read_debounce<ss::manual_clock>;

} // namespace cloud_topics::l0
