/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/fetch_request_handler.h"

#include "cloud_topics/level_zero/pipeline/read_request.h"
#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/internal/timers.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

#include <exception>

namespace cloud_topics::l0 {

fetch_handler::fetch_handler(
  l0::read_pipeline<>::stage pipeline_stage,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* remote,
  cloud_io::basic_cache_service_api<>* cache)
  : read_pipeline_actor<>(std::move(pipeline_stage))
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _cache(cache)
  , _rtc(&this->stage().get_root_rtc())
  , _logger(cd_log, _rtc, "ct:l0_fetch_handler") {}

ss::future<> fetch_handler::start() { co_await read_pipeline_actor<>::start(); }

ss::future<> fetch_handler::stop() {
    co_await _gate.close();
    co_await read_pipeline_actor<>::stop();
}

ss::future<> fetch_handler::process(pipeline_notification) {
    // The limit here defines how much memory can be used by all
    // fetch requests on a shard. The pipeline has its own limit
    // but it should only be used to avoid OOM'ing on read_request
    // instances.
    // TODO: use proper limit
    auto to_process = this->stage().pull_fetch_requests_nowait(100_MiB);
    vlog(
      _logger.trace,
      "got {} requests from the pipeline, completeness: {}",
      to_process.requests.size(),
      to_process.complete);
    chunked_vector<ss::future<>> bg;
    for (auto& req : to_process.requests) {
        bg.push_back(process_single_request(&req));
    }
    co_await ss::when_all_succeed(bg.begin(), bg.end());

    // This is the last stage, no need to notify next actor
    co_return;
}

void fetch_handler::on_error(std::exception_ptr e) noexcept {
    vlog(_logger.error, "Fetch handler error: {}", e);
    this->stage().register_pipeline_error(errc::unexpected_failure);
}

ss::future<> fetch_handler::process_single_request(l0::read_request<>* req) {
    auto h = _gate.hold();
    auto auto_dispose = ss::defer([req] {
        // Handle situation when the request is not handled correctly
        // during shutdown or in any other case.
        vlog(req->rtc_logger.error, "Auto-dispose triggered");
        req->set_value(errc::unexpected_failure);
    });
    std::optional<model::record_batch_reader> prepared;
    std::optional<chunked_vector<model::tx_range>> aborted_tx;
    try {
        auto meta = std::move(req->query.meta);

        auto extent = co_await ss::coroutine::as_future(
          materialize_placeholders(
            _bucket,
            std::move(meta),
            *_remote,
            *_cache,
            req->rtc,
            req->rtc_logger));

        if (extent.failed()) {
            vlog(
              req->rtc_logger.warn,
              "Failed to materialize placeholders, error: {}",
              extent.get_exception());
            req->set_value(errc::download_failure);
            auto_dispose.cancel();
            co_return;
        }

        auto [res, probe] = extent.get();
        // The registration happens even for failed requests because
        // failed requests are consuming resources (API calls).
        this->stage().register_micro_probe(probe);
        if (!res.has_value()) {
            vlog(
              req->rtc_logger.warn,
              "Failed to materialize placeholders, error: {} ({})",
              res.error(),
              res.error().message());
            std::error_code ec = res.error();
            if (ec.category() == error_category()) {
                req->set_value(static_cast<errc>(res.error().value()));
            } else {
                req->set_value(errc::unexpected_failure);
            }
            auto_dispose.cancel();
            co_return;
        }

        auto data = std::move(res.value());
        if (data.empty()) {
            vlog(req->rtc_logger.debug, "Empty response");
        }

        auto_dispose.cancel();
        req->set_value(l0::dataplane_query_result{.results = std::move(data)});

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(req->rtc_logger.debug, "Failed to fetch due to shutdown");
            req->set_value(errc::shutting_down);
        } else {
            vlog(
              req->rtc_logger.error,
              "Failed to fetch, exception: {}",
              std::current_exception());
            req->set_value(errc::unexpected_failure);
        }
        auto_dispose.cancel();
        co_return;
    }
    vlog(req->rtc_logger.debug, "Request processing completed");
}

} // namespace cloud_topics::l0
