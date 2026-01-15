// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/event_filter.h"
#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/read_debounce/read_debounce.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>

using namespace std::chrono_literals;
namespace cloud_topics {

/// Mock fetch handler that uses the actor pattern for benchmarks
class fetch_handler : public l0::read_pipeline_actor<> {
public:
    explicit fetch_handler(l0::read_pipeline<>::stage s)
      : l0::read_pipeline_actor<>(std::move(s)) {
        _batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset(0),
            .allow_compression = false,
            .count = 1,
            .records = 1,
            .record_sizes = std::vector<size_t>{4096},
          });
    }

    ss::future<> start() { co_await l0::read_pipeline_actor<>::start(); }

    ss::future<> stop() {
        co_await _gate.close();
        co_await l0::read_pipeline_actor<>::stop();
    }

    ss::future<> process(l0::pipeline_notification) override {
        auto result = this->stage().pull_fetch_requests_nowait(
          std::numeric_limits<size_t>::max());

        for (auto& r : result.requests) {
            process_single_request(&r);
        }
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

    void process_single_request(l0::read_request<>* req) {
        auto auto_dispose = ss::defer(
          [req] { req->set_value(errc::unexpected_failure); });

        auto meta = std::move(req->query.meta);
        chunked_vector<model::record_batch> data;
        for (auto& m : meta) {
            std::ignore = m;
            data.push_back(_batch->copy());
        }
        auto_dispose.cancel();
        req->set_value({{std::move(data)}});
    }

    std::optional<model::record_batch> _batch;
    ss::gate _gate;
};
} // namespace cloud_topics

using namespace cloud_topics;

class read_debounce_bench {
public:
    /// Start benchmark fixture.
    /// \param enable_debounce enables or disables read debounce
    ss::future<> start(bool enable_debounce) {
        co_await pipeline.start();

        if (enable_debounce) {
            co_await debounce.start(ss::sharded_parameter([this] {
                return pipeline.local().register_read_pipeline_stage();
            }));
        }

        co_await sink.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        // Register actors with pipeline
        if (enable_debounce) {
            co_await debounce.invoke_on_all(
              [this](auto& d) { pipeline.local().register_actor(&d); });
        }

        co_await sink.invoke_on_all(
          [this](auto& s) { pipeline.local().register_actor(&s); });

        // Start actors after registration
        if (enable_debounce) {
            co_await debounce.invoke_on_all([](auto& f) { return f.start(); });
        }

        co_await sink.invoke_on_all([](auto& sink) { return sink.start(); });
    }

    ss::future<> stop() {
        co_await pipeline.stop();
        co_await sink.stop();
        if (debounce.local_is_initialized()) {
            co_await debounce.stop();
        }
    }

    // Run requests serially
    ss::future<> test_run(int num_requests) {
        for (int i = 0; i < num_requests; i++) {
            l0::dataplane_query query;
            query.output_size_estimate = 1_KiB;
            query.meta.push_back(
              // The exact values doesn't matter. The only requirement
              // is that the id is different for every request.
              extent_meta{
                .id = object_id{.name = uuid_t::create()},
                .byte_range_size = byte_range_size_t{1_KiB}});

            // The requests are processed sequentially, without any concurrency
            // so we're measuring a propagation latency and not throughput. The
            // latency is expected to be in the ballpark of 10 microseconds end
            // to end. The actual I/O (either cache or cloud) will take
            // longer.
            perf_tests::start_measuring_time();
            perf_tests::do_not_optimize(
              co_await pipeline.local().make_reader(
                model::controller_ntp,
                std::move(query),
                ss::lowres_clock::now() + std::chrono::seconds(10)));
            perf_tests::stop_measuring_time();
        }
    }

    ss::sharded<cloud_topics::l0::read_pipeline<>> pipeline;
    ss::sharded<cloud_topics::l0::read_debounce<>> debounce;
    ss::sharded<cloud_topics::fetch_handler> sink;
};

PERF_TEST_C(read_debounce_bench, baseline) {
    // This is a baseline. It is measured without the debouncing.
    co_await start(false);
    co_await test_run(100);
    co_await stop();
}

PERF_TEST_C(read_debounce_bench, debounce) {
    co_await start(true);
    co_await test_run(100);
    co_await stop();
}
