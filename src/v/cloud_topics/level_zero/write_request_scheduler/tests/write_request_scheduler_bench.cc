// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/pipeline_stage.h"
#include "cloud_topics/level_zero/write_request_scheduler/write_request_scheduler.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstddef>
#include <exception>
#include <limits>

using namespace std::chrono_literals;

static cloud_topics::cluster_epoch min_epoch{3840};

namespace cloud_topics {
namespace l0 {
struct write_request_balancer_accessor {
    static void disable_data_threshold_uploads(write_request_scheduler<>* s) {
        s->_test_only_disable_data_threshold = true;
    }
    static void disable_time_based_fallback(write_request_scheduler<>* s) {
        s->_test_only_disable_time_based_fallback = true;
    }
    static ss::future<>
    apply_time_based_fallback(write_request_scheduler<>* s) {
        return s->apply_time_based_fallback();
    }
};
} // namespace l0

struct pipeline_sink : public l0::write_pipeline_actor<> {
    explicit pipeline_sink(l0::write_pipeline<>& p)
      : l0::write_pipeline_actor<>(p.register_write_pipeline_stage())
      , _pipeline(p) {
        _pipeline.register_actor(this);
    }

    ss::future<> start() { co_await l0::write_pipeline_actor<>::start(); }

    ss::future<> stop() { co_await l0::write_pipeline_actor<>::stop(); }

    ss::future<> process(l0::pipeline_notification) override {
        auto result = stage().pull_write_requests(
          std::numeric_limits<size_t>::max());
        for (auto& r : result.requests) {
            r.set_value(chunked_vector<extent_meta>{});
        }
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

    l0::write_pipeline<>& _pipeline;
};
} // namespace cloud_topics

class write_request_scheduler_bench {
public:
    ss::future<>
    start(bool disable_data_threshold, bool disable_time_based_fallback) {
        co_await pipeline.start();

        co_await scheduler.start(ss::sharded_parameter([this] {
            return pipeline.local().register_write_pipeline_stage();
        }));

        co_await scheduler.invoke_on_all(
          [disable_data_threshold, disable_time_based_fallback](
            cloud_topics::l0::write_request_scheduler<>& s) {
              if (disable_data_threshold) {
                  cloud_topics::l0::write_request_balancer_accessor::
                    disable_data_threshold_uploads(&s);
              }
              if (disable_time_based_fallback) {
                  cloud_topics::l0::write_request_balancer_accessor::
                    disable_time_based_fallback(&s);
              }
          });

        // Register scheduler as actor with the pipeline
        co_await scheduler.invoke_on_all(
          [this](cloud_topics::l0::write_request_scheduler<>& s) {
              pipeline.local().register_actor(&s);
          });

        co_await scheduler.invoke_on_all(
          [](auto& sched) { return sched.start(); });

        co_await request_sink.start(
          ss::sharded_parameter([this] { return std::ref(pipeline.local()); }));
        co_await request_sink.invoke_on_all(
          [](cloud_topics::pipeline_sink& sink) { return sink.start(); });
    }

    ss::future<> stop() {
        co_await request_sink.stop();
        co_await scheduler.stop();
        co_await pipeline.stop();
    }

    ss::sharded<cloud_topics::l0::write_pipeline<>> pipeline;
    ss::sharded<cloud_topics::l0::write_request_scheduler<>> scheduler;
    ss::sharded<cloud_topics::pipeline_sink> request_sink;
};

PERF_TEST_C(write_request_scheduler_bench, data_threshold) {
    // Boring case, data is just propagated to the next stage once
    // the threshold is reached. To reach the threshold we need to
    // generate right amount of data.
    constexpr size_t size_threshold = 0x1000;
    scoped_config cfg{};
    cfg.get("cloud_topics_produce_batching_size_threshold")
      .set_value(size_threshold);

    co_await start(false, true);

    chunked_vector<model::record_batch> batches;
    auto batch = model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = model::offset(0),
        .allow_compression = false,
        .count = 1,
        .records = 1,
        .record_sizes = std::vector<size_t>{size_threshold},
      });
    if (batch.size_bytes() < (int32_t)size_threshold) {
        throw std::runtime_error(
          fmt::format(
            "Batch size {} does not match threshold {}",
            batch.size_bytes(),
            size_threshold));
    }
    batches.push_back(std::move(batch));

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(
      co_await pipeline.local().write_and_debounce(
        model::controller_ntp,
        min_epoch,
        std::move(batches),
        ss::lowres_clock::now() + std::chrono::milliseconds(10)));
    perf_tests::stop_measuring_time();

    co_await stop();
}

PERF_TEST_C(write_request_scheduler_bench, time_fallback) {
    // This test measures the time needed to propagate write requests
    // from all shards.

    static constexpr size_t batch_size = 1000;

    // Disable all background activity and invoke time based fallback
    // manually.
    co_await start(true, true);

    auto invoke_fut = pipeline.invoke_on_all(
      [](cloud_topics::l0::write_pipeline<>& p) -> ss::future<> {
          chunked_vector<model::record_batch> batches;
          // Make shard 1 have more data so the uploads are moved to shard 1.
          size_t size = ss::this_shard_id() == ss::shard_id(1) ? batch_size * 2
                                                               : batch_size;
          auto batch = model::test::make_random_batch(
            model::test::record_batch_spec{
              .offset = model::offset(0),
              .allow_compression = false,
              .count = 1,
              .records = 1,
              .record_sizes = std::vector<size_t>{size},
            });
          batches.push_back(std::move(batch));
          return p
            .write_and_debounce(
              model::controller_ntp,
              min_epoch,
              std::move(batches),
              ss::lowres_clock::now() + std::chrono::milliseconds(10))
            .discard_result();
      });

    // Make sure requests are enqueued on all shards
    co_await ss::sleep(100ms);

    perf_tests::start_measuring_time();
    co_await cloud_topics::l0::write_request_balancer_accessor::
      apply_time_based_fallback(&scheduler.local());
    perf_tests::stop_measuring_time();

    co_await std::move(invoke_fut);
    co_await stop();
}
