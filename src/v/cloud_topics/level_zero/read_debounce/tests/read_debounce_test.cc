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
#include "cloud_topics/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <expected>
#include <limits>

using namespace std::chrono_literals;

static ss::logger test_log("L0_read_debounce_test");

namespace cloud_topics {

/// Mock fetch handler that uses the actor pattern for testing.
/// Unlike a real fetch handler, this one stores requests for the test
/// to inspect and respond to.
class fetch_handler : public l0::read_pipeline_actor<ss::manual_clock> {
public:
    explicit fetch_handler(l0::read_pipeline<ss::manual_clock>::stage s)
      : l0::read_pipeline_actor<ss::manual_clock>(std::move(s)) {}

    using read_requests_list = l0::requests_list<
      l0::read_pipeline<ss::manual_clock>,
      l0::read_request<ss::manual_clock>>;

    ss::future<> start() {
        co_await l0::read_pipeline_actor<ss::manual_clock>::start();
    }

    ss::future<> stop() {
        _cv.broken();
        co_await l0::read_pipeline_actor<ss::manual_clock>::stop();
    }

    ss::future<> process(l0::pipeline_notification) override {
        auto result = this->stage().pull_fetch_requests_nowait(
          std::numeric_limits<size_t>::max());
        // Move requests to the pending list for the test to inspect
        while (!result.requests.empty()) {
            auto& req = result.requests.front();
            _pending_requests.push_back(&req);
            result.requests.pop_front();
        }
        // Signal that new requests are available
        _cv.signal();
        co_return;
    }

    void on_error(std::exception_ptr e) noexcept override {
        vlog(test_log.error, "Fetch handler error: {}", e);
    }

    /// Wait for and return pending requests
    ss::future<std::vector<l0::read_request<ss::manual_clock>*>>
    get_next_requests() {
        while (_pending_requests.empty()) {
            if (this->stage().stopped()) {
                co_return std::vector<l0::read_request<ss::manual_clock>*>{};
            }
            co_await _cv.wait();
        }
        auto result = std::move(_pending_requests);
        _pending_requests.clear();
        co_return result;
    }

private:
    std::vector<l0::read_request<ss::manual_clock>*> _pending_requests;
    ss::condition_variable _cv;
};

} // namespace cloud_topics

using namespace cloud_topics;

class read_debounce_fixture : public seastar_test {
public:
    ss::future<> start() {
        co_await pipeline.start();

        co_await debounce.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        co_await fetch_handler.start(ss::sharded_parameter([this] {
            return pipeline.local().register_read_pipeline_stage();
        }));

        // Register actors with pipeline in order: debounce -> fetch_handler
        co_await debounce.invoke_on_all(
          [this](l0::read_debounce<ss::manual_clock>& s) {
              pipeline.local().register_actor(&s);
          });

        co_await fetch_handler.invoke_on_all([this](class fetch_handler& h) {
            pipeline.local().register_actor(&h);
        });

        // Start the actors after registration
        co_await debounce.invoke_on_all(
          [](l0::read_debounce<ss::manual_clock>& s) { return s.start(); });

        co_await fetch_handler.invoke_on_all(
          [](class fetch_handler& handler) { return handler.start(); });
    }

    ss::future<> stop() {
        co_await pipeline.invoke_on_all([](auto& s) { return s.shutdown(); });
        co_await fetch_handler.stop();
        co_await debounce.stop();
        co_await pipeline.stop();
    }

    ss::sharded<l0::read_pipeline<ss::manual_clock>> pipeline;
    ss::sharded<l0::read_debounce<ss::manual_clock>> debounce;
    ss::sharded<fetch_handler> fetch_handler;
};

static const model::topic_namespace
  test_topic(model::kafka_namespace, model::topic("tapioca"));

static const model::ntp
  test_ntp(test_topic.ns, test_topic.tp, model::partition_id(0));

TEST_F_CORO(read_debounce_fixture, test_happy_path) {
    // Check that the read request is going through and the correct results
    // are propagated back to the caller.
    co_await start();
    l0::dataplane_query query;
    query.output_size_estimate = 1_MiB;
    query.meta.push_back(
      extent_meta{.byte_range_size = byte_range_size_t{1_MiB}});
    auto result_fut = pipeline.local().make_reader(
      test_ntp, std::move(query), ss::manual_clock::now() + 10s);

    auto requests = co_await fetch_handler.local().get_next_requests();
    ASSERT_FALSE_CORO(requests.empty());
    ASSERT_EQ_CORO(requests.size(), 1);

    chunked_vector<model::record_batch> batches;
    model::test::record_batch_spec spec{};
    spec.offset = model::offset{0};
    spec.count = 1;
    spec.records = 1;
    auto rb = model::test::make_random_batch(spec);
    batches.push_back(std::move(rb));

    requests.front()->set_value({{std::move(batches)}});

    auto result = co_await std::move(result_fut);

    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.value().results.size(), 1);
    ASSERT_EQ_CORO(
      result.value().results.front().header().base_offset, model::offset{0});
    co_await stop();
}

TEST_F_CORO(read_debounce_fixture, test_error_propagation) {
    // Check that the error is propagated to the caller.
    co_await start();
    l0::dataplane_query query;
    query.output_size_estimate = 1_MiB;
    query.meta.push_back(
      extent_meta{.byte_range_size = byte_range_size_t{1_MiB}});
    auto result_fut = pipeline.local().make_reader(
      test_ntp, std::move(query), ss::manual_clock::now() + 10s);

    auto requests = co_await fetch_handler.local().get_next_requests();
    ASSERT_FALSE_CORO(requests.empty());
    ASSERT_EQ_CORO(requests.size(), 1);

    requests.front()->set_value(errc::timeout);

    auto result = co_await std::move(result_fut);

    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), errc::timeout);
    co_await stop();
}
