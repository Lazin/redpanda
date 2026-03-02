/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/level_zero/reader/tests/materialized_extent_fixture.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/later.hh>

#include <queue>

ss::logger test_log("materialized_extent_test_log");

TEST_F_CORO(materialized_extent_fixture, materialize_from_cache) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    ASSERT_EQ_CORO(probe.num_cache_reads, 1);
}

TEST_F_CORO(materialized_extent_fixture, materialize_from_cloud) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(false, 1);

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(res.has_value());

    chunked_vector<model::record_batch> actual;
    actual.emplace_back(
      cloud_topics::l0::make_raft_data_batch(std::move(extent)));

    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
    // NOTE: the cloud_io::remote is mocked so the callbacks
    // that update cloud_* metrics are not invoked. The raw_object_cache
    // put is not tracked in the probe.
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_failure) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_failure}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_failure);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_shutdown) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_shutdown}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::shutting_down);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_notfound) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_notfound}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::download_not_found);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_return_timeout) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::return_timeout}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::timeout);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}

TEST_F_CORO(materialized_extent_fixture, cloud_get_throw_error) {
    const int num_batches = 1;
    co_await add_random_batches(num_batches);
    produce_placeholders(
      false,
      1,
      std::queue<injected_failure>(
        {{.cloud_get = injected_cloud_get_failure::throw_error}}));

    ss::abort_source as;
    retry_chain_node rtc(as, 10s, 200ms, retry_strategy::disallow);

    auto extent = make_materialized_extent(partition.front().copy());
    cloud_topics::l0::micro_probe probe;
    auto res = co_await cloud_topics::l0::materialize(
      &extent,
      cloud_storage_clients::bucket_name("foo"),
      &remote,
      &_l0_cache,
      &rtc,
      &probe);

    ASSERT_TRUE_CORO(!res.has_value());
    ASSERT_EQ_CORO(res.error(), cloud_topics::errc::unexpected_failure);
    ASSERT_EQ_CORO(probe.num_cache_reads, 0);
}
