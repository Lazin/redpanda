/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/hydrated_object_cache.h"
#include "cloud_topics/types.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

using namespace std::chrono_literals;

TEST(hydrated_object_cache_test, basic_insert_and_find) {
    cloud_topics::l0::hydrated_object_cache cache(1024, 10s);

    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data;
    data.append("test_data", 9);

    cache.insert(id, data.copy());

    auto result = cache.find(id);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size_bytes(), 9);
    ASSERT_EQ(cache.size(), 1);
    ASSERT_EQ(cache.current_size_bytes(), 9);
}

TEST(hydrated_object_cache_test, find_nonexistent) {
    cloud_topics::l0::hydrated_object_cache cache(1024, 10s);

    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    auto result = cache.find(id);

    ASSERT_FALSE(result.has_value());
}

TEST(hydrated_object_cache_test, eviction_when_full) {
    cloud_topics::l0::hydrated_object_cache cache(100, 10s);

    // Insert 80 bytes
    auto id1 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data1;
    auto str1 = ss::sstring(40, 'a');
    data1.append(str1.data(), str1.size());
    cache.insert(id1, data1.copy());

    auto id2 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(2));
    iobuf data2;
    auto str2 = ss::sstring(40, 'b');
    data2.append(str2.data(), str2.size());
    cache.insert(id2, data2.copy());

    ASSERT_EQ(cache.size(), 2);
    ASSERT_EQ(cache.current_size_bytes(), 80);

    // Trigger eviction
    auto id3 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(3));
    iobuf data3;
    auto str3 = ss::sstring(50, 'c');
    data3.append(str3.data(), str3.size());
    cache.insert(id3, data3.copy());

    // Should have evicted enough to fit the new object
    ASSERT_LE(cache.current_size_bytes(), 100);
    ASSERT_LE(cache.size(), 2);

    // The new object should be findable
    auto result = cache.find(id3);
    ASSERT_TRUE(result.has_value());
}

TEST(hydrated_object_cache_test, insert_oversized_object) {
    cloud_topics::l0::hydrated_object_cache cache(100, 10s);

    auto id1 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data1;
    auto str1 = ss::sstring(40, 'a');
    data1.append(str1.data(), str1.size());
    cache.insert(id1, data1.copy());

    auto id2 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(2));
    iobuf data2;
    auto str2 = ss::sstring(40, 'b');
    data2.append(str2.data(), str2.size());
    cache.insert(id2, data2.copy());

    ASSERT_EQ(cache.size(), 2);

    // Trigger eviction. Should fit despite being larger than the cache size
    // limit.
    auto id3 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(3));
    iobuf data3;
    auto str3 = ss::sstring(150, 'c');
    data3.append(str3.data(), str3.size());
    cache.insert(id3, data3.copy());

    // Should remove all old objects
    ASSERT_EQ(cache.size(), 1);
    ASSERT_EQ(cache.current_size_bytes(), 150);
    ASSERT_TRUE(cache.find(id3).has_value());
}

TEST(hydrated_object_cache_test, clear) {
    cloud_topics::l0::hydrated_object_cache cache(1024, 10s);

    auto id1 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data1;
    data1.append("Foo", 3);
    cache.insert(id1, data1.copy());

    auto id2 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(2));
    iobuf data2;
    data2.append("Bar", 3);
    cache.insert(id2, data2.copy());

    ASSERT_EQ(cache.size(), 2);
    ASSERT_EQ(cache.current_size_bytes(), 6);

    cache.clear();

    ASSERT_EQ(cache.size(), 0);
    ASSERT_EQ(cache.current_size_bytes(), 0);
    ASSERT_FALSE(cache.find(id1).has_value());
    ASSERT_FALSE(cache.find(id2).has_value());
}

TEST_CORO(hydrated_object_cache_test, cleanup_on_idle) {
    cloud_topics::l0::basic_hydrated_object_cache<ss::manual_clock> cache(
      1024, 100ms);

    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data;
    data.append("Foo", 3);
    cache.insert(id, data.copy());

    ASSERT_EQ_CORO(cache.size(), 1);
    ASSERT_TRUE_CORO(cache.find(id).has_value());

    // Advance past the idle timeout
    ss::manual_clock::advance(150ms);

    // Yield for async work to complete
    co_await ss::sleep(1ms);

    ASSERT_EQ_CORO(cache.size(), 0);
}

TEST_CORO(hydrated_object_cache_test, cache_hit_resets_idle_timer) {
    cloud_topics::l0::basic_hydrated_object_cache<ss::manual_clock> cache(
      1024, 100ms);

    auto id1 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(1));
    iobuf data1;
    data1.append("test_data_1", 11);
    cache.insert(id1, data1.copy());

    // Advance time but not past timeout
    ss::manual_clock::advance(50ms);
    co_await ss::sleep(1ms);

    // Cache hit (should reset timer back to 100ms)
    auto id2 = cloud_topics::object_id::create(cloud_topics::cluster_epoch(2));
    iobuf data2;
    data2.append("test_data_2", 11);
    cache.insert(id2, data2.copy());

    ss::manual_clock::advance(60ms);
    co_await ss::sleep(1ms);

    ASSERT_EQ_CORO(cache.size(), 2);
}
