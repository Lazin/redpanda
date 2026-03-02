/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/raw_object_cache.h"
#include "cloud_topics/types.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/batch_cache.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <cstring>

namespace {

iobuf make_test_payload(size_t size) {
    auto s = random_generators::gen_alphanum_string(size);
    iobuf buf;
    buf.append(s.data(), s.size());
    return buf;
}

/// Return the linearised content of an iobuf as a string for comparison.
ss::sstring iobuf_to_string(const iobuf& buf) {
    ss::sstring result(ss::sstring::initialized_later{}, buf.size_bytes());
    size_t pos = 0;
    for (const auto& frag : buf) {
        std::memcpy(result.data() + pos, frag.get(), frag.size());
        pos += frag.size();
    }
    return result;
}

} // namespace

/// Test fixture using the log_manager's batch_cache. The chunk_size is set
/// large enough (64 KiB) that each chunk occupies its own batch_cache range,
/// avoiding cross-object interference when individual ranges are evicted.
class raw_object_cache_test_fixture
  : public redpanda_thread_fixture
  , public ::testing::Test {
public:
    /// Chunk size larger than batch_cache::range::range_size (32 KiB) so
    /// that each chunk gets its own range and eviction is independent.
    static constexpr size_t chunk_size = 64_KiB;

    raw_object_cache_test_fixture()
      : redpanda_thread_fixture()
      , _helper_idx(
          app.storage.local().log_mgr().create_cache(
            storage::with_cache::yes)
            .value())
      , _cache(
          _helper_idx.testing_get_cache(),
          chunk_size) {}

    cloud_topics::raw_object_cache& cache() { return _cache; }

    /// Trigger reclaim on the batch_cache backing our raw_object_cache.
    void force_reclaim(size_t bytes) {
        _helper_idx.testing_reclaim_from_cache(bytes);
    }

private:
    storage::batch_cache_index _helper_idx;
    cloud_topics::raw_object_cache _cache;
};

TEST_F(raw_object_cache_test_fixture, put_and_get_extent) {
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    // Use a payload larger than one chunk to verify multi-chunk reads.
    constexpr size_t obj_size = chunk_size + 512;
    auto payload = make_test_payload(obj_size);
    auto expected = iobuf_to_string(payload);

    ASSERT_TRUE(cache().put(id, payload.copy()));
    ASSERT_EQ(cache().object_count(), 1);
    ASSERT_EQ(cache().size_bytes(), obj_size);

    // Read back the first 100 bytes.
    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{100});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size_bytes(), 100);
    EXPECT_EQ(iobuf_to_string(*result), expected.substr(0, 100));
}

TEST_F(raw_object_cache_test_fixture, put_duplicate_returns_false) {
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    auto payload = make_test_payload(chunk_size);

    ASSERT_TRUE(cache().put(id, payload.copy()));
    ASSERT_FALSE(cache().put(id, payload.copy()));
    ASSERT_EQ(cache().object_count(), 1);
}

TEST_F(raw_object_cache_test_fixture, get_nonexistent_returns_nullopt) {
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{10});
    ASSERT_FALSE(result.has_value());
}

TEST_F(raw_object_cache_test_fixture, auto_evict_on_full_consumption) {
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    constexpr size_t obj_size = chunk_size;
    auto payload = make_test_payload(obj_size);

    ASSERT_TRUE(cache().put(id, payload.copy()));
    ASSERT_EQ(cache().object_count(), 1);

    // Read the entire object in one go -- triggers auto-eviction.
    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{obj_size});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size_bytes(), obj_size);

    // Object should have been evicted after full consumption.
    EXPECT_EQ(cache().object_count(), 0);
    EXPECT_EQ(cache().size_bytes(), 0);

    // Subsequent get should return nullopt.
    auto again = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{1});
    EXPECT_FALSE(again.has_value());
}

TEST_F(raw_object_cache_test_fixture, explicit_evict) {
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    auto payload = make_test_payload(chunk_size + 1024);

    ASSERT_TRUE(cache().put(id, payload.copy()));
    ASSERT_EQ(cache().object_count(), 1);

    cache().evict(id);

    EXPECT_EQ(cache().object_count(), 0);
    EXPECT_EQ(cache().size_bytes(), 0);

    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{10});
    EXPECT_FALSE(result.has_value());
}

TEST_F(raw_object_cache_test_fixture, evict_by_epoch) {
    auto id1 = cloud_topics::object_id::create(
      cloud_topics::cluster_epoch{1});
    auto id2 = cloud_topics::object_id::create(
      cloud_topics::cluster_epoch{2});
    auto id3 = cloud_topics::object_id::create(
      cloud_topics::cluster_epoch{1});

    constexpr size_t sz1 = chunk_size;
    constexpr size_t sz2 = chunk_size + 1024;
    constexpr size_t sz3 = chunk_size;

    ASSERT_TRUE(cache().put(id1, make_test_payload(sz1)));
    ASSERT_TRUE(cache().put(id2, make_test_payload(sz2)));
    ASSERT_TRUE(cache().put(id3, make_test_payload(sz3)));
    ASSERT_EQ(cache().object_count(), 3);

    // Evict all objects with epoch == 1.
    auto evicted = cache().evict_by_epoch(
      [](cloud_topics::cluster_epoch e) {
          return e == cloud_topics::cluster_epoch{1};
      });

    EXPECT_EQ(evicted, sz1 + sz3);
    EXPECT_EQ(cache().object_count(), 1);
    EXPECT_EQ(cache().size_bytes(), sz2);

    // The epoch-2 object should still be partially readable.
    auto result = cache().get_extent(
      id2,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{50});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size_bytes(), 50);
}

TEST_F(raw_object_cache_test_fixture, get_extent_spanning_chunk_boundary) {
    // With our chunk_size, a payload of (chunk_size + 512) bytes occupies
    // 2 chunks. A read that straddles the chunk boundary verifies correct
    // cross-chunk assembly.
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    constexpr size_t obj_size = chunk_size + 512;
    auto payload = make_test_payload(obj_size);
    auto expected = iobuf_to_string(payload);

    ASSERT_TRUE(cache().put(id, payload.copy()));

    // Read a 1024-byte range crossing the chunk boundary.
    constexpr size_t read_off = chunk_size - 256;
    constexpr size_t read_len = 1024;
    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{read_off},
      cloud_topics::byte_range_size_t{read_len});
    ASSERT_TRUE(result.has_value());

    // The read is clamped to the object size if it would exceed it.
    const size_t expected_len = std::min(read_len, obj_size - read_off);
    ASSERT_EQ(result->size_bytes(), expected_len);
    EXPECT_EQ(
      iobuf_to_string(*result), expected.substr(read_off, expected_len));
}

TEST_F(raw_object_cache_test_fixture, multiple_objects_independent) {
    auto id1 = cloud_topics::object_id::create(
      cloud_topics::cluster_epoch{1});
    auto id2 = cloud_topics::object_id::create(
      cloud_topics::cluster_epoch{2});

    constexpr size_t sz1 = chunk_size + 512;
    constexpr size_t sz2 = chunk_size + 1024;
    auto payload1 = make_test_payload(sz1);
    auto payload2 = make_test_payload(sz2);
    auto expected1 = iobuf_to_string(payload1);
    auto expected2 = iobuf_to_string(payload2);

    ASSERT_TRUE(cache().put(id1, payload1.copy()));
    ASSERT_TRUE(cache().put(id2, payload2.copy()));
    ASSERT_EQ(cache().object_count(), 2);
    ASSERT_EQ(cache().size_bytes(), sz1 + sz2);

    // Read from each independently.
    auto r1 = cache().get_extent(
      id1,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{50});
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(iobuf_to_string(*r1), expected1.substr(0, 50));

    auto r2 = cache().get_extent(
      id2,
      cloud_topics::first_byte_offset_t{100},
      cloud_topics::byte_range_size_t{50});
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(iobuf_to_string(*r2), expected2.substr(100, 50));

    // Evict object 1, object 2 should still be accessible.
    cache().evict(id1);
    EXPECT_EQ(cache().object_count(), 1);

    auto r2_again = cache().get_extent(
      id2,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{50});
    ASSERT_TRUE(r2_again.has_value());
    EXPECT_EQ(iobuf_to_string(*r2_again), expected2.substr(0, 50));
}

TEST_F(raw_object_cache_test_fixture, get_extent_after_lru_eviction) {
    // Insert an object, then force a reclaim through the batch cache so the
    // underlying ranges are evicted. get_extent should return nullopt and
    // the stale entry should be cleaned up.
    auto id = cloud_topics::object_id::create(cloud_topics::cluster_epoch{1});
    constexpr size_t obj_size = chunk_size + 512;
    auto payload = make_test_payload(obj_size);

    ASSERT_TRUE(cache().put(id, payload.copy()));
    ASSERT_EQ(cache().object_count(), 1);

    // Reclaim a large amount to force eviction of all cached ranges.
    force_reclaim(1_MiB);

    // The object's chunks should now be invalid (evicted by LRU).
    auto result = cache().get_extent(
      id,
      cloud_topics::first_byte_offset_t{0},
      cloud_topics::byte_range_size_t{obj_size});
    EXPECT_FALSE(result.has_value());

    // The stale entry should have been cleaned up by get_extent.
    EXPECT_EQ(cache().object_count(), 0);
    EXPECT_EQ(cache().size_bytes(), 0);
}
