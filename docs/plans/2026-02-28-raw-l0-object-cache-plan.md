# Raw L0 Object Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the cloud storage disk cache in the L0 read path with an in-memory raw object cache backed by `storage::batch_cache` LRU infrastructure.

**Architecture:** A new `raw_object_cache` class stores whole L0 objects wrapped as `model::record_batch` entries in a `storage::batch_cache_index`. This gives free LRU eviction alongside materialized batches. A side map keyed by `object_id` tracks consumption and epoch for auto-eviction and epoch-based eviction. The reader's `materialize()` function is modified to use this cache instead of `cloud_io::basic_cache_service_api`.

**Tech Stack:** C++23, Seastar, Bazel, Google Test

**Design doc:** `docs/plans/2026-02-28-raw-l0-object-cache-design.md`

---

### Task 0: Add `get_range()` Accessor to `batch_cache_index`

**Files:**
- Modify: `src/v/storage/batch_cache.h`

**Step 1: Add `get_range()` method to `batch_cache_index`**

Add this public method to `storage::batch_cache_index` (after the existing
`get()` method):

```cpp
/// Return a weak_ptr to the range holding the batch at the given offset.
/// Returns an empty weak_ptr if the offset is not in the index.
batch_cache::range_ptr get_range(model::offset o) const {
    auto it = _index.find(o);
    if (it == _index.end()) {
        return {};
    }
    return it->second.range();
}
```

This is needed by `raw_object_cache` to store a `range_ptr` alongside each
cached object, enabling cheap detection of LRU eviction without calling
`_index.get()`.

**Step 2: Build to verify**

Run: `bazel build //src/v/storage:batch_cache`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/v/storage/batch_cache.h
git commit -m "storage/cache: add get_range() accessor to batch_cache_index

Returns a weak_ptr to the range at a given offset. Needed
by raw_object_cache to detect LRU eviction."
```

---

### Task 1: Create `raw_object_cache` Header

**Files:**
- Create: `src/v/cloud_topics/batch_cache/raw_object_cache.h`

**Step 1: Write the header file**

```cpp
/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "storage/batch_cache.h"

#include <absl/container/node_hash_map.h>

#include <cstddef>
#include <functional>
#include <optional>

namespace cloud_topics {

/// Per-shard cache for raw L0 objects downloaded from cloud storage.
///
/// Stores whole L0 objects in memory, indexed by object_id. Uses a
/// storage::batch_cache_index internally so cached data participates
/// in the same LRU and Seastar memory reclaim as materialized batches.
///
/// Objects are wrapped in model::record_batch for storage. On retrieval
/// the requested byte range is returned via iobuf::share().
class raw_object_cache {
public:
    explicit raw_object_cache(storage::batch_cache& cache);

    /// Store a downloaded L0 object. Returns false if already cached
    /// (another reader got there first).
    bool put(const object_id& id, iobuf data);

    /// Read an extent from a cached L0 object. Returns the byte range
    /// as an iobuf. Increments bytes_consumed; triggers auto-eviction
    /// when fully consumed. Returns nullopt on cache miss (object not
    /// cached or evicted by memory pressure).
    std::optional<iobuf> get_extent(
      const object_id& id,
      first_byte_offset_t offset,
      byte_range_size_t size);

    /// Evict all cached L0 objects where pred(epoch) returns true.
    /// Returns number of bytes evicted.
    size_t evict_by_epoch(std::function<bool(cluster_epoch)> pred);

    /// Explicit eviction of a single object.
    void evict(const object_id& id);

    /// Total bytes held in cache.
    size_t size_bytes() const;

    /// Number of cached objects.
    size_t object_count() const;

    /// Remove stale entries whose underlying range was evicted by
    /// memory pressure. Call periodically to keep accounting accurate.
    void cleanup_stale_entries();

private:
    struct object_entry {
        model::offset synthetic_offset;
        storage::batch_cache::range_ptr range;  // weak_ptr to detect eviction
        size_t total_size;
        size_t bytes_consumed{0};
    };

    void evict_entry(
      absl::node_hash_map<object_id, object_entry>::iterator it);

    bool is_entry_valid(const object_entry& entry) const;

    storage::batch_cache_index _index;
    model::offset _next_offset{0};
    absl::node_hash_map<object_id, object_entry> _objects;
    size_t _total_bytes{0};
};

} // namespace cloud_topics
```

Key decisions reflected here:
- Constructor takes `storage::batch_cache&` (same as `batch_cache_index`)
- `put()` takes `object_id` and `iobuf` — epoch is extracted from `object_id.epoch`
- `get_extent()` uses the same `first_byte_offset_t` and `byte_range_size_t` types as `extent_meta`
- Private `evict_entry()` helper shared by consumption-triggered and explicit eviction

**Step 2: Verify it compiles (no .cc yet, just header parse check)**

Run: `bazel build //src/v/cloud_topics/batch_cache:raw_object_cache`
Expected: Will fail because BUILD target doesn't exist yet — that's expected, move on.

---

### Task 2: Create `raw_object_cache` Implementation

**Files:**
- Create: `src/v/cloud_topics/batch_cache/raw_object_cache.cc`

**Step 1: Write the implementation**

```cpp
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

#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_utils.h"

namespace cloud_topics {

raw_object_cache::raw_object_cache(storage::batch_cache& cache)
  : _index(cache) {}

bool raw_object_cache::put(const object_id& id, iobuf data) {
    if (_objects.contains(id)) {
        return false;
    }

    auto offset = _next_offset;
    _next_offset = model::next_offset(offset);

    auto sz = data.size_bytes();

    // Wrap the raw iobuf in a minimal record_batch.
    // Use a raft_data batch type so the batch_cache treats it normally.
    auto header = model::record_batch_header{
      .size_bytes = static_cast<int32_t>(
        model::packed_record_batch_header_size + sz),
      .base_offset = offset,
      .type = model::record_batch_type::raft_data,
      .record_count = 1,
    };
    auto batch = model::record_batch(
      header, std::move(data), model::record_batch::tag_ctor_ng{});

    _index.put(batch, storage::batch_cache::is_dirty_entry::no);

    auto range = _index.get_range(offset);
    _objects.emplace(
      id,
      object_entry{
        .synthetic_offset = offset,
        .range = std::move(range),
        .total_size = sz,
      });
    _total_bytes += sz;

    return true;
}

std::optional<iobuf> raw_object_cache::get_extent(
  const object_id& id,
  first_byte_offset_t offset,
  byte_range_size_t size) {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
        return std::nullopt;
    }

    auto& entry = it->second;

    // Check weak_ptr first — cheap detection of LRU eviction.
    if (!is_entry_valid(entry)) {
        _total_bytes -= entry.total_size;
        _objects.erase(it);
        return std::nullopt;
    }

    auto batch = _index.get(entry.synthetic_offset);
    if (!batch.has_value()) {
        _total_bytes -= entry.total_size;
        _objects.erase(it);
        return std::nullopt;
    }

    // Extract the records iobuf from the batch and share the requested
    // byte range.
    auto records = std::move(batch->release_data());
    auto result = records.share(offset(), size());

    entry.bytes_consumed += size();
    if (entry.bytes_consumed >= entry.total_size) {
        evict_entry(it);
    }

    return result;
}

size_t
raw_object_cache::evict_by_epoch(std::function<bool(cluster_epoch)> pred) {
    size_t evicted = 0;
    for (auto it = _objects.begin(); it != _objects.end();) {
        if (pred(it->first.epoch)) {
            evicted += it->second.total_size;
            evict_entry(it++);
        } else {
            ++it;
        }
    }
    return evicted;
}

void raw_object_cache::evict(const object_id& id) {
    auto it = _objects.find(id);
    if (it != _objects.end()) {
        evict_entry(it);
    }
}

void raw_object_cache::evict_entry(
  absl::node_hash_map<object_id, object_entry>::iterator it) {
    _index.testing_evict_from_cache(it->second.synthetic_offset);
    _total_bytes -= it->second.total_size;
    _objects.erase(it);
}

size_t raw_object_cache::size_bytes() const { return _total_bytes; }

size_t raw_object_cache::object_count() const { return _objects.size(); }

bool raw_object_cache::is_entry_valid(const object_entry& entry) const {
    return entry.range && entry.range->valid();
}

void raw_object_cache::cleanup_stale_entries() {
    for (auto it = _objects.begin(); it != _objects.end();) {
        if (!is_entry_valid(it->second)) {
            _total_bytes -= it->second.total_size;
            _objects.erase(it++);
        } else {
            ++it;
        }
    }
}

} // namespace cloud_topics
```

Notes on the implementation:
- `_index.testing_evict_from_cache()` is used in `evict_entry()` — this is
  the same method used by the existing `batch_cache_test`. It removes the entry
  from the index and evicts the range from the LRU. If this is test-only,
  we'll need to check and potentially add a proper eviction method. See Step 2.
- `model::record_batch::tag_ctor_ng{}` constructs a batch directly from
  header + iobuf without serialization overhead.
- `batch->release_data()` extracts the records iobuf from the batch.

**Step 2: Verify `testing_evict_from_cache` is appropriate for production use**

Check `src/v/storage/batch_cache.h` for the method signature. If it's test-only,
we need an alternative. Options:
- Use `_index.truncate(offset)` which removes entries at and above the offset.
  Since we control synthetic offsets, we can truncate at the exact offset.
- Or find another eviction method.

Search for `testing_evict_from_cache` in `src/v/storage/batch_cache.h`:

Run: `bazel build //src/v/cloud_topics/batch_cache:raw_object_cache`

This will surface any compilation issues with the approach. Fix as needed.

---

### Task 3: Add BUILD Target for `raw_object_cache`

**Files:**
- Modify: `src/v/cloud_topics/batch_cache/BUILD`

**Step 1: Add the library target**

Add this target after the existing `batch_cache` target:

```python
redpanda_cc_library(
    name = "raw_object_cache",
    srcs = [
        "raw_object_cache.cc",
    ],
    hdrs = [
        "raw_object_cache.h",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/storage:batch_cache",
        "//src/v/storage:record_batch_utils",
        "@abseil-cpp//absl/container:node_hash_map",
    ],
)
```

**Step 2: Build to verify compilation**

Run: `bazel build //src/v/cloud_topics/batch_cache:raw_object_cache`
Expected: BUILD SUCCESS

---

### Task 4: Write Unit Tests for `raw_object_cache`

**Files:**
- Create: `src/v/cloud_topics/batch_cache/tests/raw_object_cache_test.cc`
- Modify: `src/v/cloud_topics/batch_cache/tests/BUILD`

**Step 1: Write the test file**

```cpp
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

#include <gtest/gtest.h>

namespace cloud_topics {

class raw_object_cache_test_fixture
  : public redpanda_thread_fixture
  , public ::testing::Test {
public:
    raw_object_cache_test_fixture()
      : redpanda_thread_fixture()
      , _cache(app.storage.local().log_mgr().batch_cache()) {}

    raw_object_cache _cache;

    static object_id make_object_id(
      cluster_epoch epoch = cluster_epoch{1}) {
        return object_id::create(epoch);
    }

    static iobuf make_payload(size_t size) {
        auto data = random_generators::gen_alphanum_string(size);
        iobuf buf;
        buf.append(data.data(), data.size());
        return buf;
    }
};

TEST_F(raw_object_cache_test_fixture, test_put_and_get_extent) {
    auto id = make_object_id();
    auto payload = make_payload(1024);
    auto payload_copy = payload.copy();

    ASSERT_TRUE(_cache.put(id, std::move(payload)));
    ASSERT_EQ(_cache.object_count(), 1);
    ASSERT_EQ(_cache.size_bytes(), 1024);

    // Read first 512 bytes
    auto result = _cache.get_extent(
      id, first_byte_offset_t{0}, byte_range_size_t{512});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size_bytes(), 512);

    // Verify content matches
    auto expected = payload_copy.share(0, 512);
    ASSERT_EQ(*result, expected);
}

TEST_F(raw_object_cache_test_fixture, test_put_duplicate_returns_false) {
    auto id = make_object_id();
    ASSERT_TRUE(_cache.put(id, make_payload(1024)));
    ASSERT_FALSE(_cache.put(id, make_payload(2048)));
    ASSERT_EQ(_cache.object_count(), 1);
    ASSERT_EQ(_cache.size_bytes(), 1024);
}

TEST_F(raw_object_cache_test_fixture, test_get_nonexistent_returns_nullopt) {
    auto id = make_object_id();
    auto result = _cache.get_extent(
      id, first_byte_offset_t{0}, byte_range_size_t{100});
    ASSERT_FALSE(result.has_value());
}

TEST_F(raw_object_cache_test_fixture, test_auto_evict_on_full_consumption) {
    auto id = make_object_id();
    ASSERT_TRUE(_cache.put(id, make_payload(1024)));

    // Read first half
    _cache.get_extent(id, first_byte_offset_t{0}, byte_range_size_t{512});
    ASSERT_EQ(_cache.object_count(), 1);

    // Read second half — should trigger auto-eviction
    _cache.get_extent(id, first_byte_offset_t{512}, byte_range_size_t{512});
    ASSERT_EQ(_cache.object_count(), 0);
    ASSERT_EQ(_cache.size_bytes(), 0);
}

TEST_F(raw_object_cache_test_fixture, test_explicit_evict) {
    auto id = make_object_id();
    ASSERT_TRUE(_cache.put(id, make_payload(1024)));
    ASSERT_EQ(_cache.object_count(), 1);

    _cache.evict(id);
    ASSERT_EQ(_cache.object_count(), 0);
    ASSERT_EQ(_cache.size_bytes(), 0);

    // get_extent after evict returns nullopt
    auto result = _cache.get_extent(
      id, first_byte_offset_t{0}, byte_range_size_t{100});
    ASSERT_FALSE(result.has_value());
}

TEST_F(raw_object_cache_test_fixture, test_evict_by_epoch) {
    auto id1 = make_object_id(cluster_epoch{1});
    auto id2 = make_object_id(cluster_epoch{2});
    auto id3 = make_object_id(cluster_epoch{3});

    ASSERT_TRUE(_cache.put(id1, make_payload(1000)));
    ASSERT_TRUE(_cache.put(id2, make_payload(2000)));
    ASSERT_TRUE(_cache.put(id3, make_payload(3000)));

    ASSERT_EQ(_cache.object_count(), 3);

    // Evict epochs <= 2
    auto evicted = _cache.evict_by_epoch(
      [](cluster_epoch e) { return e <= cluster_epoch{2}; });

    ASSERT_EQ(evicted, 3000); // 1000 + 2000
    ASSERT_EQ(_cache.object_count(), 1);

    // Epoch 3 object still accessible
    auto result = _cache.get_extent(
      id3, first_byte_offset_t{0}, byte_range_size_t{100});
    ASSERT_TRUE(result.has_value());
}

TEST_F(raw_object_cache_test_fixture, test_multiple_objects_independent) {
    auto id1 = make_object_id();
    auto id2 = make_object_id();

    ASSERT_TRUE(_cache.put(id1, make_payload(1024)));
    ASSERT_TRUE(_cache.put(id2, make_payload(2048)));

    ASSERT_EQ(_cache.object_count(), 2);
    ASSERT_EQ(_cache.size_bytes(), 3072);

    // Read from each independently
    auto r1 = _cache.get_extent(
      id1, first_byte_offset_t{0}, byte_range_size_t{512});
    auto r2 = _cache.get_extent(
      id2, first_byte_offset_t{0}, byte_range_size_t{1024});

    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    ASSERT_EQ(r1->size_bytes(), 512);
    ASSERT_EQ(r2->size_bytes(), 1024);
}

TEST_F(
  raw_object_cache_test_fixture,
  test_get_extent_after_lru_eviction_returns_nullopt) {
    auto id = make_object_id();
    ASSERT_TRUE(_cache.put(id, make_payload(1024)));

    // Force reclaim via the storage batch_cache
    app.storage.local().log_mgr().batch_cache().reclaim(1_MiB);

    // get_extent should detect the evicted range and clean up
    auto result = _cache.get_extent(
      id, first_byte_offset_t{0}, byte_range_size_t{100});
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(_cache.object_count(), 0);
}

} // namespace cloud_topics
```

**Step 2: Add test BUILD target**

Add to `src/v/cloud_topics/batch_cache/tests/BUILD`:

```python
redpanda_cc_gtest(
    name = "raw_object_cache_test",
    timeout = "short",
    srcs = [
        "raw_object_cache_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/batch_cache:raw_object_cache",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
```

**Step 3: Run tests**

Run: `bazel test //src/v/cloud_topics/batch_cache/tests:raw_object_cache_test`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/v/cloud_topics/batch_cache/raw_object_cache.h \
        src/v/cloud_topics/batch_cache/raw_object_cache.cc \
        src/v/cloud_topics/batch_cache/BUILD \
        src/v/cloud_topics/batch_cache/tests/raw_object_cache_test.cc \
        src/v/cloud_topics/batch_cache/tests/BUILD
git commit -m "ct/l0: add raw_object_cache component

Introduces raw_object_cache that stores whole L0 objects
in memory using storage::batch_cache LRU infrastructure.
Supports auto-eviction on full consumption and epoch-based
eviction."
```

---

### Task 5: Modify `materialize()` to Accept `raw_object_cache`

**Files:**
- Modify: `src/v/cloud_topics/level_zero/reader/materialized_extent.h`
- Modify: `src/v/cloud_topics/level_zero/reader/materialized_extent.cc`

**Step 1: Update the `materialize()` signature**

In `materialized_extent.h`, replace the `cloud_io::basic_cache_service_api<>*`
parameter with `raw_object_cache*`:

```cpp
// Old:
ss::future<result<bool>> materialize(
  materialized_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe);

// New:
ss::future<result<bool>> materialize(
  materialized_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  raw_object_cache* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe);
```

Update includes: replace `cloud_io/basic_cache_service_api.h` with
`cloud_topics/batch_cache/raw_object_cache.h`.

**Step 2: Rewrite `materialize()` implementation**

In `materialized_extent.cc`, replace the body of `materialize()`. The new
flow is:

1. Try `cache->get_extent(id, offset, size)` — if hit, done.
2. If miss, download from S3 via `api->download_object()`.
3. Call `cache->put(id, data)` to store the full object.
4. Call `cache->get_extent(id, offset, size)` to get the byte range.

Remove `materialize_from_cache()` and `materialize_from_cloud_storage()` helper
functions. Replace with a simpler implementation:

```cpp
ss::future<result<bool>> materialize(
  materialized_extent* ext,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  raw_object_cache* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe) {
    // Try cache first
    auto cached = cache->get_extent(
      ext->meta.id,
      ext->meta.first_byte_offset,
      ext->meta.byte_range_size);
    if (cached.has_value()) {
        ext->object = std::move(cached.value());
        ext->meta.first_byte_offset = first_byte_offset_t{0};
        probe->num_cache_reads++;
        probe->cache_read_bytes += ext->object.size_bytes();
        co_return true;
    }

    // Cache miss — download from S3
    auto obj_path = cloud_storage_clients::object_key{
      object_path_factory::level_zero_path(ext->meta.id)};

    iobuf payload;
    auto dl_result = co_await api->download_object({
      .transfer_details = {
        .bucket = bucket,
        .key = obj_path,
        .parent_rtc = *rtc,
      },
      .payload = payload,
    });

    if (dl_result != cloud_io::download_result::success) {
        co_return map_download_result(dl_result);
    }

    probe->num_cloud_reads++;
    probe->cloud_read_bytes += payload.size_bytes();

    // Store in raw cache
    cache->put(ext->meta.id, std::move(payload));

    // Now read the extent from cache
    auto extent_data = cache->get_extent(
      ext->meta.id,
      ext->meta.first_byte_offset,
      ext->meta.byte_range_size);
    if (extent_data.has_value()) {
        ext->object = std::move(extent_data.value());
        ext->meta.first_byte_offset = first_byte_offset_t{0};
    } else {
        // This shouldn't happen — we just put the object.
        // But handle gracefully: fall back to using the full payload.
        // This can happen if memory pressure evicted it between put and get.
        co_return errc::download_failure;
    }

    co_return false;
}
```

**Step 3: Remove old cache helper functions**

Delete `materialize_from_cache()` and `materialize_from_cloud_storage()`
from `materialized_extent.cc`. Remove corresponding includes for
`cloud_io/basic_cache_service_api.h`.

**Step 4: Update BUILD deps**

In `src/v/cloud_topics/level_zero/reader/BUILD`, for the
`materialized_extent` target:
- Remove: `"//src/v/cloud_io:basic_cache_service_api"`
- Add: `"//src/v/cloud_topics/batch_cache:raw_object_cache"`

**Step 5: Build to verify**

Run: `bazel build //src/v/cloud_topics/level_zero/reader:materialized_extent`
Expected: BUILD SUCCESS (callers may fail — that's Task 6)

**Step 6: Commit**

```bash
git add src/v/cloud_topics/level_zero/reader/materialized_extent.h \
        src/v/cloud_topics/level_zero/reader/materialized_extent.cc \
        src/v/cloud_topics/level_zero/reader/BUILD
git commit -m "ct/l0: replace disk cache with raw_object_cache in materialize

Replaces cloud_io::basic_cache_service_api with
raw_object_cache in the materialize() function. Objects
are now cached in memory via batch_cache LRU instead of
written to disk."
```

---

### Task 6: Update `materialize_sorted_run()` and Callers

**Files:**
- Modify: `src/v/cloud_topics/level_zero/reader/materialized_extent_reader.h`
- Modify: `src/v/cloud_topics/level_zero/reader/materialized_extent_reader.cc`
- Modify: `src/v/cloud_topics/level_zero/reader/BUILD` (materialized_extent_reader target)

**Step 1: Update `materialize_sorted_run()` signature**

Replace `cloud_io::basic_cache_service_api<>* cache` parameter with
`raw_object_cache* cache`.

**Step 2: Remove the `hydrated` map**

The `absl::node_hash_map<object_id, iobuf> hydrated` map (line 42) is no longer
needed — the `raw_object_cache` itself deduplicates across extents from the same
object.

Simplify the loop body: remove the `hydrated.find()` / `hydrated.insert()`
logic. Each extent just calls `materialize()` directly. The `raw_object_cache`
handles the case where the same object was already downloaded for a prior extent.

**Step 3: Update BUILD deps**

For `materialized_extent_reader` target:
- Remove: `"//src/v/cloud_io:basic_cache_service_api"`
- Add: `"//src/v/cloud_topics/batch_cache:raw_object_cache"`

**Step 4: Build**

Run: `bazel build //src/v/cloud_topics/level_zero/reader:materialized_extent_reader`
Expected: BUILD SUCCESS

**Step 5: Commit**

```bash
git add src/v/cloud_topics/level_zero/reader/materialized_extent_reader.h \
        src/v/cloud_topics/level_zero/reader/materialized_extent_reader.cc \
        src/v/cloud_topics/level_zero/reader/BUILD
git commit -m "ct/l0: simplify materialize_sorted_run with raw_object_cache

Remove the hydrated dedup map since raw_object_cache
handles object-level deduplication."
```

---

### Task 7: Update `fetch_handler` and Pipeline Wiring

**Files:**
- Modify: `src/v/cloud_topics/level_zero/reader/fetch_request_handler.h`
- Modify: `src/v/cloud_topics/level_zero/reader/fetch_request_handler.cc`
- Modify: `src/v/cloud_topics/level_zero/reader/BUILD` (fetch_handler target)

**Step 1: Update `fetch_handler` to hold `raw_object_cache*` instead of disk cache**

Replace the `cloud_io::basic_cache_service_api<>*` member/parameter with
`raw_object_cache*`. Thread it through to `materialize_sorted_run()`.

**Step 2: Update BUILD deps**

For `fetch_handler` target:
- Remove: `"//src/v/cloud_io:basic_cache_service_api"`
- Add: `"//src/v/cloud_topics/batch_cache:raw_object_cache"`

**Step 3: Build**

Run: `bazel build //src/v/cloud_topics/level_zero/reader:fetch_handler`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add src/v/cloud_topics/level_zero/reader/fetch_request_handler.h \
        src/v/cloud_topics/level_zero/reader/fetch_request_handler.cc \
        src/v/cloud_topics/level_zero/reader/BUILD
git commit -m "ct/l0: wire raw_object_cache into fetch_handler"
```

---

### Task 8: Wire `raw_object_cache` into `data_plane_impl`

**Files:**
- Modify: `src/v/cloud_topics/data_plane_impl.cc`
- Possibly modify: `src/v/cloud_topics/data_plane_api.h`

**Step 1: Create and manage `raw_object_cache` in data_plane_impl**

Add `ss::sharded<raw_object_cache>` member (or per-shard instance) to
`data_plane_impl`. Initialize it with the `storage::batch_cache` reference
from `log_manager`.

Wire it through to the `fetch_handler` instead of the cloud storage cache.

**Step 2: Build the full data plane**

Run: `bazel build //src/v/cloud_topics:data_plane`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/v/cloud_topics/data_plane_impl.cc \
        src/v/cloud_topics/data_plane_api.h
git commit -m "ct/l0: wire raw_object_cache into data plane"
```

---

### Task 9: Update Existing Tests

**Files:**
- Modify: `src/v/cloud_topics/level_zero/reader/tests/materialized_extent_test.cc`
- Modify: `src/v/cloud_topics/level_zero/reader/tests/materialized_extent_reader_test.cc`
- Modify: `src/v/cloud_topics/level_zero/reader/tests/fetch_handler_test.cc`
- Modify: `src/v/cloud_topics/level_zero/reader/tests/BUILD`

**Step 1: Update test fixtures**

Replace disk cache mocks/stubs with `raw_object_cache` instances. Tests that
previously mocked `cloud_io::basic_cache_service_api` should now create a
`storage::batch_cache` and `raw_object_cache` directly.

**Step 2: Run all reader tests**

Run: `bazel test //src/v/cloud_topics/level_zero/reader/tests/...`
Expected: All PASS

**Step 3: Run batch_cache tests**

Run: `bazel test //src/v/cloud_topics/batch_cache/tests/...`
Expected: All PASS

**Step 4: Commit**

```bash
git add src/v/cloud_topics/level_zero/reader/tests/
git commit -m "ct/l0: update reader tests for raw_object_cache"
```

---

### Task 10: Full Build and Integration Check

**Step 1: Build everything**

Run: `bazel build //src/v/cloud_topics/...`
Expected: BUILD SUCCESS — no remaining references to the old disk cache
in the L0 reader path.

**Step 2: Run all cloud_topics tests**

Run: `bazel test //src/v/cloud_topics/...`
Expected: All PASS

**Step 3: Check for stale disk cache references**

Search for remaining `basic_cache_service_api` references in the reader:

Run: `grep -r "basic_cache_service_api" src/v/cloud_topics/level_zero/reader/`
Expected: No matches

**Step 4: Final commit if any fixups needed**

```bash
git commit -m "ct/l0: clean up stale disk cache references"
```
