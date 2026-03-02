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

#include "absl/container/node_hash_map.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>

namespace cloud_topics {

/// Per-shard cache for raw L0 objects downloaded from cloud storage.
///
/// Stores L0 objects split into fixed-size chunks in memory, indexed by
/// object_id. Uses a storage::batch_cache_index internally so cached data
/// participates in the same LRU and Seastar memory reclaim as materialized
/// batches.
///
/// Each chunk is wrapped in a model::record_batch for storage. On retrieval
/// the requested byte range is returned, potentially spanning multiple chunks.
class raw_object_cache {
public:
    static constexpr size_t default_chunk_size = 128_KiB;

    explicit raw_object_cache(
      storage::batch_cache& cache,
      size_t chunk_size = default_chunk_size);

    /// Store a downloaded L0 object. Returns false if already cached.
    bool put(const object_id& id, iobuf data);

    /// Read an extent from a cached L0 object. Returns the byte range
    /// as an iobuf. Increments bytes_consumed; triggers auto-eviction
    /// when fully consumed. Returns nullopt on cache miss.
    std::optional<iobuf> get_extent(
      const object_id& id,
      first_byte_offset_t offset,
      byte_range_size_t size);

    /// Evict all cached L0 objects where pred(epoch) returns true.
    size_t evict_by_epoch(std::function<bool(cluster_epoch)> pred);

    /// Explicit eviction of a single object.
    void evict(const object_id& id);

    /// Total bytes held in cache.
    size_t size_bytes() const;

    /// Number of cached objects.
    size_t object_count() const;

    /// Remove stale entries whose underlying range was evicted.
    void cleanup_stale_entries();

private:
    struct chunk_entry {
        model::offset synthetic_offset;
        size_t size;
    };

    struct object_entry {
        std::vector<chunk_entry> chunks;
        size_t total_size;
        size_t bytes_consumed{0};
    };

    void evict_entry(
      absl::node_hash_map<object_id, object_entry>::iterator it);

    bool has_any_valid_chunk(const object_entry& entry) const;
    void maybe_cleanup();

    std::optional<iobuf> read_chunk(
      const chunk_entry& chunk, size_t local_offset, size_t len);

    storage::batch_cache_index _index;
    model::offset _next_offset{0};
    absl::node_hash_map<object_id, object_entry> _objects;
    size_t _total_bytes{0};
    size_t _last_valid_count{0};
    size_t _chunk_size;
};

} // namespace cloud_topics
