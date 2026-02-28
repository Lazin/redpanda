# Raw L0 Object Cache Design

## Problem

The level_zero read path (`src/v/cloud_topics/level_zero/reader/`) depends on
the cloud storage disk cache (`cloud_io::basic_cache_service_api`). On cache
miss the reader downloads L0 objects from S3, writes them to disk, then serves
subsequent reads via range reads from disk. This limits performance and
consumes disk bandwidth. Network is faster than disk and memory is cheaper to
grow than disk in cloud environments.

## Goal

Replace the cloud storage disk cache dependency in the L0 reader with an
in-memory raw data cache built on top of existing `storage::batch_cache`
infrastructure. Downloaded L0 objects are cached in memory and served directly,
eliminating disk I/O on the read path entirely.

## Design Decisions

- **Granularity:** Store whole L0 objects (not per-extent chunks). Simpler,
  avoids re-downloading when multiple partitions need data from the same
  object.
- **Index:** Do not use `batch_cache_index` as the primary lookup. Use a
  separate `object_id -> entry` map. However, use a `batch_cache_index`
  internally as the adapter to plug into `storage::batch_cache`'s LRU and
  Seastar memory reclaim.
- **Synthetic offsets:** Assign monotonically increasing synthetic
  `model::offset` values to L0 objects. The `batch_cache_index` is keyed by
  these synthetic offsets. A side map translates `object_id` to synthetic
  offset for lookup.
- **Eviction - LRU:** Ranges holding L0 data participate in the same LRU list
  as all other batch_cache ranges. The Seastar reclaimer evicts oldest-first
  regardless of content type. No prioritization between materialized batches
  and raw L0 data.
- **Eviction - consumption:** Track bytes consumed per object. When all bytes
  have been read (`bytes_consumed >= total_size`), the object is auto-evicted.
  L0 objects are unlikely to be read more than once.
- **Eviction - epoch:** Store the cluster epoch from the L0 object name.
  Provide an `evict_by_epoch()` method with a caller-supplied predicate. Exact
  policy (all prior epochs, specific epoch, etc.) is TBD.
- **Memory management:** Shared with existing `storage::batch_cache`. No
  separate memory budget or reclaimer.
- **Data duplication:** Accept transient duplication between the raw L0 cache
  and the materialized `batch_cache`. Raw data is evicted once fully consumed,
  so duplication is short-lived.

## Architecture

### New Component: `raw_object_cache`

Location: `src/v/cloud_topics/batch_cache/raw_object_cache.{h,cc}`

Per-shard cache storing whole L0 objects downloaded from cloud storage.

```cpp
class raw_object_cache {
    storage::batch_cache_index _index;  // Adapter into batch_cache LRU
    model::offset _next_offset{0};      // Monotonic synthetic offset counter

    struct object_entry {
        model::offset synthetic_offset;
        size_t total_size;
        size_t bytes_consumed{0};
        model::cluster_epoch epoch;
    };
    absl::node_hash_map<object_id, object_entry> _objects;
};
```

### Public Interface

```cpp
/// Store a downloaded L0 object. Returns false if already cached.
/// The iobuf is wrapped in a minimal record_batch for storage in
/// batch_cache. Large objects (>32 KiB) get their own dedicated range.
bool put(const object_id& id, model::cluster_epoch epoch, iobuf data);

/// Read an extent from a cached L0 object. Returns the byte range as
/// an iobuf via iobuf::share(). Increments bytes_consumed; triggers
/// auto-eviction when fully consumed. Returns nullopt on cache miss.
std::optional<iobuf> get_extent(
    const object_id& id,
    first_byte_offset_t offset,
    byte_range_size_t size);

/// Evict all cached L0 objects where pred(epoch) returns true.
size_t evict_by_epoch(
    std::function<bool(model::cluster_epoch)> pred);

/// Explicit eviction of a single object.
void evict(const object_id& id);

/// Total bytes held in cache.
size_t size_bytes() const;
```

### Record Batch Wrapping

L0 objects are wrapped in a `model::record_batch` for storage via
`batch_cache_index::put()`. The records payload of the batch is the raw L0
iobuf. Overhead is one `record_batch_header` (~60 bytes) per object, negligible
for objects typically hundreds of KiB to MiB.

On retrieval via `_index.get(synthetic_offset)`, the records iobuf is extracted
from the returned `record_batch` and the requested byte range is shared out via
`iobuf::share()`.

### Reader Integration

Changes to `materialize()` in `materialized_extent.cc`:

```
Current flow:                         New flow:
1. is_cached() on disk cache          1. get_extent() on raw_object_cache
2. Hit -> range read from disk        2. Hit -> iobuf::share() from memory
3. Miss -> download from S3           3. Miss -> download from S3
4. Write to disk cache                4. put() into raw_object_cache
5. Return iobuf                       5. get_extent() -> return iobuf
```

The existing `hydrated` deduplication map in `materialize_sorted_run()` becomes
redundant since the `raw_object_cache` itself deduplicates across extents from
the same L0 object.

### Eviction Flows

**LRU (memory pressure):** Automatic via Seastar reclaimer. The batch_cache
range holding the L0 data is evicted like any other range. On next access,
`_index.get()` returns nullopt (weak_ptr invalidated), `get_extent()` returns
nullopt, reader falls back to S3 download.

**Consumption (read-once):** After `get_extent()` increments `bytes_consumed`
past `total_size`, the object is evicted from both `_objects` map and `_index`.

**Epoch (reconciliation):** Caller invokes `evict_by_epoch(predicate)` which
iterates `_objects`, removes matching entries, and evicts their ranges.

## Non-Goals

- Replacing the materialized batch_cache (write-path populated cache). That
  continues to work as before.
- Partial object caching or per-extent granularity.
- Cross-shard sharing of cached L0 objects.
