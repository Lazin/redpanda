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

- **Granularity:** Split each L0 object into fixed-size chunks (128 KiB).
  Each chunk is stored as a separate `record_batch` in the batch_cache,
  giving per-chunk LRU eviction. This avoids the extremes of whole-object
  storage (too coarse for effective eviction) and per-record-batch storage
  (too many index entries). If a `get_extent()` call spans a chunk boundary,
  it reads from both chunks and concatenates.
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
static constexpr size_t chunk_size = 128_KiB;

class raw_object_cache {
    storage::batch_cache_index _index;  // Adapter into batch_cache LRU
    model::offset _next_offset{0};      // Monotonic synthetic offset counter

    struct chunk_entry {
        model::offset synthetic_offset;
        storage::batch_cache::range_ptr range;  // weak_ptr to detect eviction
        size_t size;                            // Actual size (last chunk may be smaller)
    };

    struct object_entry {
        std::vector<chunk_entry> chunks;  // Ordered by L0 byte position
        size_t total_size;
        size_t bytes_consumed{0};
    };
    absl::node_hash_map<object_id, object_entry> _objects;
};
```

Each L0 object is split into `ceil(total_size / chunk_size)` chunks. Chunks
are stored as separate `record_batch` entries in the `batch_cache_index`,
each with its own synthetic offset and LRU range. This enables per-chunk
eviction under memory pressure.

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

### Chunk Storage

Each 128 KiB chunk is wrapped in a `model::record_batch` for storage via
`batch_cache_index::put()`. The records payload of the batch is the raw chunk
iobuf. Overhead is one `record_batch_header` (~60 bytes) per chunk. For a
typical 1 MiB L0 object this means 8 chunks and ~480 bytes overhead.

On retrieval, the chunk index is computed as `byte_offset / chunk_size`. If
the requested byte range spans a chunk boundary, both chunks are read and
the relevant portions concatenated into the result iobuf.

```
get_extent(id, offset=140KiB, size=20KiB):
  chunk 1 (128-256KiB): read bytes 12KiB..128KiB  → 116KiB  (wait, no)
```

Example: `get_extent(id, offset=140KiB, size=20KiB)`:
- first_chunk = 140 / 128 = 1 (covers bytes 128-256KiB)
- last_chunk  = (140+20-1) / 128 = 1 (same chunk)
- Read from chunk 1, share bytes at local offset 12KiB, length 20KiB

Example: `get_extent(id, offset=120KiB, size=20KiB)`:
- first_chunk = 120 / 128 = 0 (covers bytes 0-128KiB)
- last_chunk  = (120+20-1) / 128 = 1 (covers bytes 128-256KiB)
- Read 8KiB from end of chunk 0, 12KiB from start of chunk 1, concatenate

### Stale Entry Detection

Each `object_entry` stores a `batch_cache::range_ptr` (weak_ptr to the
underlying range). When the Seastar memory reclaimer evicts a range, the
weak_ptr becomes invalid. This allows `raw_object_cache` to detect eviction:

- **On access:** `get_extent()` checks `entry.range` validity before calling
  `_index.get()`. If invalid, removes the stale entry from `_objects` and
  returns nullopt.
- **Size-triggered sweep:** On `put()`, if `_objects.size()` exceeds a
  threshold (e.g., 2x the number of entries with valid ranges last time we
  checked), run `cleanup_stale_entries()` before inserting. This bounds the
  stale entry overhead without requiring a timer.
- **Periodic sweep:** A `cleanup_stale_entries()` method iterates `_objects`
  and removes entries with invalid range pointers, keeping `_total_bytes` and
  `object_count()` accurate. Can also be called externally on a timer.

To obtain the `range_ptr` after insertion, a new `get_range(model::offset)`
accessor is added to `storage::batch_cache_index`:

```cpp
batch_cache::range_ptr get_range(model::offset o) const {
    auto it = _index.find(o);
    if (it == _index.end()) {
        return {};
    }
    return it->second.range();
}
```

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

**LRU (memory pressure):** Automatic via Seastar reclaimer. Individual chunks
are evicted independently — the reclaimer may evict some chunks of an L0
object while others remain. On `get_extent()`, if any required chunk has been
evicted (detected via `range_ptr`), the call returns nullopt and the reader
falls back to S3 download. A size-triggered sweep also cleans fully-stale
object entries to keep accounting accurate.

**Consumption (read-once):** After `get_extent()` increments `bytes_consumed`
past `total_size`, the object is evicted from both `_objects` map and `_index`.

**Epoch (reconciliation):** Caller invokes `evict_by_epoch(predicate)` which
iterates `_objects`, removes matching entries, and evicts their ranges.

## Non-Goals

- Replacing the materialized batch_cache (write-path populated cache). That
  continues to work as before.
- Partial object caching or per-extent granularity.
- Cross-shard sharing of cached L0 objects.
