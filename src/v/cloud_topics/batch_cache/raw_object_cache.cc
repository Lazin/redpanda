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

#include "base/units.h"
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "model/record.h"

#include <algorithm>
#include <cstddef>

namespace cloud_topics {

raw_object_cache::raw_object_cache(
  storage::batch_cache& cache, size_t chunk_size)
  : _index(cache)
  , _chunk_size(chunk_size) {}

bool raw_object_cache::put(const object_id& id, iobuf data) {
    if (_objects.contains(id)) {
        return false;
    }

    const size_t data_size = data.size_bytes();
    object_entry entry{
      .chunks = {},
      .total_size = data_size,
    };

    // Split data into fixed-size chunks and store each as a record_batch.
    size_t remaining = data_size;
    while (remaining > 0) {
        const size_t chunk_bytes = std::min(remaining, _chunk_size);
        auto chunk_data = data.share(data_size - remaining, chunk_bytes);

        auto offset = _next_offset;
        _next_offset = model::offset{_next_offset() + 1};

        // Build a synthetic record_batch wrapping the chunk data.
        auto records_size = chunk_data.size_bytes();
        model::record_batch_header hdr{
          .header_crc = 0,
          .size_bytes = static_cast<int32_t>(
            model::packed_record_batch_header_size + records_size),
          .base_offset = offset,
          .type = model::record_batch_type::raft_data,
          .crc = 0,
          .attrs = model::record_batch_attributes{},
          .last_offset_delta = 0,
          .first_timestamp = model::timestamp::now(),
          .max_timestamp = model::timestamp::now(),
          .producer_id = 0,
          .producer_epoch = 0,
          .base_sequence = 0,
          .record_count = 1,
        };

        model::record_batch batch(
          hdr, std::move(chunk_data), model::record_batch::tag_ctor_ng{});

        // Insert into the batch_cache_index. The batch_cache takes
        // ownership of the data behind the scenes.
        _index.put(batch, storage::batch_cache::is_dirty_entry::no);

        entry.chunks.push_back(chunk_entry{
          .synthetic_offset = offset,
          .size = chunk_bytes,
        });

        remaining -= chunk_bytes;
    }

    _total_bytes += data_size;
    _objects.emplace(id, std::move(entry));
    return true;
}

std::optional<iobuf>
raw_object_cache::read_chunk(
  const chunk_entry& chunk, size_t local_offset, size_t len) {
    auto batch = _index.get(chunk.synthetic_offset);
    if (!batch) {
        return std::nullopt;
    }

    // The record data is the raw chunk bytes we stored.
    auto records = std::move(*batch).release_data();
    if (local_offset == 0 && len == records.size_bytes()) {
        return records;
    }
    return records.share(local_offset, len);
}

std::optional<iobuf> raw_object_cache::get_extent(
  const object_id& id,
  first_byte_offset_t offset,
  byte_range_size_t size) {
    maybe_cleanup();

    auto it = _objects.find(id);
    if (it == _objects.end()) {
        return std::nullopt;
    }

    auto& entry = it->second;

    // Check that at least some chunks are still valid.
    if (!has_any_valid_chunk(entry)) {
        evict_entry(it);
        return std::nullopt;
    }

    const size_t req_offset = offset();
    const size_t req_size = size();

    if (req_offset >= entry.total_size) {
        return std::nullopt;
    }

    const size_t actual_size = std::min(
      req_size, entry.total_size - req_offset);

    iobuf result;
    size_t bytes_left = actual_size;
    size_t cursor = req_offset;
    size_t cumulative = 0;

    for (const auto& chunk : entry.chunks) {
        if (bytes_left == 0) {
            break;
        }

        size_t chunk_end = cumulative + chunk.size;
        if (cursor >= chunk_end) {
            cumulative = chunk_end;
            continue;
        }

        size_t local_off = cursor - cumulative;
        size_t to_read = std::min(bytes_left, chunk.size - local_off);

        auto chunk_data = read_chunk(chunk, local_off, to_read);
        if (!chunk_data) {
            // Chunk was evicted by memory pressure.
            return std::nullopt;
        }

        result.append(std::move(*chunk_data));
        bytes_left -= to_read;
        cursor += to_read;
        cumulative = chunk_end;
    }

    if (bytes_left > 0) {
        // Could not read all requested bytes.
        return std::nullopt;
    }

    entry.bytes_consumed += actual_size;

    // Auto-evict when fully consumed.
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
            auto next = std::next(it);
            evict_entry(it);
            it = next;
            ++evicted;
        } else {
            ++it;
        }
    }
    return evicted;
}

void raw_object_cache::evict(const object_id& id) {
    if (auto it = _objects.find(id); it != _objects.end()) {
        evict_entry(it);
    }
}

size_t raw_object_cache::size_bytes() const { return _total_bytes; }

size_t raw_object_cache::object_count() const { return _objects.size(); }

void raw_object_cache::cleanup_stale_entries() {
    for (auto it = _objects.begin(); it != _objects.end();) {
        if (!has_any_valid_chunk(it->second)) {
            auto next = std::next(it);
            evict_entry(it);
            it = next;
        } else {
            ++it;
        }
    }
}

void raw_object_cache::evict_entry(
  absl::node_hash_map<object_id, object_entry>::iterator it) {
    auto& entry = it->second;

    // Remove each chunk from the batch cache via truncate. We use truncate
    // with offset+1 so that only the batch at that exact offset is removed.
    for (const auto& chunk : entry.chunks) {
        _index.truncate(chunk.synthetic_offset);
    }

    _total_bytes -= entry.total_size;
    _objects.erase(it);
}

bool raw_object_cache::has_any_valid_chunk(const object_entry& entry) const {
    return std::ranges::any_of(entry.chunks, [this](const chunk_entry& c) {
        return _index.is_cached(c.synthetic_offset);
    });
}

void raw_object_cache::maybe_cleanup() {
    // Heuristic: if the number of objects shrank significantly since last
    // cleanup (e.g. memory reclaim evicted ranges), do a full scan.
    size_t valid = 0;
    for (const auto& [_, entry] : _objects) {
        if (has_any_valid_chunk(entry)) {
            ++valid;
        }
    }
    if (valid < _last_valid_count / 2 && _last_valid_count > 0) {
        cleanup_stale_entries();
    }
    _last_valid_count = _objects.size();
}

} // namespace cloud_topics
