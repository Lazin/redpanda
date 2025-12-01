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

#include "absl/container/btree_map.h"
#include "base/seastarx.h"
#include "cloud_io/access_time_tracker.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/cache_probe.h"
#include "cloud_io/cache_service.h"
#include "config/configuration.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/sstring.h"
#include "storage/disk.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>

#include <fmt/core.h>

#include <filesystem>
#include <optional>
#include <ranges>
#include <set>
#include <string_view>

namespace cloud_io {

namespace detail {

/// The chunk could be either primary or secondary.
/// The primary chunk maintains an index. The secondary
/// chunk only caches the index.
/// If the chunk is full there is no difference between
/// these two types of chunks.
enum class fifo_chunk_status : std::uint8_t {
    primary,
    secondary,
};

struct fifo_index_entry
  : serde::
      envelope<fifo_index_entry, serde::version<0>, serde::compat_version<0>> {
    uint64_t offset;
    uint64_t payload_size;
    uint64_t slot_size;
    /// True if the space is allocated but the payload is not yet written.
    bool dirty{false};

    auto serde_fields() {
        return std::tie(offset, payload_size, slot_size, dirty);
    }
};

struct fifo_index
  : serde::envelope<fifo_index, serde::version<0>, serde::compat_version<0>> {
    /// Collection of all index entries
    absl::btree_map<ss::sstring, fifo_index_entry> entries;

    /// If the index is known to be complete this is set to true.
    /// The index being complete means that it has all entries and
    /// new entries can not be added or the index exists on a primary
    /// copy of the fifo_chunk.
    bool complete{false};

    /// Number of bytes allocated.
    uint64_t allocated{0};

    /// File size
    uint64_t total_bytes{0};

    auto serde_fields() {
        return std::tie(entries, complete, allocated, total_bytes);
    }
};

} // namespace detail

/**
 * The chunk is a unit of eviction in the fifo_cache.
 * It is a fixed size file on disk (preallocated) with slotted
 * organization. New elements are added starting from the beginning
 * of the chunk and corresponding index entries are added starting
 * from the end of the file.
 *
 * The chunk could either manage the underlying file or just use it.
 * In the first case the chunk maintains the index and grants write
 * permissions by providing write slots. In the second case the chunk
 * can only write when it's granted a write slot by the main chunk.
 *
 * Different fifo_chunk objects that correspond to the same file on
 * disk could be working on different shards. This is safe because
 * write slots are not overlapping and are always aligned. There is
 * no race with fallocate because the fifo_chunk expects that the file
 * is created and fallocated before the fifo_chunk objects are created.
 * The higher level system that manages fifo_chunk instances is
 * responsible for disk space allocation and fifo_chunk lifetimes.
 */
class fifo_chunk {
public:
    using status_t = detail::fifo_chunk_status;

    /// The chunk could either manage the underlying file
    /// or just use it.
    /// In the first case the chunk maintains the index and
    /// grants write permissions. In the second case the chunk
    /// can only write when it's granted a write slot by the
    /// chunk that owns the storage. Only one chunk should own
    /// the underlying file.
    fifo_chunk(ss::file, status_t, size_t file_size);

    /// Write slots are always aligned using disk alignment.
    struct write_slot {
        /// Offset of the write slot in the chunk.
        uint64_t offset;
        /// Size of the payload (logical size, not aligned).
        uint64_t payload_size_bytes;
        /// Actually occupied space. Always greater or equal to
        /// payload_size_bytes.
        uint64_t slot_size_bytes;
    };

    /// At the moment it's essentially the same as write slot
    /// but potentially some other stuff could be added here
    /// (locking).
    struct read_slot {
        uint64_t offset;
        uint64_t payload_size_bytes;
        uint64_t slot_size_bytes;
    };

    ss::future<> stop();

    uint64_t usage_bytes();

    /// Prepare the slot for the future write operation.
    std::optional<write_slot>
    prepare(const ss::sstring& key, size_t payload_size);

    /// Write the data using the previously allocated slot
    ss::future<> put(write_slot slot, ss::input_stream<char> payload);

    /// Mark key as clean and allow others to read it
    void mark_clean(const ss::sstring& key);

    /// Check if the key is cached.
    /// If the slot for the cache element was allocated but not comitted
    /// yet the 'cache_element_status::in_progress' is returned.
    /// The index of the secondary chunk could be obsolete. The elements
    /// of the chunk are never rewritten so it's safe to cache the index.
    cache_element_status is_cached(const ss::sstring& key) const;

    /// Find the element in the cache
    std::optional<read_slot> find(const ss::sstring& key);

    /// Get the read file stream
    ss::input_stream<char> stream_at(read_slot slot);

    /// Serializes the index (only works on a primary).
    iobuf serialize_index() const;

    /// Installs the new index (only works on a secondary).
    /// Rests existing index before applying the new one.
    void install_index(iobuf);

    void set_index_complete(bool) noexcept;

    bool is_index_complete() const noexcept;

    /// Get a range view over the keys in the index
    /// Returns keys in lexicographical order (btree_map maintains sorted order)
    auto get_keys() const {
        return _index.entries
               | std::views::transform(
                 [](const auto& pair) -> const ss::sstring& {
                     return pair.first;
                 });
    }

    /// Check if the chunk is full (all space allocated)
    bool is_full() const noexcept {
        return _index.allocated >= _index.total_bytes;
    }

private:
    void require_primary();
    void require_secondary();

    ss::file _file;
    ss::gate _gate;

    status_t _status;

    /// Chunk index. The primary chunk maintains the index using this field.
    /// The secondary chunk uses it to cache the index. The cached index
    /// is propagated by the higher layer.
    detail::fifo_index _index;
};

} // namespace cloud_io

template<>
struct fmt::formatter<cloud_io::detail::fifo_index_entry> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(
      const cloud_io::detail::fifo_index_entry& entry, FormatContext& ctx) const
      -> decltype(ctx.out()) {
        return fmt::format_to(
          ctx.out(),
          "fifo_index_entry{{offset: {}, payload_size: {}, slot_size: {}}}",
          entry.offset,
          entry.payload_size,
          entry.slot_size);
    }
};
