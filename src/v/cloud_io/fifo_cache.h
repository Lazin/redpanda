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

#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/fifo_chunk.h"
#include "ssx/checkpoint_mutex.h"
#include "ssx/semaphore.h"

#include <seastar/core/lowres_clock.hh>

#include <filesystem>
#include <memory>
#include <queue>
#include <ranges>

namespace cloud_io {

// Default size for newly created fifo chunks
inline constexpr uint64_t default_fifo_chunk_size = 2_GiB;
inline constexpr uint64_t default_fifo_cache_size = 20_GiB;
inline constexpr uint64_t default_fifo_cache_max_objects = 1000000;


/// Configuration for fifo_cache
struct fifo_cache_config {
    uint64_t cache_size = default_fifo_cache_size;
    uint64_t chunk_size = default_fifo_chunk_size;
    uint64_t max_objects = default_fifo_cache_max_objects;
};

/// Default configuration for fifo_cache
class fifo_cache
  : public basic_cache_service_api<ss::lowres_clock>
  , public ss::peering_sharded_service<fifo_cache> {
public:
    explicit fifo_cache(
      std::filesystem::path cache_dir,
      fifo_cache_config config = {});
    ~fifo_cache() override = default;

    /// Start the cache - enumerate existing chunks and initialize them
    ss::future<> start();

    /// Stop the cache - close all chunks
    ss::future<> stop();

    /// Get cached value as a stream if it exists
    ss::future<std::optional<cache_item_stream>> get_stream(
      std::filesystem::path key,
      size_t read_buffer_size = default_read_buffer_size,
      unsigned int read_ahead = default_read_ahead) override;

    /// Add new value to the cache
    ss::future<> put(
      std::filesystem::path key,
      ss::input_stream<char>& data,
      basic_space_reservation_guard<ss::lowres_clock>& reservation,
      size_t write_buffer_size = default_write_buffer_size,
      unsigned int write_behind = default_write_behind) override;

    /// Check if value is cached
    ss::future<cache_element_status>
    is_cached(const std::filesystem::path& key) override;

    /// Reserve space in cache
    /// This calls prepare() and populates the reservation guard fields
    ss::future<basic_space_reservation_guard<ss::lowres_clock>>
    reserve_space(uint64_t bytes, size_t objects) override;

    /// Release reserved space
    void reserve_space_release(
      uint64_t reserved_bytes,
      size_t reserved_objects,
      uint64_t used_bytes,
      size_t used_objects,
      std::optional<uint64_t> id = std::nullopt,
      std::optional<uint64_t> offset = std::nullopt,
      std::optional<uint64_t> payload_size = std::nullopt) override;

    /// Scan all keys starting from the prefix.
    /// The keys are returned in lexicographical order using N-way merge.
    /// If prefix is nullopt, all keys are returned.
    /// If prefix is not nullopt, returns keys >= prefix.
    seastar::coroutine::experimental::generator<ss::sstring>
    scan_keys(std::optional<std::filesystem::path> prefix = std::nullopt) const;

    /// Get a range of chunk file paths.
    /// Can be used in range-based for loops.
    /// This method should only be used in tests.
    auto get_chunk_file_paths() const {
        return _chunks
               | std::views::transform(
                 [](const chunk_info& info) -> const std::filesystem::path& {
                     return info.file_path;
                 });
    }

private:
    struct chunk_info {
        uint64_t chunk_id;
        std::unique_ptr<fifo_chunk> chunk;
        std::filesystem::path file_path;
    };

    /// Calculate total disk space used by all chunks
    uint64_t calculate_disk_usage() const;

    /// Evict oldest chunks to make room for new allocation
    /// Returns true if enough space was freed
    ss::future<bool> evict_chunks(uint64_t required_space);

    /// Remove oldest chunk from cache and delete files
    ss::future<> remove_oldest_chunk();

    /// Get the current chunk for writing or roll to a new chunk if needed
    /// Returns a pointer to the chunk that should be used for writing
    ss::future<fifo_chunk*> get_or_roll_chunk();

    std::filesystem::path _cache_dir;
    uint64_t _chunk_size;
    uint64_t _cache_size;
    size_t _max_objects_per_chunk;
    chunked_vector<chunk_info> _chunks;

    /// Semaphore to track available cache space
    /// Initialized with the total cache size
    ssx::semaphore _space_sem{0, "fifo_cache/space"};

    /// Semaphore to track available object slots
    /// Initialized with max_objects
    ssx::semaphore _objects_sem{0, "fifo_cache/objects"};

    /// Mutex to protect chunk modifications (rolling, eviction)
    /// Prevents concurrent modifications to _chunks collection
    ssx::checkpoint_mutex _chunks_mutex{"fifo_cache/chunks"};

    /// Current bytes used in the cache
    uint64_t _current_cache_size{0};

    /// Current number of objects in the cache
    size_t _current_cache_objects{0};
};

} // namespace cloud_io
