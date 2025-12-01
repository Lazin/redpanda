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

#include <seastar/core/lowres_clock.hh>

#include <filesystem>

namespace cloud_io {

class fifo_cache : public basic_cache_service_api<ss::lowres_clock> {
public:
    fifo_cache() = default;
    ~fifo_cache() override = default;

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
    ss::future<basic_space_reservation_guard<ss::lowres_clock>>
    reserve_space(uint64_t bytes, size_t objects) override;

    /// Release reserved space
    void reserve_space_release(
      uint64_t reserved_bytes,
      size_t reserved_objects,
      uint64_t used_bytes,
      size_t used_objects) override;
};

} // namespace cloud_io
