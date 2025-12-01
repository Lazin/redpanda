/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/fifo_cache.h"

#include <stdexcept>

namespace cloud_io {

ss::future<std::optional<cache_item_stream>> fifo_cache::get_stream(
  [[maybe_unused]] std::filesystem::path key,
  [[maybe_unused]] size_t read_buffer_size,
  [[maybe_unused]] unsigned int read_ahead) {
    throw std::runtime_error("not implemented");
}

ss::future<> fifo_cache::put(
  [[maybe_unused]] std::filesystem::path key,
  [[maybe_unused]] ss::input_stream<char>& data,
  [[maybe_unused]] basic_space_reservation_guard<ss::lowres_clock>& reservation,
  [[maybe_unused]] size_t write_buffer_size,
  [[maybe_unused]] unsigned int write_behind) {
    throw std::runtime_error("not implemented");
}

ss::future<cache_element_status>
fifo_cache::is_cached([[maybe_unused]] const std::filesystem::path& key) {
    throw std::runtime_error("not implemented");
}

ss::future<basic_space_reservation_guard<ss::lowres_clock>>
fifo_cache::reserve_space(
  [[maybe_unused]] uint64_t bytes, [[maybe_unused]] size_t objects) {
    throw std::runtime_error("not implemented");
}

void fifo_cache::reserve_space_release(
  [[maybe_unused]] uint64_t reserved_bytes,
  [[maybe_unused]] size_t reserved_objects,
  [[maybe_unused]] uint64_t used_bytes,
  [[maybe_unused]] size_t used_objects) {
    throw std::runtime_error("not implemented");
}

} // namespace cloud_io
