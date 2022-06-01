/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/cache_utils.h"

namespace cloud_storage {

access_time_tracker::access_time_tracker(size_t size)
  : _num_levels(4)
  , _size(closest_power_of_two(_num_levels * size))
  , _mask(_size - 1)
  , _table(_size) {}

void access_time_tracker::add_timestamp(
  std::string_view key, std::chrono::system_clock::time_point ts) {
    size_t hash = xxhash_64(key.data(), key.size());
    insert_hash(hash, ts);
}

std::optional<std::chrono::system_clock::time_point>
access_time_tracker::estimate_timestamp(std::string_view key) const {
    size_t hash = xxhash_64(key.data(), key.size());
    return hash_lookup(hash);
}

void access_time_tracker::insert_hash(
  size_t hash, std::chrono::system_clock::time_point ts) {
    auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(ts)
                     .time_since_epoch()
                     .count();
    for (uint32_t i = 0; i < _num_levels; i++) {
        auto ix = hash & _mask;
        _table[ix].value = seconds;
        std::array<char, sizeof(hash)> buf{};
        std::memcpy(buf.data(), &hash, sizeof(hash));
        hash = xxhash_64(buf.data(), sizeof(hash));
    }
}

std::optional<std::chrono::system_clock::time_point>
access_time_tracker::hash_lookup(size_t hash) const {
    std::optional<int64_t> result = std::nullopt;
    for (uint32_t i = 0; i < _num_levels; i++) {
        auto ix = hash & _mask;
        auto tmp = _table[ix].value.load();
        if (tmp != empty_value && result) {
            result = std::min(tmp, *result);
        } else if (tmp != empty_value) {
            result = tmp;
        }
        std::array<char, sizeof(hash)> buf{};
        std::memcpy(buf.data(), &hash, sizeof(hash));
        hash = xxhash_64(buf.data(), sizeof(hash));
    }
    if (result == empty_value) {
        return std::nullopt;
    }
    auto seconds = std::chrono::seconds(*result);
    std::chrono::system_clock::time_point ts(seconds);
    return ts;
}

} // namespace cloud_storage