/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "hashing/xx.h"
#include "seastarx.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/future.hh>

#include <atomic>
#include <chrono>
#include <new>
#include <set>
#include <string_view>

namespace cloud_storage {

#ifdef __cpp_lib_hardware_interference_size
constexpr size_t cache_line_size = std::hardware_destructive_interference_size;
#else
constexpr size_t cache_line_size = 64;
#endif

/// Probabilistic data structure that can be used to track
/// access times effeciently.
///
/// This is an augmented CM-sketch that uses timestamps in place of
/// counters. When the access time is added we're updating N cells in
/// the hash table. The locations are computed using the xxHash hash
/// function.
/// To read the access time we need to locate all N cells and chose the
/// smallest value (except 0 which encodes absense of value).
///
/// In case of conflicts:
/// - The larger access time can overwrite up to N cells occupied by
///   another key.
/// - If number of overwritten cells is lower than N we can still read
///   the correct access time.
/// - If number of overwritten cells is equal to N we will read larger
///   value. In this case we will overestimate which is OK for SI cache.
///
/// We can also try to read the key which was never added and get some
/// result. This can be avoided by always writing an access time for new
/// segments when they're just added.
///
/// The data structure is lock free and supposed to be used by many shards.
/// The design avoids contention by using hashing.
class access_time_tracker {
    struct timestamp {
        alignas(cache_line_size) std::atomic<int64_t> value;
    };

    static constexpr int64_t empty_value = 0UL;

public:
    explicit access_time_tracker(size_t size);

    void add_timestamp(
      std::string_view key, std::chrono::system_clock::time_point ts);

    std::optional<std::chrono::system_clock::time_point>
    estimate_timestamp(std::string_view key) const;

private:
    void insert_hash(size_t hash, std::chrono::system_clock::time_point ts);

    std::optional<std::chrono::system_clock::time_point>
    hash_lookup(size_t hash) const;

    static constexpr size_t closest_power_of_two(size_t value) {
        value--;
        value |= value >> 1U;
        value |= value >> 2U;
        value |= value >> 4U;
        value |= value >> 8U;
        value |= value >> 16U;
        value++;
        return value;
    }

    /// Number of hashes
    const uint32_t _num_levels;
    /// Estimated population estimate (number of elements)
    const uint32_t _size;
    /// Bitmask for hash values
    const uint32_t _mask;
    /// Hash table
    fragmented_vector<timestamp> _table;
};

} // namespace cloud_storage
