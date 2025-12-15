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

#include "bytes/iobuf.h"
#include "cloud_topics/types.h"

#include "absl/container/node_hash_map.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include <cstddef>
#include <optional>

namespace cloud_topics::l0 {

/// Cache for storing hydrated L0 objects with a size limit.
/// Uses a simple random eviction policy when the size limit is exceeded.
/// Includes an idle timer that automatically clears the cache when it is
/// inactive for a configurable duration.
/// The idea is that the cache will be used to keep few active objects for
/// very short time to avoid going through disk.
template<class Clock = ss::lowres_clock>
class basic_hydrated_object_cache {
public:
    using duration_t = typename Clock::duration;

    /// Construct a cache with the given size limit in bytes and idle timeout
    /// \param size_limit_bytes Maximum size of cached objects in bytes
    /// \param idle_timeout Duration of inactivity after which cache is cleared
    explicit basic_hydrated_object_cache(
      size_t size_limit_bytes,
      duration_t idle_timeout)
      : _size_limit_bytes(size_limit_bytes)
      , _idle_timeout(idle_timeout)
      , _idle_timer([this] { on_idle_timeout(); }) {}

    /// Find an object in the cache by its ID
    /// \return The cached object if found, std::nullopt otherwise
    std::optional<iobuf> find(const object_id& id);

    /// Insert an object into the cache
    /// If the object doesn't fit and is smaller than the limit, removes
    /// arbitrary elements until it fits. If the object is larger than the
    /// limit, clears the entire cache and inserts the object anyway.
    /// \param id The unique object ID
    /// \param data The object data to cache
    void insert(object_id id, iobuf data);

    /// Get the current total size of cached objects in bytes
    size_t current_size_bytes() const { return _current_size_bytes; }

    /// Get the size limit in bytes
    size_t size_limit_bytes() const { return _size_limit_bytes; }

    /// Get the number of cached objects
    size_t size() const { return _cache.size(); }

    /// Clear all cached objects
    void clear();

private:
    /// Called when the idle timer fires - clears the cache
    void on_idle_timeout() { clear(); }

    /// Arms or rearms the idle timer
    void rearm_idle_timer() {
        if (_idle_timer.armed()) {
            _idle_timer.cancel();
        }
        _idle_timer.arm(_idle_timeout);
    }

    absl::node_hash_map<object_id, iobuf> _cache;
    size_t _size_limit_bytes;
    size_t _current_size_bytes{0};
    duration_t _idle_timeout;
    ss::timer<Clock> _idle_timer;
};

using hydrated_object_cache = basic_hydrated_object_cache<ss::lowres_clock>;

} // namespace cloud_topics::l0
