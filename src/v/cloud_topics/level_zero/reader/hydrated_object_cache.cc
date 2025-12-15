/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/hydrated_object_cache.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

namespace cloud_topics::l0 {

template<class Clock>
std::optional<iobuf>
basic_hydrated_object_cache<Clock>::find(const object_id& id) {
    auto it = _cache.find(id);
    if (it == _cache.end()) {
        return std::nullopt;
    }
    // Rearm the idle timer on cache hit. This means that there is
    // some in-flight reconciliation that keeps hitting the same object
    // over and over again.
    rearm_idle_timer();

    return it->second.share();
}

template<class Clock>
void basic_hydrated_object_cache<Clock>::insert(object_id id, iobuf data) {
    rearm_idle_timer();

    auto data_size = data.size_bytes();

    // If the object is larger than the limit, clear everything and insert it.
    // We don't want oversized L0 objects to block the read path. This approach
    // is less efficient because it causes cache churn but it guarantees that
    // the progress can be made.
    if (data_size > _size_limit_bytes) {
        clear();
        _cache.insert({id, std::move(data)});
        _current_size_bytes = data_size;
        return;
    }

    auto existing_it = _cache.find(id);
    if (existing_it != _cache.end()) {
        // Objects are immutable so the new version is expected to
        // be exactly the same as the old one.
        return;
    }

    // Remove arbitrary elements until we have enough space
    while (_current_size_bytes + data_size > _size_limit_bytes
           && !_cache.empty()) {
        auto it = _cache.begin();
        _current_size_bytes -= it->second.size_bytes();
        _cache.erase(it);
    }

    // Insert the new object
    _cache.insert({id, std::move(data)});
    _current_size_bytes += data_size;
}

template<class Clock>
void basic_hydrated_object_cache<Clock>::clear() {
    _cache.clear();
    _current_size_bytes = 0;
}

// Explicit template instantiations
template class basic_hydrated_object_cache<ss::lowres_clock>;
template class basic_hydrated_object_cache<ss::manual_clock>;

} // namespace cloud_topics::l0
