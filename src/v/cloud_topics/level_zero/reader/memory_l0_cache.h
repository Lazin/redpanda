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

#include "cloud_topics/batch_cache/raw_object_cache.h"
#include "cloud_topics/level_zero/reader/l0_object_cache.h"

namespace cloud_topics::l0 {

/// In-memory L0 object cache adapter.
///
/// Wraps \c raw_object_cache which stores L0 objects as chunks in the
/// shared \c storage::batch_cache LRU.  All operations are synchronous
/// under the hood; futures are returned only to satisfy the interface.
class memory_l0_cache final : public l0_object_cache {
public:
    explicit memory_l0_cache(raw_object_cache& cache)
      : _cache(cache) {}

    ss::future<std::optional<iobuf>> get_extent(
      const object_id& id,
      first_byte_offset_t offset,
      byte_range_size_t size,
      basic_retry_chain_node<>&) override {
        co_return _cache.get_extent(id, offset, size);
    }

    ss::future<> put(
      const object_id& id,
      iobuf data,
      basic_retry_chain_node<>&) override {
        _cache.put(id, std::move(data));
        co_return;
    }

    raw_object_cache& underlying() { return _cache; }

private:
    raw_object_cache& _cache;
};

} // namespace cloud_topics::l0
