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
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

#include <optional>

namespace cloud_topics::l0 {

/// Abstract interface for L0 object caching.
///
/// The read path uses this to look up and store downloaded L0 objects
/// without knowing whether the backing store is in-memory or on disk.
class l0_object_cache {
public:
    virtual ~l0_object_cache() = default;

    /// Try to read a byte range from a cached L0 object.
    /// Returns nullopt on cache miss.
    virtual ss::future<std::optional<iobuf>> get_extent(
      const object_id& id,
      first_byte_offset_t offset,
      byte_range_size_t size,
      basic_retry_chain_node<>& rtc)
      = 0;

    /// Store a downloaded L0 object in cache.
    virtual ss::future<> put(
      const object_id& id, iobuf data, basic_retry_chain_node<>& rtc)
      = 0;
};

} // namespace cloud_topics::l0
