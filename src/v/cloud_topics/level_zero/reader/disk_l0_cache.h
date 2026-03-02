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

#include "cloud_topics/level_zero/reader/l0_object_cache.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_io {
template<class Clock>
class basic_cache_service_api;
} // namespace cloud_io

namespace cloud_topics::l0 {

struct micro_probe;

/// Disk-backed L0 object cache adapter.
///
/// Wraps \c cloud_io::basic_cache_service_api to store L0 objects on the
/// local filesystem. Implements the polling loop for \c is_cached() and
/// the \c reserve_space() / \c put() flow that was previously inlined in
/// \c materialized_extent.cc.
class disk_l0_cache final : public l0_object_cache {
public:
    explicit disk_l0_cache(
      cloud_io::basic_cache_service_api<ss::lowres_clock>* cache,
      micro_probe* probe);

    ss::future<std::optional<iobuf>> get_extent(
      const object_id& id,
      first_byte_offset_t offset,
      byte_range_size_t size,
      basic_retry_chain_node<>& rtc) override;

    ss::future<> put(
      const object_id& id, iobuf data, basic_retry_chain_node<>& rtc) override;

private:
    cloud_io::basic_cache_service_api<ss::lowres_clock>* _cache;
    micro_probe* _probe;
};

} // namespace cloud_topics::l0
