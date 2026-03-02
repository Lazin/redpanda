/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/remote.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

class l0_object_cache;
struct micro_probe;

// Materialized placeholder extent
//
// Extent represents ctp_placeholder with the data
// that it represents stored in the raw object cache or
// main memory.
struct materialized_extent {
    extent_meta meta;
    iobuf object;
};

/// Fetch data referenced by the placeholder batch and the content of the
/// ctp_placeholder.
/// Return 'true' if the object was served from the cache.
/// Otherwise, if the object was downloaded from cloud storage, return 'false'.
ss::future<result<bool>> materialize(
  materialized_extent* extent,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  l0_object_cache* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe);

// Get ctp_placeholder and the payload of the object and generate a record
// batch
model::record_batch make_raft_data_batch(materialized_extent extent);

} // namespace cloud_topics::l0
