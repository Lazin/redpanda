/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_topics/batch_cache/raw_object_cache.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/level_zero/reader/memory_l0_cache.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "container/chunked_vector.h"
#include "mocks.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>

#include <chrono>
#include <exception>
#include <limits>
#include <queue>

enum class injected_cloud_get_failure {
    none,
    return_failure,  // returns 'failed' error code
    return_notfound, // returns 'KeyNotFound' error
    return_timeout,  // returns timeout
    throw_shutdown,  // throws 'shutdown' error
    throw_error,     // throws unexpected exception
};

/// The struct describes the injected failures for one particular placeholder
struct injected_failure {
    // cloud storage get
    injected_cloud_get_failure cloud_get{injected_cloud_get_failure::none};
};

class materialized_extent_fixture : public seastar_test {
public:
    ss::future<> TearDownAsync() override {
        co_await _batch_cache.stop();
    }

    // Generate random batches.
    // This is a source of truth for the test. The goal is to consume
    // these batches from placeholder/cache/cloud indirection.
    ss::future<> add_random_batches(int record_count);

    // Generate the 'partition' collection from the source of truth. If the
    // 'use_cache' is set to 'true' the data is pre-populated into the
    // raw_object_cache. The 'group_by' parameter controls how many batches
    // are stored per L0 object.
    // 'failures' parameter contains set of injected failures (cloud only)
    void produce_placeholders(
      bool use_cache,
      int group_by,
      std::queue<injected_failure> failures = {},
      int begin = std::numeric_limits<int>::min(),
      int end = std::numeric_limits<int>::max());

    model::offset get_expected_committed_offset();

    /// Create a list of batches that contain placeholders
    chunked_vector<model::record_batch> make_underlying();

    static cloud_topics::l0::materialized_extent
    make_materialized_extent(model::record_batch batch) {
        cloud_topics::extent_meta e{
          .base_offset = model::offset_cast(batch.base_offset()),
          .last_offset = model::offset_cast(batch.last_offset()),
        };
        iobuf payload = std::move(batch).release_data();
        iobuf_parser parser(std::move(payload));
        auto record = model::parse_one_record_from_buffer(parser);
        iobuf value = std::move(record).release_value();
        auto placeholder = serde::from_iobuf<cloud_topics::ctp_placeholder>(
          std::move(value));
        e.id = placeholder.id;
        e.first_byte_offset = placeholder.offset;
        e.byte_range_size = placeholder.size_bytes;
        return cloud_topics::l0::materialized_extent{
          .meta = e,
        };
    }

    chunked_vector<model::record_batch> partition;
    chunked_vector<model::record_batch> expected;
    remote_mock remote;
    storage::batch_cache _batch_cache{storage::batch_cache::reclaim_options{
      .growth_window = std::chrono::seconds(3),
      .stable_window = std::chrono::seconds(10),
      .min_size = 128_KiB,
      .max_size = 4_MiB,
      .min_free_memory = 0,
    }};
    cloud_topics::raw_object_cache _raw_cache{_batch_cache};
    cloud_topics::l0::memory_l0_cache _l0_cache{_raw_cache};
};
