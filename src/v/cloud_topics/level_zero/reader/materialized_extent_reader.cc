/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"

#include "cloud_io/remote.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

namespace {

ss::future<result<chunked_vector<materialized_extent>>> materialize_sorted_run(
  chunked_vector<extent_meta> query,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  l0_object_cache* cache,
  retry_chain_node* rtc,
  micro_probe* probe) {
    chunked_vector<materialized_extent> extents;
    for (const auto& extent : query) {
        extents.push_back(materialized_extent{.meta = extent});
        auto& back = extents.back();
        // The raw_object_cache handles deduplication internally —
        // if the same L0 object was already downloaded for a prior extent,
        // get_extent() will serve it from cache without re-downloading.
        auto res = co_await materialize(
          &back, bucket, api, cache, rtc, probe);
        if (!res.has_value()) {
            co_return res.error();
        }
    }
    co_return std::move(extents);
}

} // namespace

ss::future<materialize_result> materialize_placeholders(
  cloud_storage_clients::bucket_name bucket,
  chunked_vector<extent_meta> query,
  cloud_io::remote_api<ss::lowres_clock>& api,
  l0_object_cache& cache,
  retry_chain_node& rtc,
  retry_chain_logger& logger) {
    micro_probe probe;
    auto extents = co_await materialize_sorted_run(
      std::move(query), bucket, &api, &cache, &rtc, &probe);
    if (!extents.has_value()) {
        vlog(
          logger.warn,
          "Failed to materialize sorted run: {}",
          extents.error().message());
        co_return materialize_result{
          .batches = extents.error(),
          .probe = probe,
        };
    }

    chunked_vector<model::record_batch> results;
    for (auto& e : extents.value()) {
        results.push_back(make_raft_data_batch(std::move(e)));
    }
    co_return materialize_result{
      .batches = std::move(results),
      .probe = probe,
    };
}

} // namespace cloud_topics::l0
