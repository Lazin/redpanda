/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/spillover_manifest.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace cloud_storage;

static ss::logger test_log("async_manifest_view_log");
static const model::ntp manifest_ntp(
  model::ns("test-ns"), model::topic("test-topic"), model::partition_id(42));
static const model::initial_revision_id manifest_rev(111);

static spillover_manifest make_manifest(model::offset base) {
    spillover_manifest manifest(manifest_ntp, manifest_rev);
    segment_meta meta {
        .size_bytes = 1024,
        .base_offset = base,
        .committed_offset = model::next_offset(base),
    };
    manifest.add(base, meta);
}

SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_basic) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(1, ctxlog, as);

    auto fut = cache.prepare(1);
    BOOST_REQUIRE(fut.available());
    cache.put(std::move(fut.get()), make_manifest(model::offset(34)));

    auto res = cache.get(model::offset(34));
    BOOST_REQUIRE(res != nullptr);
}
