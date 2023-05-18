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
#include "cloud_storage/tests/cloud_storage_fixture.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/testing/seastar_test.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace cloud_storage;

static ss::logger test_log("async_manifest_view_log");
static const model::initial_revision_id manifest_rev(111);

static spillover_manifest make_manifest(model::offset base) {
    spillover_manifest manifest(manifest_ntp, manifest_rev);
    segment_meta meta{
      .size_bytes = 1024,
      .base_offset = base,
      .committed_offset = model::next_offset(base),
    };
    manifest.add(meta);
    return manifest;
}

// Add elements to an empty cache and verify that they are added correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_empty) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(10, ctxlog);
    cache.start().get();

    auto fut = cache.prepare(10);
    BOOST_REQUIRE(fut.available());
    const auto expected_so = model::offset(34);
    cache.put(std::move(fut.get()), make_manifest(expected_so));

    auto res = cache.get(expected_so);
    BOOST_REQUIRE(res != nullptr);
    auto actual_so = res->manifest.get_start_offset();
    BOOST_REQUIRE(actual_so.has_value());
    BOOST_REQUIRE(actual_so.has_value() && actual_so.value() == expected_so);
    BOOST_REQUIRE(cache.size() == 1);
    BOOST_REQUIRE(cache.size_bytes() == 10);
}

// Add elements to a non-empty cache and verify that the cache size increases
// and the new elements are added correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_non_empty) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(100, ctxlog);
    cache.start().get();

    auto fut0 = cache.prepare(20);
    BOOST_REQUIRE(fut0.available());
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)));

    auto fut1 = cache.prepare(20);
    BOOST_REQUIRE(fut1.available());
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)));

    auto fut2 = cache.prepare(20);
    BOOST_REQUIRE(fut2.available());
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)));

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(model::offset(0));
    check_segment(model::offset(1));
    check_segment(model::offset(2));
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_evict) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50, ctxlog);
    cache.start().get();

    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)));

    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)));

    auto fut2 = cache.prepare(20);
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)));

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    // First manifest should be missing at this point
    auto res = cache.get(model::offset{0});
    BOOST_REQUIRE(res == nullptr);
    check_segment(model::offset(1));
    check_segment(model::offset(2));
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements. Hold
// the reference to the least used element to postpone eviction. Check that
// the eviction happens after the referenced element is deleted.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_wait_evict) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50, ctxlog);
    cache.start().get();

    auto m0 = model::offset(0);
    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(m0));
    auto p0 = cache.get(m0);
    BOOST_REQUIRE(p0);

    auto m1 = model::offset(1);
    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(m1));
    auto p1 = cache.get(m1);
    BOOST_REQUIRE(p1);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20);
    // The future can't become available yet because the
    // m0 manifest is referenced through p0 shared pointer.
    ss::sleep(100ms).get();
    BOOST_REQUIRE(!fut2.available());
    // This should unstuck the 'prepare' future
    p0 = nullptr;
    cache.put(std::move(fut2.get()), make_manifest(m2));

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(m1);
    check_segment(m2);
}

// Add elements beyond the capacity of the cache and verify that the least
// recently used elements are removed to make room for new elements. Hold
// the reference to the least used element to postpone eviction. Check that
// the prepare method throws when timeout expires.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_wait_evict_timeout) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50, ctxlog);
    cache.start().get();

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    auto m0 = model::offset(0);
    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(m0));
    auto p0 = cache.get(m0);
    BOOST_REQUIRE(p0);

    auto m1 = model::offset(1);
    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(m1));
    auto p1 = cache.get(m1);
    BOOST_REQUIRE(p1);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20, 100ms);

    // The eviction candidate should be accessible through the
    // '_eviction_rollback' list. The 'size' and 'size_bytes' should also give
    // consistent results. The manifests are moved into the eviction list before
    // scheduling point.
    check_segment(m0);
    check_segment(m1);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    BOOST_REQUIRE_THROW(
      cache.put(std::move(fut2.get()), make_manifest(m2)), ss::timed_out_error);

    // After the failed attempt to put new manifest the state should stay the
    // same.
    check_segment(m0);
    check_segment(m1);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Fill the cache to its capacity and access elements to verify that the least
// recently used elements are evicted correctly.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_get) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50, ctxlog);
    cache.start().get();

    auto m0 = model::offset(0);
    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(m0));

    auto m1 = model::offset(1);
    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(m1));
    auto p0 = cache.get(m0);
    BOOST_REQUIRE(p0);
    p0 = nullptr;

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20);

    cache.put(std::move(fut2.get()), make_manifest(m2));

    // Element 1 should be evicted
    check_segment(m0);
    check_segment(m2);
    auto p1 = cache.get(m1);
    BOOST_REQUIRE(p1 == nullptr);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Fill the cache to its capacity and access elements to verify that the least
// recently used elements are evicted correctly. Use 'promote' method instead of
// 'get'.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_promote) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(50, ctxlog);
    cache.start().get();

    auto m0 = model::offset(0);
    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(m0));

    auto m1 = model::offset(1);
    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(m1));
    cache.promote(m0);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20);

    cache.put(std::move(fut2.get()), make_manifest(m2));

    // Element 1 should be evicted
    check_segment(m0);
    check_segment(m2);
    auto p1 = cache.get(m1);
    BOOST_REQUIRE(p1 == nullptr);
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_remove) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(60, ctxlog);
    cache.start().get();

    auto m0 = model::offset(0);
    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(m0));

    auto m1 = model::offset(1);
    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(m1));

    auto m2 = model::offset(2);
    auto fut2 = cache.prepare(20);
    cache.put(std::move(fut2.get()), make_manifest(m2));

    auto p1 = cache.get(m1);
    BOOST_REQUIRE(p1 != nullptr);

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    cache.remove(m1);

    p1 = cache.get(m1);
    BOOST_REQUIRE(p1 == nullptr);
    auto p2 = cache.get(m2);
    BOOST_REQUIRE(p2 != nullptr);
    auto p0 = cache.get(m0);
    BOOST_REQUIRE(p0 != nullptr);

    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);
}

// Add elements to fill cache capacity and then shrink the cache.
// Check that the element is evicted from it.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_shrink) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(60, ctxlog);
    cache.start().get();

    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)));

    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)));

    auto fut2 = cache.prepare(20);
    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)));

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so, bool null_expected) {
        auto res = cache.get(expected_so);
        if (null_expected) {
            BOOST_REQUIRE(res == nullptr);
        } else {
            BOOST_REQUIRE(res != nullptr);
            auto actual_so = res->manifest.get_start_offset();
            BOOST_REQUIRE(actual_so.has_value());
            BOOST_REQUIRE(
              actual_so.has_value() && actual_so.value() == expected_so);
        }
    };

    check_segment(model::offset(0), false);
    check_segment(model::offset(1), false);
    check_segment(model::offset(2), false);

    cache.set_capacity(20).get();

    check_segment(model::offset(0), true);
    check_segment(model::offset(1), true);
    check_segment(model::offset(2), false);
}

// Add elements to fill cache capacity and then grow the cache.
// Check that the 'prepare' operation which was waiting for eviction
// succeeded.
SEASTAR_THREAD_TEST_CASE(test_materialized_manifest_cache_grow) {
    ss::abort_source as;
    retry_chain_node rtc(as);
    retry_chain_logger ctxlog(test_log, rtc);
    materialized_manifest_cache cache(40, ctxlog);
    cache.start().get();

    auto fut0 = cache.prepare(20);
    cache.put(std::move(fut0.get()), make_manifest(model::offset(0)));
    auto p0 = cache.get(model::offset(0));

    auto fut1 = cache.prepare(20);
    cache.put(std::move(fut1.get()), make_manifest(model::offset(1)));
    auto p1 = cache.get(model::offset(1));

    // Cache is full at this point

    auto fut2 = cache.prepare(20);
    ss::sleep(100ms).get();
    BOOST_REQUIRE(!fut2.available());
    BOOST_REQUIRE(cache.size() == 2);
    BOOST_REQUIRE(cache.size_bytes() == 40);

    // Increase capacity and unblock 'fut2'
    cache.set_capacity(60).get();

    cache.put(std::move(fut2.get()), make_manifest(model::offset(2)));

    BOOST_REQUIRE(cache.size() == 3);
    BOOST_REQUIRE(cache.size_bytes() == 60);

    auto check_segment = [&](model::offset expected_so) {
        auto res = cache.get(expected_so);
        BOOST_REQUIRE(res != nullptr);
        auto actual_so = res->manifest.get_start_offset();
        BOOST_REQUIRE(actual_so.has_value());
        BOOST_REQUIRE(
          actual_so.has_value() && actual_so.value() == expected_so);
    };

    check_segment(model::offset(0));
    check_segment(model::offset(1));
    check_segment(model::offset(2));

    // Element 0 is still being evicted by last 'prepare' call which doesn't
    // know about the fact that cache grow bigger. This is a side effect which
    // shouldn't cause any problems.
    p0 = nullptr;
    p1 = nullptr;
    ss::sleep(100ms).get();
    p0 = cache.get(model::offset(0));
    BOOST_REQUIRE(p0 == nullptr);
    p1 = cache.get(model::offset(1));
    BOOST_REQUIRE(p1 != nullptr);
}

class set_config_mixin {
public:
    set_config_mixin() {
        config::shard_local_cfg().cloud_storage_manifest_cache_size.set_value(
          (size_t)40960);
    }
};

class async_manifest_view_fixture
  : public cloud_storage_fixture
  , set_config_mixin {
public:
    async_manifest_view_fixture()
      : cloud_storage_fixture()
      , stm_manifest(manifest_ntp, manifest_rev)
      , bucket("test-bucket")
      , rtc(as)
      , ctxlog(test_log, rtc)
      , probe(manifest_ntp)
      , view(api, cache, stm_manifest, bucket, probe) {
        stm_manifest.set_archive_start_offset(
          model::offset{0}, model::offset_delta{0});
        stm_manifest.set_archive_clean_offset(model::offset{0}, 0);
        view.start().get();
    }

    ~async_manifest_view_fixture() { view.stop().get(); }

    // The current content of the manifest will be spilled over to the archive
    // and new elements will be generated.
    void generate_manifest_section(int num_segments, bool hydrate = true) {
        if (stm_manifest.empty()) {
            add_random_segments(stm_manifest, num_segments);
        }
        auto so = model::next_offset(stm_manifest.get_last_offset());
        add_random_segments(stm_manifest, num_segments);
        auto tmp = stm_manifest.truncate(so);
        spillover_manifest spm(manifest_ntp, manifest_rev);
        for (const auto& meta : tmp) {
            spm.add(meta);
        }
        // update cache
        auto path = spm.get_manifest_path();
        if (hydrate) {
            auto stream = spm.serialize().get();
            cache.local()
              .put(path, stream.stream, ss::default_priority_class())
              .get();
            stream.stream.close().get();
        }
        // upload to the cloud
        std::stringstream body;
        spm.serialize(body);
        BOOST_REQUIRE(!body.fail());
        expectation exp{
          .url = path().string(),
          .body = body.str(),
        };
        _expectations.push_back(std::move(exp));
        spillover_start_offsets.push_back(so);
    }

    void listen() { set_expectations_and_listen(_expectations); }

    void collect_segments_to(std::vector<segment_meta>& meta) {
        all_segments = std::ref(meta);
    }

    // Generate random segments and add them to the manifest
    void add_random_segments(partition_manifest& manifest, int num_segments) {
        auto base = manifest.empty()
                      ? model::offset(0)
                      : model::next_offset(manifest.get_last_offset());
        auto delta = model::offset_delta(0);
        static constexpr int64_t ts_step = 1000;
        static constexpr size_t segment_size = 4097;
        for (int i = 0; i < num_segments; i++) {
            auto last = base
                        + model::offset(random_generators::get_int(1, 100));
            auto delta_end = model::offset_delta(
              random_generators::get_int(delta(), delta() + delta()));
            segment_meta meta{
              .is_compacted = false,
              .size_bytes = segment_size,
              .base_offset = base,
              .committed_offset = last,
              .base_timestamp = model::timestamp(i * ts_step),
              .max_timestamp = model::timestamp(i * ts_step + (ts_step - 1)),
              .delta_offset = delta,
              .ntp_revision = manifest_rev,
              .archiver_term = model::term_id(1),
              .segment_term = model::term_id(1),
              .delta_offset_end = delta_end,
              .sname_format = segment_name_format::v3,
            };
            base = model::next_offset(last);
            delta = delta_end;
            manifest.add(meta);
            if (all_segments.has_value()) {
                all_segments->get().push_back(manifest.last_segment().value());
            }
        }
    }

    void print_diff(
      const std::vector<segment_meta>& actual,
      const std::vector<segment_meta>& expected,
      int limit = 4) {
        int quota = limit;
        if (expected != actual) {
            auto lhs = expected.begin();
            auto rhs = actual.begin();
            while (lhs != expected.end()) {
                if (*lhs != *rhs) {
                    vlog(
                      test_log.info,
                      "{} - expected: {}, actual: {}",
                      limit - quota,
                      *lhs,
                      *rhs);
                }
                quota--;
                if (quota > 0) {
                    break;
                }
                ++lhs;
                ++rhs;
            }
        }
    }

    partition_manifest stm_manifest;
    cloud_storage_clients::bucket_name bucket;
    ss::abort_source as;
    retry_chain_node rtc;
    retry_chain_logger ctxlog;
    partition_probe probe;
    async_manifest_view view;
    std::vector<expectation> _expectations;
    std::vector<model::offset> spillover_start_offsets;
    model::offset _last_spillover_offset;
    std::optional<std::reference_wrapper<std::vector<segment_meta>>>
      all_segments;
};

FIXTURE_TEST(test_async_manifest_view_base, async_manifest_view_fixture) {
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    auto cursor = view.get_active(model::offset{0}).get();
    BOOST_REQUIRE(cursor.has_value());
}

static void
dump_manifest_debug_info(std::string_view name, const partition_manifest& m) {
    std::stringstream s;
    m.serialize(s);
    vlog(test_log.debug, "Manifest {}: \n{}", name, s.str());
}

FIXTURE_TEST(test_async_manifest_view_fetch, async_manifest_view_fixture) {
    // Generate series of spillover manifests and query them individually
    // using `view.get_cursor(offset)` calls.
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    for (auto so : spillover_start_offsets) {
        vlog(test_log.info, "Get cursor for offset {}", so);
        auto cursor = view.get_active(so).get();
        BOOST_REQUIRE(cursor.has_value());

        cursor.value()->manifest([so](const partition_manifest& m) {
            BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), so);
        });

        auto next = std::upper_bound(
          spillover_start_offsets.begin(), spillover_start_offsets.end(), so);

        if (next != spillover_start_offsets.end()) {
            cursor.value()->manifest([next](const partition_manifest& m) {
                vlog(test_log.info, "Checking spillover manifest");
                BOOST_REQUIRE_EQUAL(
                  model::next_offset(m.get_last_offset()), *next);
            });
        } else {
            cursor.value()->manifest([this](const partition_manifest& m) {
                vlog(test_log.info, "Checking STM manifest");
                BOOST_REQUIRE_EQUAL(
                  m.get_start_offset(),
                  stm_manifest.get_start_offset().value());
            });
        }
    }
}

FIXTURE_TEST(test_async_manifest_view_iter, async_manifest_view_fixture) {
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    std::vector<segment_meta> actual;
    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_active(so).get();
    if (maybe_cursor.has_failure()) {
        BOOST_REQUIRE(
          maybe_cursor.error() == error_outcome::manifest_not_found);
    }
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor->manifest([&](const partition_manifest& m) {
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);
}

FIXTURE_TEST(test_async_manifest_view_truncate, async_manifest_view_fixture) {
    // Check that segments in the truncated part are not accessible
    std::vector<segment_meta> expected;
    collect_segments_to(expected);
    generate_manifest_section(100);
    auto clean_offset = stm_manifest.get_start_offset().value();
    generate_manifest_section(100);
    generate_manifest_section(100);
    auto new_so = model::next_offset(
      stm_manifest.last_segment()->committed_offset);
    auto new_delta = stm_manifest.last_segment()->delta_offset_end;
    std::vector<segment_meta> removed;
    std::swap(expected, removed);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    generate_manifest_section(100);
    listen();

    stm_manifest.set_archive_start_offset(new_so, new_delta);

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_active(so).get();
    BOOST_REQUIRE(maybe_cursor.has_failure());
    BOOST_REQUIRE(maybe_cursor.error() == error_outcome::manifest_not_found);

    maybe_cursor = view.get_active(new_so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());

    std::vector<segment_meta> actual;
    auto cursor = std::move(maybe_cursor.value());
    do {
        cursor->manifest([&](const partition_manifest& m) {
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, expected);
    BOOST_REQUIRE_EQUAL(expected.size(), actual.size());
    BOOST_REQUIRE(expected == actual);

    auto backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());

    actual.clear();
    cursor = std::move(backlog_cursor.value());
    do {
        cursor->manifest([&](const partition_manifest& m) {
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, removed);
    BOOST_REQUIRE_EQUAL(removed.size(), actual.size());
    BOOST_REQUIRE(removed == actual);

    // Move clean offset and check that the backlog is updated
    // correctly.
    stm_manifest.set_archive_clean_offset(clean_offset, 0);
    std::erase_if(removed, [clean_offset](const segment_meta& m) {
        return m.committed_offset < clean_offset;
    });
    actual.clear();
    backlog_cursor = view.get_retention_backlog().get();
    BOOST_REQUIRE(!backlog_cursor.has_failure());
    cursor = std::move(backlog_cursor.value());
    do {
        cursor->manifest([&](const partition_manifest& m) {
            for (auto meta : m) {
                actual.push_back(meta);
            }
        });
    } while (cursor->next().get().value());
    print_diff(actual, removed);
    BOOST_REQUIRE_EQUAL(removed.size(), actual.size());
    BOOST_REQUIRE(removed == actual);
}

FIXTURE_TEST(test_async_manifest_view_evict, async_manifest_view_fixture) {
    for (int i = 0; i < 10; i++) {
        generate_manifest_section(100);
    }
    listen();

    model::offset so = model::offset{0};
    auto maybe_cursor = view.get_active(so).get();
    BOOST_REQUIRE(!maybe_cursor.has_failure());
    auto stale_cursor = std::move(maybe_cursor.value());

    // Force eviction of the stale_cursor
    vlog(test_log.debug, "Saturating cache");
    std::vector<std::unique_ptr<cloud_storage::async_manifest_view_cursor>>
      cursors;
    for (auto it = std::next(spillover_start_offsets.begin());
         it != spillover_start_offsets.end();
         it++) {
        auto o = *it;
        vlog(test_log.debug, "Fetching manifest for offset {}", o);
        auto tmp_cursor = view.get_active(o).get();
        BOOST_REQUIRE(!tmp_cursor.has_failure());
        auto cursor = std::move(tmp_cursor.value());
        cursor->manifest([o](const partition_manifest& m) {
            BOOST_REQUIRE_EQUAL(o, m.get_start_offset().value());
        });
        cursors.emplace_back(std::move(cursor));
    }
    BOOST_REQUIRE_EQUAL(cursors.size(), spillover_start_offsets.size() - 1);

    ss::sleep(2s).get();

    vlog(
      test_log.debug,
      "Cursor's actual status: {}, expected status: {}",
      stale_cursor->get_status(),
      async_manifest_view_cursor_status::evicted);
    BOOST_REQUIRE(
      stale_cursor->get_status() == async_manifest_view_cursor_status::evicted);
}

// TODO
// Eviction