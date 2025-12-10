/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_io/fifo_cache.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

using namespace cloud_io;

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_cross_shard_consistency) {
    temporary_dir tmp_dir("fifo_cache_mt_test");
    auto cache_dir = tmp_dir.get_path();

    // Configuration
    const uint64_t chunk_size = 1_MiB;
    const uint64_t cache_size = 10_MiB;
    const fifo_cache_config config{
      .cache_size = cache_size,
      .chunk_size = chunk_size,
      .max_objects = 1000,
    };

    // Phase 1: Create initial chunk files on shard 0
    {
        fifo_cache cache(cache_dir, config);
        cache.start().get();

        // Add some test data to create multiple chunks
        auto make_stream = [](const std::string& data) {
            iobuf buf;
            buf.append(data.data(), data.size());
            return make_iobuf_input_stream(std::move(buf));
        };

        // Create data in multiple chunks (write 5 small objects to ensure
        // multiple chunks)
        for (int i = 0; i < 5; ++i) {
            std::string key = fmt::format("key_{}", i);
            size_t data_size = 250_KiB;
            std::string data(data_size, 'A' + i);

            auto reservation = cache.reserve_space(data_size, 1).get();
            auto stream = make_stream(data);
            cache.put(key, stream, reservation).get();
        }

        cache.stop().get();
    }

    // Phase 2: Start sharded service and verify consistency
    ss::sharded<fifo_cache> sharded_cache;

    // Start the sharded service
    sharded_cache.start(cache_dir, config).get();

    // Start each shard (shard 0 does enumeration, others wait)
    sharded_cache.invoke_on_all(&fifo_cache::start).get();

    // Collect chunk file paths from all shards
    struct shard_info {
        ss::shard_id shard;
        std::vector<std::string> chunk_paths;
    };

    auto results = sharded_cache
                     .map([](fifo_cache& cache) -> shard_info {
                         std::vector<std::string> paths;
                         for (const auto& path : cache.get_chunk_file_paths()) {
                             paths.push_back(path.string());
                         }
                         std::sort(paths.begin(), paths.end());
                         return shard_info{
                           .shard = ss::this_shard_id(),
                           .chunk_paths = std::move(paths),
                         };
                     })
                     .get();

    // Verify all shards have the same chunk list
    BOOST_REQUIRE_GT(results.size(), 1); // At least 2 shards

    const auto& reference_paths = results[0].chunk_paths;
    BOOST_REQUIRE_GT(reference_paths.size(), 0); // At least 1 chunk created

    for (size_t i = 1; i < results.size(); ++i) {
        const auto& shard_paths = results[i].chunk_paths;

        BOOST_REQUIRE_EQUAL(shard_paths.size(), reference_paths.size());

        for (size_t j = 0; j < reference_paths.size(); ++j) {
            BOOST_CHECK_EQUAL(shard_paths[j], reference_paths[j]);
        }
    }

    // Verify all shards can read the same keys
    for (int i = 0; i < 5; ++i) {
        std::string key = fmt::format("key_{}", i);

        auto statuses = sharded_cache
                          .map([key](fifo_cache& cache) {
                              return cache.is_cached(key).get();
                          })
                          .get();

        // All shards should report the key as available
        for (const auto& status : statuses) {
            BOOST_CHECK_EQUAL(status, cache_element_status::available);
        }
    }

    // Cleanup
    sharded_cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_empty_directory_multi_shard) {
    temporary_dir tmp_dir("fifo_cache_mt_test");
    auto cache_dir = tmp_dir.get_path();

    const fifo_cache_config config{
      .cache_size = 10_MiB,
      .chunk_size = 1_MiB,
      .max_objects = 1000,
    };

    ss::sharded<fifo_cache> sharded_cache;
    sharded_cache.start(cache_dir, config).get();
    sharded_cache.invoke_on_all(&fifo_cache::start).get();

    // All shards should have 0 chunks
    auto results = sharded_cache
                     .map([](fifo_cache& cache) {
                         size_t count = 0;
                         for (auto _ : cache.get_chunk_file_paths()) {
                             ++count;
                         }
                         return count;
                     })
                     .get();

    for (const auto& count : results) {
        BOOST_CHECK_EQUAL(count, 0);
    }

    sharded_cache.stop().get();
}
