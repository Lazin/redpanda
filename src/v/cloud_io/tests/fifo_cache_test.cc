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

#include <seastar/core/file.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <set>

using namespace cloud_io;

// Helper coroutine to collect all keys from scan_keys generator
static ss::future<std::vector<ss::sstring>>
collect_keys(seastar::coroutine::experimental::generator<ss::sstring> gen) {
    std::vector<ss::sstring> keys;
    while (auto key = co_await gen()) {
        keys.push_back(*key);
    }
    co_return keys;
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_start_empty_directory) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with empty directory
    fifo_cache cache(cache_dir);

    // Start should succeed with no chunks
    cache.start().get();

    size_t count = 0;
    for (auto _ : cache.get_chunk_file_paths()) {
        ++count;
    }
    BOOST_CHECK_EQUAL(count, 0);
}

static ss::future<>
create_file_chunk(std::filesystem::path p, size_t chunk_size) {
    auto file0 = co_await ss::open_file_dma(
      p.string(),
      ss::open_flags::rw | ss::open_flags::create | ss::open_flags::truncate);
    co_await file0.allocate(0, chunk_size);

    // Create an empty fifo_chunk to serialize its index
    auto temp_chunk = fifo_chunk(
      std::move(file0), fifo_chunk::status_t::primary, chunk_size);
    auto index_buf = temp_chunk.serialize_index();
    co_await temp_chunk.stop();

    // Write the index file
    auto index_path = p;
    index_path.replace_extension(".index");

    auto index_file = co_await ss::open_file_dma(
      index_path.string(),
      ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);
    auto out = co_await ss::make_file_output_stream(index_file);
    co_await write_iobuf_to_output_stream(std::move(index_buf), out);
    co_await out.flush();
    co_await out.close();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_start_with_matching_files) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    const size_t chunk_size = 1_MiB;

    // Create cache_0.chunk
    auto file0_path = cache_dir / "cache_0.chunk";
    create_file_chunk(file0_path, chunk_size).get();

    // Create cache_1.chunk
    auto file1_path = cache_dir / "cache_1.chunk";
    create_file_chunk(file1_path, chunk_size).get();

    // Create cache_42.chunk
    auto file42_path = cache_dir / "cache_42.chunk";
    create_file_chunk(file42_path, chunk_size).get();

    // Create cache with directory containing matching files
    fifo_cache cache(cache_dir);

    // Start should succeed and load the chunks
    cache.start().get();

    // Verify all matching chunks were loaded
    std::set<std::filesystem::path> expected_paths = {
      file0_path, file1_path, file42_path};
    std::set<std::filesystem::path> loaded_paths;

    for (const auto& path : cache.get_chunk_file_paths()) {
        loaded_paths.insert(path);
    }

    BOOST_CHECK_EQUAL(loaded_paths.size(), 3);
    BOOST_CHECK(loaded_paths == expected_paths);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_start_with_non_matching_files) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    const size_t file_size = 1_KiB;

    // Create files that do NOT match the pattern
    // Wrong prefix
    auto wrong_prefix_path = cache_dir / "chunk_0.chunk";
    auto wrong_prefix = ss::open_file_dma(
                          wrong_prefix_path.string(),
                          ss::open_flags::rw | ss::open_flags::create
                            | ss::open_flags::truncate)
                          .get();
    wrong_prefix.allocate(0, file_size).get();
    wrong_prefix.close().get();

    // Wrong extension
    auto wrong_ext_path = cache_dir / "cache_0.data";
    auto wrong_ext = ss::open_file_dma(
                       wrong_ext_path.string(),
                       ss::open_flags::rw | ss::open_flags::create
                         | ss::open_flags::truncate)
                       .get();
    wrong_ext.allocate(0, file_size).get();
    wrong_ext.close().get();

    // No number
    auto no_number_path = cache_dir / "cache_.chunk";
    auto no_number = ss::open_file_dma(
                       no_number_path.string(),
                       ss::open_flags::rw | ss::open_flags::create
                         | ss::open_flags::truncate)
                       .get();
    no_number.allocate(0, file_size).get();
    no_number.close().get();

    // Random file
    auto random_path = cache_dir / "random.txt";
    auto random_file = ss::open_file_dma(
                         random_path.string(),
                         ss::open_flags::rw | ss::open_flags::create
                           | ss::open_flags::truncate)
                         .get();
    random_file.allocate(0, file_size).get();
    random_file.close().get();

    // Create cache with directory containing non-matching files
    fifo_cache cache(cache_dir);

    // Start should succeed but load no chunks
    cache.start().get();

    // Verify no chunks were loaded
    size_t count = 0;
    for (const auto& path : cache.get_chunk_file_paths()) {
        (void)path;
        ++count;
    }
    BOOST_CHECK_EQUAL(count, 0);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_start_without_index_files) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    const size_t chunk_size = 1_MiB;

    // Create chunk files without index files
    auto file0_path = cache_dir / "cache_0.chunk";
    auto file0 = ss::open_file_dma(
                   file0_path.string(),
                   ss::open_flags::rw | ss::open_flags::create
                     | ss::open_flags::truncate)
                   .get();
    file0.allocate(0, chunk_size).get();
    file0.close().get();

    auto file1_path = cache_dir / "cache_1.chunk";
    auto file1 = ss::open_file_dma(
                   file1_path.string(),
                   ss::open_flags::rw | ss::open_flags::create
                     | ss::open_flags::truncate)
                   .get();
    file1.allocate(0, chunk_size).get();
    file1.close().get();

    // Create cache with directory containing chunks without index files
    fifo_cache cache(cache_dir);

    // Start should succeed but skip chunks without index files
    cache.start().get();

    // Verify no chunks were loaded (both were skipped due to missing index)
    size_t count = 0;
    for (const auto& path : cache.get_chunk_file_paths()) {
        (void)path;
        ++count;
    }
    BOOST_CHECK_EQUAL(count, 0);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_start_with_mixed_files) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    const size_t chunk_size = 1_MiB;
    const size_t file_size = 1_KiB;

    // Create matching files
    auto match1_path = cache_dir / "cache_0.chunk";
    create_file_chunk(match1_path, chunk_size).get();

    auto match2_path = cache_dir / "cache_100.chunk";
    create_file_chunk(match2_path, chunk_size).get();

    // Create non-matching files
    auto non_match1_path = cache_dir / "cache_backup.chunk";
    auto non_match1 = ss::open_file_dma(
                        non_match1_path.string(),
                        ss::open_flags::rw | ss::open_flags::create
                          | ss::open_flags::truncate)
                        .get();
    non_match1.allocate(0, file_size).get();
    non_match1.close().get();

    auto non_match2_path = cache_dir / "metadata.json";
    auto non_match2 = ss::open_file_dma(
                        non_match2_path.string(),
                        ss::open_flags::rw | ss::open_flags::create
                          | ss::open_flags::truncate)
                        .get();
    non_match2.allocate(0, file_size).get();
    non_match2.close().get();

    // Create cache with directory containing mixed files
    fifo_cache cache(cache_dir);

    // Start should succeed and load only matching chunks
    cache.start().get();

    // Verify only matching chunks were loaded
    std::set<std::filesystem::path> expected_paths = {match1_path, match2_path};
    std::set<std::filesystem::path> loaded_paths;

    for (const auto& path : cache.get_chunk_file_paths()) {
        loaded_paths.insert(path);
    }

    BOOST_CHECK_EQUAL(loaded_paths.size(), 2);
    BOOST_CHECK(loaded_paths == expected_paths);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_scan_keys) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with empty directory
    fifo_cache cache(cache_dir);
    cache.start().get();

    // Helper to create input stream from string
    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Add several keys in non-alphabetical order
    std::vector<std::string> keys_to_add = {
      "zebra", "apple", "mango", "banana", "cherry", "date"};

    for (const auto& key : keys_to_add) {
        std::string data_str = "test_data_for_" + key;
        auto reservation = cache.reserve_space(data_str.size(), 1).get();
        auto stream = make_stream(data_str);
        cache.put(key, stream, reservation).get();
    }

    // Scan all keys and collect them
    auto scanned_keys = collect_keys(cache.scan_keys()).get();

    // Keys should be returned in lexicographical order
    std::vector<std::string> expected_keys = {
      "apple", "banana", "cherry", "date", "mango", "zebra"};

    BOOST_REQUIRE_EQUAL(scanned_keys.size(), expected_keys.size());
    for (size_t i = 0; i < expected_keys.size(); ++i) {
        BOOST_CHECK_EQUAL(scanned_keys[i], expected_keys[i]);
    }
}

// Helper to read entire stream into string
static ss::sstring read_cache_stream(ss::input_stream<char> stream) {
    ss::sstring result;
    while (!stream.eof()) {
        auto buf = stream.read().get();
        if (buf.size() > 0) {
            result += ss::sstring(buf.get(), buf.size());
        }
    }
    stream.close().get();
    return result;
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_get_stream_empty) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    // Try to get a stream from empty cache
    auto result = cache.get_stream("nonexistent").get();
    BOOST_CHECK(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_get_stream_not_found) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Add one key
    std::string test_data = "test_data";
    auto reservation = cache.reserve_space(test_data.size(), 1).get();
    auto stream = make_stream(test_data);
    cache.put("existing_key", stream, reservation).get();

    // Try to get a non-existent key
    auto result = cache.get_stream("nonexistent_key").get();
    BOOST_CHECK(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_get_stream_found) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Add a key with specific data
    std::string test_data = "Hello, FIFO Cache!";
    auto reservation = cache.reserve_space(test_data.size(), 1).get();
    auto write_stream = make_stream(test_data);
    cache.put("test_key", write_stream, reservation).get();

    // Get the stream back
    auto result = cache.get_stream("test_key").get();
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK_EQUAL(result->size, test_data.size());

    // Read and verify the data
    auto read_data = read_cache_stream(std::move(result->body));
    BOOST_CHECK_EQUAL(read_data, test_data);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_get_stream_multiple_keys) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Add multiple keys with different data
    std::map<std::string, std::string> test_data = {
      {"key1", "data_for_key1"},
      {"key2", "data_for_key2"},
      {"key3", "data_for_key3"},
    };

    for (const auto& [key, data] : test_data) {
        auto reservation = cache.reserve_space(data.size(), 1).get();
        auto stream = make_stream(data);
        cache.put(key, stream, reservation).get();
    }

    // Verify each key can be retrieved with correct data
    for (const auto& [key, expected_data] : test_data) {
        auto result = cache.get_stream(key).get();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(result->size, expected_data.size());

        auto read_data = read_cache_stream(std::move(result->body));
        BOOST_CHECK_EQUAL(read_data, expected_data);
    }
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_get_stream_large_data) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Create a large data payload (1 MB)
    const size_t data_size = 1_MiB;
    std::string large_data(data_size, 'X');
    // Add a pattern to verify correctness
    for (size_t i = 0; i < data_size; i += 256) {
        large_data[i] = 'A' + (i / 256) % 26;
    }

    auto reservation = cache.reserve_space(data_size, 1).get();
    auto stream = make_stream(large_data);
    cache.put("large_key", stream, reservation).get();

    // Get the stream back
    auto result = cache.get_stream("large_key").get();
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK_EQUAL(result->size, data_size);

    // Read and verify the data
    auto read_data = read_cache_stream(std::move(result->body));
    BOOST_REQUIRE_EQUAL(read_data.size(), large_data.size());
    BOOST_CHECK_EQUAL(read_data, large_data);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_multiple_chunks) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size to force multiple chunks
    const size_t small_chunk_size = 512_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    auto make_stream = [](const std::string& data) {
        iobuf buf;
        buf.append(data.data(), data.size());
        return make_iobuf_input_stream(std::move(buf));
    };

    // Write data that will span multiple chunks
    // Each entry is ~100KB, so we'll need multiple entries to fill chunks
    const size_t entry_size = 100_KiB;
    const int num_entries = 20; // Should create ~4 chunks
    std::map<std::string, std::string> test_data;

    for (int i = 0; i < num_entries; ++i) {
        std::string key = fmt::format("key_{:03d}", i);
        std::string data(entry_size, 'A' + (i % 26));
        // Add a pattern to verify correctness
        for (size_t j = 0; j < entry_size; j += 100) {
            data[j] = '0' + (i % 10);
        }
        test_data[key] = data;

        auto reservation = cache.reserve_space(data.size(), 1).get();
        auto stream = make_stream(data);
        cache.put(key, stream, reservation).get();
    }

    // Verify multiple chunks were created
    size_t chunk_count = 0;
    for (auto _ : cache.get_chunk_file_paths()) {
        ++chunk_count;
    }
    BOOST_CHECK_GT(chunk_count, 1);
    BOOST_CHECK_LE(chunk_count, num_entries);

    // Verify all keys can be retrieved with correct data
    for (const auto& [key, expected_data] : test_data) {
        auto result = cache.get_stream(key).get();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(result->size, expected_data.size());

        auto read_data = read_cache_stream(std::move(result->body));
        BOOST_REQUIRE_EQUAL(read_data.size(), expected_data.size());
        BOOST_CHECK_EQUAL(read_data, expected_data);
    }

    // Verify scan_keys returns all keys in order
    auto scanned_keys = collect_keys(cache.scan_keys()).get();
    BOOST_REQUIRE_EQUAL(scanned_keys.size(), test_data.size());

    // Keys should be in lexicographical order
    for (size_t i = 0; i < scanned_keys.size(); ++i) {
        std::string expected_key = fmt::format("key_{:03d}", i);
        BOOST_CHECK_EQUAL(scanned_keys[i], expected_key);
    }
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_persistence) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size to force multiple chunks
    const size_t small_chunk_size = 512_KiB;
    const size_t entry_size = 100_KiB;
    const int num_entries = 15; // Should create multiple chunks

    // Store keys in non-alphabetical order for testing
    std::vector<std::string> keys_to_write = {
      "zebra", "apple", "mango", "banana", "cherry",
      "date", "fig", "grape", "kiwi", "lemon",
      "orange", "peach", "quince", "raspberry", "strawberry"};

    std::map<std::string, std::string> test_data;

    // Phase 1: Write data to cache
    {
        fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
        cache.start().get();

        auto make_stream = [](const std::string& data) {
            iobuf buf;
            buf.append(data.data(), data.size());
            return make_iobuf_input_stream(std::move(buf));
        };

        // Write keys in non-alphabetical order
        for (const auto& key : keys_to_write) {
            std::string data(entry_size, key[0]); // Fill with first char of key
            // Add a pattern to verify correctness
            for (size_t j = 0; j < entry_size; j += 100) {
                data[j] = key[key.size() - 1]; // Use last char
            }
            test_data[key] = data;

            auto reservation = cache.reserve_space(data.size(), 1).get();
            auto stream = make_stream(data);
            cache.put(key, stream, reservation).get();
        }

        // Verify multiple chunks were created
        size_t chunk_count = 0;
        std::vector<std::string> chunk_paths;
        for (const auto& path : cache.get_chunk_file_paths()) {
            chunk_paths.push_back(path.string());
            ++chunk_count;
        }
        BOOST_CHECK_GT(chunk_count, 1);
        BOOST_CHECK_LE(chunk_count, num_entries);

        // Verify scan_keys returns all keys in lexicographical order
        auto scanned_keys = collect_keys(cache.scan_keys()).get();
        BOOST_REQUIRE_EQUAL(scanned_keys.size(), test_data.size());

        // Keys should be in lexicographical order (not insertion order)
        std::vector<std::string> expected_sorted_keys;
        for (const auto& [key, _] : test_data) {
            expected_sorted_keys.push_back(key);
        }
        std::sort(expected_sorted_keys.begin(), expected_sorted_keys.end());

        for (size_t i = 0; i < scanned_keys.size(); ++i) {
            BOOST_CHECK_EQUAL(scanned_keys[i], expected_sorted_keys[i]);
        }

        // Stop the cache to close all files
        cache.stop().get();
    }

    // Phase 2: Reload cache and verify data persisted
    {
        fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
        cache.start().get();

        // Verify chunks were loaded
        size_t chunk_count = 0;
        for (const auto& path : cache.get_chunk_file_paths()) {
            (void)path;
            ++chunk_count;
        }
        BOOST_CHECK_GT(chunk_count, 1);

        // Verify all keys can be retrieved with correct data
        for (const auto& [key, expected_data] : test_data) {
            auto result = cache.get_stream(key).get();
            BOOST_REQUIRE(result.has_value());
            BOOST_CHECK_EQUAL(result->size, expected_data.size());

            auto read_data = read_cache_stream(std::move(result->body));
            BOOST_REQUIRE_EQUAL(read_data.size(), expected_data.size());
            BOOST_CHECK_EQUAL(read_data, expected_data);
        }

        // Verify scan_keys returns all keys in the same order
        auto scanned_keys = collect_keys(cache.scan_keys()).get();
        BOOST_REQUIRE_EQUAL(scanned_keys.size(), test_data.size());

        std::vector<std::string> expected_sorted_keys;
        for (const auto& [key, _] : test_data) {
            expected_sorted_keys.push_back(key);
        }
        std::sort(expected_sorted_keys.begin(), expected_sorted_keys.end());

        for (size_t i = 0; i < scanned_keys.size(); ++i) {
            BOOST_CHECK_EQUAL(scanned_keys[i], expected_sorted_keys[i]);
        }

        cache.stop().get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_reserve_space) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size
    const size_t small_chunk_size = 1_MiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    // Reserve some space
    const size_t reserve_size = 100_KiB;
    auto reservation = cache.reserve_space(reserve_size, 1).get();

    // Verify reservation was created
    BOOST_CHECK_EQUAL(reservation.reserved_bytes(), reserve_size);
    BOOST_CHECK_EQUAL(reservation.reserved_objects(), 1);

    // Write some data using the reservation
    const std::string test_data(50_KiB, 'X');
    iobuf buf;
    buf.append(test_data.data(), test_data.size());
    auto stream = make_iobuf_input_stream(std::move(buf));

    cache.put("test_key", stream, reservation).get();

    // The reservation guard will automatically release when it goes out of scope
    // and update the cache statistics

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_reserve_space_multiple) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size
    const size_t small_chunk_size = 512_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    // Make multiple reservations and writes
    const size_t entry_size = 50_KiB;
    const int num_entries = 5;

    for (int i = 0; i < num_entries; ++i) {
        auto reservation = cache.reserve_space(entry_size, 1).get();
        BOOST_CHECK_EQUAL(reservation.reserved_bytes(), entry_size);

        std::string key = fmt::format("key_{}", i);
        std::string data(entry_size, 'A' + i);
        iobuf buf;
        buf.append(data.data(), data.size());
        auto stream = make_iobuf_input_stream(std::move(buf));

        cache.put(key, stream, reservation).get();
    }

    // Verify all keys were written
    for (int i = 0; i < num_entries; ++i) {
        std::string key = fmt::format("key_{}", i);
        auto result = cache.get_stream(key).get();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(result->size, entry_size);
    }

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_reserve_space_exact_fit) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size
    const size_t chunk_size = 256_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = chunk_size});
    cache.start().get();

    // Fill the cache with multiple entries that fit exactly
    const size_t entry_size = 64_KiB;
    const int num_entries = 4; // 4 * 64KiB = 256KiB

    for (int i = 0; i < num_entries; ++i) {
        auto reservation = cache.reserve_space(entry_size, 1).get();

        std::string key = fmt::format("key_{}", i);
        std::string data(entry_size, 'A' + i);
        iobuf buf;
        buf.append(data.data(), data.size());
        auto stream = make_iobuf_input_stream(std::move(buf));

        cache.put(key, stream, reservation).get();
    }

    // Verify all keys are present
    for (int i = 0; i < num_entries; ++i) {
        std::string key = fmt::format("key_{}", i);
        auto result = cache.get_stream(key).get();
        BOOST_REQUIRE(result.has_value());
    }

    cache.stop().get();
}
SEASTAR_THREAD_TEST_CASE(test_fifo_cache_is_cached_not_found) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    // Check for non-existent key in empty cache
    auto status = cache.is_cached("nonexistent").get();
    BOOST_CHECK_EQUAL(status, cache_element_status::not_available);

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_is_cached_available) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    fifo_cache cache(cache_dir);
    cache.start().get();

    // Add a key
    std::string test_data(100, 'X');
    auto reservation = cache.reserve_space(test_data.size(), 1).get();
    iobuf buf;
    buf.append(test_data.data(), test_data.size());
    auto stream = make_iobuf_input_stream(std::move(buf));
    cache.put("test_key", stream, reservation).get();

    // Check if key is cached
    auto status = cache.is_cached("test_key").get();
    BOOST_CHECK_EQUAL(status, cache_element_status::available);

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_is_cached_multiple_chunks) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size to force multiple chunks
    const size_t small_chunk_size = 256_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    // Add entries across multiple chunks
    const size_t entry_size = 100_KiB;
    const int num_entries = 5;

    for (int i = 0; i < num_entries; ++i) {
        auto reservation = cache.reserve_space(entry_size, 1).get();
        std::string key = fmt::format("key_{}", i);
        std::string data(entry_size, 'A' + i);
        iobuf buf;
        buf.append(data.data(), data.size());
        auto stream = make_iobuf_input_stream(std::move(buf));
        cache.put(key, stream, reservation).get();
    }

    // Verify each key is cached
    for (int i = 0; i < num_entries; ++i) {
        std::string key = fmt::format("key_{}", i);
        auto status = cache.is_cached(key).get();
        BOOST_CHECK_EQUAL(status, cache_element_status::available);
    }

    // Check for non-existent key
    auto status = cache.is_cached("nonexistent").get();
    BOOST_CHECK_EQUAL(status, cache_element_status::not_available);

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_multi_chunk_write_read) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size to force multiple chunks
    const size_t small_chunk_size = 512_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    // Write data that will span multiple chunks
    const size_t entry_size = 200_KiB;
    const int num_entries = 6; // Should create ~3 chunks
    std::map<std::string, std::string> test_data;

    for (int i = 0; i < num_entries; ++i) {
        std::string key = fmt::format("multi_key_{:02d}", i);
        std::string data(entry_size, 'A' + (i % 26));
        // Add a unique pattern for verification
        for (size_t j = 0; j < entry_size; j += 1000) {
            data[j] = '0' + (i % 10);
        }
        test_data[key] = data;

        auto reservation = cache.reserve_space(data.size(), 1).get();
        iobuf buf;
        buf.append(data.data(), data.size());
        auto stream = make_iobuf_input_stream(std::move(buf));
        cache.put(key, stream, reservation).get();
    }

    // Verify multiple chunks were created
    size_t chunk_count = 0;
    for (auto _ : cache.get_chunk_file_paths()) {
        ++chunk_count;
    }
    BOOST_CHECK_GT(chunk_count, 1);
    BOOST_CHECK_LE(chunk_count, num_entries);

    // Read back all entries and verify data integrity
    for (const auto& [key, expected_data] : test_data) {
        // Check is_cached
        auto status = cache.is_cached(key).get();
        BOOST_CHECK_EQUAL(status, cache_element_status::available);

        // Get stream and verify data
        auto result = cache.get_stream(key).get();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(result->size, expected_data.size());

        ss::sstring read_data;
        while (!result->body.eof()) {
            auto buf = result->body.read().get();
            if (buf.size() > 0) {
                read_data += ss::sstring(buf.get(), buf.size());
            }
        }
        result->body.close().get();

        BOOST_REQUIRE_EQUAL(read_data.size(), expected_data.size());
        BOOST_CHECK_EQUAL(read_data, expected_data);
    }

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_multi_chunk_scan_keys) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    // Create cache with small chunk size
    const size_t small_chunk_size = 256_KiB;
    fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
    cache.start().get();

    // Add keys in non-alphabetical order across multiple chunks
    std::vector<std::string> keys_to_write = {
      "zebra", "apple", "mango", "banana", "cherry",
      "date", "fig", "grape", "kiwi", "lemon"};
    const size_t entry_size = 80_KiB;

    for (const auto& key : keys_to_write) {
        auto reservation = cache.reserve_space(entry_size, 1).get();
        std::string data(entry_size, key[0]);
        iobuf buf;
        buf.append(data.data(), data.size());
        auto stream = make_iobuf_input_stream(std::move(buf));
        cache.put(key, stream, reservation).get();
    }

    // Verify multiple chunks were created
    size_t chunk_count = 0;
    for (auto _ : cache.get_chunk_file_paths()) {
        ++chunk_count;
    }
    BOOST_CHECK_GT(chunk_count, 1);

    // Scan keys and verify they're in lexicographical order
    auto scanned_keys = collect_keys(cache.scan_keys()).get();
    BOOST_REQUIRE_EQUAL(scanned_keys.size(), keys_to_write.size());

    // Keys should be sorted
    std::vector<std::string> expected_keys = keys_to_write;
    std::sort(expected_keys.begin(), expected_keys.end());

    for (size_t i = 0; i < scanned_keys.size(); ++i) {
        BOOST_CHECK_EQUAL(scanned_keys[i], expected_keys[i]);
    }

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_fifo_cache_multi_chunk_persistence) {
    temporary_dir tmp_dir("fifo_cache_test");
    auto cache_dir = tmp_dir.get_path();

    const size_t small_chunk_size = 256_KiB;
    const size_t entry_size = 80_KiB;
    const int num_entries = 8;

    std::map<std::string, std::string> test_data;

    // Phase 1: Write data across multiple chunks
    {
        fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
        cache.start().get();

        for (int i = 0; i < num_entries; ++i) {
            std::string key = fmt::format("persist_key_{}", i);
            std::string data(entry_size, 'A' + (i % 26));
            test_data[key] = data;

            auto reservation = cache.reserve_space(data.size(), 1).get();
            iobuf buf;
            buf.append(data.data(), data.size());
            auto stream = make_iobuf_input_stream(std::move(buf));
            cache.put(key, stream, reservation).get();
        }

        // Verify multiple chunks
        size_t chunk_count = 0;
        for (auto _ : cache.get_chunk_file_paths()) {
            ++chunk_count;
        }
        BOOST_CHECK_GT(chunk_count, 1);

        cache.stop().get();
    }

    // Phase 2: Reload and verify all data
    {
        fifo_cache cache(
      cache_dir, {.cache_size = 20_GiB, .chunk_size = small_chunk_size});
        cache.start().get();

        // Verify chunks were loaded
        size_t chunk_count = 0;
        for (auto _ : cache.get_chunk_file_paths()) {
            ++chunk_count;
        }
        BOOST_CHECK_GT(chunk_count, 1);

        // Verify all keys are accessible
        for (const auto& [key, expected_data] : test_data) {
            auto status = cache.is_cached(key).get();
            BOOST_CHECK_EQUAL(status, cache_element_status::available);

            auto result = cache.get_stream(key).get();
            BOOST_REQUIRE(result.has_value());
            BOOST_CHECK_EQUAL(result->size, expected_data.size());
        }

        cache.stop().get();
    }
}
