/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_io/fifo_chunk.h"
#include "serde/envelope.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/file.hh>

#include <boost/test/unit_test.hpp>

using namespace cloud_io;

// Mock file for testing - we don't need actual I/O
static ss::file make_mock_file() {
    // Create an uninitialized file - we won't use it for actual I/O
    return ss::file{};
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_basic) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Test basic prepare
    auto slot = chunk.prepare("key1", 100);
    BOOST_REQUIRE(slot.has_value());
    BOOST_CHECK_EQUAL(slot->offset, 0);
    BOOST_CHECK_EQUAL(slot->payload_size_bytes, 100);
    BOOST_CHECK_EQUAL(slot->slot_size_bytes, 128_KiB);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_alignment) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto slot1 = chunk.prepare("key1", 100);
    BOOST_REQUIRE(slot1.has_value());
    BOOST_CHECK_EQUAL(slot1->offset, 0);
    BOOST_CHECK_EQUAL(slot1->payload_size_bytes, 100);
    BOOST_CHECK_EQUAL(slot1->slot_size_bytes, 128_KiB);

    auto slot2 = chunk.prepare("key2", 200);
    BOOST_REQUIRE(slot2.has_value());
    BOOST_CHECK_EQUAL(slot2->offset, 128_KiB);
    BOOST_CHECK_EQUAL(slot2->payload_size_bytes, 200);
    BOOST_CHECK_EQUAL(slot2->slot_size_bytes, 128_KiB);

    // Slot that takes multiple 128_KiB pages
    auto slot3 = chunk.prepare("key3", 256_KiB);
    BOOST_REQUIRE(slot3.has_value());
    BOOST_CHECK_EQUAL(slot3->offset, 256_KiB);
    BOOST_CHECK_EQUAL(slot3->payload_size_bytes, 256_KiB);
    BOOST_CHECK_EQUAL(slot3->slot_size_bytes, 256_KiB);

    // Slot that doesn't fit
    auto slot4 = chunk.prepare("key3", 600_KiB); // 512K remaining
    BOOST_REQUIRE(!slot4.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_duplicate_key) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Prepare a slot
    auto slot1 = chunk.prepare("key1", 100);
    BOOST_REQUIRE(slot1.has_value());

    // Try to prepare the same key again - should return nullopt
    auto slot2 = chunk.prepare("key1", 100);
    BOOST_CHECK(!slot2.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_is_cached) {
    // This test validates details of is_cached behavior.
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Key should not be cached initially
    BOOST_CHECK_EQUAL(
      chunk.is_cached("key1"), cache_element_status::not_available);

    // After prepare, it should be in progress (dirty)
    auto slot = chunk.prepare("key1", 100);
    BOOST_REQUIRE(slot.has_value());
    BOOST_CHECK_EQUAL(
      chunk.is_cached("key1"), cache_element_status::in_progress);

    // Mark as clean
    chunk.mark_clean("key1");
    BOOST_CHECK_EQUAL(chunk.is_cached("key1"), cache_element_status::available);

    // Key doesn't exist, not an error but no observable effects
    chunk.mark_clean("key2");
    BOOST_CHECK_EQUAL(
      chunk.is_cached("key2"), cache_element_status::not_available);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_nonexistent) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto slot = chunk.find("key1");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_dirty_slot) {
    // Check that if the allocated slot is dirty it's not
    // searchable.
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Prepare creates a dirty slot
    auto prepared_slot = chunk.prepare("key1", 100);
    BOOST_REQUIRE(prepared_slot.has_value());

    // Find should return nullopt for dirty slots
    auto found_slot = chunk.find("key1");
    BOOST_CHECK(!found_slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_index_complete_flag) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    BOOST_CHECK(!chunk.is_index_complete());

    chunk.set_index_complete(true);
    BOOST_CHECK(chunk.is_index_complete());

    chunk.set_index_complete(false);
    BOOST_CHECK(!chunk.is_index_complete());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_serialize_deserialize_index) {
    // This test checks serialization and deserialization. The methods
    // are supposed to be invoked by the upper layer that manages fifo_chunk
    // instances.
    const size_t file_size = 1_MiB;
    auto chunk1 = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto slot1 = chunk1.prepare("key1", 100);
    auto slot2 = chunk1.prepare("key2", 200);
    BOOST_REQUIRE(slot1.has_value());
    BOOST_REQUIRE(slot2.has_value());

    chunk1.set_index_complete(true);
    auto usage = chunk1.usage_bytes();

    auto serialized = chunk1.serialize_index();
    BOOST_CHECK_GT(serialized.size_bytes(), 0);

    auto chunk2 = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::secondary, file_size);
    chunk2.install_index(std::move(serialized));

    BOOST_CHECK_EQUAL(usage, chunk2.usage_bytes());
    BOOST_CHECK(chunk2.is_index_complete());
    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key1"), cache_element_status::in_progress);
    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key2"), cache_element_status::in_progress);
    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key3"), cache_element_status::not_available);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_serialize_empty_index) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto serialized = chunk.serialize_index();
    BOOST_CHECK_GT(serialized.size_bytes(), 0);

    auto chunk2 = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::secondary, file_size);
    chunk2.install_index(std::move(serialized));

    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key1"), cache_element_status::not_available);
}

// Helper function to create and preallocate a chunk file
static ss::file
create_chunk_file(const std::filesystem::path& path, size_t size) {
    auto flags = ss::open_flags::rw | ss::open_flags::create
                 | ss::open_flags::truncate;
    auto file = ss::open_file_dma(path.native(), flags).get();
    file.allocate(0, size).get();
    return file;
}

// Helper to create an input stream from a string
static ss::input_stream<char> make_stream(const ss::sstring& data) {
    iobuf buf;
    buf.append(data.data(), data.size());
    return make_iobuf_input_stream(std::move(buf));
}

// Helper to read entire stream into string
static ss::sstring read_stream(ss::input_stream<char> stream) {
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

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_put_basic) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare a slot
    auto slot = chunk.prepare("key1", 1024);
    BOOST_REQUIRE(slot.has_value());
    BOOST_CHECK_EQUAL(slot->payload_size_bytes, 1024);

    // Create test data
    ss::sstring test_data(1024, 'A');
    auto stream = make_stream(test_data);

    // Put data into the slot
    chunk.put(*slot, std::move(stream), 128_KiB, 4).get();

    // Key should still be in_progress (dirty)
    BOOST_CHECK_EQUAL(
      chunk.is_cached("key1"), cache_element_status::in_progress);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_put_and_mark_clean) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and write
    auto slot = chunk.prepare("key1", 1024);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring test_data(1024, 'B');
    auto stream = make_stream(test_data);
    chunk.put(*slot, std::move(stream), 128_KiB, 4).get();

    // Mark as clean
    chunk.mark_clean("key1");

    // Now it should be available
    BOOST_CHECK_EQUAL(chunk.is_cached("key1"), cache_element_status::available);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_stream_at_after_put) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and write
    auto slot = chunk.prepare("key1", 256);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring test_data = "Hello, FIFO chunk! This is test data.";
    test_data.resize(256, ' '); // Pad to 256 bytes
    auto stream = make_stream(test_data);
    chunk.put(*slot, std::move(stream), 128_KiB, 4).get();

    // Mark clean
    chunk.mark_clean("key1");

    // Find and read back
    auto read_slot = chunk.find("key1");
    BOOST_REQUIRE(read_slot.has_value());
    BOOST_CHECK_EQUAL(read_slot->payload_size_bytes, 256);

    auto input_stream = chunk.stream_at(*read_slot, 128_KiB, 4);
    auto result = read_stream(std::move(input_stream));

    BOOST_CHECK_EQUAL(result.size(), 256);
    BOOST_CHECK_EQUAL(result, test_data);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_concurrent_puts) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 2_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare multiple slots
    auto slot1 = chunk.prepare("key1", 512);
    auto slot2 = chunk.prepare("key2", 1024);
    auto slot3 = chunk.prepare("key3", 768);

    BOOST_REQUIRE(slot1.has_value());
    BOOST_REQUIRE(slot2.has_value());
    BOOST_REQUIRE(slot3.has_value());

    // Verify slots are sequential and non-overlapping
    BOOST_CHECK_LE(slot1->offset + slot1->slot_size_bytes, slot2->offset);
    BOOST_CHECK_LE(slot2->offset + slot2->slot_size_bytes, slot3->offset);

    // Write to all slots concurrently
    ss::sstring data1(512, '1');
    ss::sstring data2(1024, '2');
    ss::sstring data3(768, '3');

    auto fut1 = chunk.put(*slot1, make_stream(data1), 128_KiB, 4);
    auto fut2 = chunk.put(*slot2, make_stream(data2), 128_KiB, 4);
    auto fut3 = chunk.put(*slot3, make_stream(data3), 128_KiB, 4);

    // Wait for all writes to complete
    fut1.get();
    fut2.get();
    fut3.get();

    // Mark all as clean
    chunk.mark_clean("key1");
    chunk.mark_clean("key2");
    chunk.mark_clean("key3");

    // Verify all keys are available
    BOOST_CHECK_EQUAL(chunk.is_cached("key1"), cache_element_status::available);
    BOOST_CHECK_EQUAL(chunk.is_cached("key2"), cache_element_status::available);
    BOOST_CHECK_EQUAL(chunk.is_cached("key3"), cache_element_status::available);

    // Read back and verify data
    auto rs1 = chunk.find("key1");
    auto rs2 = chunk.find("key2");
    auto rs3 = chunk.find("key3");

    BOOST_REQUIRE(rs1.has_value());
    BOOST_REQUIRE(rs2.has_value());
    BOOST_REQUIRE(rs3.has_value());

    auto result1 = read_stream(chunk.stream_at(*rs1, 128_KiB, 4));
    auto result2 = read_stream(chunk.stream_at(*rs2, 128_KiB, 4));
    auto result3 = read_stream(chunk.stream_at(*rs3, 128_KiB, 4));

    BOOST_CHECK_EQUAL(result1, data1);
    BOOST_CHECK_EQUAL(result2, data2);
    BOOST_CHECK_EQUAL(result3, data3);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_put_roundtrip) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 4_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Check that writes that require multiple write buffers
    // are working correctly.
    const size_t large_size = 512_KiB;
    auto slot = chunk.prepare("large_key", large_size);
    BOOST_REQUIRE(slot.has_value());

    // Create pattern data that we can verify
    ss::sstring pattern_data(large_size, '\0');
    for (size_t i = 0; i < large_size; ++i) {
        pattern_data[i] = static_cast<char>('A' + (i % 26));
    }

    auto stream = make_stream(pattern_data);
    chunk.put(*slot, std::move(stream), 128_KiB, 4).get();

    chunk.mark_clean("large_key");

    // Read back and verify
    auto read_slot = chunk.find("large_key");
    BOOST_REQUIRE(read_slot.has_value());
    BOOST_CHECK_EQUAL(read_slot->payload_size_bytes, large_size);

    auto result = read_stream(chunk.stream_at(*read_slot, 128_KiB, 4));
    BOOST_CHECK_EQUAL(result.size(), large_size);
    BOOST_CHECK_EQUAL_COLLECTIONS(
      result.begin(), result.end(), pattern_data.begin(), pattern_data.end());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_get_keys_empty) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Get keys from empty chunk
    auto keys = chunk.get_keys();
    auto begin = keys.begin();
    auto end = keys.end();

    BOOST_CHECK(begin == end);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_get_keys_single) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Add one key
    auto slot = chunk.prepare("alpha", 100);
    BOOST_REQUIRE(slot.has_value());

    // Get keys
    auto keys = chunk.get_keys();
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    BOOST_REQUIRE_EQUAL(key_vec.size(), 1);
    BOOST_CHECK_EQUAL(key_vec[0], "alpha");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_get_keys_multiple_sorted) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("zebra", 100);
    chunk.prepare("alpha", 100);
    chunk.prepare("delta", 100);
    chunk.prepare("beta", 100);

    // Get keys - should be in lexicographical order
    auto keys = chunk.get_keys();
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    BOOST_REQUIRE_EQUAL(key_vec.size(), 4);
    BOOST_CHECK_EQUAL(key_vec[0], "alpha");
    BOOST_CHECK_EQUAL(key_vec[1], "beta");
    BOOST_CHECK_EQUAL(key_vec[2], "delta");
    BOOST_CHECK_EQUAL(key_vec[3], "zebra");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_empty) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Lower bound on empty chunk
    auto keys = chunk.lower_bound("any_key");
    auto begin = keys.begin();
    auto end = keys.end();

    BOOST_CHECK(begin == end);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_exact_match) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);
    chunk.prepare("date", 100);

    // Lower bound with exact match
    auto keys = chunk.lower_bound("banana");
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    BOOST_REQUIRE_EQUAL(key_vec.size(), 3);
    BOOST_CHECK_EQUAL(key_vec[0], "banana");
    BOOST_CHECK_EQUAL(key_vec[1], "cherry");
    BOOST_CHECK_EQUAL(key_vec[2], "date");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_between_keys) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);
    chunk.prepare("date", 100);

    auto keys = chunk.lower_bound("blueberry");
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    // Should return keys >= "blueberry", which are "cherry" and "date"
    BOOST_REQUIRE_EQUAL(key_vec.size(), 2);
    BOOST_CHECK_EQUAL(key_vec[0], "cherry");
    BOOST_CHECK_EQUAL(key_vec[1], "date");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_before_all) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);
    chunk.prepare("date", 100);

    auto keys = chunk.lower_bound("aaa");
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    // Should return all keys
    BOOST_REQUIRE_EQUAL(key_vec.size(), 3);
    BOOST_CHECK_EQUAL(key_vec[0], "banana");
    BOOST_CHECK_EQUAL(key_vec[1], "cherry");
    BOOST_CHECK_EQUAL(key_vec[2], "date");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_after_all) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);

    auto keys = chunk.lower_bound("zzz");
    auto begin = keys.begin();
    auto end = keys.end();

    // Should return empty range
    BOOST_CHECK(begin == end);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_first_key) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);

    auto keys = chunk.lower_bound("apple");
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    // Should return all keys starting from "apple"
    BOOST_REQUIRE_EQUAL(key_vec.size(), 3);
    BOOST_CHECK_EQUAL(key_vec[0], "apple");
    BOOST_CHECK_EQUAL(key_vec[1], "banana");
    BOOST_CHECK_EQUAL(key_vec[2], "cherry");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_last_key) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);
    chunk.prepare("cherry", 100);

    auto keys = chunk.lower_bound("cherry");
    std::vector<ss::sstring> key_vec;
    for (const auto& key : keys) {
        key_vec.push_back(key);
    }

    // Should return only "cherry"
    BOOST_REQUIRE_EQUAL(key_vec.size(), 1);
    BOOST_CHECK_EQUAL(key_vec[0], "cherry");
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_empty) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Find in empty chunk
    auto slot = chunk.find("any_key");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_not_found) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    chunk.prepare("apple", 100);
    chunk.prepare("banana", 100);

    // Find non-existent key
    auto slot = chunk.find("cherry");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_dirty) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Prepare but don't mark clean - key is dirty
    chunk.prepare("apple", 100);

    // Find should return nullopt for dirty keys
    auto slot = chunk.find("apple");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_clean) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Prepare and mark clean
    auto write_slot = chunk.prepare("apple", 100);
    BOOST_REQUIRE(write_slot.has_value());
    chunk.mark_clean("apple");

    // Find should succeed
    auto read_slot = chunk.find("apple");
    BOOST_REQUIRE(read_slot.has_value());
    BOOST_CHECK_EQUAL(read_slot->offset, write_slot->offset);
    BOOST_CHECK_EQUAL(
      read_slot->payload_size_bytes, write_slot->payload_size_bytes);
    BOOST_CHECK_EQUAL(read_slot->slot_size_bytes, write_slot->slot_size_bytes);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_multiple_keys) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Add multiple keys
    chunk.prepare("apple", 100);
    chunk.mark_clean("apple");
    chunk.prepare("banana", 200);
    chunk.mark_clean("banana");
    chunk.prepare("cherry", 300);
    chunk.mark_clean("cherry");

    // Find each key and verify slots
    auto found1 = chunk.find("apple");
    BOOST_REQUIRE(found1.has_value());
    BOOST_CHECK_EQUAL(found1->payload_size_bytes, 100);

    auto found2 = chunk.find("banana");
    BOOST_REQUIRE(found2.has_value());
    BOOST_CHECK_EQUAL(found2->payload_size_bytes, 200);

    auto found3 = chunk.find("cherry");
    BOOST_REQUIRE(found3.has_value());
    BOOST_CHECK_EQUAL(found3->payload_size_bytes, 300);
}
