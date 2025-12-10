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

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_basic) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Test basic prepare
    auto slot = chunk.prepare(100);
    BOOST_REQUIRE(slot.has_value());
    BOOST_CHECK_EQUAL(slot->offset, 0);
    BOOST_CHECK_EQUAL(slot->slot_size_bytes, 128_KiB);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_alignment) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto slot1 = chunk.prepare(100);
    BOOST_REQUIRE(slot1.has_value());
    BOOST_CHECK_EQUAL(slot1->offset, 0);
    BOOST_CHECK_EQUAL(slot1->slot_size_bytes, 128_KiB);

    auto slot2 = chunk.prepare(200);
    BOOST_REQUIRE(slot2.has_value());
    BOOST_CHECK_EQUAL(slot2->offset, 128_KiB);
    BOOST_CHECK_EQUAL(slot2->slot_size_bytes, 128_KiB);

    // Slot that takes multiple 128_KiB pages
    auto slot3 = chunk.prepare(256_KiB);
    BOOST_REQUIRE(slot3.has_value());
    BOOST_CHECK_EQUAL(slot3->offset, 256_KiB);
    BOOST_CHECK_EQUAL(slot3->slot_size_bytes, 256_KiB);

    // Slot that doesn't fit
    auto slot4 = chunk.prepare(600_KiB); // 512K remaining
    BOOST_REQUIRE(!slot4.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_prepare_exhaustion) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    // Fill the chunk by repeatedly calling prepare
    std::vector<fifo_chunk::write_slot> slots;
    while (true) {
        auto slot = chunk.prepare(128_KiB);
        if (!slot.has_value()) {
            break;
        }
        slots.push_back(*slot);
    }

    // Should have allocated 8 slots (1 MiB / 128 KiB = 8)
    BOOST_CHECK_EQUAL(slots.size(), 8);

    // Next prepare should fail
    auto slot = chunk.prepare(100);
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_is_cached) {
    // This test validates details of is_cached behavior.
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Key should not be cached initially
    BOOST_CHECK_EQUAL(
      chunk.is_cached("key1"), cache_element_status::not_available);

    // After put, the entry is added to the index (already flushed)
    auto slot = chunk.prepare(100);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring test_data(100, 'T');
    auto stream = make_stream(test_data);
    chunk.put("key1", *slot, 100, std::move(stream), 128_KiB, 4).get();

    BOOST_CHECK_EQUAL(chunk.is_cached("key1"), cache_element_status::available);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_nonexistent) {
    const size_t file_size = 1_MiB;
    auto chunk = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::primary, file_size);

    auto slot = chunk.find("key1");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_after_put) {
    // Check that after put, the entry is searchable (added after flush)
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and put adds entry to index after flush
    auto prepared_slot = chunk.prepare(100);
    BOOST_REQUIRE(prepared_slot.has_value());

    ss::sstring test_data(100, 'D');
    auto stream = make_stream(test_data);
    chunk.put("key1", *prepared_slot, 100, std::move(stream), 128_KiB, 4).get();

    // Find should return the slot after put completes
    auto found_slot = chunk.find("key1");
    BOOST_REQUIRE(found_slot.has_value());
    BOOST_CHECK_EQUAL(found_slot->offset, prepared_slot->offset);
    BOOST_CHECK_EQUAL(found_slot->payload_size_bytes, 100);
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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file1 = create_chunk_file(chunk_path, file_size);
    auto chunk1 = fifo_chunk(
      std::move(file1), fifo_chunk::status_t::primary, file_size);

    // Prepare and put some entries
    auto slot1 = chunk1.prepare(100);
    BOOST_REQUIRE(slot1.has_value());
    ss::sstring data1(100, 'X');
    chunk1.put("key1", *slot1, 100, make_stream(data1), 128_KiB, 4).get();

    auto slot2 = chunk1.prepare(200);
    BOOST_REQUIRE(slot2.has_value());
    ss::sstring data2(200, 'Y');
    chunk1.put("key2", *slot2, 200, make_stream(data2), 128_KiB, 4).get();

    chunk1.set_index_complete(true);
    auto usage = chunk1.usage_bytes();

    auto serialized = chunk1.serialize_index();
    BOOST_CHECK_GT(serialized.size_bytes(), 0);

    auto chunk2 = fifo_chunk(
      make_mock_file(), fifo_chunk::status_t::secondary, file_size);
    chunk2.install_index(std::move(serialized));

    BOOST_CHECK_EQUAL(usage, chunk2.usage_bytes());
    BOOST_CHECK(chunk2.is_index_complete());
    // Entries are available (added to index only after flush in primary)
    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key1"), cache_element_status::available);
    BOOST_CHECK_EQUAL(
      chunk2.is_cached("key2"), cache_element_status::available);
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

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_put_basic) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare a slot
    auto slot = chunk.prepare(1024);
    BOOST_REQUIRE(slot.has_value());

    // Create test data
    ss::sstring test_data(1024, 'A');
    auto stream = make_stream(test_data);

    // Put data into the slot
    chunk.put("key1", *slot, 1024, std::move(stream), 128_KiB, 4).get();

    // Key should be available (added to index after flush)
    BOOST_CHECK_EQUAL(chunk.is_cached("key1"), cache_element_status::available);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_put_available) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and write
    auto slot = chunk.prepare(1024);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring test_data(1024, 'B');
    auto stream = make_stream(test_data);
    chunk.put("key1", *slot, 1024, std::move(stream), 128_KiB, 4).get();

    // After put, it should be available
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
    auto slot = chunk.prepare(256);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring test_data = "Hello, FIFO chunk! This is test data.";
    test_data.resize(256, ' '); // Pad to 256 bytes
    auto stream = make_stream(test_data);
    chunk.put("key1", *slot, 256, std::move(stream), 128_KiB, 4).get();

    // Find and read back
    auto read_slot = chunk.find("key1");
    BOOST_REQUIRE(read_slot.has_value());

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
    auto slot1 = chunk.prepare(512);
    auto slot2 = chunk.prepare(1024);
    auto slot3 = chunk.prepare(768);

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

    auto fut1 = chunk.put("key1", *slot1, 512, make_stream(data1), 128_KiB, 4);
    auto fut2 = chunk.put("key2", *slot2, 1024, make_stream(data2), 128_KiB, 4);
    auto fut3 = chunk.put("key3", *slot3, 768, make_stream(data3), 128_KiB, 4);

    // Wait for all writes to complete
    fut1.get();
    fut2.get();
    fut3.get();

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
    auto slot = chunk.prepare(large_size);
    BOOST_REQUIRE(slot.has_value());

    // Create pattern data that we can verify
    ss::sstring pattern_data(large_size, '\0');
    for (size_t i = 0; i < large_size; ++i) {
        pattern_data[i] = static_cast<char>('A' + (i % 26));
    }

    auto stream = make_stream(pattern_data);
    chunk.put("large_key", *slot, large_size, std::move(stream), 128_KiB, 4)
      .get();

    // Read back and verify
    auto read_slot = chunk.find("large_key");
    BOOST_REQUIRE(read_slot.has_value());

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add one key
    auto slot = chunk.prepare(100);
    BOOST_REQUIRE(slot.has_value());

    ss::sstring data(100, 'A');
    chunk.put("alpha", *slot, 100, make_stream(data), 128_KiB, 4).get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add multiple keys in non-sorted order
    auto slot1 = chunk.prepare(100);
    BOOST_REQUIRE(slot1.has_value());
    chunk
      .put("zebra", *slot1, 100, make_stream(ss::sstring(100, 'Z')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    BOOST_REQUIRE(slot2.has_value());
    chunk
      .put("alpha", *slot2, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    BOOST_REQUIRE(slot3.has_value());
    chunk
      .put("delta", *slot3, 100, make_stream(ss::sstring(100, 'D')), 128_KiB, 4)
      .get();

    auto slot4 = chunk.prepare(100);
    BOOST_REQUIRE(slot4.has_value());
    chunk
      .put("beta", *slot4, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot3, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

    auto slot4 = chunk.prepare(100);
    chunk
      .put("date", *slot4, 100, make_stream(ss::sstring(100, 'D')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot3, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

    auto slot4 = chunk.prepare(100);
    chunk
      .put("date", *slot4, 100, make_stream(ss::sstring(100, 'D')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot1, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot2, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put("date", *slot3, 100, make_stream(ss::sstring(100, 'D')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot3, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

    auto keys = chunk.lower_bound("zzz");
    auto begin = keys.begin();
    auto end = keys.end();

    // Should return empty range
    BOOST_CHECK(begin == end);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_lower_bound_first_key) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot3, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    auto slot3 = chunk.prepare(100);
    chunk
      .put(
        "cherry", *slot3, 100, make_stream(ss::sstring(100, 'C')), 128_KiB, 4)
      .get();

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
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add some keys
    auto slot1 = chunk.prepare(100);
    chunk
      .put("apple", *slot1, 100, make_stream(ss::sstring(100, 'A')), 128_KiB, 4)
      .get();

    auto slot2 = chunk.prepare(100);
    chunk
      .put(
        "banana", *slot2, 100, make_stream(ss::sstring(100, 'B')), 128_KiB, 4)
      .get();

    // Find non-existent key
    auto slot = chunk.find("cherry");
    BOOST_CHECK(!slot.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_after_put_no_mark_clean) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and put - entry is added to index after flush
    auto write_slot = chunk.prepare(100);
    BOOST_REQUIRE(write_slot.has_value());

    ss::sstring test_data(100, 'A');
    chunk.put("apple", *write_slot, 100, make_stream(test_data), 128_KiB, 4)
      .get();

    // Find should return the slot (entry is already in index)
    auto slot = chunk.find("apple");
    BOOST_REQUIRE(slot.has_value());
    BOOST_CHECK_EQUAL(slot->offset, write_slot->offset);
    BOOST_CHECK_EQUAL(slot->payload_size_bytes, 100);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_clean) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Prepare and put
    auto write_slot = chunk.prepare(100);
    BOOST_REQUIRE(write_slot.has_value());

    ss::sstring test_data(100, 'A');
    auto stream = make_stream(test_data);
    chunk.put("apple", *write_slot, 100, std::move(stream), 128_KiB, 4).get();

    // Find should succeed
    auto read_slot = chunk.find("apple");
    BOOST_REQUIRE(read_slot.has_value());
    BOOST_CHECK_EQUAL(read_slot->offset, write_slot->offset);
    BOOST_CHECK_EQUAL(read_slot->slot_size_bytes, write_slot->slot_size_bytes);
}

SEASTAR_THREAD_TEST_CASE(test_fifo_chunk_find_multiple_keys) {
    temporary_dir tmpdir("fifo-chunk");
    const std::filesystem::path chunk_path = tmpdir.get_path() / "chunk.dat";
    const size_t file_size = 1_MiB;

    auto file = create_chunk_file(chunk_path, file_size);
    auto chunk = fifo_chunk(
      std::move(file), fifo_chunk::status_t::primary, file_size);

    // Add multiple keys
    auto slot1 = chunk.prepare(100);
    BOOST_REQUIRE(slot1.has_value());
    ss::sstring data1(100, 'A');
    chunk.put("apple", *slot1, 100, make_stream(data1), 128_KiB, 4).get();

    auto slot2 = chunk.prepare(200);
    BOOST_REQUIRE(slot2.has_value());
    ss::sstring data2(200, 'B');
    chunk.put("banana", *slot2, 200, make_stream(data2), 128_KiB, 4).get();

    auto slot3 = chunk.prepare(300);
    BOOST_REQUIRE(slot3.has_value());
    ss::sstring data3(300, 'C');
    chunk.put("cherry", *slot3, 300, make_stream(data3), 128_KiB, 4).get();

    // Find each key and verify slots
    auto found1 = chunk.find("apple");
    BOOST_REQUIRE(found1.has_value());

    auto found2 = chunk.find("banana");
    BOOST_REQUIRE(found2.has_value());

    auto found3 = chunk.find("cherry");
    BOOST_REQUIRE(found3.has_value());
}
