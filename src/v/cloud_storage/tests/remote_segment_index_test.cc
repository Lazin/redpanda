/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_storage/remote_segment_index.h"
#include "common_def.h"
#include "model/record_batch_types.h"
#include "random/generators.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

using namespace cloud_storage;
using namespace cloud_storage::details;

template<class TVal>
std::vector<TVal> populate_encoder(
  deltafor_encoder<TVal>& c,
  uint64_t initial_value,
  const std::vector<TVal>& deltas) {
    std::vector<TVal> result;
    auto p = initial_value;
    for (TVal delta : deltas) {
        std::array<TVal, FOR_buffer_depth> buf = {};
        for (int x = 0; x < FOR_buffer_depth; x++) {
            result.push_back(p);
            buf.at(x) = p;
            p += random_generators::get_int(delta);
            if (p < buf.at(x)) {
                throw std::out_of_range("delta can't be represented");
            }
        }
        c.add(buf);
    }
    return result;
}

BOOST_AUTO_TEST_CASE(roundtrip_test_2) {
    static constexpr int64_t initial_value = 0;
    deltafor_encoder<int64_t> enc(initial_value);
    std::vector<int64_t> deltas = {
      0LL,
      10LL,
      100LL,
      1000LL,
      10000LL,
      100000LL,
      1000000LL,
      10000000LL,
      100000000LL,
      1000000000LL,
      10000000000LL,
      100000000000LL,
      1000000000000LL,
      10000000000000LL,
      100000000000000LL,
      1000000000000000LL,
      10000000000000000LL,
      100000000000000000LL,
      1000000000000000000LL,
    };
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<int64_t> dec(
      initial_value, enc.get_row_count(), enc.copy());

    std::vector<int64_t> actual;
    std::array<int64_t, FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(roundtrip_test_1) {
    static constexpr uint64_t initial_value = 0;
    deltafor_encoder<uint64_t> enc(initial_value);
    std::vector<uint64_t> deltas = {
      0ULL,
      10ULL,
      100ULL,
      1000ULL,
      10000ULL,
      100000ULL,
      1000000ULL,
      10000000ULL,
      100000000ULL,
      1000000000ULL,
      10000000000ULL,
      100000000000ULL,
      1000000000000ULL,
      10000000000000ULL,
      100000000000000ULL,
      1000000000000000ULL,
      10000000000000000ULL,
      100000000000000000ULL,
      1000000000000000000ULL,
    };
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<uint64_t> dec(
      initial_value, enc.get_row_count(), enc.copy());

    std::vector<uint64_t> actual;
    std::array<uint64_t, FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

template<class TVal>
void test_random_walk_roundtrip(int test_size, int max_delta) {
    static constexpr TVal initial_value = 0;
    deltafor_encoder<TVal> enc(initial_value);
    std::vector<TVal> deltas;
    deltas.reserve(test_size);
    for (int i = 0; i < test_size; i++) {
        deltas.push_back(random_generators::get_int(max_delta));
    }
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<TVal> dec(initial_value, enc.get_row_count(), enc.copy());

    std::vector<TVal> actual;
    std::array<TVal, FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(random_walk_test_1) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_2) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_3) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_4) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_5) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_6) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_7) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_8) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(remote_segment_index_search_test) {
    // This value is a power of two - 1 on purpose. This way we
    // will read from the compressed part and from the buffer of
    // recent values. This is because the row widht is 16 and the
    // buffer has 16 elements. 1024 is 64 rows and 1023 is 63
    // rows + almost full buffer.
    size_t segment_num_batches = 1023;
    model::offset segment_base_rp_offset{1234};
    model::offset segment_base_kaf_offset{1210};

    std::vector<model::offset> rp_offsets;
    std::vector<model::offset> kaf_offsets;
    std::vector<size_t> file_offsets;
    int64_t rp = segment_base_rp_offset();
    int64_t kaf = segment_base_kaf_offset();
    size_t fpos = 0;
    bool is_config = false;
    for (size_t i = 0; i < segment_num_batches; i++) {
        if (!is_config) {
            rp_offsets.push_back(model::offset(rp));
            kaf_offsets.push_back(model::offset(kaf));
            file_offsets.push_back(fpos);
        }
        // The test queries every element using the key that matches the element
        // exactly and then it queries the element using the key which is
        // smaller than the element. In order to do this we need a way to
        // guarantee that the distance between to elements in the sequence is at
        // least 2, so we can decrement the key safely.
        auto batch_size = random_generators::get_int(2, 100);
        is_config = random_generators::get_int(20) == 0;
        rp += batch_size;
        kaf += is_config ? batch_size - 1 : batch_size;
        fpos += random_generators::get_int(1, 1000);
    }

    offset_index index(segment_base_rp_offset, segment_base_kaf_offset, 0U);
    model::offset last;
    model::offset klast;
    size_t flast;
    for (size_t i = 0; i < rp_offsets.size(); i++) {
        index.add(rp_offsets.at(i), kaf_offsets.at(i), file_offsets.at(i));
        last = rp_offsets.at(i);
        klast = kaf_offsets.at(i);
        flast = file_offsets.at(i);
    }

    // Query element before the first one
    auto opt_first = index.lower_bound_rp_offset(
      segment_base_rp_offset - model::offset(1));
    BOOST_REQUIRE(!opt_first.has_value());

    auto kopt_first = index.lower_bound_kaf_offset(
      segment_base_kaf_offset - model::offset(1));
    BOOST_REQUIRE(!kopt_first.has_value());

    for (unsigned ix = 0; ix < rp_offsets.size(); ix++) {
        auto opt = index.lower_bound_rp_offset(
          rp_offsets[ix] + model::offset(1));
        auto [rp, kaf, fpos] = *opt;
        BOOST_REQUIRE_EQUAL(rp, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kaf, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(fpos, file_offsets[ix]);

        auto kopt = index.lower_bound_kaf_offset(
          kaf_offsets[ix] + model::offset(1));
        BOOST_REQUIRE_EQUAL(kopt->rp_offset, rp_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->kaf_offset, kaf_offsets[ix]);
        BOOST_REQUIRE_EQUAL(kopt->file_pos, file_offsets[ix]);
    }

    // Query after the last element
    auto opt_last = index.lower_bound_rp_offset(last + model::offset(1));
    auto [rp_last, kaf_last, file_last] = *opt_last;
    BOOST_REQUIRE_EQUAL(rp_last, last);
    BOOST_REQUIRE_EQUAL(kaf_last, klast);
    BOOST_REQUIRE_EQUAL(file_last, flast);

    auto kopt_last = index.lower_bound_kaf_offset(klast + model::offset(1));
    BOOST_REQUIRE_EQUAL(kopt_last->rp_offset, last);
    BOOST_REQUIRE_EQUAL(kopt_last->kaf_offset, klast);
    BOOST_REQUIRE_EQUAL(kopt_last->file_pos, flast);
}

SEASTAR_THREAD_TEST_CASE(test_remote_segment_index_builder) {
    static const model::offset base_offset{100};
    std::vector<batch_t> batches;
    for (int i = 0; i < 1000; i++) {
        auto num_records = random_generators::get_int(1, 20);
        std::vector<size_t> record_sizes;
        for (int i = 0; i < num_records; i++) {
            record_sizes.push_back(random_generators::get_int(1, 100));
        }
        batch_t batch = {
          .num_records = num_records,
          .type = model::record_batch_type::raft_data,
          .record_sizes = std::move(record_sizes),
        };
        batches.push_back(std::move(batch));
    }
    auto segment = generate_segment(base_offset, batches);
    auto is = make_iobuf_input_stream(std::move(segment));
    offset_index ix(base_offset, base_offset, 0);
    auto parser = make_remote_segment_index_builder(
      std::move(is), ix, model::offset(0), 1);
    auto result = parser->consume().get();
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE(result.value() != 0);
    parser->close().get();

    auto offset = base_offset;
    for (const auto& batch : batches) {
        auto res = ix.lower_bound_rp_offset(offset);

        BOOST_REQUIRE_EQUAL(res->rp_offset, offset);
        BOOST_REQUIRE_EQUAL(res->kaf_offset, offset);

        offset += batch.num_records;
    }
}