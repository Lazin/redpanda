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
#include "bytes/iobuf_parser.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "s3/client.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/segment_appender_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

FIXTURE_TEST(
  test_remote_segment_successful_download, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    iobuf segment_bytes = generate_segment(model::offset(1), 20);
    uint64_t clen = segment_bytes.size_bytes();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [&segment_bytes] {
        auto out = iobuf_deep_copy(segment_bytes);
        std::cerr << "reset stream called " << std::endl;
        std::cerr << "reset stream out size = " << out.size_bytes()
                  << std::endl;
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(1000ms, 200ms);
    auto upl_res
      = remote.upload_segment(bucket, name, clen, reset_stream, m, fib).get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    offset_translator ot;
    remote_segment segment(remote, bucket, m, name, fib);
    auto stream = segment.data_stream(0, ot);

    iobuf downloaded;
    auto rds = make_iobuf_ref_output_stream(downloaded);
    ss::copy(stream, rds).get();
    stream.close().get();

    if (downloaded.size_bytes() == 8) {
        auto i = iobuf_deep_copy(downloaded);
        iobuf_parser p(std::move(i));
        std::cout << "Invalid HTTP response: " << p.read_string(p.bytes_left())
                  << std::endl;
    }
    BOOST_REQUIRE_EQUAL(downloaded.size_bytes(), segment_bytes.size_bytes());
    BOOST_REQUIRE(downloaded == segment_bytes);
}

FIXTURE_TEST(test_remote_segment_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("7-8-v1.log");
    retry_chain_node fib(100ms, 20ms);

    offset_translator ot;
    remote_segment segment(remote, bucket, m, name, fib);
    auto stream = segment.data_stream(0, ot);

    iobuf downloaded;
    auto rds = make_iobuf_ref_output_stream(downloaded);
    BOOST_REQUIRE_THROW(ss::copy(stream, rds).get(), download_exception);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_single_batch,
  s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    iobuf segment_bytes = generate_segment(model::offset(1), 100);
    uint64_t clen = segment_bytes.size_bytes();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [&segment_bytes] {
        auto out = iobuf_deep_copy(segment_bytes);
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(1000ms, 200ms);
    auto upl_res
      = remote.upload_segment(bucket, name, clen, reset_stream, m, fib).get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    offset_translator ot;
    storage::log_reader_config reader_config(
      model::offset(1), model::offset(1), ss::default_priority_class());
    remote_segment segment(remote, bucket, m, name, fib);
    remote_segment_batch_reader reader(segment, reader_config, ot);

    auto s = reader.read_some(model::no_timeout).get();
    BOOST_REQUIRE(static_cast<bool>(s));

    std::vector<model::offset> offsets;
    for (const auto& batch : s.value()) {
        // should only recv one batch
        offsets.push_back(batch.base_offset());
    }
    reader.close().get();

    BOOST_REQUIRE(offsets.size() == 1);
    BOOST_REQUIRE(offsets.at(0) == model::offset(1));
}

static std::unique_ptr<storage::continuous_batch_parser>
make_recording_batch_parser(
  iobuf buf,
  std::vector<model::record_batch_header>& headers,
  std::vector<iobuf>& records,
  std::vector<uint64_t>& file_offsets) {
    auto stream = make_iobuf_input_stream(std::move(buf));
    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<recording_batch_consumer>(
        headers, records, file_offsets),
      std::move(stream));
    return parser;
}

void test_remote_segment_batch_reader(
  s3_imposter_fixture& s3_imposter,
  int num_batches,
  int ix_begin,
  int ix_end) { // NOLINT
    std::vector<model::record_batch_header> headers;
    std::vector<iobuf> records;
    std::vector<uint64_t> file_offsets;
    s3_imposter.set_expectations_and_listen(default_expectations);
    auto conf = s3_imposter.get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    iobuf segment_bytes = generate_segment(model::offset(1), num_batches);
    uint64_t clen = segment_bytes.size_bytes();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [&segment_bytes] {
        auto out = iobuf_deep_copy(segment_bytes);
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(1000ms, 200ms);
    auto upl_res
      = remote.upload_segment(bucket, name, clen, reset_stream, m, fib).get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    // account all batches
    auto parser = make_recording_batch_parser(
      iobuf_deep_copy(segment_bytes), headers, records, file_offsets);
    parser->consume().get();
    parser->close().get();

    // pick offsets for fetch request
    model::offset begin = headers.at(ix_begin).base_offset;
    model::offset end = headers.at(ix_end).last_offset();

    offset_translator ot;
    storage::log_reader_config reader_config(
      begin, end, ss::default_priority_class());

    remote_segment segment(remote, bucket, m, name, fib);
    remote_segment_batch_reader reader(segment, reader_config, ot);

    size_t batch_ix = 0;
    while (true) {
        auto s = reader.read_some(model::no_timeout).get();
        if (!s && s.error() == storage::parser_errc::end_of_stream) {
            break;
        }
        BOOST_REQUIRE(static_cast<bool>(s));

        for (const auto& batch : s.value()) {
            BOOST_REQUIRE(headers.at(ix_begin + batch_ix) == batch.header());
            BOOST_REQUIRE(records.at(ix_begin + batch_ix) == batch.data());
            batch_ix++;
        }
    }
    reader.close().get();
    BOOST_REQUIRE_EQUAL(batch_ix, (ix_end - ix_begin) + 1 /*inclusive range*/);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_0_99,
  s3_imposter_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 0, 99);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_0_20,
  s3_imposter_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 0, 20);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_10_20,
  s3_imposter_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 10, 20);
}

FIXTURE_TEST(
  test_remote_segment_batch_reader_batches_70_99,
  s3_imposter_fixture) { // NOLINT
    test_remote_segment_batch_reader(*this, 100, 70, 99);
}
