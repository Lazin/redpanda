// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_operations_api.h"
#include "archival/archiver_operations_impl.h"
#include "archival/archiver_scheduler_api.h"
#include "archival/async_data_uploader.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "consensus.h"
#include "container/fragmented_vector.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/record_batch_utils.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/available_promise.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <gmock/gmock.h>

#include <exception>
#include <memory>

inline ss::logger test_log("arch_op_impl_test");

namespace seastar {
// Print seastar strings in gmock error messages
template<typename Ch, typename Size, Size max_size, bool null_terminate>
void PrintTo(
  const basic_sstring<Ch, Size, max_size, null_terminate>& s, std::ostream* o) {
    *o << s;
}
} // namespace seastar

using namespace cloud_storage;
namespace archival {

struct partition_mock : public detail::cluster_partition_api {
    MOCK_METHOD(
      const cloud_storage::partition_manifest&, manifest, (), (const));

    MOCK_METHOD(model::offset, get_uploaded_offset, (), (const));

    MOCK_METHOD(model::offset, get_applied_offset, (), (const));

    MOCK_METHOD(model::offset_delta, offset_delta, (model::offset), (const));

    MOCK_METHOD(
      std::optional<model::term_id>, get_offset_term, (model::offset), (const));

    MOCK_METHOD(model::initial_revision_id, get_initial_revision, (), (const));

    MOCK_METHOD(
      ss::future<fragmented_vector<model::tx_range>>,
      aborted_transactions,
      (model::offset, model::offset),
      (const));

    void expect_aborted_transactions(
      model::offset base,
      model::offset last,
      fragmented_vector<model::tx_range> tx) {
        auto f = ss::make_ready_future<fragmented_vector<model::tx_range>>(
          std::move(tx));
        EXPECT_CALL(*this, aborted_transactions(base, last))
          .Times(1)
          .WillOnce(::testing::Return(std::move(f)));
    }

    void expect_aborted_transactions(
      model::offset base, model::offset last, std::exception_ptr err) {
        auto f = ss::make_exception_future<fragmented_vector<model::tx_range>>(
          std::move(err));
        EXPECT_CALL(*this, aborted_transactions(base, last))
          .Times(1)
          .WillOnce(::testing::Return(std::move(f)));
    }

    void
    expect_manifest(const cloud_storage::partition_manifest& m, int times = 1) {
        EXPECT_CALL(*this, manifest)
          .Times(times)
          .WillRepeatedly(::testing::ReturnRef(m));
    }

    void expect_get_uploaded_offset(model::offset o) {
        EXPECT_CALL(*this, get_uploaded_offset)
          .Times(1)
          .WillOnce(::testing::Return(o));
    }

    void
    expect_get_offset_term(model::offset o, std::optional<model::term_id> t) {
        EXPECT_CALL(*this, get_offset_term(o))
          .Times(1)
          .WillOnce(::testing::Return(t));
    }

    void expect_get_offset_term(model::offset o, std::exception_ptr e) {
        EXPECT_CALL(*this, get_offset_term(o))
          .Times(1)
          .WillOnce(::testing::Throw(e));
    }

    void expect_offset_delta(model::offset o, model::offset_delta d) {
        EXPECT_CALL(*this, offset_delta(o))
          .Times(1)
          .WillOnce(::testing::Return(d));
    }

    void expect_offset_delta(model::offset o, std::exception_ptr e) {
        EXPECT_CALL(*this, offset_delta(o))
          .Times(1)
          .WillOnce(::testing::Throw(e));
    }

    void expect_get_applied_offset(model::offset o) {
        EXPECT_CALL(*this, get_applied_offset)
          .Times(1)
          .WillOnce(::testing::Return(o));
    }

    void expect_offset_delta(model::offset_delta d) {
        EXPECT_CALL(*this, offset_delta)
          .Times(1)
          .WillOnce(::testing::Return(d));
    }

    void expect_get_offset_term(model::term_id t) {
        EXPECT_CALL(*this, get_offset_term)
          .Times(1)
          .WillOnce(::testing::Return(t));
    }

    void
    expect_get_initial_revision(model::initial_revision_id id, int times = 1) {
        EXPECT_CALL(*this, get_initial_revision)
          .Times(times)
          .WillOnce(::testing::Return(id));
    }
};

struct partition_manager_mock : public detail::cluster_partition_manager_api {
    MOCK_METHOD(
      ss::shared_ptr<detail::cluster_partition_api>,
      get_partition,
      (const model::ktp&),
      ());

    void
    expect_get_partition(ss::shared_ptr<detail::cluster_partition_api> result) {
        EXPECT_CALL(*this, get_partition)
          .Times(1)
          .WillOnce(::testing::Return(std::move(result)));
    }
};

struct cache_mock : public detail::cloud_storage_cache_put_api {};

struct remote_mock_base {
    // This method is supposed to be mocked instead of the
    // 'upload_stream'. The 'upload_stream' accepts stream
    // object that has to be consumed asynchronously. This is
    // not a trivial thing to do in the mock. So instead the
    // 'upload_stream' mock implementation will redirect the
    // call to `_upload_stream' but will pass bytes instead
    // of the stream.
    virtual ss::future<upload_result> _upload_stream(
      ss::sstring bucket,
      ss::sstring key,
      uint64_t content_length,
      bytes payload,
      cloud_storage::upload_type type)
      = 0;
};
struct remote_mock
  : public detail::cloud_storage_remote_api
  , remote_mock_base {
    ss::future<upload_result> upload_manifest(
      const cloud_storage_clients::bucket_name& bucket,
      const cloud_storage::base_manifest& manifest,
      retry_chain_node& parent) {
        auto key = manifest.get_manifest_path();
        auto payload = co_await manifest.serialize();
        iobuf outbuf;
        auto out_str = make_iobuf_ref_output_stream(outbuf);
        co_await ss::copy(payload.stream, out_str);
        auto buf = iobuf_to_bytes(outbuf);
        co_return co_await _upload_stream(
          bucket(),
          key().native(),
          payload.size_bytes,
          std::move(buf),
          cloud_storage::upload_type::manifest);
    }

    static ss::sstring hexdump(const bytes& b, size_t sz = 512) {
        auto ib = bytes_to_iobuf(b);
        return ib.hexdump(sz);
    }

    ss::future<upload_result> upload_stream(
      const cloud_storage_clients::bucket_name& bucket,
      cloud_storage_clients::object_key key,
      uint64_t content_length,
      ss::input_stream<char> stream,
      cloud_storage::upload_type type,
      retry_chain_node& parent) {
        iobuf outbuf;
        auto out_str = make_iobuf_ref_output_stream(outbuf);

        // Consume the stream and propagate its content to the
        // _upload_stream method.
        co_await ss::copy(stream, out_str);
        auto buf = iobuf_to_bytes(outbuf);

        vlog(
          test_log.debug,
          "Mock upload_stream invoked, key: {}, size: {}, payload: {}",
          key,
          content_length,
          hexdump(buf));

        co_return co_await _upload_stream(
          bucket(), key().native(), content_length, std::move(buf), type);
    }

    MOCK_METHOD(
      ss::future<upload_result>,
      _upload_stream,
      (ss::sstring bucket,
       ss::sstring key,
       uint64_t content_length,
       bytes payload,
       cloud_storage::upload_type type),
      ());

    void expect_upload_stream(
      ss::sstring bucket,
      ss::sstring key,
      uint64_t content_length,
      bytes expected,
      cloud_storage::upload_type type,
      cloud_storage::upload_result expected_result) {
        auto ret = ss::make_ready_future<upload_result>(expected_result);
        vlog(
          test_log.debug,
          "Expect upload_stream invoked, key: {}, size: {}, payload: {}",
          key,
          content_length,
          hexdump(expected));
        EXPECT_CALL(
          *this,
          _upload_stream(
            std::move(bucket),
            std::move(key),
            content_length,
            std::move(expected),
            type))
          .Times(1)
          .WillOnce(testing::Return(std::move(ret)));
    }

    void expect_upload_stream(
      ss::sstring bucket,
      ss::sstring key,
      uint64_t content_length,
      bytes expected,
      cloud_storage::upload_type type,
      std::exception_ptr error) {
        auto ret = ss::make_exception_future<upload_result>(error);
        vlog(
          test_log.debug,
          "Expect upload_stream to fail, key: {}, size: {}, payload: {}",
          key,
          content_length,
          hexdump(expected));
        EXPECT_CALL(
          *this,
          _upload_stream(
            std::move(bucket),
            std::move(key),
            content_length,
            std::move(expected),
            type))
          .Times(1)
          .WillOnce(testing::Return(std::move(ret)));
    }

    void expect_upload_manifest(
      ss::sstring bucket,
      ss::sstring key,
      bytes payload,
      upload_result ret_val) {
        auto ret = ss::make_ready_future<upload_result>(ret_val);
        size_t content_length = payload.size();
        EXPECT_CALL(
          *this,
          _upload_stream(
            bucket,
            std::move(key),
            content_length,
            std::move(payload),
            cloud_storage::upload_type::manifest))
          .Times(1)
          .WillOnce(testing::Return(std::move(ret)));
    }

    void expect_upload_manifest(
      ss::sstring bucket,
      ss::sstring key,
      bytes payload,
      std::exception_ptr err) {
        auto ret = ss::make_exception_future<upload_result>(err);
        size_t content_length = payload.size();
        EXPECT_CALL(
          *this,
          _upload_stream(
            bucket,
            std::move(key),
            content_length,
            std::move(payload),
            cloud_storage::upload_type::manifest))
          .Times(1)
          .WillOnce(testing::Return(std::move(ret)));
    }
};

struct upload_builder_mock : public detail::segment_upload_builder_api {
    MOCK_METHOD(
      ss::future<result<std::unique_ptr<detail::prepared_segment_upload>>>,
      prepare_segment_upload,
      (ss::shared_ptr<detail::cluster_partition_api> part,
       size_limited_offset_range range,
       size_t read_buffer_size,
       ss::scheduling_group sg,
       model::timeout_clock::time_point deadline),
      ());

    void expect_prepare_segment_upload(
      size_limited_offset_range range,
      size_t read_buffer_size,
      result<std::unique_ptr<detail::prepared_segment_upload>> res) {
        auto fut = ss::make_ready_future<
          result<std::unique_ptr<detail::prepared_segment_upload>>>(
          std::move(res));
        EXPECT_CALL(
          *this,
          prepare_segment_upload(
            testing::_, range, read_buffer_size, testing::_, testing::_))
          .Times(1)
          .WillOnce(testing::Return(std::move(fut)));
    }

    void expect_prepare_segment_upload(std::exception_ptr res) {
        auto fut = ss::make_exception_future<
          result<std::unique_ptr<detail::prepared_segment_upload>>>(
          std::move(res));
        EXPECT_CALL(
          *this,
          prepare_segment_upload(
            testing::_, testing::_, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce(testing::Return(std::move(fut)));
    }
};

const model::ktp
  expected_ntp(model::topic("panda-topic"), model::partition_id(137));
const model::initial_revision_id expected_revision_id(42);
const model::term_id expected_archiver_term{91};
const model::term_id expected_segment_term{81};
const model::offset expected_applied_offset{10};
const model::offset expected_uploaded_offset{100};
const model::offset expected_read_write_fence{45};
const size_t expected_read_buffer_size = 4096;
const size_t expected_target_size{80000};
const size_t expected_min_size{30000};
const size_t expected_upload_size_quota{160000};
const size_t expected_upload_requests_quota{4};
const ss::sstring expected_bucket{"test-bucket"};
const cloud_storage_clients::bucket_name c_expected_bucket{"test-bucket"};
const auto expected_manifest = []() noexcept {
    partition_manifest m(expected_ntp.to_ntp(), expected_revision_id);

    m.add(segment_meta{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(100),
      .base_timestamp = model::timestamp(1000000),
      .max_timestamp = model::timestamp(1000100),
      .delta_offset = model::offset_delta(0),
      .archiver_term = expected_archiver_term,
      .segment_term = expected_segment_term,
      .delta_offset_end = model::offset_delta(1),
    });
    m.advance_applied_offset(expected_applied_offset);
    return m;
}();

// Condensed version of the record batch header
struct record_batch_desc_t {
    model::offset base;
    model::offset last;
    model::timestamp ts_base;
    model::timestamp ts_last;
    size_t size_bytes;
    model::record_batch_type type;
    size_t physical_offset;
};

struct payload_t {
    ss::input_stream<char> stream;
    size_t size;
    bytes content;
    std::vector<record_batch_desc_t> batches;
};

// Generate payload that contains only data batches
payload_t expected_data_payload(model::offset base, model::offset last) {
    iobuf payload;
    std::vector<record_batch_desc_t> batches;
    for (size_t i = base(); i <= last(); i++) {
        auto batch = model::test::make_random_batch(model::offset(i), 1, false);
        auto header_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
        batches.push_back(record_batch_desc_t{
          .base = batch.base_offset(),
          .last = batch.last_offset(),
          .ts_base = batch.header().first_timestamp,
          .ts_last = batch.header().max_timestamp,
          .size_bytes = header_iobuf.size_bytes() + batch.data().size_bytes(),
          .type = batch.header().type,
          .physical_offset = payload.size_bytes(),
        });
        payload.append(std::move(header_iobuf));
        payload.append(batch.data().copy());
    }
    auto size = payload.size_bytes();
    auto content = iobuf_to_bytes(payload);
    auto stream = make_iobuf_input_stream(std::move(payload));
    //
    return payload_t{
      .stream = std::move(stream),
      .size = size,
      .content = content,
      .batches = std::move(batches)};
}

struct index_payload_t {
    cloud_storage::segment_record_stats stats;
    bytes content;
};

// Generate index payload based on segment content
index_payload_t expected_index_payload(
  model::offset_delta initial_delta, std::vector<record_batch_desc_t> bts) {
    auto filter = raft::offset_translator_batch_types(expected_ntp.to_ntp());
    index_payload_t result;
    const auto sampling_step = 64_KiB;
    auto first = bts.front();
    offset_index ix(
      first.base,
      first.base - initial_delta,
      initial_delta(),
      sampling_step,
      first.ts_base);
    size_t window = 0;
    model::offset_delta running_delta = initial_delta;
    auto it = std::next(bts.begin());
    for (; it < bts.end(); it++) {
        auto is_config = [&]() {
            auto i = std::find(filter.begin(), filter.end(), it->type);
            return i != filter.end();
        }();
        auto delta = it->last - it->base + model::offset(1);
        if (is_config) {
            running_delta += delta;
        } else {
            // only add batch after the step interval (64KiB)
            if (window >= sampling_step) {
                auto ko = it->base - running_delta;
                ix.add(it->base, ko, int64_t(it->physical_offset), it->ts_last);
                window = 0;
            }
        }
        window += it->size_bytes;

        // Update stats
        if (is_config) {
            result.stats.total_conf_records += delta;
        } else {
            result.stats.total_data_records += delta;
        }
        if (result.stats.base_rp_offset == model::offset{}) {
            result.stats.base_rp_offset = it->base;
        }
        result.stats.last_rp_offset = it->last;
        if (result.stats.base_timestamp == model::timestamp{}) {
            result.stats.base_timestamp = it->ts_base;
        }
        result.stats.last_timestamp = it->ts_last;
        result.stats.size_bytes += it->size_bytes;
    }

    result.content = iobuf_to_bytes(ix.to_iobuf());
    return result;
}

struct segment_desc {
    model::offset base;
    model::offset last;
    model::offset_delta base_delta;
    model::offset_delta last_delta;
    model::timestamp base_ts;
    model::timestamp last_ts;
};

auto make_upload(
  const segment_desc& d, size_t upload_size, ss::input_stream<char> stream) {
    auto prep_upl = std::make_unique<detail::prepared_segment_upload>();
    prep_upl->is_compacted = false;
    prep_upl->meta = segment_meta{
      .is_compacted = false,
      .size_bytes = upload_size,
      .base_offset = d.base,
      .committed_offset = d.last,
      .base_timestamp = d.base_ts,
      .max_timestamp = d.last_ts,
      .delta_offset = d.base_delta,
      .ntp_revision = expected_revision_id,
      .archiver_term = expected_archiver_term,
      .segment_term = expected_segment_term,
      .delta_offset_end = d.last_delta,
      .sname_format = segment_name_format::v3,
    };
    prep_upl->size_bytes = upload_size;
    prep_upl->payload = std::move(stream);
    prep_upl->offsets = inclusive_offset_range(d.base, d.last);
    return prep_upl;
}

TEST_CORO(
  archiver_operations_impl_test,
  find_upload_candidates_success_1_segment_no_tx) {
    scoped_config cfg;
    cfg.get("storage_read_buffer_size").set_value(expected_read_buffer_size);
    cfg.get("cloud_storage_segment_size_target")
      .set_value(std::make_optional<size_t>(expected_target_size));
    cfg.get("cloud_storage_segment_size_min")
      .set_value(std::make_optional<size_t>(expected_min_size));

    // Find single upload candidate
    auto remote = ss::make_shared<remote_mock>();
    auto cache = ss::make_shared<cache_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();
    auto builder = ss::make_shared<upload_builder_mock>();
    auto partition = ss::make_shared<partition_mock>();
    partition->expect_get_applied_offset(expected_applied_offset);
    partition->expect_get_uploaded_offset(expected_uploaded_offset);
    pm->expect_get_partition(partition);

    model::offset expected_base(101);
    model::offset expected_last(200);
    segment_meta expected_meta;

    {
        auto [upload_stream, upload_size, content, batches]
          = expected_data_payload(expected_base, expected_last);

        auto upload = make_upload(
          {
            .base = expected_base,
            .last = expected_last,
            .base_delta = model::offset_delta(1),
            .last_delta = model::offset_delta(2),
            .base_ts = model::timestamp(1000000),
            .last_ts = model::timestamp(1000100),
          },
          upload_size,
          std::move(upload_stream));
        expected_meta = upload->meta;
        // these fields are set to proper values later
        expected_meta.base_timestamp = {};
        expected_meta.max_timestamp = {};

        testing::InSequence s;
        // First call find upload candidate
        builder->expect_prepare_segment_upload(
          archival::size_limited_offset_range(
            expected_base, expected_target_size, expected_min_size),
          expected_read_buffer_size,
          std::move(upload));

        // Second call finds that there is not enough data to start a new upload
        builder->expect_prepare_segment_upload(
          archival::size_limited_offset_range(
            model::next_offset(expected_last),
            expected_target_size,
            expected_min_size),
          expected_read_buffer_size,
          error_outcome::not_enough_data);
    }

    partition->expect_offset_delta(expected_base, model::offset_delta(1));
    // The offset_delta is called for the committed_offset+1
    partition->expect_offset_delta(
      model::next_offset(expected_last), model::offset_delta(2));
    partition->expect_get_offset_term(expected_base, expected_segment_term);
    partition->expect_get_initial_revision(expected_revision_id);
    partition->expect_aborted_transactions(
      expected_base, expected_last, fragmented_vector<model::tx_range>{});

    auto ops = detail::make_archiver_operations_api(
      remote, cache, pm, builder, c_expected_bucket);
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 1ms);
    auto arg = archiver_operations_api::find_upload_candidates_arg{
      .ntp = expected_ntp,
      .archiver_term = expected_archiver_term,
      .target_size = expected_target_size,
      .min_size = expected_min_size,
      .upload_size_quota = expected_upload_size_quota,
      .upload_requests_quota = expected_upload_requests_quota,
      .compacted_reupload = false,
      .inline_manifest = false,
    };
    auto res = co_await ops->find_upload_candidates(rtc, arg);
    ASSERT_TRUE_CORO(!res.has_error());
    ASSERT_EQ_CORO(res.value().ntp, expected_ntp);
    ASSERT_EQ_CORO(res.value().results.size(), 1);
    ASSERT_EQ_CORO(res.value().results.back()->metadata, expected_meta);
}

TEST_CORO(
  archiver_operations_impl_test,
  find_upload_candidates_success_2_segment_no_tx) {
    scoped_config cfg;
    cfg.get("storage_read_buffer_size").set_value(expected_read_buffer_size);
    cfg.get("cloud_storage_segment_size_target")
      .set_value(std::make_optional<size_t>(expected_target_size));
    cfg.get("cloud_storage_segment_size_min")
      .set_value(std::make_optional<size_t>(expected_min_size));

    // Find single upload candidate
    auto remote = ss::make_shared<remote_mock>();
    auto cache = ss::make_shared<cache_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();
    auto builder = ss::make_shared<upload_builder_mock>();
    auto partition = ss::make_shared<partition_mock>();
    partition->expect_get_applied_offset(expected_applied_offset);
    partition->expect_get_uploaded_offset(expected_uploaded_offset);
    pm->expect_get_partition(partition);

    std::vector<segment_desc> expected = {
      {
        .base = model::offset(101),
        .last = model::offset(200),
        .base_delta = model::offset_delta(1),
        .last_delta = model::offset_delta(2),
        .base_ts = model::timestamp(1000100),
        .last_ts = model::timestamp(1000200),
      },
      {
        .base = model::offset(201),
        .last = model::offset(300),
        .base_delta = model::offset_delta(2),
        .last_delta = model::offset_delta(3),
        .base_ts = model::timestamp(1000200),
        .last_ts = model::timestamp(1000300),
      }};

    std::vector<segment_meta> expected_meta;

    {
        testing::InSequence s;
        for (int i = 0; i < 2; i++) {
            auto desc = expected[i];
            auto [upload_stream, upload_size, content, batches]
              = expected_data_payload(desc.base, desc.last);

            auto upload = make_upload(
              desc, upload_size, std::move(upload_stream));

            expected_meta.push_back(upload->meta);

            builder->expect_prepare_segment_upload(
              archival::size_limited_offset_range(
                desc.base, expected_target_size, expected_min_size),
              expected_read_buffer_size,
              std::move(upload));
        }

        // Second call finds that there is not enough data to start a new upload
        builder->expect_prepare_segment_upload(
          archival::size_limited_offset_range(
            model::next_offset(expected.back().last),
            expected_target_size,
            expected_min_size),
          expected_read_buffer_size,
          error_outcome::not_enough_data);
    }
    {
        testing::InSequence s;
        for (int i = 0; i < 2; i++) {
            auto d = expected[i];
            partition->expect_offset_delta(d.base, d.base_delta);
            // The offset_delta is called for the committed_offset+1
            partition->expect_offset_delta(
              model::next_offset(d.last), d.last_delta);
            partition->expect_get_offset_term(d.base, expected_segment_term);
            partition->expect_get_initial_revision(expected_revision_id);
            partition->expect_aborted_transactions(
              d.base, d.last, fragmented_vector<model::tx_range>{});
        }
    }

    auto ops = detail::make_archiver_operations_api(
      remote, cache, pm, builder, c_expected_bucket);
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 1ms);
    auto arg = archiver_operations_api::find_upload_candidates_arg{
      .ntp = expected_ntp,
      .archiver_term = expected_archiver_term,
      .target_size = expected_target_size,
      .min_size = expected_min_size,
      .upload_size_quota = expected_upload_size_quota,
      .upload_requests_quota = 2 * expected_upload_requests_quota,
      .compacted_reupload = false,
      .inline_manifest = false,
    };
    auto res = co_await ops->find_upload_candidates(rtc, arg);
    ASSERT_TRUE_CORO(!res.has_error());
    ASSERT_EQ_CORO(res.value().ntp, expected_ntp);
    ASSERT_EQ_CORO(res.value().read_write_fence, expected_applied_offset);
    ASSERT_EQ_CORO(res.value().results.size(), 2);
    for (int i = 0; i < 2; i++) {
        auto m = expected_meta[i];
        // these fields should be default initialized at this stage
        m.base_timestamp = {};
        m.max_timestamp = {};
        ASSERT_EQ_CORO(res.value().results.at(i)->metadata, m);
    }
}

TEST_CORO(
  archiver_operations_impl_test,
  find_upload_candidates_success_1_segment_plus_tx) {
    // find_upload_candidates finds one segment that has transactions
    scoped_config cfg;
    cfg.get("storage_read_buffer_size").set_value(expected_read_buffer_size);
    cfg.get("cloud_storage_segment_size_target")
      .set_value(std::make_optional<size_t>(expected_target_size));
    cfg.get("cloud_storage_segment_size_min")
      .set_value(std::make_optional<size_t>(expected_min_size));

    // Find single upload candidate
    auto remote = ss::make_shared<remote_mock>();
    auto cache = ss::make_shared<cache_mock>();
    auto pm = ss::make_shared<partition_manager_mock>();
    auto builder = ss::make_shared<upload_builder_mock>();
    auto partition = ss::make_shared<partition_mock>();
    partition->expect_get_applied_offset(expected_applied_offset);
    partition->expect_get_uploaded_offset(expected_uploaded_offset);
    pm->expect_get_partition(partition);

    segment_meta expected_meta;

    model::offset expected_base(101);
    model::offset expected_last(200);
    model::producer_identity pid(876, 17);
    {
        auto [upload_stream, upload_size, content, batches]
          = expected_data_payload(expected_base, expected_last);

        auto upload = make_upload(
          {
            .base = expected_base,
            .last = expected_last,
            .base_delta = model::offset_delta(1),
            .last_delta = model::offset_delta(2),
            .base_ts = model::timestamp(1000000),
            .last_ts = model::timestamp(1000100),
          },
          upload_size,
          std::move(upload_stream));
        upload->meta.metadata_size_hint = 1;

        expected_meta = upload->meta;

        testing::InSequence s;
        // First call find upload candidate
        builder->expect_prepare_segment_upload(
          archival::size_limited_offset_range(
            expected_base, expected_target_size, expected_min_size),
          expected_read_buffer_size,
          std::move(upload));

        // Second call finds that there is not enough data to start a new upload
        builder->expect_prepare_segment_upload(
          archival::size_limited_offset_range(
            model::next_offset(expected_last),
            expected_target_size,
            expected_min_size),
          expected_read_buffer_size,
          error_outcome::not_enough_data);
    }

    partition->expect_offset_delta(expected_base, model::offset_delta(1));
    // The offset_delta is called for the committed_offset+1
    partition->expect_offset_delta(
      model::next_offset(expected_last), model::offset_delta(2));
    partition->expect_get_offset_term(expected_base, expected_segment_term);
    partition->expect_get_initial_revision(expected_revision_id);
    auto expected_tx = model::tx_range{
      .pid = pid,
      .first = expected_base,
      .last = model::next_offset(expected_base),
    };
    partition->expect_aborted_transactions(
      expected_base,
      expected_last,
      fragmented_vector<model::tx_range>{{
        expected_tx,
      }});

    auto ops = detail::make_archiver_operations_api(
      remote, cache, pm, builder, c_expected_bucket);
    ss::abort_source as;
    retry_chain_node rtc(as, 1s, 1ms);
    auto arg = archiver_operations_api::find_upload_candidates_arg{
      .ntp = expected_ntp,
      .archiver_term = expected_archiver_term,
      .target_size = expected_target_size,
      .min_size = expected_min_size,
      .upload_size_quota = expected_upload_size_quota,
      .upload_requests_quota = expected_upload_requests_quota,
      .compacted_reupload = false,
      .inline_manifest = false,
    };
    auto res = co_await ops->find_upload_candidates(rtc, arg);
    ASSERT_TRUE_CORO(!res.has_error());
    ASSERT_EQ_CORO(res.value().ntp, expected_ntp);
    ASSERT_EQ_CORO(res.value().results.size(), 1);
    auto candidate = res.value().results.back();
    ASSERT_EQ_CORO(candidate->tx.size(), 1);
    ASSERT_EQ_CORO(candidate->tx.back(), expected_tx);
    // The candidate will not have these fields set because
    // they're actually set during the upload.
    expected_meta.base_timestamp = {};
    expected_meta.max_timestamp = {};
    ASSERT_EQ_CORO(candidate->metadata, expected_meta);
}

} // namespace archival
