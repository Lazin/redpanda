// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_operations_api.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "base/vlog.h"
#include "bytes/iostream.h"
#include "config/configuration.h"
#include "gmock/gmock.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

#include <gmock/gmock.h>

#include <exception>
#include <stdexcept>

namespace archival {

class ops_api_mock : public archiver_operations_api {
public:
    /// Return upload candidate(s) if data is available or nullopt
    /// if there is not enough data to start an upload.
    MOCK_METHOD(
      ss::future<result<find_upload_candidates_result>>,
      find_upload_candidates,
      (retry_chain_node&, find_upload_candidates_arg),
      (override, noexcept));

    /// Upload data to S3 and return results
    MOCK_METHOD(
      ss::future<result<schedule_upload_results>>,
      schedule_uploads,
      (retry_chain_node&, find_upload_candidates_result),
      (override, noexcept));

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    MOCK_METHOD(
      ss::future<result<admit_uploads_result>>,
      admit_uploads,
      (retry_chain_node&, schedule_upload_results),
      (override, noexcept));

    MOCK_METHOD(
      ss::future<result<manifest_upload_result>>,
      upload_manifest,
      (retry_chain_node&, manifest_upload_arg),
      (override, noexcept));

    MOCK_METHOD(
      ss::future<result<apply_archive_retention_result>>,
      apply_archive_retention,
      (retry_chain_node&, apply_archive_retention_arg),
      (override, noexcept));

    void expect_find_upload_candidates(
      find_upload_candidates_arg input,
      result<find_upload_candidates_result> output) {
        auto success
          = ss::make_ready_future<result<find_upload_candidates_result>>(
            std::move(output));
        EXPECT_CALL(*this, find_upload_candidates(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_find_upload_candidates(
      find_upload_candidates_arg input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<find_upload_candidates_result>>(
            std::move(output));
        EXPECT_CALL(*this, find_upload_candidates(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_schedule_uploads(
      find_upload_candidates_result input,
      result<schedule_upload_results> output) {
        auto success = ss::make_ready_future<result<schedule_upload_results>>(
          std::move(output));
        EXPECT_CALL(*this, schedule_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_schedule_uploads(
      find_upload_candidates_result input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<schedule_upload_results>>(
            std::move(output));
        EXPECT_CALL(*this, schedule_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_admit_uploads(
      schedule_upload_results input, result<admit_uploads_result> output) {
        auto success = ss::make_ready_future<result<admit_uploads_result>>(
          std::move(output));
        EXPECT_CALL(*this, admit_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_admit_uploads(
      schedule_upload_results input, std::exception_ptr output) {
        auto failure = ss::make_exception_future<result<admit_uploads_result>>(
          std::move(output));
        EXPECT_CALL(*this, admit_uploads(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_upload_manifest(
      manifest_upload_arg input, result<manifest_upload_result> output) {
        auto success = ss::make_ready_future<result<manifest_upload_result>>(
          std::move(output));
        EXPECT_CALL(*this, upload_manifest(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_upload_manifest(
      manifest_upload_arg input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<manifest_upload_result>>(
            std::move(output));
        EXPECT_CALL(*this, upload_manifest(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }

    void expect_apply_archive_retention(
      apply_archive_retention_arg input,
      result<apply_archive_retention_result> output) {
        auto success
          = ss::make_ready_future<result<apply_archive_retention_result>>(
            std::move(output));
        EXPECT_CALL(
          *this, apply_archive_retention(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(success)));
    }

    void expect_apply_archive_retention(
      apply_archive_retention_arg input, std::exception_ptr output) {
        auto failure
          = ss::make_exception_future<result<apply_archive_retention_result>>(
            std::move(output));
        EXPECT_CALL(
          *this, apply_archive_retention(testing::_, std::move(input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(failure)));
    }
};

using find_upload_candidates_arg
  = archiver_operations_api::find_upload_candidates_arg;
using segment_upload_candidate_t
  = archiver_operations_api::segment_upload_candidate_t;
using find_upload_candidates_result
  = archival::archiver_operations_api::find_upload_candidates_result;
using schedule_upload_results
  = archival::archiver_operations_api::schedule_upload_results;
using admit_uploads_result
  = archival::archiver_operations_api::admit_uploads_result;
using manifest_upload_arg
  = archival::archiver_operations_api::manifest_upload_arg;
using manifest_upload_result
  = archival::archiver_operations_api::manifest_upload_result;

} // namespace archival
