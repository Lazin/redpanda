// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_scheduler_api.h"
#include "archival/types.h"
#include "gmock/gmock.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>

#include <gmock/gmock.h>

namespace archival {

class archiver_scheduler_mock : public archiver_scheduler_api {
public:
    /// Applies throttling or backoff
    ///
    /// \param usage describes resources used by the partition
    /// \param ntp is an partition that invokes the manager
    /// \returns true if throttling was applied
    MOCK_METHOD(
      ss::future<result<archiver_scheduler_api::next_upload_action_hint>>,
      maybe_suspend_upload,
      (suspend_upload_arg),
      (override, noexcept));

    void expect_maybe_suspend_upload(
      suspend_upload_arg expected_input,
      result<next_upload_action_hint> expected_output) {
        auto out = ss::make_ready_future<
          result<archiver_scheduler_api::next_upload_action_hint>>(
          expected_output);
        EXPECT_CALL(*this, maybe_suspend_upload(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }

    void expect_maybe_suspend_upload(
      suspend_upload_arg expected_input, std::exception_ptr expected_output) {
        auto out = ss::make_exception_future<
          result<archiver_scheduler_api::next_upload_action_hint>>(
          expected_output);
        EXPECT_CALL(*this, maybe_suspend_upload(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }

    MOCK_METHOD(
      ss::future<result<archiver_scheduler_api::next_housekeeping_action_hint>>,
      maybe_suspend_housekeeping,
      (suspend_housekeeping_arg),
      (override, noexcept));

    void expect_maybe_suspend_housekeeping(
      suspend_housekeeping_arg expected_input,
      result<next_housekeeping_action_hint> expected_output) {
        auto out = ss::make_ready_future<
          result<archiver_scheduler_api::next_housekeeping_action_hint>>(
          expected_output);
        EXPECT_CALL(
          *this, maybe_suspend_housekeeping(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }

    void expect_maybe_suspend_housekeeping(
      suspend_housekeeping_arg expected_input,
      std::exception_ptr expected_output) {
        auto out = ss::make_exception_future<
          result<archiver_scheduler_api::next_housekeeping_action_hint>>(
          expected_output);
        EXPECT_CALL(
          *this, maybe_suspend_housekeeping(std::move(expected_input)))
          .Times(1)
          .WillOnce(::testing::Return(std::move(out)));
    }
};

using next_upload_action_hint = archiver_scheduler_api::next_upload_action_hint;
using suspend_upload_request = archiver_scheduler_api::suspend_upload_arg;
using next_upload_action_type = archiver_scheduler_api::next_upload_action_type;
using next_housekeeping_action_hint
  = archiver_scheduler_api::next_housekeeping_action_hint;
using suspend_housekeeping_request
  = archiver_scheduler_api::suspend_housekeeping_arg;
using next_housekeeping_action_type
  = archiver_scheduler_api::next_housekeeping_action_type;

} // namespace archival
