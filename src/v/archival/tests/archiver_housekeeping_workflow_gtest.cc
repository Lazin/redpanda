// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "archival/archiver_housekeeping_workflow.h"
#include "archival/archiver_operations_api.h"
#include "archival/archiver_scheduler_api.h"
#include "archival/logger.h"
#include "archival/tests/archiver_operations_api_mock.h"
#include "archival/tests/archiver_scheduler_api_mock.h"
#include "archival/types.h"
#include "base/vlog.h"
#include "bytes/iostream.h"
#include "config/configuration.h"
#include "gmock/gmock.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "utils/available_promise.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <gmock/gmock.h>

#include <exception>
#include <stdexcept>

namespace archival {

const model::ktp ntp(model::topic("panda-topic"), model::partition_id(137));
const model::term_id archiver_term(42);
const size_t expected_requests_quota = 10;
const size_t expected_segments_removed = 33;
const model::offset expected_fence(42);
const size_t expected_replaced_segments = 10;

/// Create workflow and its dependencies
inline auto setup_housekeeping_test_suite() {
    auto rm_api = ss::make_shared<archiver_scheduler_mock>();
    auto op_api = ss::make_shared<ops_api_mock>();
    auto wf = make_housekeeping_workflow(ntp, archiver_term, op_api, rm_api);

    auto cfg = ss::make_shared<scoped_config>();

    return std::make_tuple(
      std::move(wf), std::move(rm_api), std::move(op_api), std::move(cfg));
}

TEST(archiver_housekeeping_workflow_test, test_immediate_shutdown) {
    // Test case: the workflow is created and receives
    // a shutdown event immediately.
    auto [wf, rm_api, op_api, cfg] = setup_housekeeping_test_suite();

    rm_api->expect_maybe_suspend_housekeeping(
      suspend_housekeeping_request{
        .ntp = ntp,
        .manifest_dirty = std::nullopt,
      },
      make_error_code(error_outcome::shutting_down));

    wf->start().get();
    wf->stop().get();
}

TEST(archiver_housekeeping_workflow_test, test_stm_housekeeping) {
    // Test case: the workflow is doing housekeeping of the
    // STM region of the log
    auto [wf, rm_api, op_api, cfg] = setup_housekeeping_test_suite();

    {
        ::testing::InSequence seq;
        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .manifest_dirty = std::nullopt,
          },
          next_housekeeping_action_hint{
            .type = next_housekeeping_action_type::stm_housekeeping,
            .requests_quota = expected_requests_quota,
          });

        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .num_delete_requests_used = expected_segments_removed,
            .manifest_dirty = true,
          },
          make_error_code(error_outcome::shutting_down));
    }

    op_api->expect_apply_stm_retention(
      apply_stm_retention_arg{
        .ntp = ntp,
        .delete_op_quota = expected_requests_quota,
      },
      apply_stm_retention_result{
        .ntp = ntp,
        .segments_removed = expected_segments_removed,
        .read_write_fence = expected_fence,
      });

    op_api->expect_garbage_collect_stm(
      apply_stm_retention_result{
        .ntp = ntp,
        .segments_removed = expected_segments_removed,
        .read_write_fence = expected_fence,
      },
      garbage_collect_result{
        .ntp = ntp,
        .type = ops_api_mock::gc_type::stm,
        .num_delete_requests = expected_segments_removed,
        .num_failures = 0,
        .manifest_dirty = true,
        .read_write_fence = expected_fence,
      });

    wf->start().get();
    wf->stop().get();
}

TEST(archiver_housekeeping_workflow_test, test_archive_housekeeping) {
    // Test case: the workflow is doing housekeeping of the
    // spillover region of the log
    auto [wf, rm_api, op_api, cfg] = setup_housekeeping_test_suite();

    {
        ::testing::InSequence seq;
        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .manifest_dirty = std::nullopt,
          },
          next_housekeeping_action_hint{
            .type = next_housekeeping_action_type::archive_housekeeping,
            .requests_quota = expected_requests_quota,
          });

        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .num_delete_requests_used = expected_segments_removed,
            .manifest_dirty = true,
          },
          make_error_code(error_outcome::shutting_down));
    }

    op_api->expect_apply_archive_retention(
      apply_archive_retention_arg{
        .ntp = ntp,
        .delete_op_quota = expected_requests_quota,
      },
      apply_archive_retention_result{
        .ntp = ntp,
        .archive_segments_removed = expected_segments_removed,
        .read_write_fence = expected_fence,
      });

    op_api->expect_garbage_collect_archive(
      apply_archive_retention_result{
        .ntp = ntp,
        .archive_segments_removed = expected_segments_removed,
        .read_write_fence = expected_fence,
      },
      garbage_collect_result{
        .ntp = ntp,
        .type = ops_api_mock::gc_type::archive,
        .num_delete_requests = expected_segments_removed,
        .num_failures = 0,
        .manifest_dirty = true,
        .num_replaced_segments_to_remove = expected_replaced_segments,
        .read_write_fence = expected_fence,
      });

    op_api->expect_garbage_collect_stm(
      apply_stm_retention_result{
        .ntp = ntp,
        .segments_removed = expected_replaced_segments,
        .read_write_fence = expected_fence,
      },
      garbage_collect_result{
        .ntp = ntp,
        .type = ops_api_mock::gc_type::stm,
        .num_delete_requests = expected_replaced_segments,
        .num_failures = 0,
        .manifest_dirty = true,
        .read_write_fence = expected_fence,
      });

    wf->start().get();
    wf->stop().get();
}

TEST(
  archiver_housekeeping_workflow_test,
  test_stm_housekeeping_apply_retention_errc) {
    // Test case: the workflow is doing housekeeping of the
    // STM region of the log and apply_stm_retention method returns
    // an error.
    auto [wf, rm_api, op_api, cfg] = setup_housekeeping_test_suite();

    {
        ::testing::InSequence seq;
        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .manifest_dirty = std::nullopt,
          },
          next_housekeeping_action_hint{
            .type = next_housekeeping_action_type::stm_housekeeping,
            .requests_quota = expected_requests_quota,
          });

        rm_api->expect_maybe_suspend_housekeeping(
          suspend_housekeeping_request{
            .ntp = ntp,
            .manifest_dirty = false,
            .errc = error_outcome::timed_out,
          },
          make_error_code(error_outcome::shutting_down));
    }

    op_api->expect_apply_stm_retention(
      apply_stm_retention_arg{
        .ntp = ntp,
        .delete_op_quota = expected_requests_quota,
      },
      error_outcome::timed_out);

    wf->start().get();
    wf->stop().get();
}

TEST(
  archiver_housekeeping_workflow_test,
  test_stm_housekeeping_apply_retention_exception) {
    // Test case: the workflow is doing housekeeping of the
    // STM region of the log and apply_stm_retention method
    // throws an exception.
    auto [wf, rm_api, op_api, cfg] = setup_housekeeping_test_suite();

    rm_api->expect_maybe_suspend_housekeeping(
      suspend_housekeeping_request{
        .ntp = ntp,
        .manifest_dirty = std::nullopt,
      },
      next_housekeeping_action_hint{
        .type = next_housekeeping_action_type::stm_housekeeping,
        .requests_quota = expected_requests_quota,
      });

    op_api->expect_apply_stm_retention(
      apply_stm_retention_arg{
        .ntp = ntp,
        .delete_op_quota = expected_requests_quota,
      },
      std::make_exception_ptr(std::runtime_error("Fatal error")));

    wf->start().get();
    ASSERT_THROW(wf->stop().get(), std::runtime_error);
}

} // namespace archival
