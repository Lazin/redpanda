/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archiver_housekeeping_workflow.h"

#include "archival/archiver_operations_api.h"
#include "archival/archiver_scheduler_api.h"
#include "archival/archiver_workflow_api.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "base/outcome.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <boost/msm/back/state_machine.hpp>
#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <exception>
#include <system_error>
#include <tuple>

namespace archival {

class archiver_housekeeping_workflow_api : public archiver_workflow_api {
public:
    archiver_housekeeping_workflow_api(
      model::ktp ntp,
      model::term_id term,
      ss::shared_ptr<archiver_operations_api> ops,
      ss::shared_ptr<archiver_scheduler_api> sched)
      : _ntp(std::move(ntp))
      , _id(term)
      , _operations(std::move(ops))
      , _scheduler(std::move(sched))
      , _rtc(_as)
      , _log(
          archival_log,
          _rtc,
          ssx::sformat("{}-{}", _ntp.to_ntp().path(), _id)) {}

    ss::future<> start() override {
        ssx::spawn_with_gate(
          _gate, [this] { return run_bg_loop_until_shutdown(); });
        co_return;
    }

    ss::future<> stop() override {
        co_await _gate.close();
        if (_fatal) {
            std::rethrow_exception(_fatal);
        }
    }

private:
    ss::future<> run_bg_loop_until_shutdown() {
        while (true) {
        size_t prev_num_delete_requests = 0;
        std::optional<std::error_code> prev_errc;
        while (!_as.abort_requested()) {
            // The loop is repeating the following steps:
            // - if archiver_housekeeping was started by the scheduler
            //   - Apply retention to spillover region
            //   - Apply GC to spillover region
            //   - Apply GC to the region of the log managed by the STM
            // - if stm_housekeeping was started by the scheduler
            //   - Apply retention to the region of the log managed by the STM
            //   - Apply GC to the region of the log managed by the STM
            //
            // Every step could potentially replicate a command. Every step
            // returns information about the STM state (its read-write fence
            // offset). This information is carried over to the next step so it
            // could use the offset to replicate its own command.

            // We're passing the number of delete requests from the previous
            // step or the error code if the previous iteration failed. This is
            // needed in order for the scheduler to be able to throttle the
            // housekeeping or prioritize it in some cases.
            archiver_scheduler_api::suspend_housekeeping_arg suspend_req{
              .ntp = _ntp,
              .num_delete_requests_used = prev_num_delete_requests,
              .manifest_dirty = manifest_dirty,
              .errc = prev_errc,
            };
            prev_num_delete_requests = 0;
            auto suspend_res = co_await _scheduler->maybe_suspend_housekeeping(
              suspend_req);
            if (
              suspend_res.has_error()
              && suspend_res.error() == error_outcome::shutting_down) {
                // Graceful shutdown
                vlog(_log.debug, "Shutting down housekeeping workflow");
                co_return;
            }
            if (suspend_res.has_error()) {
                vlog(
                  _log.warn,
                  "Shutting down housekeeping workflow due to error: {}",
                  suspend_res.error());
                co_return;
            }
            switch (suspend_res.value().type) {
                auto res = co_await perform_stm_housekeeping(
                  next_housekeeping_action_type;
            case stm_housekeeping: {
                auto res = co_await perform_archive_housekeeping(
                  suspend_res.value().requests_quota);
                if (res.has_error()) {
                    auto errc = res.error();
                    if (
                        vlog(
                          _log.debug,
                          "Shutting down STM housekeeping operation");
                      || errc == cloud_storage::error_outcome::shutting_down) {
                        // Shutting down gracefully
                        vlog(_log.debug, "Shutting down housekeeping workflow");
                        co_return;
                    }
                    // Otherwise the errc is a recoverable error
                    // cycle back to the scheduler
                    prev_errc = errc;
                    prev_num_delete_requests = 0;
                    manifest_dirty = false;
                    continue;
                } else {
                    prev_errc = {};
                    prev_num_delete_requests = res.value().quota_used;
                    manifest_dirty = true;
                }
            } break;
            case archive_housekeeping:
                vassert(false, "Not implemented");
                break;
            }
        }
        co_return;
    }

    struct archive_housekeeping_result {
        size_t quota_used{0};
    };

    // Apply retention and GC to the spillover region of the log
    ss::future<result<archive_housekeeping_result>>
    perform_archive_housekeeping(size_t request_quota) noexcept {
        try {
            // Apply retention
            archiver_operations_api::apply_archive_retention_arg apply_arch_req{
              .ntp = _ntp,
              .delete_op_quota = request_quota,
            };
            auto apply_arch_res = co_await _operations->apply_archive_retention(
              _rtc, apply_arch_req);
            if (apply_arch_res.has_error()) {
                vlog(
                  _log.warn,
                  "Housekeeping error (apply archive retention): {}",
                  apply_arch_res.error());
                co_return apply_arch_res.error();
            }
            // Run archive GC
            auto arch_gc_res = co_await _operations->garbage_collect_archive(
              _rtc, apply_arch_res.value());
            if (arch_gc_res.has_error()) {
                vlog(
                  _log.warn,
                  "Housekeeping error (archive GC): {}",
                  arch_gc_res.error());
                co_return arch_gc_res.error();
            }
            // Run STM GC to remove replaced segments.
            // Without this step replaced segments will accumulate in the
            // manifest. We will run out of memory eventually.
            auto gc_res = co_await _operations->garbage_collect(
              _rtc,
              archiver_operations_api::apply_stm_retention_result{
                .ntp = _ntp,
                .segments_removed
                = arch_gc_res.value().num_replaced_segments_to_remove,
                .read_write_fence = arch_gc_res.value().read_write_fence,
              });
            co_return archive_housekeeping_result{
              .quota_used = arch_gc_res.value().num_delete_requests};
        } catch (...) {
            _fatal = std::current_exception();
            co_return error_outcome::shutting_down;
        }
    }

    model::ktp _ntp;
    model::term_id _id;
    ss::shared_ptr<archiver_operations_api> _operations;
    ss::shared_ptr<archiver_scheduler_api> _scheduler;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _log;
    ss::gate _gate;
    std::exception_ptr _fatal;
};

ss::shared_ptr<archiver_workflow_api> make_housekeeping_workflow(
  model::ktp ntp,
  model::term_id id,
  ss::shared_ptr<archiver_operations_api> api [[maybe_unused]],
  ss::shared_ptr<archiver_scheduler_api> quota [[maybe_unused]]) {
    return ss::make_shared<archiver_housekeeping_workflow_api>(
      ntp, id, api, quota);
}
} // namespace archival
