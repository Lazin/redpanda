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
        ssx::spawn_with_gate(_gate, [this] { return run_bg_loop(); });
        co_return;
    }

    ss::future<> stop() override {
        co_await _gate.close();
        if (_fatal) {
            std::rethrow_exception(_fatal);
        }
    }

private:
    ss::future<> run_bg_loop() {
        auto h = _gate.hold();
        while (true) {
            archiver_scheduler_api::suspend_housekeeping_arg suspend_req{
              .ntp = _ntp,
              .num_delete_requests_used = 0, // TODO: propagate value from GC
            };
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
            archiver_operations_api::apply_archive_retention_arg apply_arch_req{
              .ntp = _ntp,
              .delete_op_quota = 1000, // TODO: use value from the config
            };
            auto apply_arch_res = co_await _operations->apply_archive_retention(
              _rtc, apply_arch_req);
            if (apply_arch_res.has_error()) {
                // Recoverable error
                vlog(
                  _log.warn,
                  "Housekeeping error (apply archive retention): {}",
                  apply_arch_res.error());
                continue;
            }
            // TODO: add remaining housekeeping ops
        }
        co_return;
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
