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
    ss::future<> start() override { co_return; }
    ss::future<> stop() override { co_return; }

private:
    model::ktp _ntp;
    model::term_id _id;
    ss::shared_ptr<archiver_operations_api> _operations;
    ss::shared_ptr<archiver_scheduler_api> _scheduler;
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
