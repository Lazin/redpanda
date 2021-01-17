/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/service.h"

#include "arch/error.h"
#include "arch/logger.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>

namespace arch {

// archiver_service::archiver_service(
//   const s3::configuration& conf, const storage::log_manager& lm) {
//     // for(const auto& ntp: lm.get()) {
//     // }
// }

// ss::future<> archiver_service::start() {
//     // TODO: move to timer cb
//     auto ntps = _log_manager.get();
//     for (auto ntp : ntps) {
//         // TODO: add or keep ntp_archiver
//     }
//     return ss::now();
// }

// ss::future<> archiver_service::stop() { return ss::now(); }

} // end namespace arch
