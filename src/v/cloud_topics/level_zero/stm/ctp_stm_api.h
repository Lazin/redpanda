/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/gate.hh>

#include <expected>
#include <ostream>

struct ctp_stm_api_accessor;

namespace experimental::cloud_topics {

class ctp_stm;

enum class ctp_stm_api_errc {
    timeout,
    not_leader,
};

std::ostream& operator<<(std::ostream& o, ctp_stm_api_errc errc);

class ctp_stm_api {
    friend struct ::ctp_stm_api_accessor;

public:
    ctp_stm_api(retry_chain_node& rtc, ss::shared_ptr<ctp_stm> stm);
    ctp_stm_api(const ctp_stm_api&) noexcept = delete;
    ctp_stm_api& operator=(const ctp_stm_api&) noexcept = delete;
    ctp_stm_api(ctp_stm_api&&) noexcept = delete;
    ctp_stm_api& operator=(ctp_stm_api&&) noexcept = delete;

    ~ctp_stm_api() {
        vassert(_gate.is_closed(), "object destroyed before calling stop()");
    }

public:
    ss::future<> stop();

    /// Get the last reconciled offset from the ctp_stm state.
    kafka::offset get_last_reconciled_offset() const;

    ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
    advance_reconciled_offset(kafka::offset last_reconciled_offset);

    /// Return the smallest epoch referenced by this ctp_stm.
    std::optional<cluster_epoch> get_min_epoch() const;

    /// Fence writes
    ss::future<cluster_epoch_fence> fence_epoch(cluster_epoch e);

    std::optional<cluster_epoch> get_max_epoch() const;

    std::optional<cluster_epoch> get_projected_epoch() const;

private:
    /// Replicate a record batch and wait for it to be applied to the ctp_stm.
    /// Returns the offset at which the batch was applied.
    ss::future<std::expected<model::offset, ctp_stm_api_errc>>
    replicated_apply(model::record_batch&& batch);

private:
    retry_chain_node _rtc;
    retry_chain_logger _rtclog;

    /// Gate held by async operations to ensure that the API is not destroyed
    /// while an operation is in progress.
    ss::gate _gate;

    ss::shared_ptr<ctp_stm> _stm;
};

} // namespace experimental::cloud_topics
