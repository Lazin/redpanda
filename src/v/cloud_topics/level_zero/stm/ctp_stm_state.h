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

#include "cloud_topics/types.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

/// In-memory state of the cloud-topics state machine (ctp_stm).
///
class ctp_stm_state
  : public serde::
      envelope<ctp_stm_state, serde::version<0>, serde::compat_version<0>> {
    friend class ctp_stm_state_accessor;

public:
    ctp_stm_state() = default;

    /// Register new epoch with the state.
    ///
    /// \param epoch Cluster epoch value.
    void advance_epoch(cluster_epoch epoch);

    /// This is invoked in the write path before the batch with new
    /// epoch value is even replicated.
    void advance_projected_epoch(cluster_epoch epoch) noexcept;

    /// Find the maximum cluster epoch registered in the state.
    std::optional<cluster_epoch> get_max_epoch() const noexcept;

    std::optional<cluster_epoch> get_projected_epoch() const noexcept;

    void advance_last_reconciled_offset(
      kafka::offset new_last_reconciled_offset,
      model::offset new_last_reconciled_log_offset) noexcept;

    /// Get last reconciled offset value
    std::optional<kafka::offset>
    get_last_reconciled_offset() const noexcept;
    std::optional<model::offset>
    get_last_reconciled_log_offset() const noexcept;

    auto serde_fields() {
        return std::tie(
          _max_epoch, _last_reconciled_offset, _last_reconciled_log_offset);
    }

    /// Max collectible offset is defined by the LRO.
    ///
    model::offset get_max_collectible_offset() const noexcept;

    /// Truncate the prefix of the epochs list to the given log offset.
    void truncate_to(model::offset epoch_offset) noexcept;

private:
    /// The last in-flight epoch
    std::optional<cluster_epoch> _projected_epoch;
    /// The last added epoch
    std::optional<cluster_epoch> _max_epoch;
    /// LRO value
    std::optional<kafka::offset> _last_reconciled_offset;
    /// The LRO translated to the log offset
    std::optional<model::offset> _last_reconciled_log_offset;
};

}; // namespace experimental::cloud_topics
