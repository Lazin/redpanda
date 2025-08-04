/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"

#include "model/fundamental.h"

namespace experimental::cloud_topics {

void ctp_stm_state::advance_projected_epoch(cluster_epoch epoch) noexcept {
    _projected_epoch = std::max(
      epoch, _projected_epoch.value_or(cluster_epoch{}));
}

std::optional<kafka::offset>
ctp_stm_state::get_last_reconciled_offset() const noexcept {
    return _last_reconciled_offset;
}

std::optional<model::offset>
ctp_stm_state::get_last_reconciled_log_offset() const noexcept {
    return _last_reconciled_log_offset;
}

void ctp_stm_state::advance_epoch(cluster_epoch epoch) {
    // Propagate projected epoch on a follower
    _projected_epoch = std::max(
      _projected_epoch.value_or(cluster_epoch{}), epoch);
    // Register new epoch
    _max_epoch = std::max(epoch, _max_epoch.value_or(cluster_epoch{}));
}

void ctp_stm_state::advance_last_reconciled_offset(
  kafka::offset new_last_reconciled_offset,
  model::offset new_last_reconciled_log_offset) noexcept {
    _last_reconciled_offset = std::max(
      _last_reconciled_offset.value_or(kafka::offset{}),
      new_last_reconciled_offset);
    _last_reconciled_log_offset = std::max(
      _last_reconciled_log_offset.value_or(model::offset{}),
      new_last_reconciled_log_offset);
}

std::optional<cluster_epoch> ctp_stm_state::get_max_epoch() const noexcept {
    return _max_epoch;
}

std::optional<cluster_epoch>
ctp_stm_state::get_projected_epoch() const noexcept {
    return _projected_epoch;
}

model::offset ctp_stm_state::get_max_collectible_offset() const noexcept {
    if (_last_reconciled_log_offset.has_value()) {
        return _last_reconciled_log_offset.value();
    }
    // Truncation is impossible without LRO
    return model::offset::min();
}

} // namespace experimental::cloud_topics
