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

#include <algorithm>
#include <iterator>

namespace experimental::cloud_topics {

bool ctp_stm_epochs::add_epoch(
  cluster_epoch epoch, kafka::offset offset, model::offset insync_offset) {
    // Sanity check to ensure that the epoch change is monotonic
    if (
      !_epochs.empty() && 
      (_epochs.back().epoch >= epoch || 
      _epochs.back().changed_at >= insync_offset || 
      _epochs.back().first_epoch_offset >= offset)) {
        return false;
    }
    _epochs.push_back(epoch_change{
      .epoch = epoch,
      .first_epoch_offset = offset,
      .changed_at = insync_offset,
    });
    return true;
}

std::optional<cluster_epoch> ctp_stm_epochs::get_max_epoch() const noexcept {
    if (_epochs.empty()) {
        return std::nullopt;
    }
    return _epochs.back().epoch;
}

std::optional<ctp_stm_epochs::epoch_change>
ctp_stm_epochs::find_first_epoch(kafka::offset last_reconciled) const noexcept {
    if (_epochs.empty()) {
        return std::nullopt;
    }
    auto needle = kafka::next_offset(last_reconciled);
    auto it = std::upper_bound(
      _epochs.begin(),
      _epochs.end(),
      needle,
      [](const kafka::offset& lhs, const epoch_change& rhs) {
          return lhs < rhs.first_epoch_offset;
      });
    // It is the first element that has first_epoch_offset strictly greater than
    // needle or end iterator. In the latter case the last element of the list
    // is our search result.

    if (it == _epochs.begin()) {
        // In this case we have two options:
        // 1. The first element has first_epoch_offset equal to needle.
        //    This is normally possible if the reconciler wasn't running long
        //    enough. We need to return the first epoch.
        // 2. The first element has first_epoch_offset strictly greater than
        // needle.
        //    This is a problem because we have a gap between the LRO and the
        //    first epoch or the LRO value used to call this method is
        //    inconsistent. This is a synchronous call so races are not
        //    possible.
        if (it->first_epoch_offset == needle) {
            return *it;
        }
        vassert(
          false,
          "ctp_stm_epochs::find_first_epoch: first epoch has "
          "first_epoch_offset "
          "greater than LRO: {}, first_epoch_offset: {}, epoch: {}",
          last_reconciled,
          it->first_epoch_offset,
          it->epoch);
    }

    it = std::prev(it);

    return *it;
}

std::optional<ctp_stm_epochs::epoch_change>
ctp_stm_epochs::find_first_epoch() const noexcept {
    if (_epochs.empty()) {
        return std::nullopt;
    }
    // Return the first epoch in the list, which is always the first one added.
    return _epochs.front();
}

void ctp_stm_epochs::truncate_to(model::offset log_offset) noexcept {
    auto it = std::remove_if(
      _epochs.begin(), _epochs.end(), [log_offset](const epoch_change& ec) {
          return ec.changed_at < log_offset;
      });
    _epochs.erase(it, _epochs.end());
    _epochs.shrink_to_fit();
}

ctp_stm_epochs
ctp_stm_epochs::get_epochs_at(model::offset snapshot_at) const noexcept {
    ctp_stm_epochs result;
    // Copy epochs
    std::copy_if(
      _epochs.begin(),
      _epochs.end(),
      std::back_inserter(result._epochs),
      [snapshot_at](const epoch_change& ec) noexcept {
          return ec.changed_at <= snapshot_at;
      });

    return result;
}

bool ctp_stm_offsets::add_offset(
  kafka::offset last_reconciled, model::offset insync_offset) {
    // Sanity check to ensure that the last reconciled offset change is
    // monotonic
    if (
      !_offsets.empty()
      && (_offsets.back().last_reconciled_offset >= last_reconciled
          || _offsets.back().changed_at >= insync_offset)) {
        return false;
    }
    _offsets.push_back(offset_change{
      .last_reconciled_offset = last_reconciled,
      .changed_at = insync_offset,
    });
    return true;
}

std::optional<kafka::offset> ctp_stm_offsets::last_reconciled() const noexcept {
    if (_offsets.empty()) {
        return std::nullopt;
    }
    return _offsets.back().last_reconciled_offset;
}

void ctp_stm_offsets::truncate_to(model::offset log_offset) noexcept {
    auto it = std::remove_if(
      _offsets.begin(), _offsets.end(), [log_offset](const offset_change& oc) {
          return oc.changed_at < log_offset;
      });
    _offsets.erase(it, _offsets.end());
    _offsets.shrink_to_fit();
}

ctp_stm_offsets
ctp_stm_offsets::get_offsets_at(model::offset snapshot_at) const noexcept {
    ctp_stm_offsets result;
    // Copy offsets
    std::copy_if(
      _offsets.begin(),
      _offsets.end(),
      std::back_inserter(result._offsets),
      [snapshot_at](const offset_change& oc) noexcept {
          return oc.changed_at <= snapshot_at;
      });
    return result;
}

void ctp_stm_state::advance_projected_epoch(cluster_epoch epoch) noexcept {
    if (!_projected_epoch.has_value()) {
        _projected_epoch = epoch;
        return;
    }
    _projected_epoch = std::max(epoch, _projected_epoch.value());
}

void ctp_stm_state::advance_insync_offset(
  model::offset new_insync_offset) noexcept {
    _insync_offset = std::max(new_insync_offset, _insync_offset);
}

void ctp_stm_state::advance_applied_offset() noexcept {
    vassert(
      check_invariant(),
      "Can't advance applied offset, invariant is broken, insync: {}, applied: "
      "{}",
      _insync_offset,
      _applied_offset);
    _applied_offset = _insync_offset;
}

kafka::offset ctp_stm_state::get_last_reconciled_offset() const noexcept {
    return _offsets.last_reconciled().value_or(kafka::offset{0});
}

model::offset ctp_stm_state::get_insync_offset() const noexcept {
    return _insync_offset;
}

model::offset ctp_stm_state::get_applied_offset() const noexcept {
    return _applied_offset;
}

bool ctp_stm_state::can_apply(model::offset o) const noexcept {
    // Invariant: the version can only be applied if it's equal to
    // insync_offset. If this is not the case the metadata batch
    // is applied out of order and should be rejected. If the version
    // is equal to applied_offset then the batch was already applied
    // and should be rejected. This is necessary because we're always
    // using base_offset of the command batch to advance the offsets.
    return _applied_offset < _insync_offset && _insync_offset == o;
}

bool ctp_stm_state::add_epoch(
  cluster_epoch epoch, kafka::offset first_epoch_offset) {
    // Propagate projected epoch on a follower
    _projected_epoch = std::max(
      _projected_epoch.value_or(cluster_epoch{0}), epoch);
    // Register new epoch
    return _epochs.add_epoch(epoch, first_epoch_offset, _insync_offset);
}

void ctp_stm_state::advance_last_reconciled_offset(
  kafka::offset new_last_reconciled_offset) noexcept {
    _offsets.add_offset(new_last_reconciled_offset, _insync_offset);
}

std::optional<cluster_epoch> ctp_stm_state::get_max_epoch() const noexcept {
    return _epochs.get_max_epoch();
}

std::optional<cluster_epoch>
ctp_stm_state::get_projected_epoch() const noexcept {
    return _projected_epoch;
}

ctp_stm_state
ctp_stm_state::get_state_at(model::offset snapshot_at) const noexcept {
    ctp_stm_state snapshot;

    snapshot._projected_epoch = _projected_epoch;
    snapshot._insync_offset = snapshot_at;
    snapshot._applied_offset = snapshot_at;

    // Get epochs at the snapshot offset
    snapshot._epochs = _epochs.get_epochs_at(snapshot_at);

    // Get offsets at the snapshot offset
    snapshot._offsets = _offsets.get_offsets_at(snapshot_at);

    return snapshot;
}

std::optional<cluster_epoch> ctp_stm_state::find_first_epoch() const noexcept {
    auto lro = _offsets.last_reconciled();
    std::optional<ctp_stm_epochs::epoch_change> first_epoch;
    if (lro.has_value()) {
        first_epoch = _epochs.find_first_epoch(lro.value());
    }
    if (!first_epoch.has_value()) {
        // Nothing was reconciled yet so return first epoch.
        first_epoch = _epochs.find_first_epoch();
    }
    if (first_epoch.has_value()) {
        return first_epoch->epoch;
    }
    return std::nullopt;
}

model::offset ctp_stm_state::get_max_collectible_offset() const noexcept {
    auto lro = _offsets.last_reconciled();
    if (lro.has_value()) {
        auto first_epoch = _epochs.find_first_epoch(lro.value());
        if (first_epoch.has_value()) {
            return model::prev_offset(first_epoch->changed_at);
        }
    }
    // nothing is reconciled yet (but we might have some L0 data) so return the
    // first epoch.
    auto first_epoch = _epochs.find_first_epoch();
    if (first_epoch.has_value()) {
        return model::prev_offset(first_epoch->changed_at);
    }
    return model::offset::max();
}

void ctp_stm_state::truncate_to(model::offset epoch_offset) noexcept {
    _epochs.truncate_to(epoch_offset);
    _offsets.truncate_to(epoch_offset);
}

} // namespace experimental::cloud_topics
