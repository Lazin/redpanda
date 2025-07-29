/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm.h"

#include "bytes/iobuf.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/level_zero/stm/ctp_stm_commands.h"
#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/types.h"
#include "cluster/prefix_truncate_record.h"
#include "raft/consensus.h"
#include "serde/rw/map.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "storage/offset_translator_state.h"

#include <stdexcept>

namespace experimental::cloud_topics {

ctp_stm::ctp_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

const model::ntp& ctp_stm::ntp() const noexcept { return _raft->ntp(); }

ss::future<> ctp_stm::do_apply(const model::record_batch& batch) {
    // TODO: react to prefix_truncate commands by truncating the ctp_stm_state
    _state.advance_insync_offset(batch.base_offset());
    if (
      batch.header().type != model::record_batch_type::dl_placeholder
      && batch.header().type != model::record_batch_type::ctp_stm_command
      && batch.header().type != model::record_batch_type::prefix_truncate) {
        co_return;
    }
    vlog(_log.debug, "Applying record batch: {}", batch.header());

    if (batch.header().type == model::record_batch_type::dl_placeholder) {
        // Decode the record batch to extract the epoch
        auto base_offset = batch.base_offset();

        // Cherry-pick the placeholder from the record batch
        vassert(
          batch.record_count() > 0,
          "Record batch must have at least one record");
        iobuf value;
        batch.for_each_record([&value](model::record&& r) {
            value = std::move(r).release_value();
            return ss::stop_iteration::yes;
        });

        auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
        auto id = placeholder.id;

        if (!_state.can_apply(base_offset)) {
            vlog(
              _log.warn,
              "Record batch at offset {} is applied out of order",
              base_offset);
            co_return;
            // NOTE: in case of failure the gap between the insync offset
            // and applied offset will be closed by the next record batch.
            // This is no different from the case when the record batch
            // is of a different type.
        }

        auto current_max_epoch = _state.get_max_epoch();
        if (
          !current_max_epoch.has_value()
          || id.epoch > current_max_epoch.value()) {
            // Add the epoch to the state. For that we need to translate the
            // base offset of the batch.
            auto ko = _raft->log()->from_log_offset(base_offset);
            if (!_state.add_epoch(id.epoch, model::offset_cast(ko))) {
                vlog(
                  _log.info,
                  "Failed to add epoch {} at offset {}, it is not monotonic",
                  id,
                  base_offset);
                co_return;
                // NOTE: ditto, the gap between the insync offset will be closed
                // later.
            } else {
                vlog(
                  _log.debug,
                  "Epoch {} added successfully at offset {}",
                  id,
                  base_offset);
            }
        }
    } else if (
      batch.header().type == model::record_batch_type::ctp_stm_command) {
        // Decode the command and apply it to the state.
        batch.for_each_record([this](model::record&& r) {
            auto key = serde::from_iobuf<uint8_t>(r.release_key());
            auto cmd_key = static_cast<ctp_stm_key>(key);
            switch (cmd_key) {
            case ctp_stm_key::advance_reconciled_offset: {
                auto cmd = serde::from_iobuf<advance_reconciled_offset_cmd>(
                  r.release_value());
                _state.advance_last_reconciled_offset(
                  cmd.last_reconciled_offset);
                vlog(
                  _log.debug,
                  "Reconciled offset advanced to {}",
                  cmd.last_reconciled_offset);
                break;
            }
            default:
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format,
                  "Unknown ctp_stm_key({})",
                  static_cast<int>(key)));
            }
            return ss::stop_iteration::no;
        });
    } else if (
      batch.header().type == model::record_batch_type::prefix_truncate) {
        // Truncate the _state
        batch.for_each_record([this](model::record&& r) {
            auto key = serde::from_iobuf<uint8_t>(r.release_key());
            auto val = serde::from_iobuf<cluster::prefix_truncate_record>(
              r.release_value());
            if (key == cluster::prefix_truncate_key) {
                vlog(
                  _log.debug,
                  "Truncating epochs to offset {}",
                  val.rp_start_offset);
                _state.truncate_to(val.rp_start_offset);
            }
        });
    }

    // Close the gap between the insync offset and applied offset.
    // After this method is called the call to 'can_apply' using
    // the same version will always return false. This guarantees
    // that any command can only be applied twice even if log replay
    // is not idempotent (which is luckily not the case).
    _state.advance_applied_offset();

    co_return;
}

ss::future<raft::local_snapshot_applied>
ctp_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buf) {
    _state = serde::from_iobuf<ctp_stm_state>(std::move(buf));
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
ctp_stm::take_local_snapshot(ssx::semaphore_units) {
    auto buf = serde::to_iobuf(_state);
    co_return raft::stm_snapshot::create(
      0, _state.get_insync_offset(), std::move(buf));
}

ss::future<> ctp_stm::apply_raft_snapshot(const iobuf& buf) {
    _state = serde::from_iobuf<ctp_stm_state>(buf.copy());
    co_return;
}

ss::future<iobuf> ctp_stm::take_raft_snapshot(model::offset snapshot_at) {
    auto st = _state.get_state_at(snapshot_at);
    co_return serde::to_iobuf(std::move(st));
}

ss::future<cluster_epoch_fence> ctp_stm::fence_epoch(cluster_epoch e) {
    auto term = _raft->confirmed_term();
    if (_state.get_projected_epoch() == e) {
        // Case 1. Same epoch, need to acquire read-lock.
        auto unit = co_await _lock.hold_read_lock();
        if (_state.get_projected_epoch() == e) {
            // The projected epoch didn't advance after the scheduling point
            co_return cluster_epoch_fence{std::move(unit), term};
        }
    } else {
        // Case 2. New epoch, need to acquire write-lock.
        auto unit = co_await _lock.hold_write_lock();
        auto current_epoch = _state.get_projected_epoch();
        if (!current_epoch.has_value() || current_epoch.value() <= e) {
            _state.advance_projected_epoch(e);
            co_return cluster_epoch_fence{std::move(unit), term};
        }
    }
    // If we reach here, it means that we need to discard the batch.
    co_return cluster_epoch_fence{};
}

}; // namespace experimental::cloud_topics
