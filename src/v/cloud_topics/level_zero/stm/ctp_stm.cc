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
#include "raft/consensus.h"
#include "serde/rw/map.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "storage/offset_translator_state.h"

#include <stdexcept>

namespace experimental::cloud_topics {

static cluster_epoch extract_epoch(model::record_batch&& batch) {
    vassert(
      batch.header().type == model::record_batch_type::dl_placeholder,
      "Expected batch type to be dl_placeholder, got {}",
      batch.header().type);
    iobuf value;
    batch.for_each_record([&value](model::record&& r) {
        value = std::move(r).release_value();
        return ss::stop_iteration::yes;
    });

    auto placeholder = serde::from_iobuf<dl_placeholder>(std::move(value));
    return placeholder.id.epoch;
}

ss::future<ss::stop_iteration>
ctp_stm_consumer::operator()(model::record_batch batch) {
    _first_epoch = extract_epoch(std::move(batch));
    co_return _first_epoch.has_value() ? ss::stop_iteration::yes
                                       : ss::stop_iteration::no;
}

std::optional<cluster_epoch> ctp_stm_consumer::end_of_stream() {
    return _first_epoch;
}

ctp_stm::ctp_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

const model::ntp& ctp_stm::ntp() const noexcept { return _raft->ntp(); }

ss::future<std::optional<cluster_epoch>> ctp_stm::get_min_epoch() {
    // Consume the first epoch from the partition starting from
    // start offset if nothing was reconciled yet or from the last
    // reconciled offset + 1 otherwise.
    auto so = _raft->start_offset();
    auto co = _raft->committed_offset();
    auto lro = _state.get_last_reconciled_log_offset().value_or(model::prev_offset(so));
    storage::log_reader_config cfg(
      model::next_offset(lro),
      co,
      0,
      4_KiB,
      std::make_optional(model::record_batch_type::dl_placeholder),
      std::nullopt,
      std::nullopt);

    auto reader = co_await _raft->make_reader(cfg);
    auto result = co_await std::move(reader).consume(ctp_stm_consumer{}, model::no_timeout);
    if (result.has_value()) {
        auto epoch = result.value();
        vlog(_log.debug, "Minimum epoch in partition {} is {}", _raft->ntp(), epoch);
        co_return epoch;
    } else {
        // This could naturally happen if the partition is empty because
        // everything was reconciled.
        vlog(_log.debug, "No epochs found in partition {}", _raft->ntp());
        co_return std::nullopt;
    }
}

ss::future<> ctp_stm::do_apply(const model::record_batch& batch) {
    if (
      batch.header().type != model::record_batch_type::dl_placeholder
      && batch.header().type != model::record_batch_type::ctp_stm_command) {
        co_return;
    }
    vlog(_log.debug, "Applying record batch: {}", batch.header());

    if (batch.header().type == model::record_batch_type::dl_placeholder) {
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
        _state.advance_epoch(id.epoch);

    } else if (
      batch.header().type == model::record_batch_type::ctp_stm_command) {
        // Decode the command and apply it to the state.
        kafka::offset lro;
        batch.for_each_record([&lro](model::record&& r) {
            auto key = serde::from_iobuf<uint8_t>(r.release_key());
            auto cmd_key = static_cast<ctp_stm_key>(key);
            switch (cmd_key) {
            case ctp_stm_key::advance_reconciled_offset: {
                auto cmd = serde::from_iobuf<advance_reconciled_offset_cmd>(
                  r.release_value());
                lro = cmd.last_reconciled_offset;
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
        vlog(_log.debug, "New LRO value is {}", lro);
        // LRO is expected to be within the translation range
        auto lro_log = _raft->log()->to_log_offset(kafka::offset_cast(lro));
        _state.advance_last_reconciled_offset(lro, lro_log);
    }

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
      0, this->last_applied(), std::move(buf));
}

ss::future<> ctp_stm::apply_raft_snapshot(const iobuf& buf) {
    _state = serde::from_iobuf<ctp_stm_state>(buf.copy());
    co_return;
}

ss::future<iobuf> ctp_stm::take_raft_snapshot(model::offset snapshot_at) {
    vassert(
      last_applied() >= snapshot_at,
      "The snapshot is taken at offset {} but current insync offset is {}",
      snapshot_at,
      last_applied());
    co_return serde::to_iobuf(_state);
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
