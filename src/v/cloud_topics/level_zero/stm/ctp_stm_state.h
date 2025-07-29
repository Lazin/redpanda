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

class ctp_stm_epochs
  : public serde::
      envelope<ctp_stm_epochs, serde::version<0>, serde::compat_version<0>> {
public:
    // This represents a change in the cluster epoch.
    struct epoch_change
      : serde::
          envelope<epoch_change, serde::version<0>, serde::compat_version<0>> {
        // Cluster epoch value
        cluster_epoch epoch;
        // Kafka offset of the first record batch in the epoch
        kafka::offset first_epoch_offset;
        // Offset at which the epoch was added
        model::offset changed_at;

        auto serde_fields() { return std::tie(epoch, changed_at); }
    };

    /// Add new epoch to the list.
    /// \param epoch Cluster epoch value.
    /// \param offset Kafka offset of the first record batch in the epoch.
    /// \param insync_offset Log offset at which the epoch was added.
    bool add_epoch(
      cluster_epoch epoch,
      kafka::offset last_reconciled,
      model::offset insync_offset);

    /// Get the maximum cluster epoch registered in the state.
    std::optional<cluster_epoch> get_max_epoch() const noexcept;

    /// Find first referenced cluster epoch.
    /// \param last_reconciled Last reconciled offset.
    /// \return Cluster epoch if found, std::nullopt if the STM is empty.
    /// \invariant The returned epoch is the one added at offset >=
    ///            last_reconciled.
    /// \note The last_reconciled offset can't be less than the first epoch
    ///       offset. If this is the case the assertion will be triggered.
    std::optional<epoch_change>
    find_first_epoch(kafka::offset last_reconciled) const noexcept;
    // Find first epoch without filtering by last reconciled offset.
    std::optional<epoch_change> find_first_epoch() const noexcept;

    /// Truncate the prefix of the epochs list to the given log offset.
    /// \param log_offset Log offset to truncate to.
    /// \invariant All epochs with added_at < log_offset are removed.
    void truncate_to(model::offset log_offset) noexcept;

    /// Get epochs at the given snapshot offset.
    /// \param snapshot_at Offset at which the snapshot is taken.
    /// \return ctp_stm_epochs containing epochs added at or before snapshot_at.
    /// \invariant The epochs are sorted by their added_at offset.
    /// \invariant The changed_at field of the epoch_change is always less than
    /// or
    ///            equal to snapshot_at.
    /// \note This method is used to create a snapshot of the ctp_stm_state
    ///       for Raft snapshotting mechanism.
    ctp_stm_epochs get_epochs_at(model::offset snapshot_at) const noexcept;

    auto serde_fields() { return std::tie(_epochs); }

private:
    chunked_circular_buffer<epoch_change> _epochs;
};

class ctp_stm_offsets
  : public serde::
      envelope<ctp_stm_offsets, serde::version<0>, serde::compat_version<0>> {
    // This represents a change in the last reconciled offset.
    struct offset_change
      : serde::
          envelope<offset_change, serde::version<0>, serde::compat_version<0>> {
        // Last reconciled offset value
        kafka::offset last_reconciled_offset;
        // Offset at which the last reconciled offset was changed
        model::offset changed_at;

        auto serde_fields() {
            return std::tie(last_reconciled_offset, changed_at);
        }
    };

public:
    /// Add new last reconciled offset to the list.
    /// \param last_reconciled Last reconciled offset value.
    /// \param insync_offset Log offset at which the offset was changed.
    bool add_offset(kafka::offset last_reconciled, model::offset insync_offset);

    /// Get the last reconciled offset.
    std::optional<kafka::offset> last_reconciled() const noexcept;

    /// Truncate the prefix of the offsets list to the given log offset.
    /// \param log_offset Log offset to truncate to.
    /// \invariant All offsets with changed_at < log_offset are removed.
    void truncate_to(model::offset log_offset) noexcept;

    /// Get offsets at the given snapshot offset.
    /// \param snapshot_at Offset at which the snapshot is taken.
    /// \return ctp_stm_offsets containing offsets added at or before
    /// snapshot_at.
    /// \invariant The offsets are sorted by their changed_at offset.
    /// \invariant The changed_at field of the offset_change is always less than
    /// or
    ///            equal to snapshot_at.
    /// \note This method is used to create a snapshot of the ctp_stm_state
    ///       for Raft snapshotting mechanism.
    ctp_stm_offsets get_offsets_at(model::offset snapshot_at) const noexcept;

    auto serde_fields() { return std::tie(_offsets); }

private:
    chunked_circular_buffer<offset_change> _offsets;
};

/// In-memory state of the cloud-topics state machine (ctp_stm).
///
/// The state machine tracks the current cluster epoch and
/// the offsets at which the epoch was added first. It also tracks
/// the last reconciled offset, insync offset and applied offset.
/// This allows us to compute the smallest cluster epoch being used
/// by the partition. This is used by the GC algorithm to determine
/// which epochs can be removed from the cloud storage.
///
/// The epoch values are propagated from the L0 metadata batches
/// and are used to ensure that the cluster epoch is monotonic.
class ctp_stm_state
  : public serde::
      envelope<ctp_stm_state, serde::version<0>, serde::compat_version<0>> {
    friend class ctp_stm_state_accessor;

public:
    ctp_stm_state() = default;

    /// Register new epoch with the state.
    /// \param epoch Cluster epoch value.
    /// \param first_epoch_offset Kafka offset of the first record batch in the
    ///        epoch.
    /// \return true if the epoch was added successfully, false if the epoch is
    ///         not monotonic (i.e. the epoch is less than the last registered
    ///         epoch).
    bool add_epoch(cluster_epoch epoch, kafka::offset first_epoch_offset);

    /// Find the maximum cluster epoch registered in the state.
    std::optional<cluster_epoch> get_max_epoch() const noexcept;

    std::optional<cluster_epoch> get_projected_epoch() const noexcept;

    /// This is invoked in the write path before the batch with new
    /// epoch value is even replicated.
    void advance_projected_epoch(cluster_epoch epoch) noexcept;

    void advance_insync_offset(model::offset new_insync_offset) noexcept;

    void advance_applied_offset() noexcept;

    void advance_last_reconciled_offset(
      kafka::offset new_last_reconciled_offset) noexcept;

    /// Get last reconciled offset value
    kafka::offset get_last_reconciled_offset() const noexcept;

    model::offset get_insync_offset() const noexcept;

    model::offset get_applied_offset() const noexcept;

    std::optional<cluster_epoch> find_first_epoch() const noexcept;

    bool can_apply(model::offset base_offset) const noexcept;

    auto serde_fields() {
        return std::tie(
          _projected_epoch, _insync_offset, _applied_offset, _epochs, _offsets);
    }

    /// Create snapshot of the ctp_stm_state for Raft snapshotting mechanism.
    /// The snapshot is just a copy of the ctp_stm_state that contains all
    /// changes introduced at offset 'snapshot_at' and earlier.
    ctp_stm_state get_state_at(model::offset snapshot_at) const noexcept;

    /// Max collectible offset is defined by the LRO.
    ///
    /// If the LRO is not set then the max collectible offset is defined
    /// by the changed_at offset of the first epoch.
    /// If the LRO is set then the max collectible offset is defined
    /// by the changed_at offset of the first epoch that has
    /// not been fully reconciled.
    /// This means that the L0 metadata batch can only be evicted from the
    /// local log if the epoch it references is fully reconciled.
    model::offset get_max_collectible_offset() const noexcept;

    /// Truncate the prefix of the epochs list to the given log offset.
    void truncate_to(model::offset epoch_offset) noexcept;

private:
    bool check_invariant() { return _insync_offset >= _applied_offset; }

    /// The last seen epoch which may not be added to the _epochs list yet.
    std::optional<cluster_epoch> _projected_epoch;

    model::offset _insync_offset;
    model::offset _applied_offset;

    ctp_stm_epochs _epochs;
    ctp_stm_offsets _offsets;
};

}; // namespace experimental::cloud_topics
