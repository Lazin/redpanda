/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/async_data_uploader.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "ssx/semaphore.h"
#include "utils/fragmented_vector.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace archival {

/**
 * This component implements an execution model for tiered-storage. It requires
 * the developer to define all actions and interactions between them explicitly.
 * It also uses different approach to error handling. Every Redpanda subsystem
 * can only be used through its interface. The interface is an object that
 * defines what operations are used by us. This way we can define api boundaries
 * and express various contracts.
 *
 * Goals:
 * - enable fast simulation/testing without use of fixtures
 * - clearly define api boundaries between different subsystems used by
 *   tiered-storage
 * - control execution flow explicitly (create an object for every action)
 * - simplify graceful shutdown by providing low-level abort operation for every
 *   action
 * - control resource usage explicitly by per-action resource quotas
 * - handle errors per action explicitly (the error can't be ignored)
 * - improve observability
 *
 * Currently, it's only used on the upload side of things but it can also be
 * used to run the read side.
 */

enum class poll_result_t {
    /// Not started / invalid result
    none,
    /// Action completed
    done,
    /// Action failed
    failed,
    /// Action is in progress
    in_progress,
};

using timestamp_t = ss::lowres_clock::time_point;
using duration_t = ss::lowres_clock::duration;

class execution_queue;
class cloud_storage_api_iface;
class local_storage_api_iface;
class partition_iface;

/// Created per I/O action
class pollable_action {
public:
    virtual ~pollable_action() {
        vassert(!static_cast<bool>(_err), "Exception ignored {}", _err);
    }

    /// Start running the action
    virtual void start() = 0;

    /// Poll for status
    ///
    /// The method returns current status of the action. It may also
    /// do the actual work (if it's not long). The provided timestamp
    /// should be used for all time-based computations inside the action.
    /// The poller updates current time of the action by polling it
    /// periodically.
    ///
    /// The method accepts references to scheduler and several interfaces.
    /// The action can schedule new activities (start uploads, replicate
    /// batches) using these interfaces.
    ///
    /// Invariant: the method can only be called after 'start' method.
    virtual poll_result_t poll(
      timestamp_t,
      execution_queue&,
      local_storage_api_iface&,
      cloud_storage_api_iface&,
      partition_iface&)
      = 0;

    /// Cancel asynchronous operation unconditionally
    virtual void abort() = 0;

    /// The method is called by the scheduler after 'poll' returned
    /// done or failed. If the error is detected during the call the
    /// action transitions to 'failed' state and 'error' returns the
    /// exception pointer.
    ///
    /// Invariant: the method can only be called after 'poll' returned
    /// 'done' or 'failed'.
    virtual void complete() = 0;

    /// Return true if action has failed
    bool has_failed() const noexcept { return static_cast<bool>(_err); }

    /// Returns null if the action is not failed or valid exception_ptr
    /// in case of error.
    [[nodiscard]] std::exception_ptr get_exception() {
        auto tmp = _err;
        _err = nullptr;
        return tmp;
    }

    poll_result_t last_poll_result() const { return _last_poll_result; }

private:
    friend class execution_queue;
    friend class cloud_storage_api_iface;
    friend class local_storage_api_iface;
    friend class partition_iface;

    void set_exception(std::exception_ptr e) { _err = e; }

    //
    // State managed by the scheduler

    /// Execution queue list hook
    intrusive_list_hook _hook;

    /// Execution queue units consumed by the action
    ssx::semaphore_units _units;

    /// Result of the last poll operation (managed by the execution queue)
    poll_result_t _last_poll_result;

    // Last error
    std::exception_ptr _err;
};

/// The queue holds all actions together. It's a single point that
/// can be used to control resources used by all actions.
class execution_queue {
public:
    bool schedule(pollable_action* action) {
        auto u = ss::try_get_units(_queue_limit, 1);
        if (u.has_value()) {
            _queue.push_back(*action);
            action->start();
            action->_units = std::move(u.value());
            return true;
        }
        return false;
    }

    void poll(
      timestamp_t t,
      local_storage_api_iface& local_api,
      cloud_storage_api_iface& cloud_api,
      partition_iface& replicated_stm) {
        std::vector<pollable_action*> completed;
        std::vector<pollable_action*> failed;
        for (auto& a : _queue) {
            try {
                auto res = a.poll(
                  t, *this, local_api, cloud_api, replicated_stm);
                if (res == poll_result_t::done) {
                    completed.push_back(&a);
                }
                a._last_poll_result = res;
            } catch (...) {
                a.set_exception(std::current_exception());
                a._last_poll_result = poll_result_t::failed;
                failed.push_back(&a);
            }
        }
        for (auto a : completed) {
            try {
                a->complete();
            } catch (...) {
                // The action is supposed to be disposed after this point so it
                // doesn't make much sense to call 'set_exception'
                vlog(
                  archival_log.warn,
                  "Exception thrown during finalization of the completed "
                  "pollable_action: {}",
                  std::current_exception());
            }
            a->_units.return_all();
            a->_hook.unlink();
        }
        for (auto a : completed) {
            try {
                a->complete();
            } catch (...) {
                // We can't do anything with this exception at this point
                // other than log it. The 'complete' implementation has
                // to invoke 'has_failed' if it needs to skip in case of
                // failure.
                vlog(
                  archival_log.warn,
                  "Exception thrown during finalization of the failed "
                  "pollable_action: {}",
                  std::current_exception());
            }
            a->_units.return_all();
            a->_hook.unlink();
        }
    }

private:
    intrusive_list<pollable_action, &pollable_action::_hook> _queue;
    ssx::semaphore _queue_limit{10, "archival-scheduler"};
    ss::abort_source _as;
};

/// Upload that adds new offset range.
/// This upload is supposed to advance start offset.
struct append_offset_range_upload {
    // set to false if there is not enough data to start new upload
    bool found;
    // upload metadata
    cloud_storage::segment_meta meta;
    // upload body
    ss::input_stream<char> data;
    // upload size
    size_t size;
};

/// Find upload candidate that advances the log forward by
/// appending new offset range.
struct find_next_append_uploads_action : public pollable_action {
    // the following methods can only be called when the action is completed
    // and not failed

    /// Return found upload candidate
    virtual std::optional<append_offset_range_upload> get_next() = 0;
};

class local_storage_api_iface {
public:
    local_storage_api_iface() = default;
    local_storage_api_iface(const local_storage_api_iface&) = delete;
    local_storage_api_iface(local_storage_api_iface&&) noexcept = delete;
    local_storage_api_iface& operator=(const local_storage_api_iface&) = delete;
    local_storage_api_iface& operator=(local_storage_api_iface&&) noexcept
      = delete;
    virtual ~local_storage_api_iface() = default;

public:
    /// Find new upload candidate or series of candidates
    ///
    /// \param start_offset is a base offset of the next uploaded segment
    /// \param min_size is a smallest segment size allowed to be uploaded
    /// \param max_size is a largest segment size
    /// \param max_concurrency max number of candidates to return
    virtual std::unique_ptr<find_next_append_uploads_action>
    find_next_append_upload_candidates(
      execution_queue& queue,
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency)
      = 0;

    // TODO: add missing actions
};

/// Segment upload action generated by the
/// upload interface.
struct segment_upload_action : public pollable_action {
    // NOTE: the idea is to provide some
    // additional interface here
};

/// Manifest upload action generated by the upload
/// interface.
struct manifest_upload_action : public pollable_action {};

/// Wrapper around the 'cloud_storage::remote'. It creates
/// an extra level of indirection so we could mock the cloud
/// storage api more easily without using complex fixtures.
class cloud_storage_api_iface {
public:
    cloud_storage_api_iface() = default;
    cloud_storage_api_iface(const cloud_storage_api_iface&) = delete;
    cloud_storage_api_iface(cloud_storage_api_iface&&) noexcept = delete;
    cloud_storage_api_iface& operator=(const cloud_storage_api_iface&) = delete;
    cloud_storage_api_iface& operator=(cloud_storage_api_iface&&) noexcept
      = delete;
    virtual ~cloud_storage_api_iface() = default;

public:
    // Upload segment with the index
    virtual std::unique_ptr<segment_upload_action> upload_segment(
      execution_queue& queue,
      cloud_storage::remote_segment_path path,
      ss::input_stream<char> data,
      size_t expected_size)
      = 0;

    // Upload manifest (partition/topic/tx)
    virtual std::unique_ptr<manifest_upload_action>
    upload_manifest(cloud_storage::remote_manifest_path path) = 0;
};

/// Adds new segment to the STM and advance start offset
struct add_new_segment_action : pollable_action {};

/// Manages interactions between the upload path and STM
class partition_iface {
public:
    partition_iface() = default;
    virtual ~partition_iface() = default;
    partition_iface(const partition_iface&) = delete;
    partition_iface(partition_iface&&) noexcept = delete;
    partition_iface& operator=(const partition_iface&) = delete;
    partition_iface& operator=(partition_iface&&) noexcept = delete;

public:
    virtual std::unique_ptr<add_new_segment_action>
      add_new_segment(cloud_storage::segment_meta) = 0;

    // TODO: add other ops, replacements, retention, spillover, etc
};

/*
    ss::future<cloud_storage::segment_meta>
    run_e2e(log_upload_request request, cloud_storage::remote& api) {
        retry_chain_node rtc(_as);
        retry_chain_logger rtclog(archival_log, rtc,
   request.partition->get_ntp_config().ntp().path());

        auto guard = _gate->hold();

        const auto& manifest
          = request.partition->archival_meta_stm()->manifest();

        // 1. Make the segment_upload instance
        auto upl = co_await segment_upload::make_segment_upload(
          request.partition,
          request.range,
          0x1000,
          ss::default_scheduling_group(),
          request.deadline);

        // 2. Compute segment_meta
        auto ot_state = request.partition->get_offset_translator_state();
        auto upl_meta = upl.value()->get_meta();
        cloud_storage::segment_meta meta{};
        meta.is_compacted = upl_meta.is_compacted;
        meta.size_bytes = upl_meta.size_bytes;
        meta.base_offset = upl_meta.offsets.base;
        meta.committed_offset = upl_meta.offsets.last;
        meta.base_timestamp = upl_meta.first_data_timestamp;
        meta.max_timestamp = upl_meta.last_data_timestamp;
        meta.delta_offset = meta.base_offset
                            - model::offset_cast(
                              ot_state->from_log_offset(meta.base_offset));
        meta.ntp_revision
          = request.partition->get_ntp_config().get_initial_revision();
        meta.archiver_term = request.partition->raft()->term();
        meta.segment_term = request.partition->get_term(meta.base_offset);
        meta.delta_offset_end = ot_state->next_offset_delta(
          meta.committed_offset);
        meta.sname_format = cloud_storage::segment_name_format::v3;
        meta.metadata_size_hint = 0;

        // 3. Get transactions
        std::optional<fragmented_vector<model::tx_range>> tx;
        // Compacted segments never have tx-manifests because all batches
        // generated by aborted transactions are removed during compaction.
        ss::log_level level{};
        std::exception_ptr ep;
        if (meta.is_compacted == false) {
            tx = co_await get_aborted_transactions(request, upl_meta);
            meta.metadata_size_hint = tx->size();
        }

        // 4. Compute segment path
        auto path = manifest.generate_segment_path(meta);

        // 5. Start uploads
    }
*/
// V2
/*
struct generate_uploads : pollable_action {

    void start(cloud_storage::remote& api, ss::lw_shared_ptr<pollable_queue>
queue) override { _action = run(api, queue);
    }

    io_status_t status() override {
        if (!_action.has_value()) {
            return io_status_t::none;
        }
        if (_action->available()) {
            return io_status_t::done;
        }
        return io_status_t::in_progress;
    }

    void complete() override {
        if (_action->failed()) {
            auto eptr = _action->get_exception();
            try {
                std::rethrow_exception(eptr);
            } catch (const std::system_error& se) {
                // TODO: add logging
            } catch (...) {
                // TODO: add logging
                return make_error_code(error_outcome::unexpected_failure);
            }
        } else {
            auto result = _action->get();
            _gate.reset();
            _current.reset();
            return completed_segment_upload{
              .meta = result,
            };
        }
        __builtin_unreachable();
    }

    void abort() override {}
private:
    ss::future<>
    run(cloud_storage::remote& api, ss::lw_shared_ptr<pollable_queue> queue) {
        auto guard = _gate.hold();
        retry_chain_node rtc(_as);
        retry_chain_logger rtclog(archival_log, rtc,
_partition->get_ntp_config().ntp().path());

        const auto& manifest
          = _partition->archival_meta_stm()->manifest();

        auto last_offset = manifest.get_last_offset();

        // TODO: check case when partition starts not from 0 but last_offset has
default value size_limited_offset_range range(model::next_offset(last_offset),
_target_segment_size, _min_segment_size);

        // 1. Make the segment_upload instance
        auto upl = co_await segment_upload::make_segment_upload(
          request.partition,
          range,
          0x1000,
          ss::default_scheduling_group(),
          request.deadline);

        // 2. Compute segment_meta
        auto ot_state = request.partition->get_offset_translator_state();
        auto upl_meta = upl.value()->get_meta();
        cloud_storage::segment_meta meta{};
        meta.is_compacted = upl_meta.is_compacted;
        meta.size_bytes = upl_meta.size_bytes;
        meta.base_offset = upl_meta.offsets.base;
        meta.committed_offset = upl_meta.offsets.last;
        meta.base_timestamp = upl_meta.first_data_timestamp;
        meta.max_timestamp = upl_meta.last_data_timestamp;
        meta.delta_offset = meta.base_offset
                            - model::offset_cast(
                              ot_state->from_log_offset(meta.base_offset));
        meta.ntp_revision
          = request.partition->get_ntp_config().get_initial_revision();
        meta.archiver_term = request.partition->raft()->term();
        meta.segment_term = request.partition->get_term(meta.base_offset);
        meta.delta_offset_end = ot_state->next_offset_delta(
          meta.committed_offset);
        meta.sname_format = cloud_storage::segment_name_format::v3;
        meta.metadata_size_hint = 0;

        // TODO: create and push new action

        last_offset = meta.committed_offset;
    }

    size_t _target_segment_size;
    size_t _min_segment_size;
    // TODO: add optional upload timeout
    ss::abort_source _as;
    ss::gate _gate;
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<pollable_queue> _queue;
    std::optional<ss::future<>> _action;
};
*/

} // namespace archival
