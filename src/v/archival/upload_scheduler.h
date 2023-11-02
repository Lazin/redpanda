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
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

namespace archival {

enum class poll_result_t {
    done,
    failed,
    in_progress,
};

using timestamp_t = ss::lowres_clock::time_point;
using duration_t = ss::lowres_clock::duration;

class scheduler;
class upload_queue;

using invoke_on_poll_t
  = ss::noncopyable_function<void(timestamp_t, scheduler&, upload_queue&)>;

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
    /// The method accepts references to scheduler and upload queue (TBD add
    /// replication queue) so it could potentially schedule new activities
    /// (start uploads, replicate batches).
    ///
    /// Invariant: the method can only be called after 'start' method.
    virtual poll_result_t poll(timestamp_t, scheduler&, upload_queue&) = 0;

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

protected:
    /// Wait until the action is completed
    ss::future<> wait_for(pollable_action& actie) {
        vassert(
          !actie._on_complete.has_value(), "Action is already being waited");
        actie._on_complete.emplace();
        return actie._on_complete.value().get_future();
    }

    /// The method is waiting until the 'poll' method of the pollable_action is
    /// invoked and calls the supplied functor.
    ss::future<> invoke_on_poll(invoke_on_poll_t functor) {
        _pre_poll_queue.emplace_back();
        _pre_poll_queue.back().callback = std::move(functor);
        return _pre_poll_queue.back().p.get_future();
    }

private:
    friend class scheduler;
    friend class upload_queue;

    /// This method is invoked by the scheduler
    poll_result_t do_poll(timestamp_t t, scheduler& s, upload_queue& q) {
        while (!_pre_poll_queue.empty()) {
            auto& item = _pre_poll_queue.front();
            try {
                item.callback(t, s, q);
                item.p.set_value();
            } catch (...) {
                item.p.set_exception(std::current_exception());
            }
            _pre_poll_queue.pop_front();
        }
        return poll(t, s, q);
    }

    void set_exception(std::exception_ptr e) { _err = e; }

    void on_complete() {
        if (_on_complete.has_value()) {
            _on_complete->set_value();
            _on_complete.reset();
        }
    }

    //
    // State managed by the scheduler
    intrusive_list_hook _hook;
    ssx::semaphore_units _units;

    //
    // Execution state

    // Last error
    std::exception_ptr _err;

    struct pre_poll_action_t {
        ss::promise<> p;
        invoke_on_poll_t callback;
    };

    // The promise is set on completion. It's used to wait
    // for the completion of the action from another action.
    std::optional<ss::promise<>> _on_complete;

    // List of actions to invoke on next 'poll' call
    ss::circular_buffer<pre_poll_action_t> _pre_poll_queue;
};

class scheduler {
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

    void poll(timestamp_t t, upload_queue& queue) {
        std::vector<pollable_action*> completed;
        for (auto& a : _queue) {
            if (a.do_poll(t, *this, queue) == poll_result_t::done) {
                try {
                    a.complete();
                } catch (...) {
                    a.set_exception(std::current_exception());
                }
                completed.push_back(&a);
            }
        }
        for (auto a: completed) {
            a->on_complete();
            a->_units.return_all();
            a->_hook.unlink();
        }
    }

private:
    intrusive_list<pollable_action, &pollable_action::_hook> _queue;
    ssx::semaphore _queue_limit{10, "archival-scheduler"};
    ss::abort_source _as;
};

class upload_queue {
public:
    // Upload segment with the index
    virtual std::unique_ptr<pollable_action>
    upload_segment(cloud_storage::remote_segment_path path) = 0;

    // Upload manifest (partition/topic/tx)
    virtual std::unique_ptr<pollable_action>
    upload_manifest(cloud_storage::remote_manifest_path path) = 0;
};

/*
class ntp_upload_cycle : public pollable_action {
public:
    ntp_upload_cycle(ss::lw_shared_ptr<cluster::partition> partition)
      : _partition(std::move(partition)) {}
    void start() override {}

    poll_result_t
    poll(timestamp_t t, scheduler& sch, upload_queue& queue) override {
        if (_err) {
            return poll_result_t::failed;
        }
        if (!_fut.has_value()) {
            return poll_result_t::done;
        }
        return _fut->available() ? poll_result_t::done
                                 : poll_result_t::in_progress;
    }

    void abort() override { _as.request_abort(); }

    void complete() override {
        vassert(_fut->available(), "Action is not ready");
        try {
            _fut->get();
        } catch (...) {
            auto e = std::current_exception();
            if (!ssx::is_shutdown_exception(e)) {
                _err = e;
            }
        }
        _fut.reset();
    }

private:
    ss::future<> run_once() {
        // 1. discover uploads
        co_await ss::sleep_abortable(1s, _as);
        // 2. schedule uploads
        co_await ss::sleep_abortable(1s, _as);
        // 3. replicate changes
        co_await ss::sleep_abortable(1s, _as);
    }

    ss::lw_shared_ptr<cluster::partition> _partition;
    std::optional<ss::future<>> _fut;
    ss::abort_source _as;
};
*/

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
