/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/async_data_uploader.h"
#include "archival/tests/upload_scheduler_mocks.h"
#include "archival/upload_scheduler.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "ssx/sformat.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "test_utils/fixture.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/later.hh>

#include <boost/test/tools/old/interface.hpp>

#include <exception>
#include <memory>
#include <optional>
#include <variant>

using namespace std::chrono_literals;
using namespace archival;

/**
 * The segment upload workflow:
 *
 * pre-upload:
 *    start-offset     ---> [s1] [s2] [s3] [s4]
 * upload:
 *    upload segments  ---> [u1] [u2] [u3] [u4] in-parallel
 *    upload index     ---> [i1] [i2] [i3] [i4] in-parallel
 *    upload tx        ---> [t1] [t2] [t3] [t4] in-parallel
 *    upload manifest  ---> [manifest]          in-parallel (if dirty)
 * handle upload failures:
 *    errors           ---> [  ] [  ] [  ] [e4]
 *    truncate         ---> [s1] [s2] [s3] ....
 * replicate:
 *     add segments    ---> [s1] [s2] [s3]
 *
 * by timer:
 *     retention
 *     spillover
 *     manifest upload
 */

/// Represent single upload loop cycle. Contains a set of
/// discovered upload candidates.
struct pre_upload_vec {
    std::vector<cloud_storage::segment_meta> meta;
    std::vector<ss::input_stream<char>> data;
    std::vector<size_t> size;
    std::vector<cloud_storage::remote_segment_path> path;
};

/// Contains a set of upload actions with the corresponding metadata.
/// It's supposed to be created based on pre_upload_vec.
struct upload_vec {
    std::vector<std::unique_ptr<segment_upload_action>> upload;
    std::vector<cloud_storage::segment_meta> meta;
    // TODO: add actions for index/tx/manifest
};

struct upload_error_vec {
    std::vector<cloud_storage::segment_meta> meta;
    std::vector<std::exception_ptr> err;

    std::optional<size_t> first_error_ix() const {
        for (size_t i = 0; i < err.size(); i++) {
            if (err.at(i) != nullptr) {
                return i;
            }
        }
        return std::nullopt;
    }
};

/// Wait until all actions are completed
///
/// \return vector of all poll results or nullopt if something is in-progress
template<class Action>
[[nodiscard]] std::optional<std::vector<poll_result_t>>
wait_all(std::vector<std::unique_ptr<Action>>& actions) {
    std::vector<poll_result_t> results;
    results.resize(actions.size());
    for (size_t i = 0; i < actions.size(); i++) {
        const auto& action = actions.at(i);
        auto r = action->last_poll_result();
        switch (r) {
        case poll_result_t::none:
        case poll_result_t::in_progress:
            return std::nullopt;
        case poll_result_t::failed:
        case poll_result_t::done:
            results.at(i) = r;
        }
    }
    return results;
}

[[nodiscard]] std::optional<upload_error_vec> wait_all(upload_vec& upl) {
    auto res = wait_all(upl.upload);
    if (!res.has_value()) {
        return std::nullopt;
    }
    upload_error_vec out;
    for (size_t i = 0; i < upl.upload.size(); i++) {
        const auto& u = upl.upload.at(i);
        const auto& m = upl.meta.at(i);
        if (u->has_failed()) {
            out.err.push_back(u->get_exception());
            out.meta.push_back(m);
        } else {
            out.err.emplace_back(nullptr);
            out.meta.push_back(m);
        }
    }
    return out;
}

/*
 * State diagram:
 *              ┌───────┐
 *      ┌──────►│init   │◄────────────┐
 *      │       └───┬───┘             │
 *      │           │                 │
 *      │           │                 │
 *      │           ▼                 │
 *      │       ┌───────┐no data  ┌───┴────┐not leader  ┌───────┐
 *      │       │find   ├────────►│backoff ├──────────► │done   │
 *      │       └───┬───┘failure  └────────┘            └───────┘
 *      │           │                 ▲
 *      │           │                 │
 *      │           ▼                 │
 *      │       ┌───────┐failure      │
 *      │       │upload ├─────────────┤
 *      │       └───┬───┘             │
 *      │           │                 │
 *      │           │                 │
 *      │           ▼                 │
 *      │       ┌───────┐             │
 *      └───────┤stm add│             │
 *      success └───┬───┘             │
 *                  │   failure       │
 *                  └─────────────────┘
 *
 * States:
 * - `init` is a beginning of the new iteration.
 * - `find` the loop is waiting for new upload candidates to be found
 * - `upload` the loop is waiting for uploads to be completed (includes
 * tx/index/manifest)
 * - `stm add` waiting until the metadata is replicated and applied
 * - `backoff` waiting for exponential backoff
 * - `done` is a terminal state
 */
class ntp_upload_loop : public pollable_action {
    struct initial_st;
    struct find_upload_st;
    struct do_upload_st;
    struct backoff_st;
    struct done_st;

    using state_t = std::
      variant<initial_st, find_upload_st, do_upload_st, backoff_st, done_st>;
    struct backoff_st {
        std::optional<state_t> operator()(
          timestamp_t now,
          execution_queue& q,
          local_storage_api_iface& ls,
          cloud_storage_api_iface& cs,
          partition_iface& stm) {
            if (now >= deadline) {
                return initial_st{};
            }
            return std::nullopt;
        }
        static constexpr poll_result_t result{poll_result_t::failed};
        timestamp_t deadline;
        duration_t backoff;
    };
    struct done_st {
        template<class... T>
        std::optional<state_t> operator()(T&&...) {
            return std::nullopt;
        }
        static constexpr poll_result_t result{poll_result_t::done};
    };
    struct do_upload_st {
        upload_vec vec;

        std::optional<state_t> operator()(
          timestamp_t t,
          execution_queue& q,
          local_storage_api_iface& ls,
          cloud_storage_api_iface& cs,
          partition_iface&) {
            auto res = wait_all(vec);
            if (res.has_value()) {
                // done or failed
                vassert(res.value().meta.size() > 0, "Empty upload vector");
                auto err_ix = res->first_error_ix();
                // We have at least one success so we can proceed.
                // But we need to process all errors first.
                for (size_t i = 0; i < res->err.size(); i++) {
                    auto e = res->err.at(i);
                    auto m = res->meta.at(i);
                    if (e) {
                        vlog(
                          test_log.error,
                          "Failed to upload {}, error: {}",
                          m,
                          e);
                    }
                }
                // It's safe to resize uploads list because all uploads are
                // completed at this point.
                vec.upload.resize(err_ix.value_or(vec.upload.size()));
                vec.meta.resize(err_ix.value_or(vec.upload.size()));

                if (vec.upload.empty()) {
                    vlog(test_log.error, "All uploads failed");
                    return backoff_st{
                      .deadline = t + 100ms, .backoff = 100ms,
                      // TODO: use exponential backoff, relevant
                      // config: upload_loop_max_backoff, initial
                      // backoff 100ms
                    };
                }
                // TODO: implement remaining states, for now cycle to done_st
                return done_st{};
            }
            return std::nullopt;
        }
        static constexpr poll_result_t result{poll_result_t::in_progress};
    };
    struct find_upload_st {
        // controls upload concurrency
        static constexpr size_t max_parallel_segment_uploads = 4;
        std::optional<state_t> operator()(
          timestamp_t t,
          execution_queue& q,
          local_storage_api_iface& ls,
          cloud_storage_api_iface& cs,
          partition_iface& stm) {
            auto r = action->last_poll_result(); // poll(t, q, ls, cs, stm);
            // TODO: here it can find candidates or not
            switch (r) {
            case poll_result_t::none:
            case poll_result_t::in_progress:
                return std::nullopt;
            case poll_result_t::failed:
                // TODO: use context logger
                vlog(
                  test_log.error,
                  "Failed to find upload candidate: {}",
                  action->get_exception());
                return backoff_st{
                  .deadline = t + prev_backoff * 2,
                  .backoff = prev_backoff * 2,
                };
            case poll_result_t::done: {
                vlog(test_log.debug, "find_upload_st find candidate completed");
                std::vector<std::unique_ptr<segment_upload_action>> uploads;
                std::vector<cloud_storage::segment_meta> meta;
                while (auto c = action->get_next()) {
                    auto us = cs.upload_segment(
                      q,
                      cloud_storage::remote_segment_path("foo/bar/buzz"),
                      std::move(c->data),
                      c->size);
                    meta.push_back(c->meta);
                    uploads.push_back(std::move(us));
                }
                if (uploads.empty()) {
                    // Normal backoff path
                    vlog(
                      test_log.debug,
                      "Can't find upload candidate: {}",
                      action->get_exception());
                    return backoff_st{
                      .deadline = t + prev_backoff * 2,
                      .backoff = prev_backoff * 2,
                    };
                }
                upload_vec vec{
                  .upload = std::move(uploads),
                  .meta = std::move(meta),
                };
                return do_upload_st{.vec = std::move(vec)};
            }
            }
        }

        static constexpr poll_result_t result{poll_result_t::in_progress};
        std::unique_ptr<find_next_append_uploads_action> action;
        ss::lowres_clock::duration prev_backoff;
    };
    struct initial_st {
        std::optional<state_t> operator()(
          timestamp_t t,
          execution_queue& q,
          local_storage_api_iface& ls,
          cloud_storage_api_iface& cs,
          partition_iface& part) {
            // TODO: get parameters from the partition and configuration
            model::offset s;
            const size_t min_size = 1000;
            const size_t target_size = 2000;
            const size_t max_parallel = 4;
            auto c = ls.find_next_append_upload_candidates(
              q, s, min_size, target_size, max_parallel);

            // TODO: propagate backoff
            return find_upload_st{
              .action = std::move(c), .prev_backoff = 100ms};
        }
        // TODO: add backoff
        static constexpr poll_result_t result{poll_result_t::in_progress};
    };

public:
    void start() override { vlog(test_log.info, "ntp_upload_strand start"); }

    poll_result_t poll(
      timestamp_t t,
      execution_queue& queue,
      local_storage_api_iface& local,
      cloud_storage_api_iface& cloud,
      partition_iface& stm) override {
        auto r = std::visit(
          [t, &queue, &local, &cloud, &stm](auto&& st) {
              return st(t, queue, local, cloud, stm);
          },
          state);
        if (r.has_value()) {
            state = std::move(r.value());
        }
        return std::visit([](auto&& st) { return st.result; }, state);
    }

    void abort() override {}

    void complete() override {
        vlog(test_log.info, "ntp_upload_strand complete");
    }

private:
    state_t state;
};

SEASTAR_THREAD_TEST_CASE(test_archiver_1) {
    vlog(test_log.info, "Start");
    execution_queue queue;
    mock_local_storage_api local_api;
    mock_cloud_storage_api cloud_api;
    mock_archival_metadata_stm replicated_stm;
    segment_upload_mock d(
      cloud_storage::remote_segment_path("/foo/bar"), false);

    queue.schedule(&d);
    auto t = ss::lowres_clock::now();
    const auto time_delta = 100ms;
    BOOST_REQUIRE(
      d.poll(t, queue, local_api, cloud_api, replicated_stm)
      == poll_result_t::in_progress);

    while (d.poll(t, queue, local_api, cloud_api, replicated_stm)
           == poll_result_t::in_progress) {
        t += time_delta;
        queue.poll(t, local_api, cloud_api, replicated_stm);
    }

    BOOST_REQUIRE(
      d.poll(t, queue, local_api, cloud_api, replicated_stm)
      == poll_result_t::done);

    vlog(test_log.info, "Done");
}

SEASTAR_THREAD_TEST_CASE(test_archiver_2) {
    vlog(test_log.info, "Start");
    execution_queue queue;
    mock_local_storage_api local_api;
    mock_cloud_storage_api cloud_api;
    mock_archival_metadata_stm replicated_stm;
    ntp_upload_loop d;

    queue.schedule(&d);
    auto t = ss::lowres_clock::now();
    BOOST_REQUIRE(
      d.poll(t, queue, local_api, cloud_api, replicated_stm)
      == poll_result_t::in_progress);

    while (d.poll(t, queue, local_api, cloud_api, replicated_stm)
           == poll_result_t::in_progress) {
        t = t + 1s;
        queue.poll(t, local_api, cloud_api, replicated_stm);
        ss::maybe_yield().get();
    }

    BOOST_REQUIRE(
      d.poll(t, queue, local_api, cloud_api, replicated_stm)
      == poll_result_t::done);
    vlog(test_log.info, "Done");
}

/*
class local_storage_api_iface_v2 {
public:
    local_storage_api_iface_v2() = default;
    local_storage_api_iface_v2(const local_storage_api_iface_v2&) = delete;
    local_storage_api_iface_v2(local_storage_api_iface_v2&&) noexcept = delete;
    local_storage_api_iface_v2& operator=(const local_storage_api_iface_v2&)
      = delete;
    local_storage_api_iface_v2& operator=(local_storage_api_iface_v2&&) noexcept
      = delete;
    virtual ~local_storage_api_iface_v2() = default;

public:
    /// Find new upload candidate or series of candidates
    ///
    /// \param start_offset is a base offset of the next uploaded segment
    /// \param min_size is a smallest segment size allowed to be uploaded
    /// \param max_size is a largest segment size
    /// \param max_concurrency max number of candidates to return
    virtual ss::future<std::vector<append_offset_range_upload>>
    find_next_append_upload_candidates(
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency)
      = 0;
};

/// The upload path should only interact with the rest
/// of Redpanda and platform through this context.
class upload_loop_context {
public:
    upload_loop_context(
      local_storage_api_iface_v2& ls,
      cloud_storage_api_iface& cs,
      partition_iface& part)
      : _local(ls)
      , _cloud(cs)
      , _part(part) {}

    upload_loop_context(const upload_loop_context&) = delete;
    upload_loop_context(upload_loop_context&&) noexcept = delete;
    upload_loop_context& operator=(const upload_loop_context&) = delete;
    upload_loop_context& operator=(upload_loop_context&&) noexcept = delete;
    ~upload_loop_context() = default;

    timestamp_t now() {
        if (unlikely(_now_override.has_value())) {
            return _now_override.value();
        }
        return ss::lowres_clock::now();
    }

    local_storage_api_iface_v2& local_api() { return _local.get(); }
    cloud_storage_api_iface& cloud_api() { return _cloud.get(); }
    partition_iface& partition_api() { return _part.get(); }

    // TODO: hide this using friend accessor hack
    /// Set custom timestamp for tests. Method now
    /// will only return this value from now on. It's
    /// only possible to set it to different value but
    /// not to reset it.
    void test_set_timestamp(timestamp_t t) {
        if (_now_override.has_value()) {
            vassert(
              t >= _now_override.value(),
              "Fake timestamp can only increase monotonically, new value {}, "
              "prev. value {}",
              t,
              _now_override.value());
        }
        _now_override = t;
    }

private:
    std::optional<timestamp_t> _now_override;
    std::reference_wrapper<local_storage_api_iface_v2> _local;
    std::reference_wrapper<cloud_storage_api_iface> _cloud;
    std::reference_wrapper<partition_iface> _part;
};

struct st_suspended;
struct st_reconciling;

using upload_loop_state_t = std::variant<std::unique_ptr<st_suspended>,
std::unique_ptr<st_reconciling>>;

struct st_reconciling {
    std::optional<upload_loop_state_t> operator()(upload_loop_context& ctx) {
        if (!_started_op.has_value()) {
          // Operation is not started yet. This is the first invocation.
          // We need to start searching for the result;
            model::offset
              so; //=
                  //model::next_offset(ctx.partition_api().manifest().last_uploaded_offset());
            const size_t min_size = 1000;
            const size_t target_size = 2000;
            const size_t max_parallel = 4;
            auto fut = ctx.local_api().find_next_append_upload_candidates(
              so, min_size, target_size, max_parallel);
            _started_op = std::move(fut);
        }
        vassert(_started_op.has_value(), "Operation is not started");
        if (_started_op.value().available()) {
          // Transition to the next state. It's either suspended state if we
didn't
          // find anything or upload state if we did.
          // TODO:
        }
        return std::nullopt;
    }

    std::optional<ss::future<std::vector<append_offset_range_upload>>>
_started_op;
};

struct st_suspended {
    std::optional<upload_loop_state_t> operator()(timestamp_t t) {
        // TODO: add all the token bucket stuff here
        if (t >= _next) {
            return st_reconciling{};
        }
        return std::nullopt;
    }

    timestamp_t _next;
    intrusive_list_hook _hook;
};

class upload_scheduler {
public:
    void run_once(timestamp_t ts) {
        for (auto it = _suspended.begin(); it != _suspended.end(); it++) {
            auto& s = *it;
            auto ns = s(ts);
            it = s._hook.next_;
            s._hook.unlink();
        }
    }

private:
    void link(upload_loop_state_t& s) {
        // TODO
    }
    intrusive_list<st_suspended, &st_suspended::_hook> _suspended;
    intrusive_list<st_suspended, &st_suspended::_hook> _throttled;
};

*/
// class upload_loop_stm {

//   struct st_suspended;
//   struct st_reconciling;
//   struct st_uploading;
//   struct st_admission_control;
//   struct st_replicating_stm_commands;

//   using upload_loop_state_t = std::variant<st_suspended, st_reconciling,
//   st_uploading, st_admission_control, st_replicating_stm_commands>;

//   struct st_suspended {};
//   struct st_reconciling {};
//   struct st_uploading {};
//   struct st_admission_control {};
//   struct st_replicating_stm_commands {};

// public:

//   upload_loop_stm(local_storage_api_iface& ls,
//           cloud_storage_api_iface& cs,
//           partition_iface& part)
//   : _local(ls)
//   , _cloud(cs)
//   , _partition(part)
//   , _state(st_suspended{})
//   {
//   }

// private:
//   local_storage_api_iface& _local;
//   cloud_storage_api_iface& _cloud;
//   partition_iface& _partition;
//   upload_loop_state_t _state;
// };

#include <boost/msm/back/state_machine.hpp>
#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>

enum class poll_result_v2 {
    /// Not started / invalid result
    none,
    /// Action completed
    done,
    /// Action failed
    failed,
    /// Action is in progress
    in_progress,
    /// Cancelled
    cancelled,
};

/// Non-virtual interface for different operations that can be polled
/// periodically. Combines together asynchronous execution and
/// cancellation.
template<class T, class Context>
struct pollable {
    /// Interface for implementations
    struct source_impl { // NOLINT
        virtual ~source_impl() = default;

        /// Check the result of the asynchronous operation
        virtual poll_result_v2 poll(const Context& ctx) noexcept = 0;

        /// Get the result of the operation. Can only be called if
        /// 'poll' returned 'poll_result_t::done'
        virtual T&& get() noexcept = 0;

        /// Get the exceptional result of the operation. Can only be
        /// called if 'poll' returned 'poll_result_t::failed'
        virtual std::exception_ptr get_exception() noexcept = 0;

        /// Cancel operation asynchronously. Returns 'true' when cancellation
        /// is requested. Returns 'false' when cancellation is not supported
        /// by the underlying implementation or not possible.
        virtual bool try_request_abort(const Context&) noexcept = 0;

        /// Set callback.
        /// The callback gets called when the pollable is ready
        virtual void set_callback(ss::noncopyable_function<void()> fn) = 0;
    };

    pollable() = default;
    ~pollable() = default;
    pollable(const pollable&) = delete;
    pollable& operator=(const pollable&) = delete;
    pollable(pollable&&) noexcept = default;
    pollable& operator=(pollable&&) noexcept = default;

    template<class Impl>
    explicit pollable(std::unique_ptr<Impl> s)
      : _src(std::move(s)) {}

    /// Poll for status
    poll_result_v2 poll(const Context& ctx) noexcept {
        if (!_src) {
            return poll_result_v2::none;
        }
        return _src->poll(ctx);
    }

    /// Get the result of the operation. Can only be called if
    /// 'poll' returned 'poll_result_t::done'
    T&& get() noexcept {
        vassert(_src, "'pollable' is not initialized or moved from");
        return _src->get();
    }

    /// Get the exceptional result of the operation. Can only be
    /// called if 'poll' returned 'poll_result_t::failed'
    std::exception_ptr get_exception() noexcept {
        vassert(_src, "'pollable' is not initialized or moved from");
        return _src->get_exception();
    }

    void reset() { _src.reset(); }

    bool empty() const noexcept { return _src == nullptr; }

    /// Abort the underlying operation
    bool try_cancel() noexcept {
        if (_src == nullptr) {
            return false;
        }
        return _src->try_request_abort();
    }

    void set_callback(ss::noncopyable_function<void()> fn) {
        if (_src == nullptr) {
            return;
        }
        _src->set_callback(std::move(fn));
    }

private:
    std::unique_ptr<source_impl> _src;
};

namespace details {

template<class T, class Context = void>
class wrapped_future : public pollable<T, Context>::source_impl {
public:
    explicit wrapped_future(
      ss::future<T> f,
      std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt)
      : _fut(std::move(f)) {}

    poll_result_v2 poll(const Context& ctx) noexcept override {
        if (!_fut.available()) {
            return poll_result_v2::in_progress;
        }
        if (_aborted) {
            return poll_result_v2::cancelled;
        }
        if (_error) {
            return poll_result_v2::failed;
        }
        if (_fut.failed()) {
            try {
                _fut.get();
            } catch (const ss::abort_requested_exception&) {
                _aborted = true;
            } catch (...) {
                _error = std::current_exception();
            }
            return poll(ctx);
        }
        return poll_result_v2::done;
    }

    T&& get() noexcept override {
        try {
            return _fut.get();
        } catch (...) {
            vassert(false, "Incorrect exceptional future access");
        }
    }

    std::exception_ptr get_exception() noexcept override { return _error; }

    bool try_request_abort(const Context&) noexcept override {
        if (_as.has_value()) {
            _as.value().get().request_abort();
            return true;
        }
        return false;
    }

    void set_callback(ss::noncopyable_function<void()> fn) override {
        _fut = _fut.finally([fn = std::move(fn)]() { fn(); });
    }

private:
    ss::future<T> _fut;
    std::optional<std::reference_wrapper<ss::abort_source>> _as;
    bool _aborted{false};
    std::exception_ptr _error{nullptr};
};

} // namespace details

/// Turn future into a pollable object to use with the STM
template<class T, class C>
pollable<T, C> make_pollable(ss::future<T> f) {
    return pollable<T, C>(
      std::make_unique<details::wrapped_future<T, C>>(std::move(f)));
}

struct fsm_sync;

/// Archiver API
class archiver_indirection_layer {
public:
    // TODO: use pimpl for pollable
    virtual pollable<std::vector<append_offset_range_upload>, fsm_sync>
    find_next_append_upload_candidates(
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency)
      = 0;

    struct segment_upload_result {
        // append_offset_range_upload initial;
        bool success{false};
        // Ground truth segment metadata
        cloud_storage::segment_meta meta;
    };

    // Upload segment with tx-manifest and index
    virtual pollable<std::vector<segment_upload_result>, fsm_sync>
      upload_segments(std::vector<append_offset_range_upload>) = 0;
};

class poll_queue;

/// This even is generated every 10ms independent of the
/// state of the archiver.
struct fsm_sync {
    timestamp_t timestamp;
    archiver_indirection_layer& api;
    // poll_queue& queue;
};

class new_archiver;

/// Enum class to map all existing states
enum class ntp_archiver_state_id_t {
    na,
    st_suspended,
    st_reconciling,
    st_uploading,
    st_admitting,
};

/// Simple visitor that aims to provide
/// information about the current state.
struct ntp_archiver_fms_state_visitor {
    ntp_archiver_state_id_t current_state{ntp_archiver_state_id_t::na};
};

struct ntp_archiver_state_base {
    using accept_sig
      = boost::msm::back::args<void, ntp_archiver_fms_state_visitor&>;
    void accept(ntp_archiver_fms_state_visitor& v) {}
};

/// Manages state and state transitions
class ntp_archiver_fsm
  : public boost::msm::front::
      state_machine_def<ntp_archiver_fsm, ntp_archiver_state_base> {
public:
    using state_machine_t = boost::msm::back::
      state_machine<ntp_archiver_fsm, ntp_archiver_state_base>;

    // States

    /// Base state. Implements 'visitor' access so the states don't have to.
    template<ntp_archiver_state_id_t id>
    struct state : boost::msm::front::state<ntp_archiver_state_base> {
        void accept(ntp_archiver_fms_state_visitor& v) { v.current_state = id; }
    };

    struct st_suspended : state<ntp_archiver_state_id_t::st_suspended> {};
    struct st_reconciling : state<ntp_archiver_state_id_t::st_reconciling> {
        void on_entry(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(
              test_log.info,
              "st_reconciling ENTER, empty: {}",
              _action.empty());
        }
        void on_exit(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(test_log.info, "st_reconciling EXIT");
        }
        pollable<std::vector<append_offset_range_upload>, fsm_sync> _action;
    };
    struct st_uploading : state<ntp_archiver_state_id_t::st_uploading> {
        void on_entry(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(
              test_log.info, "st_uploading ENTER, empty: {}", _action.empty());
        }
        void on_exit(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(test_log.info, "st_uploading EXIT");
        }
        pollable<
          std::vector<archiver_indirection_layer::segment_upload_result>,
          fsm_sync>
          _action;
    };
    struct st_admitting : state<ntp_archiver_state_id_t::st_admitting> {
        void on_entry(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(test_log.info, "st_admitting ENTER");
        }
        void on_exit(const fsm_sync& sync, state_machine_t& fsm) {
            vlog(test_log.info, "st_admitting EXIT");
        }
    };

    // Transitions/guards
    // NOTE: if state transition functor throws the STM stays in the
    // same state. We need to be careful because unhandled exception which
    // gets triggered on every call will make the STM stuck in one state.
    // If the guard throws an exception it's equivalent to the case when
    // the guard is returning false.
    struct suspended_to_reconciling {
        void operator()(
          const fsm_sync& sync,
          state_machine_t& fsm,
          st_suspended&,
          st_reconciling& new_state) {
            vlog(test_log.info, "suspended to reconciling transition");
            // TODO: implement properly
            auto action = sync.api.find_next_append_upload_candidates(
              model::offset(0), 0, 1024, 4);
            action.set_callback([&fsm, s = sync] {
                vlog(
                  test_log.info,
                  "reconciling to uploading transition (callback)");
                fsm.process_event(s);
            });
            new_state._action = std::move(action);
        }
    };
    struct st_suspended_guard {
        bool operator()(
          const fsm_sync& sync,
          state_machine_t& fsm,
          st_suspended&,
          st_reconciling&) {
            vlog(
              test_log.info,
              "st_suspended guard {}",
              sync.timestamp.time_since_epoch().count());
            return true;
        }
    };
    struct reconciling_to_uploading {
        void operator()(
          const fsm_sync& sync,
          state_machine_t& fsm,
          st_reconciling& prev_state,
          st_uploading& new_state) {
            vlog(test_log.info, "reconciling to uploading transition");
            // TODO: implement properly
            new_state._action = sync.api.upload_segments(
              prev_state._action.get());
            new_state._action.set_callback([&fsm, s = sync] {
                // TODO: improve this by calling method of the new state
                vlog(
                  test_log.info,
                  "uploading to admitting transition (callback)");
                fsm.process_event(s);
            });
        }
    };
    struct st_reconciling_guard {
        bool operator()(
          const fsm_sync& sync,
          state_machine_t& fsm,
          st_reconciling& st_rec,
          st_uploading&) {
            vlog(
              test_log.info,
              "st_reconciling guard {}",
              sync.timestamp.time_since_epoch().count());
            return st_rec._action.poll(sync) == poll_result_v2::done;
        }
    };
    struct reconciling_to_suspended {
        void operator()(
          const fsm_sync&,
          state_machine_t& fsm,
          st_reconciling&,
          st_suspended& new_state) {
            vlog(test_log.info, "reconciling to suspended transition");
            // TODO: implement properly
        }
    };
    struct st_reconciling_failure_guard {
        bool operator()(
          const fsm_sync& sync,
          state_machine_t& fsm,
          st_reconciling& st_rec,
          st_suspended&) {
            vlog(
              test_log.info,
              "st_reconciling_failure guard {}",
              sync.timestamp.time_since_epoch().count());
            switch (st_rec._action.poll(sync)) {
            case poll_result_v2::failed:
            case poll_result_v2::cancelled:
                return true;
            case poll_result_v2::done:
            case poll_result_v2::in_progress:
            case poll_result_v2::none:
                return false;
            };
            return false;
        }
    };

    using transition_table = boost::mpl::vector<
      boost::msm::front::Row<
        st_suspended,
        fsm_sync,
        st_reconciling,
        suspended_to_reconciling,
        st_suspended_guard>,
      boost::msm::front::Row<
        st_reconciling,
        fsm_sync,
        st_uploading,
        reconciling_to_uploading,
        st_reconciling_guard>,
      boost::msm::front::Row<
        st_reconciling,
        fsm_sync,
        st_suspended,
        reconciling_to_suspended,
        st_reconciling_failure_guard>,
      _row<st_uploading, fsm_sync, st_admitting>>;

    using initial_state = st_suspended;
};

class new_archiver : public ntp_archiver_fsm::state_machine_t {
    friend class ntp_archiver_fsm;

public:
    explicit new_archiver(archiver_indirection_layer& api)
      : _api(api) {
        // TODO: register with the queue
        start();
    }

    void poll(timestamp_t t = ss::lowres_clock::now()) {
        process_event(fsm_sync{
          .timestamp = t,
          .api = _api.get(),
        });
    }

    auto current_state() noexcept {
        ntp_archiver_fms_state_visitor vis;
        visit_current_states(std::ref(vis));
        return vis.current_state;
    }

private:
    std::reference_wrapper<archiver_indirection_layer> _api;

    // List that groups all state machines on a shard
    intrusive_list_hook _list_hook;
    friend class poll_queue;
};

class poll_queue {
    using poll_list_t = intrusive_list<new_archiver, &new_archiver::_list_hook>;

public:
private:
    ss::condition_variable _urgent_event;
    /// List of all archivers on a shard. Polled periodically with low priority.
    poll_list_t _queue;
    /// List of archivers which are already in a ready state. This list is
    /// polled on demand when the cvar is triggered.
    poll_list_t _urgent;
};

namespace details {
template<class T>
struct mock_pollable : pollable<T, fsm_sync>::source_impl {
    /// Creates normal pollable object which becomes available at 'target'
    /// or immediately if 'target' is 'nullopt'
    template<class Val>
    explicit mock_pollable(Val&& val, std::optional<timestamp_t> target)
      : _ms_to_stop()
      , _available_at(target)
      , _result(std::forward<Val>(val)) {}

    /// Creates exceptional pollable object which becomes available at 'target'
    /// or immediately if 'target' is 'nullopt'
    explicit mock_pollable(
      std::exception_ptr val,
      std::optional<timestamp_t> target,
      std::chrono::milliseconds ms_to_stop)
      : _ms_to_stop(ms_to_stop)
      , _available_at(target)
      , _result(val) {}

    poll_result_v2 poll(const fsm_sync& ctx) noexcept override {
        if (_aborted_at.has_value()) {
            // canceling logic
            if (ctx.timestamp >= _aborted_at.value()) {
                return poll_result_v2::cancelled;
            }
        }
        if (_available_at.has_value()) {
            // waiting logic
            if (ctx.timestamp < _available_at.value()) {
                return poll_result_v2::in_progress;
            }
        }
        auto result = poll_result_v2::done;
        if (std::holds_alternative<std::exception_ptr>(_result)) {
            result = poll_result_v2::failed;
        }
        if (_cb.has_value()) {
            vlog(test_log.info, "Calling CB");
            _cb.value()();
            _cb.reset();
        }
        return result;
    }

    T&& get() noexcept override { return std::move(std::get<T>(_result)); }

    std::exception_ptr get_exception() noexcept override {
        return std::get<std::exception_ptr>(_result);
    }

    bool try_request_abort(const fsm_sync& ctx) noexcept override {
        _aborted_at = ctx.timestamp + _ms_to_stop;
        return true;
    }

    void set_callback(ss::noncopyable_function<void()> fn) override {
        vlog(test_log.info, "Setting CB");
        _cb = std::move(fn);
    }

    std::chrono::milliseconds _ms_to_stop;
    std::optional<timestamp_t> _available_at;
    std::optional<timestamp_t> _aborted_at;
    std::variant<T, std::exception_ptr> _result;
    std::optional<ss::noncopyable_function<void()>> _cb;
};
} // namespace details

/// Turn future into a pollable object to use with the STM
template<class T>
pollable<T, fsm_sync>
make_mock_pollable(T&& val, std::optional<timestamp_t> t) {
    return pollable<T, fsm_sync>(
      std::make_unique<details::mock_pollable<T>>(std::forward<T>(val), t));
}
/*
template<class T>
pollable<T, fsm_sync> make_exceptional_mock_pollable(
  std::exception_ptr p, std::optional<timestamp_t> t = std::nullopt) {
    return pollable<T, fsm_sync>(
      std::make_unique<details::mock_pollable<T>>(p, t));
}
*/

class mock_api : public archiver_indirection_layer {
public:
    pollable<std::vector<append_offset_range_upload>, fsm_sync>
    find_next_append_upload_candidates(
      model::offset, size_t, size_t, size_t) override {
        std::vector<append_offset_range_upload> empty;
        return make_mock_pollable(
          std::move(empty), timestamp_t(std::chrono::milliseconds(10)));
        // return make_exceptional_mock_pollable<
        //   std::vector<append_offset_range_upload>>(
        //   std::make_exception_ptr(std::runtime_error("Err1")));
    }

    pollable<std::vector<segment_upload_result>, fsm_sync>
    upload_segments(std::vector<append_offset_range_upload>) override {
        std::vector<segment_upload_result> empty;
        return make_mock_pollable(
          std::move(empty), timestamp_t(std::chrono::milliseconds(10)));
    }
};

SEASTAR_THREAD_TEST_CASE(test_msm_1) {
    mock_api api;
    new_archiver arch(api);
    arch.poll(timestamp_t(std::chrono::milliseconds(1)));
    arch.poll(timestamp_t(std::chrono::milliseconds(2)));
    arch.poll(timestamp_t(std::chrono::milliseconds(4)));
    arch.poll(timestamp_t(std::chrono::milliseconds(8)));
    arch.poll(timestamp_t(std::chrono::milliseconds(15)));
    arch.poll(timestamp_t(std::chrono::milliseconds(20)));
}

class mock2_api : public archiver_indirection_layer {
public:
    pollable<std::vector<append_offset_range_upload>, fsm_sync>
    find_next_append_upload_candidates(
      model::offset, size_t, size_t, size_t) override {
        auto fut = ss::sleep(10ms).then([] {
            std::vector<append_offset_range_upload> empty;
            return ss::make_ready_future<
              std::vector<append_offset_range_upload>>(std::move(empty));
        });
        return make_pollable<std::vector<append_offset_range_upload>, fsm_sync>(
          std::move(fut));
    }

    pollable<std::vector<segment_upload_result>, fsm_sync>
    upload_segments(std::vector<append_offset_range_upload>) override {
        auto fut = ss::sleep(10ms).then([] {
            std::vector<segment_upload_result> empty;
            return ss::make_ready_future<std::vector<segment_upload_result>>(
              std::move(empty));
        });
        return make_pollable<std::vector<segment_upload_result>, fsm_sync>(
          std::move(fut));
    }
};

SEASTAR_THREAD_TEST_CASE(test_msm_2) {
    mock2_api api;
    new_archiver arch(api);
    for (int i = 0; i < 1000; i++) {
        arch.poll(timestamp_t(std::chrono::milliseconds(i)));
        if (arch.current_state() == ntp_archiver_state_id_t::st_admitting) {
            vlog(test_log.info, "Success!!!");
            break;
        }
        ss::sleep(1s).get();
    }
}
