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
#include "container/intrusive_list_helpers.h"
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
#include "test_utils/async.h"
#include "test_utils/fixture.h"

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

#include <boost/msm/back/state_machine.hpp>
#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <exception>
#include <memory>
#include <optional>
#include <variant>

using namespace std::chrono_literals;
using namespace archival;

/// Archiver API
/// Every state in the STM has a corresponding api call.
class ntp_archiver_api {
public:
    struct segment_upload_resumed {};

    struct segment_upload_candidates {};

    struct segment_upload_result {
        // append_offset_range_upload initial;
        std::vector<bool> success;
        // Ground truth segment metadata
        std::vector<cloud_storage::segment_meta> meta;
    };

    struct segment_admission_result {
        std::vector<bool> success;
    };

    virtual ss::future<segment_upload_resumed> suspend(bool backoff) = 0;

    virtual ss::future<segment_upload_candidates>
    find_next_append_upload_candidates(
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency)
      = 0;

    // Upload segment with tx-manifest and index
    virtual ss::future<segment_upload_result>
      upload_segments(segment_upload_candidates) = 0;

    // Validate upload result and replicate metadata
    virtual ss::future<segment_admission_result>
      admit_segments(segment_upload_result) = 0;
};

class new_archiver;

/// Enum class to map all existing states
enum class ntp_archiver_state_id_t {
    na,
    st_initial,
    st_reconciling,
    st_uploading,
    st_admitting,
    st_suspended,
};

std::ostream& operator<<(std::ostream& o, ntp_archiver_state_id_t id) {
    switch (id) {
    case ntp_archiver_state_id_t::na:
        o << "N/A";
        break;
    case ntp_archiver_state_id_t::st_initial:
        o << "st_initial";
        break;
    case ntp_archiver_state_id_t::st_reconciling:
        o << "st_reconciling";
        break;
    case ntp_archiver_state_id_t::st_uploading:
        o << "st_uploading";
        break;
    case ntp_archiver_state_id_t::st_admitting:
        o << "st_admitting";
        break;
    case ntp_archiver_state_id_t::st_suspended:
        o << "st_suspended";
        break;
    }
    return o;
}

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

    ntp_archiver_fsm(ntp_archiver_api& api)
      : _api(api) {}

    // States

    /// Base state. Implements 'visitor' access so the states don't have to.
    template<ntp_archiver_state_id_t id>
    struct state : boost::msm::front::state<ntp_archiver_state_base> {
        static constexpr auto state_id = id;
        void accept(ntp_archiver_fms_state_visitor& v) { v.current_state = id; }

        template<class T>
        void on_entry(const T& event, state_machine_t& fsm) {
            vlog(
              test_log.info,
              "{} ENTER, event type {}",
              id,
              typeid(event).name());
        }
        template<class T>
        void on_exit(const T& event, state_machine_t& fsm) {
            vlog(
              test_log.info,
              "{} EXIT, event type {}",
              id,
              typeid(event).name());
        }
    };

    struct st_initial : state<ntp_archiver_state_id_t::st_initial> {};
    struct st_reconciling : state<ntp_archiver_state_id_t::st_reconciling> {};
    struct st_uploading : state<ntp_archiver_state_id_t::st_uploading> {};
    struct st_admitting : state<ntp_archiver_state_id_t::st_admitting> {};
    struct st_suspended : state<ntp_archiver_state_id_t::st_suspended> {};

    // Transitions
    // NOTE: if state transition functor throws the STM stays in the
    // same state. We need to be careful because unhandled exception which
    // gets triggered on every call will make the STM stuck in one state.
    // If the guard throws an exception it's equivalent to the case when
    // the guard is returning false.
    struct initial_to_reconciling {
        void operator()(
          const ntp_archiver_api::segment_upload_resumed& res,
          state_machine_t& fsm,
          st_initial&,
          st_reconciling& new_state) {
            vlog(test_log.info, "suspended to reconciling transition");
            // TODO: use workflow level gate & cancellation
            ssx::background
              = fsm._api
                  .find_next_append_upload_candidates(
                    model::offset(0), 0, 1024, 4)
                  .then_wrapped(
                    [&fsm](
                      ss::future<ntp_archiver_api::segment_upload_candidates>
                        fut) {
                        if (fut.failed()) {
                            fsm.process_event(fut.get_exception());
                        } else {
                            fsm.process_event(fut.get());
                        }
                    });
        }
    };
    struct reconciling_to_uploading {
        void operator()(
          const ntp_archiver_api::segment_upload_candidates& val,
          state_machine_t& fsm,
          st_reconciling& prev_state,
          st_uploading& new_state) {
            vlog(test_log.info, "reconciling to uploading transition");
            ssx::background = fsm._api.upload_segments(val).then_wrapped(
              [&fsm](ss::future<ntp_archiver_api::segment_upload_result> fut) {
                  fsm.process_event(fut.get());
              });
        }
    };
    struct uploading_to_admitting {
        void operator()(
          const ntp_archiver_api::segment_upload_result& val,
          state_machine_t& fsm,
          st_uploading& prev_state,
          st_admitting& new_state) {
            vlog(test_log.info, "uploading to admitting transition");
            ssx::background = fsm._api.admit_segments(val).then_wrapped(
              [&fsm](
                ss::future<ntp_archiver_api::segment_admission_result> fut) {
                  fsm.process_event(fut.get());
              });
        }
    };

    struct admitting_to_suspended {
        void operator()(
          const ntp_archiver_api::segment_admission_result& val,
          state_machine_t& fsm,
          st_admitting& prev_state,
          st_suspended& new_state) {
            vlog(test_log.info, "admitting to suspended transition");
            /*
            ssx::background = fsm._api.suspend(false).then_wrapped(
              [&fsm](
                ss::future<ntp_archiver_api::segment_upload_resumed> fut) {
                  fsm.process_event(fut.get());
              });
            */
        }
    };

    // Generic error handling state transition
    template<class st_source>
    struct any_to_suspended {
        void operator()(
          const std::exception_ptr& err,
          state_machine_t& fsm,
          st_source&,
          st_suspended&) {
            vlog(
              test_log.info,
              "transition from {} to st_suspended state due to error: {}",
              st_source::state_id,
              err);
        }
    };

    using transition_table = boost::mpl::vector<

      // Initial state transitions
      boost::msm::front::Row<
        st_initial,
        ntp_archiver_api::segment_upload_resumed,
        st_reconciling,
        initial_to_reconciling>,
      boost::msm::front::Row<
        st_initial,
        std::exception_ptr,
        st_suspended,
        any_to_suspended<st_initial>>,

      // Reconciling state transitions
      boost::msm::front::Row<
        st_reconciling,
        ntp_archiver_api::segment_upload_candidates,
        st_uploading,
        reconciling_to_uploading>,
      boost::msm::front::Row<
        st_reconciling,
        std::exception_ptr,
        st_suspended,
        any_to_suspended<st_reconciling>>,

      // Uploading state transitions
      boost::msm::front::Row<
        st_uploading,
        ntp_archiver_api::segment_upload_result,
        st_admitting,
        uploading_to_admitting>,
      boost::msm::front::Row<
        st_uploading,
        std::exception_ptr,
        st_suspended,
        any_to_suspended<st_uploading>>,

      // Admitting state transitions
      boost::msm::front::Row<
        st_admitting,
        ntp_archiver_api::segment_admission_result,
        st_suspended,
        admitting_to_suspended>,
      boost::msm::front::Row<
        st_admitting,
        std::exception_ptr,
        st_suspended,
        any_to_suspended<st_admitting>>>;

    using initial_state = st_initial;
    ntp_archiver_api& _api;
};

class new_archiver : public ntp_archiver_fsm::state_machine_t {
    friend class ntp_archiver_fsm;

public:
    explicit new_archiver(ntp_archiver_api& api)
      : ntp_archiver_fsm::state_machine_t(api)
      , _api(api) {
        start();
    }

    void start_workflow() {
      process_event(ntp_archiver_api::segment_upload_resumed{});
    }

    auto current_state() noexcept {
        ntp_archiver_fms_state_visitor vis;
        visit_current_states(std::ref(vis));
        return vis.current_state;
    }

private:
    ntp_archiver_api& _api;

    // List that groups all state machines on a shard
    intrusive_list_hook _list_hook;
    friend class poll_queue;
};

class mock_api : public ntp_archiver_api {
public:
    void unblock_suspend() { _suspend->set_value(segment_upload_resumed{}); }
    ss::future<segment_upload_resumed> suspend(bool) override {
        _suspend.emplace();
        return _suspend->get_future();
    }

    void unblock_find_candidates() {
        return _candidate->set_value(segment_upload_candidates{});
    }
    ss::future<segment_upload_candidates> find_next_append_upload_candidates(
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency) override {
        _candidate.emplace();
        return _candidate->get_future();
    }

    void unblock_upload() { _upload->set_value(segment_upload_result{}); }
    ss::future<segment_upload_result>
    upload_segments(segment_upload_candidates) override {
        _upload.emplace();
        return _upload->get_future();
    }

    void unblock_admission() {
        _admission->set_value(segment_admission_result{});
    }
    ss::future<segment_admission_result>
    admit_segments(segment_upload_result) override {
        _admission.emplace();
        return _admission->get_future();
    }

private:
    std::optional<ss::promise<segment_upload_resumed>> _suspend;
    std::optional<ss::promise<segment_upload_candidates>> _candidate;
    std::optional<ss::promise<segment_upload_result>> _upload;
    std::optional<ss::promise<segment_admission_result>> _admission;
};

SEASTAR_THREAD_TEST_CASE(test_msm_1) {
    mock_api api;
    new_archiver arch(api);
    BOOST_REQUIRE(
      arch.current_state() == ntp_archiver_state_id_t::st_initial);
    
    arch.start_workflow();  // can pass parameters here
    BOOST_REQUIRE(
      arch.current_state() == ntp_archiver_state_id_t::st_reconciling);

    api.unblock_find_candidates();
    tests::cooperative_spin_wait_with_timeout(1s, [&] {
        return arch.current_state() == ntp_archiver_state_id_t::st_uploading;
    }).get();

    api.unblock_upload();
    tests::cooperative_spin_wait_with_timeout(1s, [&] {
        return arch.current_state() == ntp_archiver_state_id_t::st_admitting;
    }).get();

    api.unblock_admission();
    tests::cooperative_spin_wait_with_timeout(1s, [&] {
        return arch.current_state() == ntp_archiver_state_id_t::st_suspended;
    }).get();

    BOOST_REQUIRE(
      arch.current_state() == ntp_archiver_state_id_t::st_suspended);
}
