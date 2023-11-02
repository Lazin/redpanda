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
#include "archival/upload_scheduler.h"
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
#include "storage/disk_log_impl.h"
#include "storage/ntp_config.h"
#include "storage/offset_to_filepos.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/abort_source.hh>
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

using namespace std::chrono_literals;
using namespace archival;

inline ss::logger test_log("arch_test");

// Simulate segment upload
struct upload_segment : pollable_action {
    explicit upload_segment(cloud_storage::remote_segment_path p, bool fail)
    : _fail(fail)
    , _path(p)
    {}

    void start() override {
        _fut = ss::sleep_abortable(3s, _as).then([this] {
            // done
            vlog(test_log.info, "Done uploading {}", _path);
        });
    }

    poll_result_t poll(timestamp_t t, scheduler&, upload_queue&) override {
        vlog(test_log.debug, "upload_segment.poll {} at {}", _path, t.time_since_epoch().count());
        if (has_failed()) {
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
        vlog(test_log.debug, "upload_segment.complete {}", _path);
        vassert(_fut->available(), "Action is not ready");
        _fut->get();
        _fut.reset();
    }

    std::optional<ss::future<>> _fut;
    ss::abort_source _as;
    bool _fail;
    cloud_storage::remote_segment_path _path;
};

struct discover_uploads : pollable_action {

    explicit discover_uploads(std::vector<cloud_storage::remote_segment_path> segments)
    : _results(std::move(segments))
    {
    }

    void start() override {
        _fut = run();
    }

    ss::future<> run() {
        vlog(test_log.info, "Discovering uploads");
        co_await ss::sleep_abortable(1ms, _as);
        vlog(test_log.info, "Done discovering uploads");
    }

    poll_result_t poll(timestamp_t, scheduler&, upload_queue&) override {
        if (has_failed()) {
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
        // remove ourselves from the queue
        vassert(_fut->available(), "Action is not ready");
        _fut->get();
        _fut.reset();
    }

    std::optional<ss::future<>> _fut;
    ss::abort_source _as;
    std::vector<cloud_storage::remote_segment_path> _results;
};


class ntp_upload_strand : public pollable_action {
public:
    void start() override { _fut = run_once(); }

    poll_result_t
    poll(timestamp_t t, scheduler& sch, upload_queue& queue) override {
        if (has_failed()) {
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
        _fut->get();
        _fut.reset();
    }

private:
    ss::future<> run_once() {
        // can run something async
        co_await ss::sleep_abortable(1ms, _as);
        // or in lockstep with main loop
        co_await invoke_on_poll([this] (timestamp_t t, scheduler& s, upload_queue& q) {
            using rsp = cloud_storage::remote_segment_path;
            std::vector<rsp> vec = {
                rsp("/foo/1"),
                rsp("/foo/2"),
                rsp("/foo/3"),
            };
            // TODO: add another queue for replication and working with the storage layer
            // e.g. replicate metadata or discover the upload. So the full list of non-deterministic 
            // things the archiver can do will be limited to uploads (via upload_queue), metadata
            // replication (via archival_metadata_stm_queue), upload reconciliation (via storage_queue)
            // etc.
            // Internal fiber of the action should be free from any non-deterministic effects. It should
            // only use time passed via poll (TBD: add method similar to invoke_on_poll that returns a
            // cached timestamp). It should only rely on queues to do non-deterministic stuff (I/O and such).
            _discover.emplace(std::move(vec));
            s.schedule(&_discover.value());
        });
        // or wait for other pollable_action instances
        co_await wait_for(_discover.value());

        // simulate uploads
        for (const auto& r: _discover->_results) {
            _upload.emplace_back(r, false);
        }
        co_await invoke_on_poll([this] (timestamp_t t, scheduler& s, upload_queue& q) {
            for (auto& u: _upload) {
                s.schedule(&u);
            }
        });


        // 
        co_await ss::sleep_abortable(1s, _as);
    }

    std::optional<ss::future<>> _fut;
    ss::abort_source _as;
    std::optional<discover_uploads> _discover;
    std::list<upload_segment> _upload;
};

struct mock_upload_queue : upload_queue {
    std::unique_ptr<pollable_action>
    upload_segment(cloud_storage::remote_segment_path path) override {
        throw "Not implemented";
    }

    // Upload manifest (partition/topic/tx)
    std::unique_ptr<pollable_action>
    upload_manifest(cloud_storage::remote_manifest_path path) override {
        throw "Not implemented";
    }
};

SEASTAR_THREAD_TEST_CASE(test_archiver_1) {
    vlog(test_log.info, "Start");
    scheduler sc;
    mock_upload_queue q;
    discover_uploads d({cloud_storage::remote_segment_path("foo")});

    sc.schedule(&d);
    auto t = ss::lowres_clock::now();
    BOOST_REQUIRE(d.poll(t, sc, q) == poll_result_t::in_progress);

    while (d.poll(t, sc, q) == poll_result_t::in_progress) {
        ss::sleep(1ms).get();
        t = ss::lowres_clock::now();
        sc.poll(t, q);
    }

    BOOST_REQUIRE(d.poll(t, sc, q) == poll_result_t::done);
    BOOST_REQUIRE_EQUAL(d._results.size(), 1);
    vlog(test_log.info, "Done");
}

SEASTAR_THREAD_TEST_CASE(test_archiver_2) {
    vlog(test_log.info, "Start");
    scheduler sc;
    mock_upload_queue q;
    ntp_upload_strand d;

    sc.schedule(&d);
    auto t = ss::lowres_clock::now();
    BOOST_REQUIRE(d.poll(t, sc, q) == poll_result_t::in_progress);

    while (d.poll(t, sc, q) == poll_result_t::in_progress) {
        t = t + 1s;
        sc.poll(t, q);
        ss::maybe_yield().get();
    }

    BOOST_REQUIRE(d.poll(t, sc, q) == poll_result_t::done);
    vlog(test_log.info, "Done");
}
