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

#include "archival/upload_scheduler.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"

inline ss::logger test_log("arch_test");

namespace archival {

struct find_next_upload : find_next_append_uploads_action {
    explicit find_next_upload(cloud_storage::segment_meta m, bytes b)
      : _meta(m)
      , _data(b) {}

    void start() override { vlog(test_log.info, "Start discovering uploads"); }

    poll_result_t poll(
      timestamp_t,
      execution_queue&,
      local_storage_api_iface&,
      cloud_storage_api_iface&,
      partition_iface&) override {
        vlog(test_log.info, "Poll discovering uploads");
        return poll_result_t::done;
    }

    void abort() override { vlog(test_log.info, "Abort discovering uploads"); }

    void complete() override {
        vlog(test_log.info, "Complete discovering uploads");
    }

    std::optional<append_offset_range_upload> get_next() override {
        if (_consumed) {
          return std::nullopt;
        }
        _consumed = true;
        auto buf = bytes_to_iobuf(_data.value());
        return append_offset_range_upload{
          .found = true,
          .meta = _meta.value(),
          .data = make_iobuf_input_stream(std::move(buf)),
          .size = _data->size(),
        };
    }

    std::optional<cloud_storage::segment_meta> _meta;
    std::optional<bytes> _data;
    bool _consumed{false};
};

struct mock_local_storage_api : local_storage_api_iface {
    std::unique_ptr<find_next_append_uploads_action>
    find_next_append_upload_candidates(
      execution_queue& queue,
      model::offset start_offset,
      size_t min_size,
      size_t max_size,
      size_t max_concurrency) override {
        vlog(
          test_log.info,
          "find_next_append_upload_candidates, start_offset={}, min_size={}, "
          "max_size={}, max_concurrency={}",
          start_offset,
          min_size,
          max_size,
          max_concurrency);
        bytes b;
        cloud_storage::segment_meta m {
            .base_offset = start_offset,
        };
        m.base_offset = model::offset(3);
        auto res = std::make_unique<find_next_upload>(m, b);
        res->start();
        queue.schedule(res.get());
        return res;
    }
};

// Simulate segment upload
struct segment_upload_mock : segment_upload_action {
    explicit segment_upload_mock(
      cloud_storage::remote_segment_path p, bool fail)
      : _fail(fail)
      , _path(p) {}

    void start() override { vlog(test_log.info, "Start upload_segment"); }

    poll_result_t poll(
      timestamp_t t,
      execution_queue&,
      local_storage_api_iface&,
      cloud_storage_api_iface&,
      partition_iface&) override {
        vlog(
          test_log.debug,
          "upload_segment.poll {} at {}",
          _path,
          t.time_since_epoch().count());
        _initial = _initial.value_or(t);

        if (_fail) {
            return poll_result_t::failed;
        }

        auto delta = t - _initial.value();
        if (delta > 10s || _aborted) {
            return poll_result_t::done;
        }
        return poll_result_t::in_progress;
    }

    void abort() override { _aborted = true; }

    void complete() override {
        vlog(test_log.debug, "upload_segment.complete {}", _path);
    }

    bool _fail;
    bool _aborted{false};
    cloud_storage::remote_segment_path _path;
    std::optional<timestamp_t> _initial;
};

struct mock_cloud_storage_api : cloud_storage_api_iface {
    std::unique_ptr<segment_upload_action> upload_segment(
      execution_queue& queue,
      cloud_storage::remote_segment_path path,
      ss::input_stream<char> data,
      size_t expected_size) override {
        auto res = std::make_unique<segment_upload_mock>(path, false);
        res->start();
        queue.schedule(res.get());
        return res;
    }

    // Upload manifest (partition/topic/tx)
    std::unique_ptr<manifest_upload_action>
    upload_manifest(cloud_storage::remote_manifest_path) override {
        throw "Not implemented";
    }
};

struct mock_archival_metadata_stm : partition_iface {
    std::unique_ptr<add_new_segment_action>
    add_new_segment(cloud_storage::segment_meta) override {
        throw "Not implemented";
    }
};

} // namespace archival
