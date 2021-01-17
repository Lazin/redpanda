/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "arch/manifest.h"
#include "arch/ntp_archiver_service.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/core/shared_ptr.hh>


#include <chrono>
#include <exception>
#include <vector>

struct segment_desc {
    model::ntp ntp;
    model::offset base_offset;
    model::term_id term;
};

/// Http server and client
struct ntp_archiver_fixture {
    void set_routes(
      ss::httpd::routes& r,
      const std::vector<std::pair<ss::sstring, ss::sstring>>&
        manifest_url_reply,
      const std::vector<ss::sstring>& segment_urls);

    void stop();

    /// Create a working directory with a bunch of segments
    void init_storage_api(
      const std::filesystem::path& workdir, std::vector<segment_desc>& segm);

    std::vector<ss::lw_shared_ptr<storage::segment>> list_segments() const;

    ss::lw_shared_ptr<storage::segment>
    get_segment(const arch::segment_name& name) const;

    /// Verify 'expected' segment content using the actual segment from
    /// log_manager
    void verify_segment(
      const arch::segment_name& name, const ss::sstring& expected);

    /// Verify manifest using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest(const arch::manifest& man);

    /// Verify manifest content using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest_content(const ss::sstring& manifest_content);

    /// Verify local manifest content using log_manager's state,
    /// find matching segments and check the fields.
    void verify_local_manifest();

    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<arch::ntp_archiver> archiver;
    ss::shared_ptr<storage::api> api;
    model::ntp archiver_ntp;
    /// Contains saved requests
    std::vector<ss::httpd::request> requests;

};

arch::configuration get_configuration();

/// Create server and archiver, server is initialized with default
/// testing paths and listening.
/// Initialize httpd server with expected access urls for manifest
/// and segment uploads/downloads.
ntp_archiver_fixture started_fixture(
  storage::ntp_config ntp,
  const arch::configuration& conf,
  const std::vector<std::pair<ss::sstring, ss::sstring>>& manifests_url_reply,
  const std::vector<ss::sstring>& segments_url);