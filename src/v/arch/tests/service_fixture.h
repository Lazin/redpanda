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
#include "cluster/tests/controller_test_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/tmp_file.hh>

#include <chrono>
#include <exception>
#include <vector>

/// Emulates S3 REST API for testing purposes
class s3_imposter_fixture {
public:
    s3_imposter_fixture();
    ~s3_imposter_fixture();

    s3_imposter_fixture(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture& operator=(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture(s3_imposter_fixture&&) = delete;
    s3_imposter_fixture& operator=(s3_imposter_fixture&&) = delete;

    /// Set expectaitions on REST API calls that supposed to be made
    /// Only the requests that described in this call will be possible
    /// to make. This method can only be called once per test run.
    ///
    /// \param url_with_content is a collection of pairs that represent urls
    /// that allow both GET and PUT requests, the first element of the pair
    /// represents url and the second should contain a data that should be
    /// returned by GET request
    /// \param put_only_url is a collection of urls that can only be used for
    /// PUT requests
    void set_expectations_and_listen(
      const std::vector<std::pair<ss::sstring, ss::sstring>>& url_with_content,
      const std::vector<ss::sstring>& put_only_url);

    std::vector<ss::httpd::request> get_requests() const;

private:
    void set_routes(
      ss::httpd::routes& r,
      const std::vector<std::pair<ss::sstring, ss::sstring>>&
        manifest_url_reply,
      const std::vector<ss::sstring>& segment_urls);

    ss::socket_address _server_addr;
    ss::shared_ptr<ss::httpd::http_server_control> _server;
    /// Contains saved requests
    std::vector<ss::httpd::request> _requests;
};

struct segment_desc {
    model::ntp ntp;
    model::offset base_offset;
    model::term_id term;
};

/// Maintains log segments that can be retreived and used by
/// the archiver.
/// Initializes independent storage api not shared with any
/// other component (this is not the case with the redpanda)
class archiver_storage_fixture {
public:
    archiver_storage_fixture();
    ~archiver_storage_fixture();
    archiver_storage_fixture(const archiver_storage_fixture&) = delete;
    archiver_storage_fixture& operator=(const archiver_storage_fixture&)
      = delete;
    archiver_storage_fixture(archiver_storage_fixture&&) = delete;
    archiver_storage_fixture& operator=(archiver_storage_fixture&&) = delete;

    /// Create a working directory with a bunch of segments
    void init_storage_api(std::vector<segment_desc>& segm);

    std::vector<ss::lw_shared_ptr<storage::segment>>
    list_segments(const model::ntp& ntp);

    ss::lw_shared_ptr<storage::segment>
    get_segment(const model::ntp& ntp, const arch::segment_name& name);

    storage::api& get_local_api();
    ss::sharded<storage::api>& get_api();

    /// Verify 'expected' segment content using the actual segment from
    /// log_manager
    void verify_segment(
      const model::ntp& ntp,
      const arch::segment_name& name,
      const ss::sstring& expected);

    /// Verify manifest using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest(const arch::manifest& man);

    /// Verify manifest content using log_manager's state,
    /// find matching segments and check the fields.
    void verify_manifest_content(const ss::sstring& manifest_content);

private:
    // ss::sharded<storage::api>* _api;
    ss::sharded<storage::api> _api;
    ss::tmp_dir _tmp_dir;
};

arch::configuration get_configuration();
