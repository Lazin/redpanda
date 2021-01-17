/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */


#include "arch/ntp_archiver_service.h"
#include "cluster/tests/controller_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"
#include "arch/tests/service_fixture.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/algorithm/string.hpp>

inline ss::logger test_log("test"); // NOLINT

static constexpr std::string_view manifest_payload = R"json({
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": "42",
    "revision": "0",
    "segments": {
        "1-2-v1.log": {
            "is_compacted": "false",
            "size_bytes": "100",
            "committed_offset": "2",
            "base_offset": "1",
            "deleted": "false"
        },
        "3-4-v1.log": {
            "is_compacted": "false",
            "size_bytes": "200",
            "committed_offset": "4",
            "base_offset": "3",
            "deleted": "false"
        }
    }
})json";
static const auto manifest_namespace = model::ns("test-ns");    // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::revision_id(0); // NOLINT
static const ss::sstring manifest_url = fmt::format(         // NOLINT
  "/a0000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());
static const std::vector<std::pair<ss::sstring, ss::sstring>>
  // NOLINTNEXTLINE
  default_manifest_urls = {
    std::make_pair(
      manifest_url,
      ss::sstring(manifest_payload.data(), manifest_payload.size())),
};

// NOLINTNEXTLINE
static const std::vector<ss::sstring> default_segment_urls = {
  "/3931f368/test-ns/test-topic/42_0/1-2-v1.log",
  "/47bef4d3/test-ns/test-topic/42_0/3-4-v1.log",
};


static arch::manifest load_manifest(std::string_view v) {
    arch::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}
static storage::ntp_config get_ntp_conf() {
    return storage::ntp_config(manifest_ntp, "base-dir");
}

FIXTURE_TEST(test_download_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_manifest_urls, {});
    arch::ntp_archiver archiver(get_ntp_conf(), get_configuration());
    archiver.download_manifest().get();
    auto expected = load_manifest(manifest_payload);
    BOOST_REQUIRE(expected == archiver.get_remote_manifest()); // NOLINT
}

struct fixture : s3_imposter_fixture, archiver_storage_fixture {};

FIXTURE_TEST(test_upload_segments, fixture) { // NOLINT
    set_expectations_and_listen(default_manifest_urls, default_segment_urls);
    arch::ntp_archiver archiver(get_ntp_conf(), get_configuration());

    std::vector<segment_desc> segments = {
        {manifest_ntp, model::offset(1), model::term_id(2)},
        {manifest_ntp, model::offset(3), model::term_id(4)},
    };
    this->init_storage_api(segments);

    vlog(test_log.trace, "update local manifest");
    archiver.update_local_manifest(get_local_api().log_mgr());
    archiver.upload_next_candidate(2, get_local_api().log_mgr()).get();

    BOOST_REQUIRE_EQUAL(get_requests().size(), 3);
    bool manifest_found = false;
    std::vector<ss::sstring> segment_urls;
    for (const auto& req : get_requests()) {
        auto url = req.get_url();
        vlog(test_log.trace, "s3 url: {}", url);
        if (boost::ends_with(url, "manifest.json")) {
            verify_manifest_content(req.content);
            manifest_found = true;
        } else {
            url = url.substr(url.size() - std::strlen("1-2-v1.log"));
            verify_segment(manifest_ntp, arch::segment_name(url), req.content);
            segment_urls.push_back(std::move(url));
        }
    }
    std::sort(segment_urls.begin(), segment_urls.end());
    BOOST_REQUIRE(manifest_found); // NOLINT
    BOOST_REQUIRE_EQUAL(segment_urls.size(), 2);
    BOOST_REQUIRE_EQUAL(segment_urls[0], "1-2-v1.log");
    BOOST_REQUIRE_EQUAL(segment_urls[1], "3-4-v1.log");
}