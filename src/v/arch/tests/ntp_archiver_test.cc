/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/logger.h"
#include "arch/manifest.h"
#include "arch/ntp_archiver_service.h"
#include "arch/tests/ntp_archiver_fixture.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "s3/client.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "storage/disk_log_impl.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

inline ss::logger test_log("test"); // NOLINT

static const uint16_t httpd_port_number = 4430;
static constexpr const char* httpd_host_name = "127.0.0.1";
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

static storage::ntp_config get_ntp_conf() {
    return storage::ntp_config(manifest_ntp, "base-dir");
}

static arch::manifest load_manifest(std::string_view v) {
    arch::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

/// Compare two json objects logically by parsing them first and then going
/// through fields
static bool
compare_json_objects(const std::string_view& lhs, const std::string_view& rhs) {
    namespace pt = boost::property_tree;
    auto parse = [](const std::string_view json) -> pt::ptree {
        std::stringstream str;
        str << json;
        pt::ptree tree;
        pt::json_parser::read_json(str, tree);
        return tree;
    };
    auto tostr = [](const pt::ptree& p) {
        std::stringstream str;
        pt::write_json(str, p, false);
        return str.str();
    };
    auto a = parse(lhs);
    auto b = parse(rhs);
    if (a != b) {
        vlog(
          test_log.error,
          "json objects are not equal, A: \n{}B:\n{}\n",
          tostr(a),
          tostr(b));
        return false;
    }
    return true;
}

SEASTAR_TEST_CASE(test_download_manifest) { // NOLINT
    return ss::async([] {
        auto conf = get_configuration();
        auto f = started_fixture(
          get_ntp_conf(), conf, default_manifest_urls, {});
        f.archiver->download_manifest().get();
        auto expected = load_manifest(manifest_payload);
        BOOST_REQUIRE(expected == f.archiver->get_remote_manifest()); // NOLINT
        f.server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_upload_manifest) { // NOLINT
    return ss::async([] {
        auto conf = get_configuration();
        auto f = started_fixture(
          get_ntp_conf(), conf, default_manifest_urls, {});
        auto pm = const_cast<arch::manifest*>( // NOLINT
          &f.archiver->get_remote_manifest());
        pm->add(
          arch::segment_name("1-2-v1.log"),
          {.is_compacted = false,
           .size_bytes = 100, // NOLINT
           .base_offset = model::offset(1),
           .committed_offset = model::offset(2),
           .is_deleted_locally = false

          });
        pm->add(
          arch::segment_name("3-4-v1.log"),
          {.is_compacted = false,
           .size_bytes = 200, // NOLINT
           .base_offset = model::offset(3),
           .committed_offset = model::offset(4),
           .is_deleted_locally = false

          });
        f.archiver->upload_manifest().get();
        auto req = f.requests.front();
        // NOLINTNEXTLINE
        BOOST_REQUIRE(compare_json_objects(req.content, manifest_payload));
        f.server->stop().get();
    });
}

// Test with 'storage::log_manager' as a source of manifest data

SEASTAR_TEST_CASE(test_scan_logmanager) { // NOLINT
    return ss::async([] {
        ss::tmp_dir dir = ss::make_tmp_dir("/tmp/arch-test-XXXX").get0();
        auto remove_tmp_dir = ss::defer([&dir] { dir.remove().get(); });
        std::vector<segment_desc> segments = {
          {manifest_ntp, model::offset(1), model::term_id(2)},
          {manifest_ntp, model::offset(3), model::term_id(4)},
        };
        auto conf = get_configuration();
        auto f = started_fixture(
          get_ntp_conf(), conf, default_manifest_urls, {});
        f.init_storage_api(dir.get_path(), segments);
        auto stop = ss::defer([&f] { f.stop(); });
        vlog(test_log.trace, "update local manifest");
        f.archiver->update_local_manifest(f.api->log_mgr());
        std::stringstream json;
        f.archiver->get_local_manifest().serialize(json);
        vlog(test_log.trace, "local manifest: {}", json.str());
        f.verify_local_manifest();
        f.server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_upload_segments) { // NOLINT
    return ss::async([] {
        ss::tmp_dir dir = ss::make_tmp_dir("/tmp/arch-test-XXXX").get0();
        auto remove_tmp_dir = ss::defer([&dir] { dir.remove().get(); });
        std::vector<segment_desc> segments = {
          {manifest_ntp, model::offset(1), model::term_id(2)},
          {manifest_ntp, model::offset(3), model::term_id(4)},
        };
        auto conf = get_configuration();
        auto f = started_fixture(
          get_ntp_conf(), conf, default_manifest_urls, default_segment_urls);
        f.init_storage_api(dir.get_path(), segments);
        auto stop_api = ss::defer([&f] { f.stop(); });
        vlog(test_log.trace, "update local manifest");
        f.archiver->update_local_manifest(f.api->log_mgr());
        f.archiver->upload_next_candidate(2, f.api->log_mgr()).get();
        BOOST_REQUIRE_EQUAL(f.requests.size(), 3);
        bool manifest_found = false;
        std::vector<ss::sstring> segment_urls;
        for (const auto& req : f.requests) {
            auto url = req.get_url();
            vlog(test_log.trace, "s3 url: {}", url);
            if (boost::ends_with(url, "manifest.json")) {
                f.verify_manifest_content(req.content);
                manifest_found = true;
            } else {
                url = url.substr(url.size() - std::strlen("1-2-v1.log"));
                f.verify_segment(arch::segment_name(url), req.content);
                segment_urls.push_back(std::move(url));
            }
        }
        std::sort(segment_urls.begin(), segment_urls.end());
        BOOST_REQUIRE(manifest_found); // NOLINT
        BOOST_REQUIRE_EQUAL(segment_urls.size(), 2);
        BOOST_REQUIRE_EQUAL(segment_urls[0], "1-2-v1.log");
        BOOST_REQUIRE_EQUAL(segment_urls[1], "3-4-v1.log");
    });
}