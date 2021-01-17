/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/tests/ntp_archiver_fixture.h"

#include "arch/logger.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "random/generators.h"
#include "s3/client.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "storage/disk_log_impl.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals;

inline ss::logger test_log("test"); // NOLINT

static constexpr uint16_t httpd_port_number = 4430;
static constexpr const char* httpd_host_name = "127.0.0.1";

static arch::manifest load_manifest(std::string_view v) {
    arch::manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

static void
write_random_batches(ss::lw_shared_ptr<storage::segment> seg) { // NOLINT
    auto batches = storage::test::make_random_batches(
      seg->offsets().base_offset + model::offset(1), 1);
    vlog(test_log.trace, "num batches {}", batches.size());
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        auto res = seg->append(std::move(b)).get0();
        vlog(test_log.trace, "last-offset {}", res.last_offset);
    }
    seg->flush().get();
}

arch::configuration get_configuration() {
    ss::ipv4_addr ip_addr = {httpd_host_name, httpd_port_number};
    ss::socket_address server_addr(ip_addr);
    s3::configuration s3conf{
      .uri = s3::access_point_uri(httpd_host_name),
      .access_key = s3::public_key_str("acess-key"),
      .secret_key = s3::private_key_str("secret-key"),
      .region = s3::aws_region_name("us-east-1"),
    };
    s3conf.server_addr = server_addr;
    arch::configuration conf;
    conf.client_config = s3conf;
    conf.bucket_name = s3::bucket_name("test-bucket");
    return conf;
}

ntp_archiver_fixture started_fixture(
  storage::ntp_config ntpc,
  const arch::configuration& conf,
  const std::vector<std::pair<ss::sstring, ss::sstring>>& manifests_url_reply,
  const std::vector<ss::sstring>& segments_url) {
    auto ntp = ntpc.ntp();
    auto archiver = ss::make_shared<arch::ntp_archiver>(std::move(ntpc), conf);
    auto server = ss::make_shared<ss::httpd::http_server_control>();
    ntp_archiver_fixture result = {
      .server = server,
      .archiver = archiver,
      .archiver_ntp = ntp,
    };
    server->start().get();
    server
      ->set_routes(
        [&result, &manifests_url_reply, &segments_url](ss::httpd::routes& r) {
            result.set_routes(r, manifests_url_reply, segments_url);
        })
      .get();
    server->listen(conf.client_config.server_addr).get();
    return result;
}

void ntp_archiver_fixture::set_routes(
  ss::httpd::routes& r,
  const std::vector<std::pair<ss::sstring, ss::sstring>>& manifest_url_reply,
  const std::vector<ss::sstring>& segment_urls) {
    using namespace ss::httpd;
    auto manifest_put_handler = [this](const_req req) {
        requests.push_back(req);
        vlog(test_log.trace, "put_response invoked");
        return "";
    };
    for (auto [murl, reply] : manifest_url_reply) {
        auto manifest_get_handler = [this,
                                     reply = std::move(reply)](const_req req) {
            requests.push_back(req);
            vlog(test_log.trace, "get_response invoked");
            return reply;
        };
        auto put_handler = new function_handler(manifest_put_handler, "txt");
        auto get_handler = new function_handler(manifest_get_handler, "txt");
        r.add(operation_type::PUT, url(murl), put_handler);
        r.add(operation_type::GET, url(murl), get_handler);
    }
    auto segment_handler = [this](const_req req) {
        requests.push_back(req);
        vlog(test_log.trace, "put_response url {}", req.get_url());
        vlog(test_log.trace, "put_response invoked {}", req.content_length);
        return "";
    };
    for (const auto& surl : segment_urls) {
        auto put_segment_handler = new function_handler(segment_handler, "txt");
        r.add(operation_type::PUT, url(surl), put_segment_handler);
    }
}

void ntp_archiver_fixture::stop() {
    api->stop().get();
    server->stop().get();
}

/// Create a working directory with a bunch of segments
void ntp_archiver_fixture::init_storage_api(
  const std::filesystem::path& workdir, std::vector<segment_desc>& segm) {
    storage::log_config log_config{
      storage::log_config::storage_type::disk,
      workdir.string(),
      1_KiB,
      storage::debug_sanitize_files::yes};

    api = ss::make_shared<storage::api>(
      storage::kvstore_config(
        1_MiB, 1ms, log_config.base_dir, storage::debug_sanitize_files::yes),
      log_config);
    api->start().get();
    absl::flat_hash_map<model::ntp, size_t> all_ntp;
    for (auto& d : segm) {
        vlog(test_log.trace, "make_log_segment");
        storage::ntp_config ntpc(d.ntp, workdir.string());
        storage::directories::initialize(ntpc.work_directory()).get();
        auto seg = api->log_mgr()
                     .make_log_segment(
                       storage::ntp_config(d.ntp, workdir.string()),
                       d.base_offset,
                       d.term,
                       ss::default_priority_class())
                     .get0();
        write_random_batches(seg);
        vlog(test_log.trace, "segment close");
        seg->close().get();
        all_ntp[d.ntp] += 1;
    }
    for (const auto& ntp : all_ntp) {
        vlog(test_log.trace, "manage");
        api->log_mgr()
          .manage(storage::ntp_config(ntp.first, workdir.string()))
          .get();
        BOOST_CHECK_EQUAL(
          api->log_mgr().get(ntp.first)->segment_count(), ntp.second);
    }
    BOOST_CHECK_EQUAL(all_ntp.size(), api->log_mgr().size());
}

std::vector<ss::lw_shared_ptr<storage::segment>>
ntp_archiver_fixture::list_segments() const {
    std::vector<ss::lw_shared_ptr<storage::segment>> result;
    auto log = api->log_mgr().get(archiver_ntp);
    if (auto dlog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        dlog) {
        std::copy(
          dlog->segments().begin(),
          dlog->segments().end(),
          std::back_inserter(result));
    }
    return result;
}

ss::lw_shared_ptr<storage::segment>
ntp_archiver_fixture::get_segment(const arch::segment_name& name) const {
    auto log = api->log_mgr().get(archiver_ntp);
    if (auto dlog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        dlog) {
        for (const auto& s : dlog->segments()) {
            if (boost::ends_with(s->reader().filename(), name())) {
                return s;
            }
        }
    }
    return nullptr;
}

void ntp_archiver_fixture::verify_segment(
  const arch::segment_name& name, const ss::sstring& expected) {
    auto segment = get_segment(name);
    auto pos = segment->offsets().base_offset;
    auto size = segment->size_bytes();
    auto stream = segment->offset_data_stream(
      pos, ss::default_priority_class());
    auto tmp = stream.read_exactly(size).get0();
    ss::sstring actual = {tmp.get(), tmp.size()};
    vlog(
      test_log.error,
      "expected {} bytes, got {}",
      expected.size(),
      actual.size());
    BOOST_REQUIRE(actual == expected); // NOLINT
}

void ntp_archiver_fixture::verify_manifest(const arch::manifest& man) {
    auto all_segments = list_segments();
    BOOST_REQUIRE_EQUAL(all_segments.size(), man.size());
    for (const auto& s : all_segments) {
        auto sname = arch::segment_name(
          std::filesystem::path(s->reader().filename()).filename().string());
        auto base = s->offsets().base_offset;
        auto comm = s->offsets().committed_offset;
        auto size = s->size_bytes();
        auto comp = s->is_compacted_segment();
        auto meta = man.get(sname);
        BOOST_REQUIRE(meta.has_value()); // NOLINT
        auto m = (*meta).get();
        BOOST_REQUIRE_EQUAL(base, m.base_offset);
        BOOST_REQUIRE_EQUAL(comm, m.committed_offset);
        BOOST_REQUIRE_EQUAL(size, m.size_bytes);
        BOOST_REQUIRE_EQUAL(comp, m.is_compacted);
        BOOST_REQUIRE_EQUAL(false, m.is_deleted_locally);
    }
}

void ntp_archiver_fixture::verify_manifest_content(
  const ss::sstring& manifest_content) {
    arch::manifest remote = load_manifest(manifest_content);
    verify_manifest(remote);
}

void ntp_archiver_fixture::verify_local_manifest() {
    verify_manifest(archiver->get_local_manifest());
}