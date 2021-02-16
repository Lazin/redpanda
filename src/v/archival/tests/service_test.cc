/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "archival/service.h"
#include "archival/tests/service_fixture.h"
#include "cluster/commands.h"
#include "storage/types.h"
#include "utils/unresolved_address.h"

#include <seastar/core/deleter.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <boost/algorithm/string.hpp>

#include <type_traits>

inline seastar::logger arch_svc_log("SVC-TEST");
static const model::ns test_ns = model::ns("test-namespace");
using namespace std::chrono_literals;

FIXTURE_TEST(test_reconciliation_manifest_download, archiver_fixture) {
    wait_for_controller_leadership().get();
    auto topic1 = model::topic("topic_1");
    auto topic2 = model::topic("topic_2");
    auto pid0 = model::ntp(test_ns, topic1, model::partition_id(0));
    auto pid1 = model::ntp(test_ns, topic2, model::partition_id(0));
    std::array<const char*, 2> urls = {
      "/f0000000/meta/test-namespace/topic_1/0_2/manifest.json",
      "/80000000/meta/test-namespace/topic_2/0_4/manifest.json",
    };
    ss::sstring manifest_json = R"json({
        "namespace": "test-namespace",
        "topic": "test_1",
        "partition": 0,
        "revision": 1,
        "segments": {
            "1-1-v1.log": { 
                "is_compacted": false,
                "size_bytes": 10,
                "base_offset": 1,
                "committed_offset": 2,
                "deleted": false
            }
        }
    })json";
    set_expectations_and_listen({
      {.url = urls[0], .body = manifest_json},
      {.url = urls[1], .body = std::nullopt},
    });

    add_topic_with_random_data(pid0, 20);
    add_topic_with_random_data(pid1, 20);
    wait_for_partition_leadership(pid0);
    wait_for_partition_leadership(pid1);

    auto config = get_configuration();
    auto& pm = app.partition_manager;
    auto& api = app.storage;
    archival::internal::scheduler_service_impl service(config, api, pm);
    service.reconcile_archivers().get();
    BOOST_REQUIRE(service.contains(pid0));
    BOOST_REQUIRE(service.contains(pid1));
}

FIXTURE_TEST(test_reconciliation_drop_ntp, archiver_fixture) {
    wait_for_controller_leadership().get();

    auto topic = model::topic("topic_2");
    auto ntp = model::ntp(test_ns, topic, model::partition_id(0));

    const char* url = "/a0000000/meta/test-namespace/topic_2/0_2/manifest.json";
    set_expectations_and_listen({
      {.url = url, .body = std::nullopt},
    });

    add_topic_with_random_data(ntp, 20);

    wait_for_partition_leadership(ntp);

    auto config = get_configuration();
    auto& pm = app.partition_manager;
    auto& api = app.storage;
    archival::internal::scheduler_service_impl service(config, api, pm);
    service.reconcile_archivers().get();
    BOOST_REQUIRE(service.contains(ntp));

    // delete topic
    delete_topic(ntp.ns, ntp.tp.topic);

    wait_for_topic_deletion(ntp);

    service.reconcile_archivers().get();
    BOOST_REQUIRE(!service.contains(ntp));
}

FIXTURE_TEST(test_segment_upload, archiver_fixture) {
    wait_for_controller_leadership().get();

    auto topic = model::topic("topic_3");
    auto ntp = model::ntp(test_ns, topic, model::partition_id(0));

    const char* manifest_url
      = "/e0000000/meta/test-namespace/topic_3/0_2/manifest.json";
    const char* seg000 = "/83ae5e22/test-namespace/topic_3/0_2/0-0-v1.log";
    const char* seg100 = "/fdc53a14/test-namespace/topic_3/0_2/100-0-v1.log";
    set_expectations_and_listen({
      {.url = manifest_url, .body = std::nullopt},
      {.url = seg000, .body = std::nullopt},
      {.url = seg100, .body = std::nullopt},
    });

    auto builder = get_started_log_builder(ntp, model::revision_id(2));
    using namespace storage; // NOLINT
    (*builder) | add_segment(model::offset(0))
      | add_random_batch(model::offset(0), 100, maybe_compress_batches::no)
      | add_segment(model::offset(100))
      | add_random_batch(model::offset(100), 100, maybe_compress_batches::no)
      | stop();
    vlog(
      arch_svc_log.trace,
      "{} bytes written to log {}",
      builder->bytes_written(),
      ntp.path());
    builder.reset();
    add_topic(model::topic_namespace_view(ntp)).get();

    wait_for_partition_leadership(ntp);

    auto config = get_configuration();
    auto& pm = app.partition_manager;
    auto& api = app.storage;
    archival::internal::scheduler_service_impl service(config, api, pm);
    service.reconcile_archivers().get();
    BOOST_REQUIRE(service.contains(ntp));

    service.run_uploads().get();

    BOOST_REQUIRE(get_requests().size() == 4);
    auto get_manifest = get_requests()[0];
    BOOST_REQUIRE(get_manifest._method == "GET");

    auto put_manifest = get_requests()[3];
    BOOST_REQUIRE(put_manifest._method == "PUT");
    verify_manifest_content(put_manifest.content);

    BOOST_REQUIRE(get_targets().count(seg000) == 1);
    auto put_seg000 = get_targets().find(seg000);
    BOOST_REQUIRE(put_seg000->second._method == "PUT");
    verify_segment(
      ntp, archival::segment_name("0-0-v1.log"), put_seg000->second.content);

    BOOST_REQUIRE(get_targets().count(seg100) == 1);
    auto put_seg100 = get_targets().find(seg100);
    BOOST_REQUIRE(put_seg100->second._method == "PUT");
    verify_segment(
      ntp, archival::segment_name("100-0-v1.log"), put_seg100->second.content);

    // Run uploads second time, since there is no new data nothing should be
    // sent to s3 imposter
    service.run_uploads().get();
    BOOST_REQUIRE(get_requests().size() == 4);
}

// Upload segments and then run clean up procedure to delete them
FIXTURE_TEST(test_segment_delete, archiver_fixture) {
    wait_for_controller_leadership().get();

    auto topic = model::topic("topic_4");
    auto ntp = model::ntp(test_ns, topic, model::partition_id(0));

    const char* manifest_url
      = "/c0000000/meta/test-namespace/topic_4/0_2/manifest.json";
    const char* seg000 = "/b11b83a3/test-namespace/topic_4/0_2/0-0-v1.log";
    const char* seg100 = "/c9426ce1/test-namespace/topic_4/0_2/100-0-v1.log";
    set_expectations_and_listen({
      {.url = manifest_url, .body = std::nullopt},
      {.url = seg000, .body = std::nullopt},
      {.url = seg100, .body = std::nullopt},
    });

    auto builder = get_started_log_builder(ntp, model::revision_id(2));
    using namespace storage; // NOLINT
    (*builder) | add_segment(model::offset(0))
      | add_random_batch(model::offset(0), 100, maybe_compress_batches::no)
      | add_segment(model::offset(100))
      | add_random_batch(model::offset(100), 100, maybe_compress_batches::no)
      | stop();
    vlog(
      arch_svc_log.trace,
      "{} bytes written to log {}",
      builder->bytes_written(),
      ntp.path());
    builder.reset();
    add_topic(model::topic_namespace_view(ntp)).get();

    wait_for_partition_leadership(ntp);

    auto config = get_configuration();
    auto& pm = app.partition_manager;
    auto& api = app.storage;
    archival::internal::scheduler_service_impl service(config, api, pm);
    service.reconcile_archivers().get();
    BOOST_REQUIRE(service.contains(ntp));

    service.run_uploads().get();

    BOOST_REQUIRE(get_requests().size() == 4);
    auto get_manifest = get_requests()[0];
    BOOST_REQUIRE(get_manifest._method == "GET");

    auto put_manifest = get_requests()[3];
    BOOST_REQUIRE(put_manifest._method == "PUT");
    verify_manifest_content(put_manifest.content);

    BOOST_REQUIRE(get_targets().count(seg000) == 1);
    auto put_seg000 = get_targets().find(seg000);
    BOOST_REQUIRE(put_seg000->second._method == "PUT");
    verify_segment(
      ntp, archival::segment_name("0-0-v1.log"), put_seg000->second.content);

    BOOST_REQUIRE(get_targets().count(seg100) == 1);
    auto put_seg100 = get_targets().find(seg100);
    BOOST_REQUIRE(put_seg100->second._method == "PUT");
    verify_segment(
      ntp, archival::segment_name("100-0-v1.log"), put_seg100->second.content);

    // Forcibly remove log files, this should trigger deletion
    // of the log segments in S3 (with default policy being used).
    // 0-0-v1.log will be removed.
    storage::truncate_prefix_config cfg(
      model::offset(100), ss::default_priority_class());
    app.storage.local().log_mgr().get(ntp)->truncate_prefix(cfg).get();

    service.run_cleanup().get();

    BOOST_REQUIRE(get_requests().size() == 6);
    BOOST_REQUIRE(get_targets().count(seg000) == 2);
    BOOST_REQUIRE(get_targets().count(seg100) == 1);
    auto itrange = get_targets().equal_range(seg000);
    auto it = std::find_if(
      itrange.first,
      itrange.second,
      [](const std::pair<ss::sstring, ss::httpd::request>& kv) {
          return kv.second._method == "DELETE";
      });
    BOOST_REQUIRE(it != itrange.second);
}
