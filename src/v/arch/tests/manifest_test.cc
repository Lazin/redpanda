// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "arch/manifest.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

static constexpr const char* manifest_json = R"json({
    "ns": "test-ns",
    "tp": "test-topic",
    "pt": "42"
})json";
static const size_t manifest_size_bytes = std::strlen(manifest_json);
static const model::ntp manifest_ntp(
  model::ns("test-ns"), model::topic("test-topic"), model::partition_id(42));

inline ss::input_stream<char> make_manifest_stream() {
    iobuf i;
    i.append(manifest_json, manifest_size_bytes);
    return make_iobuf_input_stream(std::move(i));
}

SEASTAR_TEST_CASE(test_manifest_path) {
    return ss::async([] { 
        arch::manifest m(manifest_ntp);
        auto path = m.get_object_name();
        BOOST_REQUIRE_EQUAL(path, "redpanda/meta/test-ns/test-topic/42/manifest.json");
    });
}

SEASTAR_TEST_CASE(test_manifest_update) {
    return ss::async([] { 
        arch::manifest m;
        m.update(make_manifest_stream()).get0();
        auto path = m.get_object_name();
        BOOST_REQUIRE_EQUAL(path, "redpanda/meta/test-ns/test-topic/42/manifest.json");
    });
}

SEASTAR_TEST_CASE(test_manifest_serialization) {
    return ss::async([] { 
        arch::manifest m(manifest_ntp);
        m.add(arch::remote_segment_name("foo"));
        m.add(arch::remote_segment_name("bar"));
        m.serialize(std::cout, true);
        auto is = m.serialize();
        iobuf buf;
        auto os = make_iobuf_output_stream(buf);
        ss::copy(is, os).get();

        auto rstr = make_iobuf_input_stream(std::move(buf));
        arch::manifest restored;
        restored.update(std::move(rstr)).get0();
        restored.serialize(std::cout, true);

        BOOST_REQUIRE(m == restored);
    });
}
