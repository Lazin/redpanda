load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest", "redpanda_test_cc_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "aip_test_message_proto",
    srcs = ["aip_test_message.proto"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:timestamp_proto",
    ],
)

redpanda_proto_library(
    name = "aip_test_message_redpanda_proto",
    protos = [":aip_test_message_proto"],
    visibility = ["//visibility:public"],
    deps = ["@abseil-cpp//absl/time:time"],
)

redpanda_test_cc_library(
    name = "aip_test_message_helpers",
    hdrs = ["aip_test_message_helpers.h"],
    deps = [
        ":aip_test_message_redpanda_proto",
        "@abseil-cpp//absl/time",
    ],
)

redpanda_cc_gtest(
    name = "aip_filter_test",
    timeout = "short",
    srcs = [
        "aip_filter_test.cc",
    ],
    deps = [
        ":aip_test_message_helpers",
        ":aip_test_message_redpanda_proto",
        "//src/v/redpanda/admin:aip_filter",
        "//src/v/serde/protobuf:rpc",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "aip_ordering_test",
    timeout = "short",
    srcs = [
        "aip_ordering_test.cc",
    ],
    deps = [
        ":aip_test_message_helpers",
        ":aip_test_message_redpanda_proto",
        "//src/v/redpanda/admin:aip_ordering",
        "//src/v/serde/protobuf:rpc",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "ct_proxy_pipeline_test",
    timeout = "short",
    srcs = ["ct_proxy_pipeline_test.cc"],
    cpu = 1,
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:ct_proxy_redpanda_proto",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_storage",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:serializer",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/model",
        "//src/v/model:batch_builder",
        "//src/v/redpanda/admin/services/internal:ct_proxy_service",
        "//src/v/redpanda/tests:fixture",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "list_kafka_connections_rpbench",
    timeout = "long",
    srcs = [
        "list_kafka_connections_bench.cc",
    ],
    deps = [
        "//proto/redpanda/core/admin/v2:cluster_redpanda_proto",
        "//proto/redpanda/core/admin/v2:kafka_connections_redpanda_proto",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/protocol",
        "//src/v/random:generators",
        "//src/v/redpanda/admin:aip_ordering",
        "//src/v/redpanda/admin:kafka_connections_service_impl",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:async_clear",
        "//src/v/ssx:async_merge_top_k",
        "@boost//:uuid",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
