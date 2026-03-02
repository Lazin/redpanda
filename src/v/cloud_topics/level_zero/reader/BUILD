load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/reader:__pkg__",
    "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
])

redpanda_cc_library(
    name = "l0_object_cache",
    hdrs = [
        "l0_object_cache.h",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_l0_cache",
    hdrs = [
        "memory_l0_cache.h",
    ],
    deps = [
        "//src/v/cloud_topics/batch_cache:raw_object_cache",
        "//src/v/cloud_topics/level_zero/reader:l0_object_cache",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "disk_l0_cache",
    srcs = [
        "disk_l0_cache.cc",
    ],
    hdrs = [
        "disk_l0_cache.h",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/reader:l0_object_cache",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "materialized_extent",
    srcs = [
        "materialized_extent.cc",
    ],
    hdrs = [
        "materialized_extent.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/reader:l0_object_cache",
        "//src/v/model",
        "//src/v/storage:record_batch_utils",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "materialized_extent_reader",
    srcs = [
        "materialized_extent_reader.cc",
    ],
    hdrs = [
        "materialized_extent_reader.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/reader:l0_object_cache",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fetch_handler",
    srcs = [
        "fetch_request_handler.cc",
    ],
    hdrs = [
        "fetch_request_handler.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/cloud_topics/level_zero/reader:l0_object_cache",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent_reader",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
