load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "level_zero_reader_impl",
    srcs = [
        "level_zero_reader_impl.cc",
    ],
    hdrs = [
        "level_zero_reader_impl.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cluster",
        "//src/v/config",
    ],
    include_prefix = "cloud_topics/frontend",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:extent_meta",
        "//src/v/model",
        "//src/v/storage",
        "@boost//:outcome",
        "@seastar",
    ],
)
