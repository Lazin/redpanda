load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "metastore_proto",
    srcs = ["metastore.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "metastore_redpanda_proto",
    protos = [":metastore_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_redpanda_proto",
    ],
)

proto_library(
    name = "level_zero_gc_proto",
    srcs = ["level_zero_gc.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "level_zero_gc_redpanda_proto",
    protos = [":level_zero_gc_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_redpanda_proto",
    ],
)

proto_library(
    name = "ct_proxy_proto",
    srcs = ["ct_proxy.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "ct_proxy_redpanda_proto",
    protos = [":ct_proxy_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_redpanda_proto",
    ],
)

go_proto_library(
    name = "ct_proxy_go_proto",
    compilers = ["@rules_go//proto:go_grpc"],
    importpath = "github.com/redpanda-data/redpanda/proto/redpanda/core/admin/internal/cloud_topics/v1",
    protos = [":ct_proxy_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_go_proto",
        "//proto/redpanda/core/pbgen:options_go_proto",
        "//proto/redpanda/core/pbgen:rpc_go_proto",
    ],
)
