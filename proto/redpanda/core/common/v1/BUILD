load("@rules_go//proto:def.bzl", "go_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "acl_proto",
    srcs = ["acl.proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "acl_redpanda_proto",
    protos = [":acl_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "ntp_proto",
    srcs = ["ntp.proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "ntp_redpanda_proto",
    protos = [":ntp_proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "ntp_go_proto",
    compilers = ["@rules_go//proto:go_proto"],
    importpath = "github.com/redpanda-data/redpanda/proto/redpanda/core/common/v1",
    protos = [":ntp_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_go_proto",
    ],
)

proto_library(
    name = "tls_proto",
    srcs = ["tls.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "@googleapis//google/api:field_behavior_proto",
    ],
)

redpanda_proto_library(
    name = "tls_redpanda_proto",
    protos = [":tls_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "security_types_proto",
    srcs = ["security_types.proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "security_types_redpanda_proto",
    protos = [":security_types_proto"],
    visibility = ["//visibility:public"],
)
