load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "storage",
    srcs = ["s3.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/storage",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/ct-proxy/pkg/config",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/s3",
    ],
)

go_test(
    name = "storage_test",
    srcs = ["s3_test.go"],
    embed = [":storage"],
)
