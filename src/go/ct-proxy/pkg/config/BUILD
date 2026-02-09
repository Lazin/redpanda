load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config",
    visibility = ["//visibility:public"],
    deps = [
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    embed = [":config"],
)
