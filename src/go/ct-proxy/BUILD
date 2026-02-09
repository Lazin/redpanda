load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "ct-proxy",
    embed = [":ct-proxy_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "ct-proxy_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy",
    visibility = ["//visibility:private"],
    deps = [
        "//src/go/ct-proxy/cmd/ct-proxy",
    ],
)
