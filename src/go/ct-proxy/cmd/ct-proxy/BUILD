load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "ct-proxy",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/cmd/ct-proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/ct-proxy/pkg/config",
        "//src/go/ct-proxy/pkg/proxy",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
