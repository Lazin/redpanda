load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "proxy",
    srcs = [
        "consumer.go",
        "metadata.go",
        "producer.go",
        "server.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/ct-proxy/pkg/admin",
        "//src/go/ct-proxy/pkg/config",
        "//src/go/ct-proxy/pkg/l0",
        "//src/go/ct-proxy/pkg/storage",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "proxy_test",
    srcs = [
        "producer_test.go",
        "server_test.go",
    ],
    embed = [":proxy"],
)
