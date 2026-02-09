load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "admin",
    srcs = ["client.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/admin",
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:ct_proxy_go_proto",
        "//src/go/ct-proxy/pkg/config",
        "//src/go/ct-proxy/pkg/l0",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
    ],
)

go_test(
    name = "admin_test",
    srcs = ["client_test.go"],
    embed = [":admin"],
)
