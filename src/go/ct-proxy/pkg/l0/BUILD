load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "l0",
    srcs = [
        "errors.go",
        "object.go",
        "placeholder.go",
        "serializer.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_google_uuid//:uuid",
    ],
)

go_test(
    name = "l0_test",
    srcs = [
        "object_test.go",
        "placeholder_test.go",
        "serializer_test.go",
    ],
    embed = [":l0"],
    deps = [
        "@com_github_google_uuid//:uuid",
    ],
)
