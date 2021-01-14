
v_cc_library(
  NAME arch
  SRCS
    service.cc
    manifest.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
)
add_subdirectory(tests)
