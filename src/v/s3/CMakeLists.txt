
v_cc_library(
  NAME s3
  SRCS
    signature.cc
    logger.cc
    error.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
)
add_subdirectory(tests)
