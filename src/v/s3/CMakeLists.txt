
v_cc_library(
  NAME s3
  SRCS
    signature.cc
    client.cc
    logger.cc
    error.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
)

add_subdirectory(test_client)
add_subdirectory(tests)
