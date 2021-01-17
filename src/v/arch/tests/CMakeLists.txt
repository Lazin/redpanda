
rp_test(
  UNIT_TEST
  BINARY_NAME test_arch_service
  SOURCES manifest_test.cc ntp_archiver_test.cc ntp_archiver_fixture.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::arch v::storage_test_utils
  ARGS "-- -c 1"
)


# second iteration of the test-suite
rp_test(
  UNIT_TEST
  BINARY_NAME test_arch_service2
  SOURCES service_test.cc service_fixture.cc
  LIBRARIES v::seastar_testing_main v::application Boost::unit_test_framework v::arch v::storage_test_utils
)

