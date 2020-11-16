function(credgen)
  cmake_parse_arguments(args "" "OUT_DIR;OUTPUT_FILES" "" ${ARGN})
  set(generator "${PROJECT_SOURCE_DIR}/tools/credgen.py")

  set(OUTPUT_CERTS
    ${args_OUT_DIR}/redpanda.crt
    ${args_OUT_DIR}/redpanda.key
    ${args_OUT_DIR}/root_certificate_authority.chain_cert)

  add_custom_command(
    DEPENDS ${generator}
    OUTPUT ${OUTPUT_CERTS}
    COMMAND ${generator} --dir ${args_OUT_DIR})

  set(OUTPUT_CERTS_OTHER
    ${args_OUT_DIR}/redpanda.other.crt
    ${args_OUT_DIR}/redpanda.other.key
    ${args_OUT_DIR}/root_certificate_authority.other.chain_cert)

  add_custom_command(
    DEPENDS ${generator}
    OUTPUT ${OUTPUT_CERTS_OTHER}
    COMMAND ${generator} --tag other --dir ${args_OUT_DIR})

  set(${args_OUTPUT_FILES} ${OUTPUT_CERTS} ${OUTPUT_CERTS_OTHER} PARENT_SCOPE)

  add_custom_target(credgen_run ALL
    DEPENDS ${OUTPUT_CERTS} ${OUTPUT_CERTS_OTHER})

endfunction ()