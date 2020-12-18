#pragma once

#include <gnutls/crypto.h>

#include <system_error>

namespace s3 {

std::error_code make_error_code(int ec) noexcept;

enum class s3_error_codes : int {
    invalid_uri,
    invalid_uri_params,
    not_enough_arguments,
};

std::error_code make_error_code(s3_error_codes ec) noexcept;

} // namespace s3
