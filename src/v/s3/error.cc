#include "s3/error.h"

namespace s3 {

class gnutls_error_category final : public std::error_category {
public:
    const char* name() const noexcept final { return "s3.gnutls"; }

    std::string message(int ec) const final { return gnutls_strerror_name(ec); }
};

class s3_error_category final : public std::error_category {
public:
    const char* name() const noexcept final { return "s3"; }

    std::string message(int ec) const final {
        std::string result = "unknown";
        switch (static_cast<s3_error_codes>(ec)) {
        case s3_error_codes::invalid_uri:
            result = "Target URI shouldn't be empty or include domain name";
            break;
        case s3_error_codes::invalid_uri_params:
            result = "Target URI contains invalid query parameters";
            break;
        case s3_error_codes::not_enough_arguments:
            result = "Can't make request, not enough arguments";
            break;
        }
        return result;
    }
};

std::error_code make_error_code(int ec) noexcept {
    static gnutls_error_category ecat;
    return {ec, ecat};
}

std::error_code make_error_code(s3_error_codes ec) noexcept {
    static s3_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

} // namespace s3