#include "s3/signature.h"

#include "s3/error.h"
#include "s3/logger.h"

#include <seastar/core/sstring.hh>

#include <boost/algorithm/string/compare.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gnutls/crypto.h>
#include <gnutls/gnutls.h>

#include <memory>
#include <system_error>

namespace s3 {

// time_source //

time_source::time_source()
  : time_source(&default_source, 0) {}

time_source::time_source(timestamp instant)
  : time_source([instant]() { return instant; }, 0) {}

std::string time_source::format(const char* fmt) const {
    std::array<char, formatted_datetime_len> out_str{};
    auto point = _gettime();
    std::time_t time = std::chrono::system_clock::to_time_t(point);
    std::tm* gm = std::gmtime(&time);
    auto ret = std::strftime(out_str.data(), out_str.size(), fmt, gm);
    vassert(ret > 0, "Invalid date format string");
    return std::string(out_str.data(), ret);
}

std::string time_source::format_date() const { return format("%Y%m%d"); }

std::string time_source::format_datetime() const {
    return format("%Y%m%dT%H%M%SZ");
}

timestamp time_source::default_source() {
    return std::chrono::system_clock::now();
}

// signature_v4 //

enum {
    sha256_digest_length = 32,
};

using hmac_digest = std::array<char, sha256_digest_length>;

template<class Container>
static std::string hexdigest(Container const& digest) {
    std::array<uint8_t, sha256_digest_length> result{};
    std::memcpy(result.data(), digest.data(), digest.size());
    return to_hex(bytes_view{result.data(), result.size()});
}

static hmac_digest hmac(std::string_view key, std::string_view value) {
    hmac_digest digest{};
    int ec = gnutls_hmac_fast(
      GNUTLS_MAC_SHA256,
      key.data(),
      key.size(),
      value.data(),
      value.size(),
      digest.data());
    vassert(ec >= 0, "HMAC error '{}'", ec);
    return digest;
}

static result<std::string> sha_256(std::string_view str) {
    std::array<uint8_t, sha256_digest_length> hash{};
    int ec = gnutls_hash_fast(
      GNUTLS_DIG_SHA256,
      str.data(),
      str.size(),
      static_cast<void*>(hash.data()));
    if (ec < 0) {
        vlog(s3_log.error, "sha256 error '{}'", ec);
        return make_error_code(ec);
    }
    return to_hex(bytes_view{hash.data(), hash.size()});
}

result<std::string> signature_v4::gen_sig_key(
  std::string_view key,
  std::string_view datestr,
  std::string_view region,
  std::string_view service) {
    hmac_digest digest{};
    std::string initial_key = "AWS4";
    initial_key.append(key.data(), key.size());
    digest = hmac(std::string_view(initial_key), datestr);
    digest = hmac(std::string_view(digest.data(), digest.size()), region);
    digest = hmac(std::string_view(digest.data(), digest.size()), service);
    digest = hmac(
      std::string_view(digest.data(), digest.size()), "aws4_request");
    return std::string{digest.data(), digest.size()};
}

static void tolower(std::string& str) {
    for (auto& c : str) {
        c = std::tolower(c);
    }
}

static void append_hex_utf8(std::string& result, char ch) {
    bytes b = {static_cast<uint8_t>(ch)};
    result.push_back('%');
    result.append(to_hex(b));
}

static std::string uri_encode(std::string const& input, bool encode_slash) {
    // The function defined here:
    //     https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    std::string result;
    for (auto ch : input) {
        if (
          (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || (ch >= '0' && ch <= '9') || (ch == '_') || (ch == '-')
          || (ch == '~') || (ch == '.')) {
            result.push_back(ch);
        } else if (ch == '/') {
            if (encode_slash) {
                result.append("%2F");
            } else {
                result.push_back(ch);
            }
        } else {
            append_hex_utf8(result, ch);
        }
    }
    return result;
}

/// \brief Get canonical URI of the request
/// Canonical URI is everything that follows domain name starting with '/'
/// without parameters (everythng after '?' including '?'). The uri is uri
/// encoded. e.g. https://foo.bar/canonical-url?param=value
///
/// \param uri is a target of the http query (everything excluding the domain
/// name)
static result<std::string> get_canonical_uri(std::string target) {
    if (target.empty() || target[0] != '/') {
        vlog(s3_log.error, "invalid URI {}", target);
        return make_error_code(s3_error_codes::invalid_uri);
    }
    auto pos = target.find('?');
    if (pos != std::string::npos) {
        target.resize(pos);
    }
    return uri_encode(target, false);
}

/// CanonicalQueryString specifies the URI-encoded query string parameters.
/// You URI-encode name and values individually. You must also sort the
/// parameters in the canonical query string alphabetically by key name.
/// The sorting occurs after encoding. The query string in the following URI
/// example is prefix=somePrefix&marker=someMarker&max-keys=20:
/// "http://s3.amazonaws.com/examplebucket?prefix=somePrefix&marker=someMarker&max-keys=20"
///
/// The canonical query string is as follows (line breaks are added to this
/// example for readability):
///   UriEncode("marker")+"="+UriEncode("someMarker")+"&"+
///   UriEncode("max-keys")+"="+UriEncode("20") + "&" +
///   UriEncode("prefix")+"="+UriEncode("somePrefix")
///
/// \param target is a target of the http query (url - domain name)
static result<std::string> get_canonical_query_string(std::string target) {
    if (target.empty() || target[0] != '/') {
        vlog(s3_log.error, "invalid URI {}", target);
        return make_error_code(s3_error_codes::invalid_uri);
    }
    auto pos = target.find('?');
    if (pos == std::string::npos || pos == target.size() - 1) {
        return "";
    }
    auto query_str = target.substr(pos + 1);
    std::vector<std::string> params;
    boost::split(params, query_str, boost::is_any_of("&"));
    std::vector<std::pair<std::string, std::string>> query_params;
    for (auto const& param : params) {
        auto p = param.find('=');
        if (p == absl::string_view::npos) {
            // parameter with empty value
            query_params.emplace_back(param, "");
        } else {
            if (p == 0) {
                // parameter value can be empty but name can't
                return make_error_code(s3_error_codes::invalid_uri_params);
            }
            std::string pname = param.substr(0, p);
            std::string pvalue = param.substr(p + 1);
            query_params.emplace_back(pname, pvalue);
        }
    }
    std::sort(query_params.begin(), query_params.end());

    // Generate canonical query string
    std::string result;
    int cnt = 0;
    for (auto [pname, pvalue] : query_params) {
        if (cnt++ > 0) {
            result.push_back('&');
        }
        result += uri_encode(pname, true);
        result += "=";
        result += uri_encode(pvalue, true);
    }
    return result;
}

struct canonical_headers {
    std::string canonical_headers; // string that contains canonical headers
    std::string signed_headers;    // string that contains list of header names
};

/// CanonicalHeaders is a list of request headers with their values.
/// Individual header name and value pairs are separated by the newline
/// character ("\n"). Header names must be in lowercase. You must sort the
/// header names alphabetically to construct the string, as shown in the
/// following example:
///
///   Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n"
///   Lowercase(<HeaderName2>)+":"+Trim(<value>)+"\n"
///   ...
///   Lowercase(<HeaderNameN>)+":"+Trim(<value>)+"\n"
///
/// The CanonicalHeaders list must include the following:
///
///   - HTTP host header.
///   - If the Content-Type header is present in the request, you must add it to
///   the
///     CanonicalHeaders list.
///   - Any x-amz-* headers that you plan to include in your request must also
///   be added.
///     For example, if you are using temporary security credentials, you need
///     to include x-amz-security-token in your request. You must add this
///     header in the list of CanonicalHeaders.
///
///
static result<canonical_headers>
get_canonical_headers(const http::client::request_header& request) {
    std::vector<std::pair<std::string, std::string>> headers;
    for (const auto& it : request) {
        auto name = it.name_string();
        auto value = it.value();
        headers.emplace_back(name, value);
    }
    std::sort(headers.begin(), headers.end());
    std::string cheaders;
    std::string snames;
    int cnt = 0;
    for (auto [name, value] : headers) {
        tolower(name);
        boost::trim(value);
        cheaders += name;
        cheaders += ":";
        cheaders += value;
        cheaders += "\n";
        if (cnt++ > 0) {
            snames.push_back(';');
        }
        snames += name;
    }
    return canonical_headers{
      .canonical_headers = cheaders, .signed_headers = snames};
}

/// Create canonical request:
/// <HTTPMethod>\n
/// <CanonicalURI>\n
/// <CanonicalQueryString>\n
/// <CanonicalHeaders>\n
/// <SignedHeaders>\n
/// <HashedPayload>
static result<std::string> create_canonical_request(
  const canonical_headers& hdr,
  const http::client::request_header& header,
  const std::string& hashed_payload) {
    auto method = std::string(header.method_string());
    auto target = std::string(header.target());
    if (target.empty() || target.at(0) != '/') {
        target = "/" + target;
    }
    std::stringstream result;
    auto canonical_uri = get_canonical_uri(target);
    if (!canonical_uri) {
        return canonical_uri.error();
    }
    auto canonical_query = get_canonical_query_string(target);
    if (!canonical_query) {
        return canonical_query.error();
    }
    result << method << '\n';
    result << canonical_uri.value() << '\n';
    result << canonical_query.value() << '\n';
    result << hdr.canonical_headers << '\n';
    result << hdr.signed_headers << '\n';
    result << hashed_payload;
    return result.str();
}

/// Genertes string-to-sign (in spec terms), example:
///
///   "AWS4-HMAC-SHA256" + "\n" +
///   timeStampISO8601Format + "\n" +
///   <Scope> + "\n" +
///   Hex(SHA256Hash(<CanonicalRequest>))
///
/// \param timestamp is an ISO8601 format timestamp
/// \param is a scope string (date/region/service)
/// \param canonical_req is a canonical request
static result<std::string> get_string_to_sign(
  const std::string& timestamp,
  const std::string& scope,
  const std::string& canonical_req) {
    auto digest = sha_256(canonical_req);
    if (!digest) {
        vlog(s3_log.error, "string_to_sign error: {}", digest.error());
        return digest.error();
    }
    std::string_view algorithm = "AWS4-HMAC-SHA256";
    std::string string_to_sign = fmt::format(
      "{}\n{}\n{}\n{}", algorithm, timestamp, scope, digest.value());
    return string_to_sign;
}

std::string signature_v4::sha256_hexdigest(std::string_view payload) {
    return sha_256(payload).value();
}

std::error_code signature_v4::sign_header(
  http::client::request_header& header, const std::string& sha256) const {
    auto amz_date = _sig_time.format_datetime();
    header.set("x-amz-date", amz_date);
    header.set("x-amz-content-sha256", sha256);
    auto canonical_headers = get_canonical_headers(header);
    if (!canonical_headers) {
        return canonical_headers.error();
    }
    auto canonical_req = create_canonical_request(
      canonical_headers.value(), header, sha256);
    if (!canonical_req) {
        return canonical_req.error();
    }
    vlog(s3_log.trace, "\n[canonical-request]\n{}\n", canonical_req.value());
    auto str_to_sign = get_string_to_sign(
      amz_date, _cred_scope, canonical_req.value());
    if (!str_to_sign) {
        return str_to_sign.error();
    }
    vlog(s3_log.trace, "\n[string-to-sign]\n{}\n", str_to_sign.value());
    auto digest = hmac(_sign_key, str_to_sign.value());
    auto auth_header = fmt::format(
      "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={}",
      std::string(_access_key),
      _cred_scope,
      canonical_headers.value().signed_headers,
      hexdigest(digest));
    header.set(http::client::field::authorization, auth_header);
    vlog(s3_log.trace, "\n[signed-header]\n\n{}", header);
    return {};
}

signature_v4::signature_v4(
  aws_region_name region,
  public_key_str access_key,
  private_key_str private_key,
  time_source&& ts)
  : _sig_time(std::move(ts))
  , _region(std::move(region))
  , _access_key(std::move(access_key))
  , _private_key(std::move(private_key)) {
    std::string date_str = _sig_time.format_date();
    std::string service = "s3";
    auto res = gen_sig_key(
      static_cast<std::string>(_private_key),
      date_str,
      static_cast<std::string>(_region),
      service);
    vassert(res, "AWS signature v4 initialization failed {}", res.error());
    _sign_key = res.value();
    _cred_scope = fmt::format(
      "{}/{}/{}/aws4_request",
      date_str,
      static_cast<std::string>(_region),
      service);
    vlog(
      s3_log.trace,
      "\n[signing key]\n{}\n[scope]\n{}\n",
      hexdigest(_sign_key),
      _cred_scope);
}
} // namespace s3
