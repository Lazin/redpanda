#include "s3/client.h"

#include "s3/error.h"
#include "s3/logger.h"
#include "s3/signature.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>

#include <fmt/core.h>
#include <gnutls/crypto.h>

namespace s3 {

// configuration //

ss::future<configuration> configuration::make_configuration(
  const public_key_str& pkey,
  const private_key_str& skey,
  const aws_region_name& region) {
    return ss::do_with(
      configuration(),
      ss::tls::credentials_builder(),
      [pkey, skey, region](
        configuration& client_cfg, ss::tls::credentials_builder& cred_builder) {
          const auto endpoint_uri = fmt::format(
            "s3.{}.amazonaws.com", region());
          // Setup credentials for TLS
          ss::tls::credentials_builder builder;
          client_cfg.access_key = pkey;
          client_cfg.secret_key = skey;
          client_cfg.region = region;
          client_cfg.uri = access_point_uri(endpoint_uri);
          return cred_builder.set_system_trust()
            .then([&client_cfg, &cred_builder] {
                return cred_builder.build_reloadable_certificate_credentials().then(
                    [&client_cfg] (ss::shared_ptr<ss::tls::certificate_credentials> creds) {
                    client_cfg.credentials = std::move(creds);
                    return ss::net::dns::resolve_name(
                    client_cfg.uri(), ss::net::inet_address::family::INET);
                });
            })
            .then([&client_cfg](ss::net::inet_address addr) {
                const uint16_t port = 443;
                client_cfg.server_addr = ss::socket_address(addr, port);
                return ss::make_ready_future<configuration>(client_cfg);
            });
      });
}

// request_creator //

request_creator::request_creator(const configuration& conf)
  : _ap(conf.uri)
  , _sign(conf.region, conf.access_key, conf.secret_key) {}

result<http::client::request_header> request_creator::make_request(
  operation op, std::initializer_list<std::string>&& args) {
    http::client::request_header header{};
    switch (op) {
    case operation::get_object: {
        // GET /{object-id} HTTP/1.1
        // Host: {bucket-name}.s3.amazonaws.com
        // x-amz-date:{req-datetime}
        // Authorization:{signature}
        // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        enum { bucket, objectid, nparams };
        std::vector<std::string> list_args = args;
        if (list_args.size() < nparams) {
            return make_error_code(s3_error_codes::not_enough_arguments);
        }
        auto host = fmt::format(
          "{}.s3.us-east-1.amazonaws.com", list_args[bucket]);
        auto target = fmt::format("/{}", list_args[objectid]);
        std::string emptysig
          = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        std::string agent = "redpanda.io";
        header.method(boost::beast::http::verb::get);
        header.target(target);
        header.insert(boost::beast::http::field::user_agent, agent);
        header.insert(boost::beast::http::field::host, host);
        header.insert(boost::beast::http::field::content_length, 0);
        header.insert("x-amz-content-sha256", emptysig);
        auto ec = _sign.sign_header(header, emptysig);
        if (ec) {
            return ec;
        }
    } break;
    case operation::put_object: {
        // PUT /my-image.jpg HTTP/1.1
        // Host: myBucket.s3.<Region>.amazonaws.com
        // Date: Wed, 12 Oct 2009 17:50:00 GMT
        // Authorization: authorization string
        // Content-Type: text/plain
        // Content-Length: 11434
        // x-amz-meta-author: Janet
        // Expect: 100-continue
        // [11434 bytes of object data]
        enum { bucket, objectid, body, nparams };
        std::vector<std::string> list_args = args;
        if (list_args.size() < nparams) {
            return make_error_code(s3_error_codes::not_enough_arguments);
        }
        auto host = fmt::format(
          "{}.s3.us-east-1.amazonaws.com", list_args[bucket]);
        auto target = fmt::format("/{}", list_args[objectid]);
        std::string sig = signature_v4::sha256_hexdigest(
          std::string_view{list_args[body].data(), list_args[body].length()});
        std::string agent = "redpanda.io";
        header.method(boost::beast::http::verb::put);
        header.target(target);
        header.insert(boost::beast::http::field::user_agent, agent);
        header.insert(boost::beast::http::field::host, host);
        header.insert(boost::beast::http::field::content_type, "text/plain");
        header.insert(
          boost::beast::http::field::content_length, list_args[body].size());
        header.insert("x-amz-content-sha256", sig);
        auto ec = _sign.sign_header(header, sig);
        if (ec) {
            return ec;
        }
    } break;
        break;
    default:
        // TBD: remaining requests
        throw "Not implemented";
        break;
    }
    return header;
}

result<http::client::request_header> request_creator::make_unsigned_put_request(
  bucket_name const& name, object_key const& key, size_t payload_size) {
    // PUT /my-image.jpg HTTP/1.1
    // Host: myBucket.s3.<Region>.amazonaws.com
    // Date: Wed, 12 Oct 2009 17:50:00 GMT
    // Authorization: authorization string
    // Content-Type: text/plain
    // Content-Length: 11434
    // x-amz-meta-author: Janet
    // Expect: 100-continue
    // [11434 bytes of object data]
    http::client::request_header header{};
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/{}", key());
    std::string sig = "UNSIGNED-PAYLOAD";
    std::string agent = "redpanda.io";
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(boost::beast::http::field::user_agent, agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, "text/plain");
    header.insert(boost::beast::http::field::content_length, payload_size);
    header.insert("x-amz-content-sha256", sig);
    auto ec = _sign.sign_header(header, sig);
    if (ec) {
        return ec;
    }
    return header;
}

result<http::client::request_header>
request_creator::make_list_objects_v2_request(const bucket_name& name) {
    // GET /?list-type=2&prefix=photos/2006/&delimiter=/ HTTP/1.1
    // Host: example-bucket.s3.<Region>.amazonaws.com
    // x-amz-date: 20160501T000433Z
    // Authorization: authorization string
    http::client::request_header header{};
    auto host = fmt::format("{}.{}", name(), _ap());
    auto target = fmt::format("/?list-type=2");
    std::string emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    std::string agent = "redpanda.io";
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::user_agent, agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, 0);
    header.insert("x-amz-content-sha256", emptysig);
    auto ec = _sign.sign_header(header, emptysig);
    vlog(s3_log.trace, "ListObjectsV2:\n {}", header);
    if (ec) {
        return ec;
    }
    return header;
}

// client //

client::client(const configuration& conf)
  : _requestor(conf)
  , _client(conf) {}

ss::future<http::client::response_stream_ref>
client::get_object(bucket_name const& name, object_key const& key) {
    auto header = _requestor.make_request(
      operation::get_object,
      {static_cast<std::string>(name), static_cast<std::string>(key)});
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()));
}

ss::future<http::client::response_stream_ref> client::put_object(
  bucket_name const& name,
  object_key const& id,
  size_t payload_size,
  ss::input_stream<char>&& body) {
    auto header = _requestor.make_unsigned_put_request(name, id, payload_size);
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return ss::do_with(
      std::move(body),
      [this, header = std::move(header)](ss::input_stream<char>& body) mutable {
          return _client.request(std::move(header.value()), body);
      });
}

ss::future<ss::input_stream<char>>
client::list_objects_v2(const bucket_name& name) {
    auto header = _requestor.make_list_objects_v2_request(name);
    if (!header) {
        return ss::make_exception_future<ss::input_stream<char>>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return _client.request(std::move(header.value()))
      .then([](http::client::response_stream_ref&& resp) {
          return ss::make_ready_future<ss::input_stream<char>>(
            resp->as_input_stream());
      });
}

} // namespace s3