#include "s3/client.h"

#include "s3/error.h"
#include "s3/logger.h"
#include "s3/signature.h"

#include <seastar/core/future.hh>

#include <fmt/core.h>
#include <gnutls/crypto.h>

namespace s3 {

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
        header.method(http::client::verb::get);
        header.target(target);
        header.insert(http::client::field::user_agent, agent);
        header.insert(http::client::field::host, host);
        header.insert(http::client::field::content_length, 0);
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
        header.method(http::client::verb::put);
        header.target(target);
        header.insert(http::client::field::user_agent, agent);
        header.insert(http::client::field::host, host);
        header.insert(http::client::field::content_type, "text/plain");
        header.insert(
          http::client::field::content_length, list_args[body].size());
        header.insert("x-amz-content-sha256", sig);
        auto ec = _sign.sign_header(header, sig);
        if (ec) {
            return ec;
        }
    } break;
    default:
        // TBD: remaining requests
        throw "Not implemented";
        break;
    }
    return header;
}

result<http::client::request_header> request_creator::make_unsigned_put_request(
  bucket_name const& name, object_key const& key, size_t payload_size) {
    // futurized version
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
    auto host = fmt::format("{}.s3.us-east-1.amazonaws.com", name());
    auto target = fmt::format("/{}", key());
    std::string sig = "UNSIGNED-PAYLOAD";
    std::string agent = "redpanda.io";
    header.method(http::client::verb::put);
    header.target(target);
    header.insert(http::client::field::user_agent, agent);
    header.insert(http::client::field::host, host);
    header.insert(http::client::field::content_type, "text/plain");
    header.insert(http::client::field::content_length, payload_size);
    header.insert("x-amz-content-sha256", sig);
    auto ec = _sign.sign_header(header, sig);
    if (ec) {
        return ec;
    }
    return header;
}

// client //

client::client(const configuration& conf)
  : _requestor(conf)
  , _client(conf) {}

ss::future<bytes>
client::get_object(bucket_name const& name, object_key const& id) {
    auto header = _requestor.make_request(
      operation::get_object,
      {static_cast<std::string>(name), static_cast<std::string>(id)});
    if (!header) {
        return ss::make_exception_future<bytes>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return http::client::fetch(_client, std::move(header.value()), iobuf());
}

ss::future<bytes> client::put_object(
  bucket_name const& name, object_key const& id, iobuf&& body) {
    iobuf_parser pbuf(std::move(body));
    std::string payload = pbuf.read_string(pbuf.bytes_left());
    auto header = _requestor.make_unsigned_put_request(
      name, id, payload.size());
    if (!header) {
        return ss::make_exception_future<bytes>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    iobuf pl;
    pl.append(payload.data(), payload.length());
    return http::client::fetch(
      _client, std::move(header.value()), std::move(pl));
}

ss::future<ss::input_stream<char>> client::put_object(
  bucket_name const& name,
  object_key const& id,
  size_t payload_size,
  ss::input_stream<char>&& body) {
    auto header = _requestor.make_unsigned_put_request(name, id, payload_size);
    if (!header) {
        return ss::make_exception_future<ss::input_stream<char>>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header);
    return ss::do_with(
      std::move(body),
      [this, header = std::move(header)](ss::input_stream<char>& body) mutable {
          using namespace std::chrono_literals;
          http::request_bounds bnds{
            .recv_timeout = 5s,
            .send_timeout = 5s,
            .max_buffered_bytes = 0x8000,
          };
          return _client.request(std::move(header.value()), body, bnds);
      });
}

} // namespace s3