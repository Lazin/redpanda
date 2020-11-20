#pragma once

#include "http/client.h"
#include "rpc/transport.h"
#include "s3/signature.h"

#include <initializer_list>

namespace s3 {

using access_point_uri = named_type<std::string, struct s3_access_point_uri>;
using bucket_name = named_type<std::string, struct s3_bucket_name>;
using object_key = named_type<std::string, struct s3_object_key>;

enum class operation {
    get_object,
    put_object,
};

/// S3 client configuration
struct configuration : rpc::base_transport::configuration {
    /// URI of the S3 access point
    access_point_uri uri;
    /// AWS access key
    public_key_str access_key;
    /// AWS secret key
    private_key_str secret_key;
    /// AWS region
    aws_region_name region;
};

/// Request formatter for AWS S3
class request_creator {
public:
    /// C-tor
    /// \param conf is a configuration container
    explicit request_creator(const configuration& conf);

    /// \brief Initialize http header
    ///
    /// \param op is an operation selector
    /// \param args is a list of query argument
    result<http::client::request_header>
    make_request(operation op, std::initializer_list<std::string>&& args);

    result<http::client::request_header> make_unsigned_put_request(
      bucket_name const& name, object_key const& key, size_t payload_size);

private:
    access_point_uri _ap;
    signature_v4 _sign;
};

/// S3 REST-API client
class client {
public:
    explicit client(const configuration& conf);

    /// Download object (TODO: refactor to use stream-like interface)
    ss::future<bytes>
    get_object(bucket_name const& name, object_key const& key);

    ss::future<bytes>
    put_object(bucket_name const& name, object_key const& key, iobuf&& body);

    /// Put object to S3 bucket. 
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \return future becomes available whan the body is sent The
    /// input_stream returned by this future can be used to get the response.
    ss::future<ss::input_stream<char>> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char>&& body);

private:
    request_creator _requestor;
    http::client _client;
};

} // namespace s3