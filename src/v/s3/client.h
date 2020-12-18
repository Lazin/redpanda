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

    /// \brief opinionated configuraiton initialization
    /// Generates uri field from region, initializes credentials for the
    /// transport, resolves the uri to get the server_addr.
    ///
    /// \param pkey is an AWS access key
    /// \param skey is an AWS secret key
    /// \param region is an AWS region code
    /// \return future that returns initialized configuration
    static ss::future<configuration> make_configuration(
      const public_key_str& pkey,
      const private_key_str& skey,
      const aws_region_name& region);
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

    /// \brief Initialize http header for ListObjectsV2 request
    ///
    /// \param name of the bucket
    /// \param region to connect
    /// \return initialized http header or error
    result<http::client::request_header> make_list_objects_v2_request(
      const bucket_name& name);

private:
    access_point_uri _ap;
    signature_v4 _sign;
};

/// S3 REST-API client
class client {
public:
    explicit client(const configuration& conf);

    /// Download object from S3 bucket
    ///
    /// \param name is a bucket name
    /// \param key is an object key
    /// \return future that gets ready after request was sent
    ss::future<http::client::response_stream_ref>
    get_object(bucket_name const& name, object_key const& key);

    /// Put object to S3 bucket.
    /// \param name is a bucket name
    /// \param key is an id of the object
    /// \param payload_size is a size of the object in bytes
    /// \param body is an input_stream that can be used to read body
    /// \return future becomes available whan the body is sent The
    /// input_stream returned by this future can be used to get the response.
    ss::future<http::client::response_stream_ref> put_object(
      bucket_name const& name,
      object_key const& key,
      size_t payload_size,
      ss::input_stream<char>&& body);

    /// List all object inside the bucket
    ///
    /// \param name is a bucket name
    /// \return (TODO: fix) response body
    ss::future<ss::input_stream<char>> list_objects_v2(const bucket_name& name);

private:
    request_creator _requestor;
    http::client _client;
};

} // namespace s3