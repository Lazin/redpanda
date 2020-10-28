#include "http/client.h"

#include "rpc/transport.h"
#include "rpc/types.h"
#include "s3/client.h"
#include "s3/signature.h"
#include "seastarx.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/report_exception.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include <boost/optional/optional.hpp>
#include <boost/outcome/detail/value_storage.hpp>
#include <boost/system/system_error.hpp>
#include <gnutls/gnutls.h>

#include <exception>
#include <optional>
#include <stdexcept>
#include <string>

static ss::logger test_log{"test"};

void cli_opts(boost::program_options::options_description_easy_init opt) {
    namespace po = boost::program_options;

    opt(
      "object",
      po::value<std::string>()->default_value("test.txt"),
      "s3 object id");

    opt(
      "bucket",
      po::value<std::string>()->default_value("test-rps3support"),
      "s3 bucket");

    opt(
      "accesskey",
      po::value<std::string>()->default_value(""),
      "aws access key");

    opt(
      "secretkey",
      po::value<std::string>()->default_value(""),
      "aws secret key");

    opt(
      "region",
      po::value<std::string>()->default_value("us-east-1"),
      "aws region");

    opt(
      "data",
      po::value<std::string>()->default_value(""),
      "data stored inside S3 object");
}

struct test_conf {
    test_conf() = default;
    test_conf(const test_conf&) = default;

    s3::bucket_name bucket;
    s3::object_key object;
    /// If data is not null, execute put request, otherwise use get request
    std::string data;

    s3::configuration client_cfg;
};

inline std::ostream& operator<<(std::ostream& out, const test_conf& cfg) {
    // make the output json-able so we can consume it in python for analysis
    return out << "["
               << "'bucket': " << cfg.bucket << ", "
               << "'object': " << cfg.object << "]";
}

test_conf cfg_from(boost::program_options::variables_map& m) {
    s3::configuration client_cfg;
    client_cfg.uri = s3::access_point_uri(
      "s3.us-east-1.amazonaws.com"); // TODO: don't use default endpont
    const uint16_t port = 443;       // TODO: don't use default
    ss::net::inet_address addr = ss::net::dns::resolve_name(
                                   static_cast<std::string>(client_cfg.uri),
                                   ss::net::inet_address::family::INET)
                                   .get0();
    client_cfg.server_addr = ss::socket_address(addr, port);
    vlog(test_log.info, "connecting to {}", client_cfg.server_addr);
    // Setup credentials for TLS
    ss::tls::credentials_builder builder;
    builder.set_system_trust().get(); // Always use system trust for s3
    client_cfg.credentials = builder;
    // AWS access key
    client_cfg.access_key = s3::public_key_str(
      m["accesskey"].as<std::string>());
    // AWS secret key
    client_cfg.secret_key = s3::private_key_str(
      m["secretkey"].as<std::string>());
    // AWS region
    client_cfg.region = s3::aws_region_name(m["region"].as<std::string>());

    return test_conf{
      .bucket = s3::bucket_name(m["bucket"].as<std::string>()),
      .object = s3::object_key(m["object"].as<std::string>()),
      .data = m["data"].as<std::string>(),
      .client_cfg = std::move(client_cfg)};
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<s3::client> client;
    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            const test_conf lcfg = cfg_from(cfg);
            s3::configuration s3_cfg = lcfg.client_cfg;
            vlog(test_log.info, "config:{}", lcfg);
            vlog(test_log.info, "constructing client");
            client.start(s3_cfg).get();
            auto cd = ss::defer([&client] {
                vlog(test_log.info, "defer:stop");
                client.stop().get();
            });
            vlog(test_log.info, "connecting");
            client
              .invoke_on(
                0,
                [lcfg](s3::client& cli) {
                    if (lcfg.data.empty()) {
                        vlog(test_log.info, "sending request");
                        auto resp
                          = cli.get_object(lcfg.bucket, lcfg.object).get0();
                        vlog(
                          test_log.info,
                          "response: {}",
                          std::string_view{
                            reinterpret_cast<const char*>(resp.data()),
                            resp.size()});
                    } else {
                        // put
                        iobuf payload;
                        payload.append(lcfg.data.data(), lcfg.data.size());
                        auto resp2 = cli
                                       .put_object(
                                         lcfg.bucket,
                                         lcfg.object,
                                         std::move(payload))
                                       .get0();
                        vlog(
                          test_log.info,
                          "response: {}",
                          std::string_view{
                            reinterpret_cast<const char*>(resp2.data()),
                            resp2.size()});
                    }
                })
              .get();
            vlog(test_log.info, "done");
        });
    });
}
