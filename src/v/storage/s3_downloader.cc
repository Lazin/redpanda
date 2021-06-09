#include "storage/s3_downloader.h"

#include "bytes/iobuf_istreambuf.h"
#include "config/configuration.h"
#include "hashing/xx.h"
#include "json/json.h"
#include "s3/error.h"
#include "storage/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include <exception>

namespace storage {

s3_downloader::s3_downloader(s3_downloader_configuration config)
  : _conf(std::move(config))
  , _dl_limit(config.num_connections()) {}

s3_downloader::s3_downloader()
  : _conf(std::nullopt)
  , _dl_limit(1) {}

s3_downloader::~s3_downloader() {
    vassert(_gate.is_closed(), "S3 downloader is not stopped properly");
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          stlog.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

ss::future<s3_downloader_configuration> s3_downloader::make_s3_config() {
    auto secret_key = s3::private_key_str(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_secret_key,
      "archival_storage_s3_secret_key"));
    auto access_key = s3::public_key_str(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_access_key,
      "archival_storage_s3_access_key"));
    auto region = s3::aws_region_name(get_value_or_throw(
      config::shard_local_cfg().archival_storage_s3_region,
      "archival_storage_s3_region"));
    auto s3_conf = co_await s3::configuration::make_configuration(
      access_key, secret_key, region);
    s3_downloader_configuration dlconf{
      .client_config = std::move(s3_conf),
      .bucket = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().archival_storage_s3_bucket,
        "archival_storage_s3_bucket")),
      .num_retries = s3_max_attempts(5),
      .num_connections = s3_max_connections(
        config::shard_local_cfg().archival_storage_max_connections.value())};
    co_return std::move(dlconf);
}

ss::future<> s3_downloader::stop() {
    if (!_cancel.abort_requested()) {
        // Just in case something is running in the background
        _cancel.request_abort();
    }
    return _gate.close();
}

void s3_downloader::cancel() { _cancel.request_abort(); }

ss::future<> s3_downloader::download_log(const ntp_config& ntpc) {
    if (_conf) {
        vlog(stlog.info, "Check conditions for {} S3 recovery", ntpc.ntp());
        bool exists = co_await ss::file_exists(ntpc.work_directory());
        if (exists) {
            co_return;
        }
        if (!ntpc.has_overrides()) {
            vlog(stlog.info, "No overrides for {} found, skipping", ntpc.ntp());
            co_return;
        }
        auto name = ntpc.get_overrides().manifest_object_name;
        if (!name.has_value()) {
            vlog(
              stlog.info,
              "No manifest override for {} found, skipping",
              ntpc.ntp());
            co_return;
        }
        vlog(stlog.info, "Downloading log for {}", ntpc.ntp());
        co_await download_log(s3::object_key(*name), ntpc);
    }
    co_return;
}

ss::future<> s3_downloader::download_log(
  const s3::object_key& manifest_key, const ntp_config& ntpc) {
    auto prefix = std::filesystem::path(ntpc.work_directory());
    auto partitions = co_await download_topic_manifest(manifest_key);
    auto part_ix = ntpc.ntp().tp.partition();
    auto part_s3_loc = partitions[part_ix];
    auto segments = co_await download_manifest(part_s3_loc, ntpc);
    // TODO: limit number of downloaded segments based on topic configuration
    // we can limit based on segment size that we want to keep and
    // base/committed offsets of individual segments (e.g. keep last 1GB of data
    // for every partition)
    vlog(
      stlog.info,
      "Downloading segments from {} into {}",
      part_s3_loc(),
      prefix);
    for (auto x : segments) {
        vlog(stlog.info, "... segment {} path {}", x.segment, x.s3_path);
    }
    co_await ss::max_concurrent_for_each(
      segments,
      _conf->num_connections(),
      [this, prefix](const s3_manifest_entry& entry) {
          _cancel.check();
          return download_file(entry, prefix);
      })
      .handle_exception([this, prefix, segments](std::exception_ptr eptr) {
          vlog(stlog.error, "Encountered an error during download: {}", eptr);
          return ss::do_for_each(
            segments, [this, prefix](const s3_manifest_entry& entry) {
                return remove_file(entry, prefix);
            });
      });
}

// FIXME: this all should come from archival::manifest implementation
static ss::sstring make_s3_segment_path(
  const model::ntp& ntp, int32_t rawrev, const ss::sstring& segment) {
    auto path = fmt::format("{}_{}/{}", ntp.path(), rawrev, segment);
    uint32_t hash = xxhash_32(path.data(), path.size());
    return fmt::format("{:08x}/{}", hash, path);
}

static s3::object_key
make_partition_manifest_path(const model::ntp& ntp, model::revision_id rev) {
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}_{}", ntp.path(), rev());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return s3::object_key(
      fmt::format("{:08x}/meta/{}_{}/manifest.json", hash, ntp.path(), rev()));
}

static std::vector<s3_manifest_entry> parse_segments(
  const rapidjson::Document& m, const model::ntp& ntp, int32_t rev) {
    using namespace rapidjson;
    std::vector<s3_manifest_entry> segments;
    if (m.HasMember("segments")) {
        const auto& s = m["segments"].GetObject();
        for (auto it = s.MemberBegin(); it != s.MemberEnd(); it++) {
            auto name = it->name.GetString();
            auto size_bytes = it->value["size_bytes"].GetInt64();
            auto boffs = it->value["base_offset"].GetInt64();
            auto coffs = it->value["committed_offset"].GetInt64();
            s3_manifest_entry entry{
              .segment = name,
              .size = static_cast<size_t>(size_bytes),
              .base_offset = model::offset(boffs),
              .committed_offset = model::offset(coffs),
              .s3_path = make_s3_segment_path(ntp, rev, name)};
            segments.push_back(std::move(entry));
        }
    }
    return segments;
}

ss::future<std::vector<s3_manifest_entry>> s3_downloader::download_manifest(
  const s3::object_key& key, const ntp_config& ntp_cfg) {
    vlog(stlog.info, "Downloading manifest {}", key());
    using namespace rapidjson;
    s3::client cl(_conf->client_config);
    auto resp = co_await cl.get_object(_conf->bucket, key);
    auto is = resp->as_input_stream();
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    Document m;
    IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    // Parse the document and validate the parameters (manifest, partition, etc)
    auto ns = model::ns(m["namespace"].GetString());
    auto tp = model::topic(m["topic"].GetString());
    auto pt = model::partition_id(m["partition"].GetInt());
    auto ntp = model::ntp(ns, tp, pt);
    auto rev = model::revision_id(m["revision"].GetInt());
    vassert(
      ntp == ntp_cfg.ntp(), "Unexpected ntp (TODO: turn into runtime error)");
    co_await cl.shutdown();
    co_return parse_segments(m, ntp, rev);
}

ss::future<std::vector<s3::object_key>>
s3_downloader::download_topic_manifest(const s3::object_key& key) {
    vlog(stlog.info, "Downloading topic manifest {}", key());
    using namespace rapidjson;
    iobuf tmp;
    auto os = make_iobuf_ref_output_stream(tmp);
    s3::client cl(_conf->client_config);
    try {
        auto resp = co_await cl.get_object(_conf->bucket, key);
        auto is = resp->as_input_stream();
        co_await ss::copy(is, os);
    } catch (const s3::rest_error_response& err) {
        vlog(
          stlog.error,
          "Error during topic manifest download: {}, code: {}, resource: {}",
          err.message(),
          err.code(),
          err.resource());
        throw;
    }
    iobuf_istreambuf ibuf(tmp);
    std::istream stream(&ibuf);
    Document m;
    IStreamWrapper wrapper(stream);
    m.ParseStream(wrapper);
    // Parse manifest fields that we need to find partition manifest files
    auto ns = m["namespace"].GetString();
    auto tp = m["topic"].GetString();
    int32_t partitions = m["partition_count"].GetInt();
    int32_t rev = m["revision_id"].GetInt();
    // TODO: parse all fields and do something meaningful (verify
    // topic_configuration or alter it).
    vlog(
      stlog.info,
      "Restoring from S3 ns: {}, topic: {}, num-partitions: {}, rev: {}",
      ns,
      tp,
      partitions,
      rev);
    std::vector<s3::object_key> result;
    for (int32_t i = 0; i < partitions; i++) {
        auto ntp = model::ntp(
          model::ns(ns), model::topic(tp), model::partition_id(i));
        result.emplace_back(
          make_partition_manifest_path(ntp, model::revision_id(rev)));
    }
    co_await cl.shutdown();
    co_return result;
}

static ss::future<ss::output_stream<char>>
open_output_file_stream(const std::filesystem::path& path) {
    auto file = co_await ss::open_file_dma(
      path.native(), ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    co_return std::move(stream);
}

ss::future<> s3_downloader::download_file(
  const s3_manifest_entry& key, const std::filesystem::path& prefix) {
    s3::client cl(_conf->client_config);
    auto resp = co_await cl.get_object(
      _conf->bucket, s3::object_key(key.s3_path));
    auto is = resp->as_input_stream();
    // parse segment name from S3
    auto localpath = prefix / std::filesystem::path(key.segment);
    vlog(
      stlog.info,
      "Copying s3 path {} to local location {}",
      key.s3_path,
      localpath.string());
    co_await ss::recursive_touch_directory(prefix.string());
    auto fs = co_await open_output_file_stream(localpath);
    co_await ss::copy(is, fs);
    co_await cl.shutdown();
    co_await fs.flush();
    co_await fs.close();
    co_return;
}

ss::future<> s3_downloader::remove_file(
  const s3_manifest_entry& key, const std::filesystem::path& prefix) {
    vlog(stlog.info, "removing file {}", prefix.string());
    auto localpath = prefix / std::filesystem::path(key.segment);
    co_await ss::remove_file(localpath.string());
    co_return;
}

} // namespace storage
