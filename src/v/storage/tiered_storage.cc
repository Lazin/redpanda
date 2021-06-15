#include "storage/tiered_storage.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "hashing/xx.h"
#include "json/json.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/lexical_cast.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include <exception>

namespace storage {
using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration download_timeout = 10000ms;
static constexpr ss::lowres_clock::duration initial_backoff = 100ms;

/// Partition that we're trying to fetch from S3 is missing
class missing_partition_exception final : public std::exception {
public:
    explicit missing_partition_exception(const ntp_config& ntpc)
      : _msg(ssx::sformat(
        "missing partition {}, rev {}", ntpc.ntp(), ntpc.get_revision())) {}

    explicit missing_partition_exception(
      const cloud_storage::remote_manifest_path& path)
      : _msg(ssx::sformat("missing partition s3://{}", path)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

// TODO:
// - initialize optionally (remote & bucket)
// - scan s3 bucket for manifest partitions instead of computing the locations
// - all manifests with revision <= topic revision are part of the topic

topic_downloader::topic_downloader(size_t max_concurrency)
  : _root(_cancel)
  , _sem{max_concurrency} {}

topic_downloader::~topic_downloader() {
    vassert(_gate.is_closed(), "S3 downloader is not stopped properly");
}

void topic_downloader::set_remote(
  s3::bucket_name bucket, cloud_storage::remote* remote) {
    _remote = remote;
    _bucket = std::move(bucket);
}

ss::future<> topic_downloader::stop() {
    _cancel.request_abort();
    co_await _gate.close();
}

void topic_downloader::cancel() { _cancel.request_abort(); }

ss::future<> topic_downloader::download_log(const ntp_config& ntpc) {
    if (_remote) {
        retry_chain_log ctxlog(stlog, _root, ntpc.ntp().path());
        vlog(ctxlog.info, "Check conditions for {} S3 recovery", ntpc.ntp());
        bool exists = co_await ss::file_exists(ntpc.work_directory());
        if (exists) {
            co_return;
        }
        if (!ntpc.has_overrides()) {
            vlog(
              ctxlog.info, "No overrides for {} found, skipping", ntpc.ntp());
            co_return;
        }
        auto name = ntpc.get_overrides().recovery_source;
        if (!name.has_value()) {
            vlog(
              ctxlog.info,
              "No manifest override for {} found, skipping",
              ntpc.ntp());
            co_return;
        }
        vlog(ctxlog.info, "Downloading log for {}", ntpc.ntp());
        try {
            co_await download_log(
              cloud_storage::remote_manifest_path(std::filesystem::path(*name)),
              ntpc);
        } catch (...) {
            // We can get here if the parttion manifest is missing (or some
            // other failure is preventing us from recovering the partition). In
            // this case the exception can't be propagated since the partition
            // manager will retry and it will create an infinite loop.
            //
            // The only possible solution here is to discard the exception and
            // continue with normal partition creation process.
            //
            // Normally, this will happen when one of the partitions doesn't
            // have any data.
            vlog(
              ctxlog.error,
              "Error during log recovery: {}",
              std::current_exception());
        }
    }
    co_return;
}

// entry point for the whole thing
ss::future<> topic_downloader::download_log(
  const cloud_storage::remote_manifest_path& manifest_key,
  const ntp_config& ntpc) {
    retry_chain_log ctxlog(stlog, _root, ntpc.ntp().path());
    auto prefix = std::filesystem::path(ntpc.work_directory());
    auto partitions = co_await download_topic_manifest(manifest_key);
    // auto part_ix = ntpc.ntp().tp.partition();
    // auto part_s3_loc = partitions[part_ix];
    for (const auto& p : partitions) {
        // /20000000/meta/test-ns/test-topic/42_0/manifest.json
        std::string path = p().native();
        vlog(ctxlog.info, "partition path: {}", path);
        path = path.substr(14);
        size_t pos = 0;
        size_t n = path.find('/');
        vassert(n != std::string::npos, "invalid s3 path");
        auto ns = path.substr(pos, n - pos);
        pos = n + 1;

        n = path.find('/', pos);
        vassert(n != std::string::npos, "invalid s3 path");
        auto topic = path.substr(pos, n - pos);
        pos = n + 1;

        n = path.find('_', pos);
        vassert(n != std::string::npos, "invalid s3 path");
        auto partid = path.substr(pos, n - pos);
        model::partition_id id(boost::lexical_cast<int64_t>(partid));

        pos = n + 1;
        n = path.find('/', pos);
        vassert(n != std::string::npos, "invalid s3 path");
        auto revid = path.substr(pos, n - pos);
        model::revision_id rev(boost::lexical_cast<int64_t>(revid));

        if (
          ntpc.ntp().tp.topic == model::topic(topic)
          && ntpc.ntp().tp.partition == id) {
            auto manifest = co_await download_manifest(p, ntpc);
            vlog(
              ctxlog.info,
              "matched manifest path: {}, {}, {}, {}, new rev {}",
              ns,
              topic,
              partid,
              revid,
              ntpc.get_revision());
            co_await download_log(manifest, prefix);
            co_return;
        }
    }
    throw missing_partition_exception(ntpc);
}

ss::future<> topic_downloader::download_log(
  const cloud_storage::manifest& manifest,
  const std::filesystem::path& prefix) {
    return ss::with_gate(_gate, [this, &manifest, prefix] {
        return ss::parallel_for_each(
          manifest, [this, &manifest, prefix](const auto& kv) {
              return ss::with_semaphore(_sem, 1, [this, kv, &manifest, prefix] {
                  return download_file(kv.first, manifest, prefix);
              });
          });
    });
}

ss::future<cloud_storage::manifest> topic_downloader::download_manifest(
  const cloud_storage::remote_manifest_path& key, const ntp_config& ntp_cfg) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_log ctxlog(stlog, caller, ntp_cfg.ntp().path());
    vlog(
      ctxlog.info,
      "Downloading manifest {}, rev {}",
      ntp_cfg.ntp().path(),
      ntp_cfg.get_revision());
    cloud_storage::manifest manifest(ntp_cfg.ntp(), ntp_cfg.get_revision());
    auto result = co_await _remote->download_manifest(
      _bucket, key, manifest, caller);
    if (result != cloud_storage::download_result::success) {
        throw missing_partition_exception(ntp_cfg);
    }
    co_return manifest;
}

ss::future<std::vector<cloud_storage::remote_manifest_path>>
topic_downloader::download_topic_manifest(
  const cloud_storage::remote_manifest_path& key) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_log ctxlog(stlog, caller);
    vlog(ctxlog.info, "Downloading topic manifest {}", key);
    cloud_storage::topic_manifest topic_manifest;
    auto result = co_await _remote->download_manifest(
      _bucket, key, topic_manifest, caller);
    if (result != cloud_storage::download_result::success) {
        throw missing_partition_exception(key);
    }
    co_return topic_manifest.get_partition_manifests();
}

static ss::future<ss::output_stream<char>>
open_output_file_stream(const std::filesystem::path& path) {
    auto file = co_await ss::open_file_dma(
      path.native(), ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    co_return std::move(stream);
}

ss::future<> topic_downloader::download_file(
  const cloud_storage::segment_name& target,
  const cloud_storage::manifest& manifest,
  const std::filesystem::path& prefix) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_log ctxlog(stlog, caller);
    vlog(
      ctxlog.info, "Downloading segment {} into {}", target, prefix.string());
    auto remote_location = manifest.get_remote_segment_path(target);
    auto stream = [prefix, target, remote_location, &ctxlog](
                    uint64_t len,
                    ss::input_stream<char> in) -> ss::future<uint64_t> {
        auto localpath = prefix / std::filesystem::path(target());
        vlog(
          ctxlog.info,
          "Copying s3 path {} to local location {}",
          remote_location,
          localpath.string());
        co_await ss::recursive_touch_directory(prefix.string());
        auto fs = co_await open_output_file_stream(localpath);
        co_await ss::copy(in, fs);
        co_await fs.flush();
        co_await fs.close();
        co_return len;
    };

    auto result = co_await _remote->download_segment(
      _bucket, target, manifest, stream, caller);

    if (result != cloud_storage::download_result::success) {
        retry_chain_log ctxlog(stlog, caller);
        // The individual segment might be missing for varios reasons but
        // it shouldn't prevent us from restoring the remaining data
        vlog(ctxlog.error, "Failed segment download for {}", target);
    }
}

} // namespace storage
