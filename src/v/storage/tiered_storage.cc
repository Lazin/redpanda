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

#include <absl/container/btree_map.h>
#include <boost/lexical_cast.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include <charconv>
#include <exception>

namespace storage {
using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration download_timeout = 300s;
static constexpr ss::lowres_clock::duration initial_backoff  = 200ms;

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

ss::future<bool> topic_downloader::download_log(const ntp_config& ntpc) {
    if (_remote) {
        retry_chain_logger ctxlog(stlog, _root, ntpc.ntp().path());
        vlog(ctxlog.debug, "Check conditions for {} S3 recovery", ntpc.ntp());
        if (!ntpc.has_overrides()) {
            vlog(
              ctxlog.debug, "No overrides for {} found, skipping", ntpc.ntp());
            co_return false;
        }
        // TODO (evgeny): check the condition differently
        bool exists = co_await ss::file_exists(ntpc.work_directory());
        if (exists) {
            co_return false;
        }
        auto name = ntpc.get_overrides().recovery_source;
        if (!name.has_value()) {
            vlog(
              ctxlog.debug,
              "No manifest override for {} found, skipping",
              ntpc.ntp());
            co_return false;
        }
        vlog(ctxlog.info, "Downloading log for {}", ntpc.ntp());
        try {
            co_await download_log(
              cloud_storage::remote_manifest_path(std::filesystem::path(*name)),
              ntpc);
            co_return true;
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
    co_return false;
}

struct manifest_path_components {
    std::filesystem::path _origin;
    model::ns _ns;
    model::topic _topic;
    model::partition_id _part;
    model::revision_id _rev;
};

std::ostream& operator<<(std::ostream& s, const manifest_path_components& c) {
    fmt::print(
      s, "{{{}: {}-{}-{}-{}}}", c._origin, c._ns, c._topic, c._part, c._rev);
    return s;
}

struct segment_path_components : manifest_path_components {
    cloud_storage::segment_name _name;
};

std::ostream& operator<<(std::ostream& s, const segment_path_components& c) {
    fmt::print(
      s,
      "{{{}: {}-{}-{}-{}-{}}}",
      c._origin,
      c._ns,
      c._topic,
      c._part,
      c._rev,
      c._name);
    return s;
}

static bool same_ntp(const manifest_path_components& c, const model::ntp& ntp) {
    return c._ns == ntp.ns && c._topic == ntp.tp.topic
           && c._part == ntp.tp.partition;
}

static bool parse_partition_and_revision(
  std::string_view s, manifest_path_components& comp) {
    auto pos = s.find('_');
    if (pos == std::string_view::npos) {
        // Invalid segment file name
        return false;
    }
    uint64_t res = 0;
    // parse first component
    auto sv = s.substr(0, pos);
    auto e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._part = model::partition_id(res);
    // parse second component
    sv = s.substr(pos + 1);
    e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._rev = model::revision_id(res);
    return true;
}

std::optional<manifest_path_components>
get_manifest_path_components(const std::filesystem::path& path) {
    // example: b0000000/meta/kafka/redpanda-test/4_2/manifest.json
    enum {
        ix_prefix,
        ix_meta,
        ix_namespace,
        ix_topic,
        ix_part_rev,
        ix_file_name,
        total_components
    };
    manifest_path_components res;
    int ix = 0;
    for (const auto& c : path) {
        ss::sstring p = c.string();
        switch (ix++) {
        case ix_prefix:
            break;
        case ix_namespace:
            res._ns = model::ns(std::move(p));
            break;
        case ix_topic:
            res._topic = model::topic(std::move(p));
            break;
        case ix_part_rev:
            if (!parse_partition_and_revision(p, res)) {
                return std::nullopt;
            }
            break;
        case ix_file_name:
            if (p != "manifest.json") {
                return std::nullopt;
            }
            break;
        }
    }
    if (ix == total_components) {
        return res;
    }
    return std::nullopt;
}

std::optional<segment_path_components>
get_segment_path_components(const std::filesystem::path& path) {
    enum {
        ix_prefix,
        ix_namespace,
        ix_topic,
        ix_part_rev,
        ix_segment_name,
        total_components
    };
    segment_path_components res;
    int ix = 0;
    for (const auto& c : path) {
        ss::sstring p = c.string();
        switch (ix++) {
        case ix_prefix:
            break;
        case ix_namespace:
            res._ns = model::ns(std::move(p));
            break;
        case ix_topic:
            res._topic = model::topic(std::move(p));
            break;
        case ix_part_rev:
            if (!parse_partition_and_revision(p, res)) {
                return std::nullopt;
            }
            break;
        case ix_segment_name:
            res._name = cloud_storage::segment_name(std::move(p));
            break;
        }
    }
    if (ix == total_components) {
        return res;
    }
    return std::nullopt;
}

// entry point for the whole thing
ss::future<> topic_downloader::download_log(
  const cloud_storage::remote_manifest_path& manifest_key,
  const ntp_config& ntpc) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_logger ctxlog(stlog, caller, ntpc.ntp().path());
    auto prefix = std::filesystem::path(ntpc.work_directory());
    vlog(
      ctxlog.info,
      "The target path: {}, ntp-config revision: {}",
      prefix,
      ntpc.get_revision());
    auto mat = co_await find_recovery_material(manifest_key, ntpc);
    // We have multiple versions of the same partition here, some segments
    // may overlap so we need to deduplicate. Also, to take retention into
    // account.
    struct segment {
        ss::sstring full_path;
        cloud_storage::manifest::segment_meta meta;
    };
    absl::btree_map<model::offset, segment> offset_map;
    for (const auto& p : mat.paths) {
        auto manifest = co_await download_manifest(p, ntpc);
        for (const auto& segm : manifest) {
            if (offset_map.contains(segm.second.base_offset)) {
                auto committed = offset_map.at(segm.second.base_offset)
                                   .meta.committed_offset;
                if (committed > segm.second.committed_offset) {
                    continue;
                }
            }
            auto path = manifest.get_remote_segment_path(segm.first);
            offset_map.insert_or_assign(
              segm.second.base_offset,
              segment{
                  .full_path = path().native(),
                  .meta = segm.second});
        }
    }
    cloud_storage::manifest target(ntpc.ntp(), ntpc.get_revision());
    for (const auto& kv : offset_map) {
        // Original manifests contain short names (e.g. 1029-4-v1.log).
        // This is because they belong to the same revision and the details are
        // encoded in the manifest itself.
        // To create a compound manifest we need to add full names (e.g.
        // 6fab5988/kafka/redpanda-test/5_6/80651-2-v1.log). Otherwise the
        // information in the manifest won't be suffecient.
        target.add(cloud_storage::segment_name(kv.second.full_path), kv.second.meta);
    }
    // Here the partition manifest 'target' may contain segments
    // that have different revision ids inside the path.
    if (target.size() == 0) {
        throw missing_partition_exception(ntpc);
    }
    co_await download_log(target, prefix);
    auto upl_result = co_await _remote->upload_manifest(
      _bucket, target, caller);
    // If the manifest upload fails we can't continue
    // since it will damage the data in S3. The archival subsystem
    // will pick new partition after the leader will be elected. Then
    // it won't find the manifest in place and will create a new one.
    // If the manifest name in S3 matches the old manifest name it will
    // be overwriten and some data may be lost as a result.
    vassert(
      upl_result == cloud_storage::upload_result::success,
      "Can't upload new manifest {} after recovery",
      target.get_manifest_path());

    // Upload topic manifest for re-created topic (here we don't prevent
    // other partitions of the same topic to read old topic manifest if the
    // revision is different). If the revision is the same there is no need
    // to re-upload the topic manifest. The old one is valid for new topic.
    if (mat.topic_manifest.get_revision() != ntpc.get_revision()) {
        mat.topic_manifest.set_revision(ntpc.get_revision());
        upl_result = co_await _remote->upload_manifest(
          _bucket, mat.topic_manifest, caller);
        if (upl_result != cloud_storage::upload_result::success) {
            // That's probably fine since the archival subsystem will
            // re-upload topic manifest eventually.
            vlog(
              ctxlog.warn,
              "Failed to upload new topic manifest {} after recovery",
              target.get_manifest_path());
        }
    }
    co_return;
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
    retry_chain_logger ctxlog(stlog, caller, ntp_cfg.ntp().path());
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

ss::future<topic_downloader::recovery_material>
topic_downloader::find_recovery_material(
  const cloud_storage::remote_manifest_path& key, const ntp_config& ntp_cfg) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_logger ctxlog(stlog, caller);
    vlog(ctxlog.info, "Downloading topic manifest {}", key);
    recovery_material recovery_mat;
    auto result = co_await _remote->download_manifest(
      _bucket, key, recovery_mat.topic_manifest, caller);
    if (result != cloud_storage::download_result::success) {
        throw missing_partition_exception(key);
    }
    recovery_mat.paths = co_await find_matching_partition_manifests(
      recovery_mat.topic_manifest, ntp_cfg);
    co_return recovery_mat;
}

ss::future<std::vector<cloud_storage::remote_manifest_path>>
topic_downloader::find_matching_partition_manifests(
  cloud_storage::topic_manifest& manifest, const ntp_config& ntp_cfg) {
    retry_chain_node caller(download_timeout, initial_backoff, &_root);
    retry_chain_logger ctxlog(stlog, caller, ntp_cfg.ntp().path());
    // TODO: use only selected prefixes
    auto topic_rev = manifest.get_revision();
    std::vector<cloud_storage::remote_manifest_path> all_manifests;
    auto obj_iter = [&all_manifests, topic_rev, ntp = ntp_cfg.ntp(), &ctxlog](
                      const ss::sstring& key,
                      std::chrono::system_clock::time_point,
                      size_t,
                      const ss::sstring&) {
        std::filesystem::path path(key);
        auto res = get_manifest_path_components(path);
        if (res.has_value() && same_ntp(*res, ntp) && res->_rev >= topic_rev) {
            vlog(ctxlog.debug, "Found matching manifest path: {}", *res);
            all_manifests.emplace_back(
              cloud_storage::remote_manifest_path(std::move(path)));
        }
        return ss::stop_iteration::no;
    };
    auto res = co_await _remote->list_objects(
      obj_iter, _bucket, std::nullopt, /*TODO: use std::nullopt*/ 10, caller);
    if (res == cloud_storage::download_result::success) {
        co_return all_manifests;
    }
    auto key = manifest.get_manifest_path();
    throw missing_partition_exception(key);
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
    retry_chain_logger ctxlog(stlog, caller);
    vlog(
      ctxlog.info, "Downloading segment {} into {}", target, prefix.string());
    auto remote_location = manifest.get_remote_segment_path(target);
    auto stream = [prefix, target, remote_location, &ctxlog](
                    uint64_t len,
                    ss::input_stream<char> in) -> ss::future<uint64_t> {
        auto path_or_fname = std::filesystem::path(target());
        auto localpath = prefix / path_or_fname.filename();
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
        retry_chain_logger ctxlog(stlog, caller);
        // The individual segment might be missing for varios reasons but
        // it shouldn't prevent us from restoring the remaining data
        vlog(ctxlog.error, "Failed segment download for {}", target);
    }
}

} // namespace storage
