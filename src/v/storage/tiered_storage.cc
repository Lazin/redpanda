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
#include "utils/gate_guard.h"

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

#include <exception>
#include <variant>

namespace storage {
using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration download_timeout = 300s;
static constexpr ss::lowres_clock::duration initial_backoff = 200ms;

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

data_recovery_provider::~data_recovery_provider() {
    vassert(_gate.is_closed(), "S3 downloader is not stopped properly");
}

ss::future<> data_recovery_provider::stop() {
    _cancel.request_abort();
    co_await _gate.close();
}

void data_recovery_provider::set_remote(
  s3::bucket_name bucket, cloud_storage::remote* remote) {
    _bucket = std::move(bucket);
    _remote = remote;
}

void data_recovery_provider::cancel() { _cancel.request_abort(); }

/// Download full log based on manifest data.
/// The 'ntp_config' should have corresponding override. If override
/// is not set nothing will happen and the returned future will be
/// ready (not in failed state).
/// \return true if log was actually downloaded, false otherwise
ss::future<bool>
data_recovery_provider::download_log(const ntp_config& ntp_cfg) {
    if (_remote) {
        topic_downloader downloader(
          ntp_cfg, _remote, _bucket, _cancel, _gate, _root);
        co_return co_await downloader.download_log();
    }
    co_return false;
}

topic_downloader::topic_downloader(
  const ntp_config& ntpc,
  cloud_storage::remote* remote,
  s3::bucket_name bucket,
  ss::abort_source& as_root,
  ss::gate& gate_root,
  retry_chain_node& parent)
  : _ntpc(ntpc)
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _cancel(as_root)
  , _gate(gate_root)
  , _rtcnode(download_timeout, initial_backoff, &parent)
  , _ctxlog(stlog, _rtcnode, ntpc.ntp().path()) {}

ss::future<bool> topic_downloader::download_log() {
    vlog(_ctxlog.debug, "Check conditions for S3 recovery");
    if (!_ntpc.has_overrides()) {
        vlog(_ctxlog.debug, "No overrides for {} found, skipping", _ntpc.ntp());
        co_return false;
    }
    // TODO (evgeny): maybe check the condition differently
    bool exists = co_await ss::file_exists(_ntpc.work_directory());
    if (exists) {
        co_return false;
    }
    auto name = _ntpc.get_overrides().recovery_source;
    if (!name.has_value()) {
        vlog(
          _ctxlog.debug,
          "No manifest override for {} found, skipping",
          _ntpc.ntp());
        co_return false;
    }
    vlog(_ctxlog.info, "Downloading log for {}", _ntpc.ntp());
    try {
        co_await download_log(
          cloud_storage::remote_manifest_path(std::filesystem::path(*name)));
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
          _ctxlog.error,
          "Error during log recovery: {}",
          std::current_exception());
    }
    co_return false;
}

static bool same_ntp(
  const cloud_storage::manifest_path_components& c, const model::ntp& ntp) {
    return c._ns == ntp.ns && c._topic == ntp.tp.topic
           && c._part == ntp.tp.partition;
}

// Parameters used to exclude data based on total size.
struct size_bound_deletion_parameters {
    size_t retention_bytes;
};

// Parameters used to exclude data based on time.
struct time_bound_deletion_parameters {
    std::chrono::milliseconds retention_duration;
};

// Retention policy that should be used during recovery
using retention = std::variant<
  std::monostate,
  size_bound_deletion_parameters,
  time_bound_deletion_parameters>;

std::ostream& operator<<(std::ostream& o, const retention& r) {
    if (std::holds_alternative<std::monostate>(r)) {
        fmt::print(o, "{{none}}");
    } else if (std::holds_alternative<size_bound_deletion_parameters>(r)) {
        auto p = std::get<size_bound_deletion_parameters>(r);
        fmt::print(o, "{{size-bytes: {}}}", p.retention_bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(r)) {
        auto p = std::get<time_bound_deletion_parameters>(r);
        fmt::print(o, "{{time-ms: {}}}", p.retention_duration.count());
    }
    return o;
}

static retention
get_retention_policy(const ntp_config::default_overrides& prop) {
    auto flags = prop.cleanup_policy_bitflags;
    if (
      flags
      && (flags.value() & model::cleanup_policy_bitflags::deletion)
           == model::cleanup_policy_bitflags::deletion) {
        if (prop.retention_bytes.has_value()) {
            return size_bound_deletion_parameters{prop.retention_bytes.value()};
        } else if (prop.retention_time.has_value()) {
            return time_bound_deletion_parameters{prop.retention_time.value()};
        }
    }
    return std::monostate();
}

ss::future<topic_downloader::offset_map_t>
topic_downloader::build_offset_map(const recovery_material& mat) const {
    // We have multiple versions of the same partition here, some segments
    // may overlap so we need to deduplicate. Also, to take retention into
    // account.
    offset_map_t offset_map;
    for (const auto& p : mat.paths) {
        auto manifest = co_await download_manifest(p);
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
              segment{.full_path = path().native(), .meta = segm.second});
        }
    }
    co_return std::move(offset_map);
}

// entry point for the whole thing
ss::future<> topic_downloader::download_log(
  const cloud_storage::remote_manifest_path& manifest_key) {
    auto prefix = std::filesystem::path(_ntpc.work_directory());
    auto retention = get_retention_policy(_ntpc.get_overrides());
    vlog(
      _ctxlog.info,
      "The target path: {}, ntp-config revision: {}, retention: {}",
      prefix,
      _ntpc.get_revision(),
      retention);
    auto mat = co_await find_recovery_material(manifest_key, _ntpc);
    auto offset_map = co_await build_offset_map(mat);
    cloud_storage::manifest target(_ntpc.ntp(), _ntpc.get_revision());
    for (const auto& kv : offset_map) {
        // Original manifests contain short names (e.g. 1029-4-v1.log).
        // This is because they belong to the same revision and the details are
        // encoded in the manifest itself.
        // To create a compound manifest we need to add full names (e.g.
        // 6fab5988/kafka/redpanda-test/5_6/80651-2-v1.log). Otherwise the
        // information in the manifest won't be suffecient.
        target.add(
          cloud_storage::segment_name(kv.second.full_path), kv.second.meta);
    }
    // Here the partition manifest 'target' may contain segments
    // that have different revision ids inside the path.
    if (target.size() == 0) {
        throw missing_partition_exception(_ntpc);
    }
    if (std::holds_alternative<std::monostate>(retention)) {
        co_await download_log(target, prefix);
    } else if (std::holds_alternative<size_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<size_bound_deletion_parameters>(retention);
        co_await download_log_with_capped_size(
          std::move(offset_map), target, prefix, r.retention_bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<time_bound_deletion_parameters>(retention);
        co_await download_log_with_capped_time(
          std::move(offset_map), prefix, r.retention_duration);
    }
    auto upl_result = co_await _remote->upload_manifest(
      _bucket, target, _rtcnode);
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
    // revision is different).
    if (mat.topic_manifest.get_revision() != _ntpc.get_revision()) {
        mat.topic_manifest.set_revision(_ntpc.get_revision());
        upl_result = co_await _remote->upload_manifest(
          _bucket, mat.topic_manifest, _rtcnode);
        if (upl_result != cloud_storage::upload_result::success) {
            // That's probably fine since the archival subsystem will
            // re-upload topic manifest eventually.
            vlog(
              _ctxlog.warn,
              "Failed to upload new topic manifest {} after recovery",
              target.get_manifest_path());
        }
    }
    co_return;
}

ss::future<> topic_downloader::download_log_with_capped_size(
  absl::btree_map<model::offset, segment> offset_map,
  const cloud_storage::manifest& manifest,
  const std::filesystem::path& prefix,
  size_t max_size,
  retry_chain_logger& ctxlog) {
    vlog(ctxlog.info, "Starting log download with size limit at {}", max_size);
    gate_guard guard(_gate);
    size_t total_size = 0;
    for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
        const auto& meta = it->second.meta;
        if (total_size > max_size) {
            vlog(
              ctxlog.debug,
              "Max size {} reached, skipping {}",
              total_size,
              it->second.full_path);
            break;
        } else {
            vlog(
              ctxlog.debug,
              "Downloading {}, total log size {}",
              it->second.full_path,
              total_size);
        }
        auto fname = cloud_storage::segment_name(
          std::filesystem::path(it->second.full_path).filename().string());
        co_await download_file(fname, manifest, prefix);
        total_size += meta.size_bytes;
    }
}

ss::future<> topic_downloader::download_log_with_capped_time(
  offset_map_t offset_map,
  const std::filesystem::path& prefix,
  ss::lowres_clock::duration time_boundary) {}

ss::future<> topic_downloader::download_log(
  const cloud_storage::manifest& manifest,
  const std::filesystem::path& prefix) {
    return ss::with_gate(_gate, [this, &manifest, prefix] {
        return ss::parallel_for_each(
          manifest, [this, &manifest, prefix](const auto& kv) {
              return download_file(kv.first, manifest, prefix).discard_result();
          });
    });
}

ss::future<cloud_storage::manifest> topic_downloader::download_manifest(
  const cloud_storage::remote_manifest_path& key) const {
    vlog(
      _ctxlog.info,
      "Downloading manifest {}, rev {}",
      _ntpc.ntp().path(),
      _ntpc.get_revision());
    cloud_storage::manifest manifest(_ntpc.ntp(), _ntpc.get_revision());
    auto result = co_await _remote->download_manifest(
      _bucket, key, manifest, _rtcnode);
    if (result != cloud_storage::download_result::success) {
        throw missing_partition_exception(ntp_cfg);
    }
    co_return manifest;
}

ss::future<topic_downloader::recovery_material>
topic_downloader::find_recovery_material(
  const cloud_storage::remote_manifest_path& key,
  const ntp_config& ntp_cfg) const {
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
        auto res = cloud_storage::get_manifest_path_components(path);
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

ss::future<std::filesystem::path> topic_downloader::download_file(
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

    co_return prefix / std::filesystem::path(target()).filename();
}

} // namespace storage
