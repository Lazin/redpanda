#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "s3/client.h"
#include "storage/ntp_config.h"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

#include <compare>
#include <iterator>
#include <vector>

namespace storage {

/// Data recovery provider is used to download topic segments from S3 (or
/// compatible storage) during topic re-creation process
class data_recovery_provider {
public:
    data_recovery_provider() = default;

    data_recovery_provider(const data_recovery_provider&) = delete;
    data_recovery_provider(data_recovery_provider&&) = delete;
    data_recovery_provider& operator=(const data_recovery_provider&) = delete;
    data_recovery_provider& operator=(data_recovery_provider&&) = delete;

    ~data_recovery_provider();

    ss::future<> stop();

    void set_remote(s3::bucket_name bucket, cloud_storage::remote* remote);

    void cancel();

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    /// \return true if log was actually downloaded, false otherwise
    ss::future<bool> download_log(const ntp_config& ntp_cfg);

private:
    s3::bucket_name _bucket;
    cloud_storage::remote* _remote;
    ss::abort_source _cancel;
    ss::gate _gate;
    retry_chain_node _root;
};

/// Topic downloader is used to download topic segments from S3 (or compatible
/// storage) during topic re-creation
class topic_downloader {
public:
    topic_downloader(
      const ntp_config& ntpc,
      cloud_storage::remote* remote,
      s3::bucket_name bucket,
      ss::abort_source& as_root,
      ss::gate& gate_root,
      retry_chain_node& parent);

    topic_downloader(const topic_downloader&) = delete;
    topic_downloader(topic_downloader&&) = delete;
    topic_downloader& operator=(const topic_downloader&) = delete;
    topic_downloader& operator=(topic_downloader&&) = delete;
    ~topic_downloader() = default;

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    /// \return true if log was actually downloaded, false otherwise
    ss::future<bool> download_log();

private:
    /// Download full log based on manifest data
    ss::future<> download_log(const cloud_storage::remote_manifest_path& key);

    ss::future<> download_log(
      const cloud_storage::manifest& manifest,
      const std::filesystem::path& prefix);

    ss::future<cloud_storage::manifest>
    download_manifest(const cloud_storage::remote_manifest_path& path) const;

    struct recovery_material {
        std::vector<cloud_storage::remote_manifest_path> paths;
        cloud_storage::topic_manifest topic_manifest;
    };

    /// Locate all data needed to recover single partition
    ss::future<recovery_material> find_recovery_material(
      const cloud_storage::remote_manifest_path& key,
      const ntp_config& ntp_cfg) const;

    /// Find all candidate partition manifests
    ss::future<std::vector<cloud_storage::remote_manifest_path>>
    find_matching_partition_manifests(
      cloud_storage::topic_manifest& manifest, const ntp_config& cfg);

    ss::future<std::filesystem::path> download_file(
      const cloud_storage::segment_name& target,
      const cloud_storage::manifest& manifest,
      const std::filesystem::path& prefix);

    struct segment {
        ss::sstring full_path;
        cloud_storage::manifest::segment_meta meta;
    };

    using offset_map_t = absl::btree_map<model::offset, segment>;

    ss::future<offset_map_t>
    build_offset_map(const recovery_material& mat) const;

    ss::future<> download_log_with_capped_size(
      offset_map_t offset_map,
      const cloud_storage::manifest& manifest,
      const std::filesystem::path& prefix,
      size_t max_size,
      retry_chain_logger& ctxlog);

    ss::future<> download_log_with_capped_time(
      offset_map_t offset_map,
      const std::filesystem::path& prefix,
      ss::lowres_clock::duration time_boundary);

    const ntp_config& _ntpc;
    s3::bucket_name _bucket;
    cloud_storage::remote* _remote;
    ss::abort_source& _cancel;
    ss::gate& _gate;
    retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
};

} // namespace storage
