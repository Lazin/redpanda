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

/// Topic downloader is used to download topic segments from S3 (or compatible
/// storage) during topic re-creation
class topic_downloader {
public:
    explicit topic_downloader(size_t max_concurrency);

    topic_downloader(const topic_downloader&) = delete;
    topic_downloader(topic_downloader&&) = delete;
    topic_downloader& operator=(const topic_downloader&) = delete;
    topic_downloader& operator=(topic_downloader&&) = delete;

    ~topic_downloader();

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
    /// Download full log based on manifest data
    ss::future<> download_log(
      const cloud_storage::remote_manifest_path& key,
      const ntp_config& ntp_cfg);

    ss::future<> download_log(
      const cloud_storage::manifest& manifest,
      const std::filesystem::path& prefix);

    ss::future<cloud_storage::manifest> download_manifest(
      const cloud_storage::remote_manifest_path& path,
      const ntp_config& ntp_cfg);
    
    struct recovery_material {
        std::vector<cloud_storage::remote_manifest_path> paths;
        cloud_storage::topic_manifest topic_manifest;
    };

    /// Locate all data needed to recover single partition
    ss::future<recovery_material>
    find_recovery_material(
      const cloud_storage::remote_manifest_path& key,
      const ntp_config& ntp_cfg);

    /// Find all candidate partition manifests
    ss::future<std::vector<cloud_storage::remote_manifest_path>>
    find_matching_partition_manifests(
      cloud_storage::topic_manifest& manifest, const ntp_config& cfg);

    ss::future<> download_file(
      const cloud_storage::segment_name& target,
      const cloud_storage::manifest& manifest,
      const std::filesystem::path& prefix);

    s3::bucket_name _bucket;
    cloud_storage::remote* _remote;
    ss::abort_source _cancel;
    ss::gate _gate;
    retry_chain_node _root;
    ss::semaphore _sem;
};

} // namespace storage
