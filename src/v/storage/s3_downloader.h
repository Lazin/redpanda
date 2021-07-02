#pragma once

#include "s3/client.h"
#include "storage/ntp_config.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>

#include <compare>
#include <iterator>
#include <vector>

namespace storage {

using s3_max_attempts = named_type<size_t, struct s3_max_attempts_tag>;
using s3_max_connections = named_type<size_t, struct s3_max_connections_tag>;

struct partition_downloader_configuration {
    s3::configuration client_config;
    s3::bucket_name bucket;
    s3_max_attempts num_retries;
    s3_max_connections num_connections;
};

struct s3_manifest_entry {
    ss::sstring segment;
    size_t size;
    model::offset base_offset;
    model::offset committed_offset;
    ss::sstring s3_path;
};

/// POC s3 integration with storage
/// We should probably refactor the hell out of it
/// The downloader maps a bunch of remote locations in S3 to
/// local locations. It can download everything in a batch
/// and handle errors (re-downloads files), fails if we ran
/// out of space, etc.
class partition_downloader {
public:
    explicit partition_downloader(partition_downloader_configuration config);
    partition_downloader();
    static ss::future<partition_downloader_configuration> make_s3_config();

    partition_downloader(const partition_downloader& config) = delete;
    partition_downloader(partition_downloader&& config) = delete;
    partition_downloader& operator=(const partition_downloader& config) = delete;
    partition_downloader& operator=(partition_downloader&& config) = delete;

    ~partition_downloader();

    ss::future<> stop();

    void cancel();

    /// Download full log based on manifest data.
    /// The 'ntp_config' should have corresponding override. If override
    /// is not set nothing will happen and the returned future will be
    /// ready (not in failed state).
    ss::future<> download_log(const ntp_config& ntp_cfg);

    /// Download full log based on manifest data
    ss::future<>
    download_log(const s3::object_key& key, const ntp_config& ntp_cfg);

private:
    ss::future<std::vector<s3_manifest_entry>>
    download_manifest(const s3::object_key& key, const ntp_config& ntp_cfg);
    /// Download topic manifest parse it and return list of partition
    /// manifest locations (array index is a partition id)
    ss::future<std::vector<s3::object_key>>
    download_topic_manifest(const s3::object_key& key);
    ss::future<> download_file(
      const s3_manifest_entry& target, const std::filesystem::path& prefix);
    ss::future<> remove_file(
      const s3_manifest_entry& key, const std::filesystem::path& prefix);

    std::optional<partition_downloader_configuration> _conf;
    ss::abort_source _cancel;
    ss::gate _gate;
    ss::semaphore _dl_limit;
};

} // namespace storage
