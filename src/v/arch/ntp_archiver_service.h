/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "arch/manifest.h"
#include "model/fundamental.h"
#include "s3/client.h"
#include "storage/api.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/segment_set.h"

#include <seastar/core/abort_source.hh>

namespace arch {

/// Augments the way archiver works
enum class archiving_policy {
    /// Archive only original, non-compacted segments
    archive_non_compacted,
};

/// Archiving policy implementation fwd decl
class arch_policy_iface;

/// Archiver service configuration
struct configuration {
    s3::configuration client_config;
    s3::bucket_name bucket_name;
    archiving_policy policy;
};

/// This class performs per-ntp arhcival workload. Every ntp can be
/// processed independently, without the knowledge about others. All
/// 'ntp_archiver' instances that the shard posesses are supposed to be
/// aggregated on a higher level in the 'archiver_service'.
///
/// The 'ntp_archiver' is responsible for manifest manitpulations and
/// generation of per-ntp candidate set. The actual file uploads are
/// handled by 'archiver_service'.
class ntp_archiver {
public:
    /// Iterator type used to retrieve candidates for upload
    using back_insert_iterator
      = std::back_insert_iterator<std::vector<arch::segment_name>>;

    /// Create new instance
    ///
    /// \param ntp is an ntp that archiver is responsible for
    /// \param conf is an S3 client configuration
    /// \param bucket is an S3 bucket that should be used to store the data
    ntp_archiver(storage::ntp_config ntp, const configuration& conf);

    /// Download manifest from pre-defined S3 location
    ss::future<> download_manifest();

    /// Upload manifest to the pre-defined S3 location
    ss::future<> upload_manifest();

    /// Upload segment and re-upload
    // ss::future<> upload_segment(ss::lw_shared_ptr<storage::segment> segment);

    /// Update local manifest
    void update_local_manifest(storage::log_manager& lm);

    const manifest& get_local_manifest() const;
    const manifest& get_remote_manifest() const;

    /// Upload next segment to S3 (if any)
    ///
    /// \param max_segments is max number of segments to send in parallel
    /// \param lm is a log manager instance
    /// \return future that returns number of bytes or zero if nothing was
    /// uploaded
    ss::future<>
    upload_next_candidate(size_t max_elements, storage::log_manager& lm);

private:
    /// Get segment from log_manager instance by path
    ///
    /// \param path is a segment path (from the manifest)
    /// \param lm is a log manager instance
    /// \return pointer to segment instance or null
    ss::lw_shared_ptr<storage::segment>
    get_segment(segment_name path, storage::log_manager& lm);

    storage::ntp_config _ntpc;
    s3::configuration _client_conf;

    ss::shared_ptr<arch_policy_iface> _policy;
    s3::bucket_name _bucket;
    /// Local manifest contains data acquired from log_manager
    manifest _local;
    /// Remote manifest contains representation of the data stored in S3 (it
    /// gets uploaded to the remote location)
    manifest _remote;
    ss::gate _gate;
    ss::abort_source _abort;
};

}  // namespace
