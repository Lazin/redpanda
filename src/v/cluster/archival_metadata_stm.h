/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cluster/persisted_stm.h"
#include "model/metadata.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/log.hh>

#include <system_error>

namespace cluster {

/// This replicated state machine allows storing archival manifest (a set of
/// segments archived to cloud storage) in the archived partition log itself.
/// This is needed to 1) avoid querying cloud storage on partition startup and
/// 2) to replicate metadata to raft followers so that they can decide which
/// segments can be safely evicted.
class archival_metadata_stm final : public persisted_stm {
public:
    explicit archival_metadata_stm(
      raft::consensus*, cloud_storage::remote& remote, ss::logger& logger);

    /// NOTE: Method is depricated!!!
    /// Add the difference between manifests to the raft log, replicate it and
    /// wait until it is applied to the STM.
    ss::future<std::error_code>
    add_segments(const cloud_storage::partition_manifest&, retry_chain_node&);

    /// Segment key and metadata
    struct segment_kv_item {
        cloud_storage::partition_manifest::key key;
        cloud_storage::partition_manifest::segment_meta meta;
    };

    /// Add segments to the archival metadata snapshot. Replicate the changes
    /// and wait until they're applied to the STM.
    ss::future<std::error_code>
    add_segments(std::vector<segment_kv_item>, retry_chain_node&);

    /// Remove segments from the archival metadata snapshot. Replicate the
    /// changes and wait until they're applied to the STM.
    ss::future<std::error_code>
    rem_segments(std::vector<segment_kv_item>, retry_chain_node&);

    /// A set of archived segments. NOTE: manifest can be out-of-date if this
    /// node is not leader; or if the STM hasn't yet performed sync; or if the
    /// node has lost leadership. But it will contain segments successfully
    /// added with `add_segments`.
    const cloud_storage::partition_manifest& manifest() const {
        return _manifest;
    }

    ss::future<> stop() override;

private:
    ss::future<std::error_code> do_add_segments(
      const cloud_storage::partition_manifest&, retry_chain_node&);

    ss::future<std::error_code> do_add_segments(
      std::vector<segment_kv_item>,
      std::vector<segment_kv_item>,
      retry_chain_node&);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> handle_eviction() override;

    /// Upload serialized manifest to S3
    ss::future<std::error_code> upload_manifest(retry_chain_node&);

    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;
    model::offset max_collectible_offset() override;

    struct segment;
    struct add_segment_cmd;
    struct rem_segment_cmd;
    struct snapshot;

    static std::vector<segment>
    segments_from_manifest(const cloud_storage::partition_manifest& manifest);

    void apply_add_segment(const segment& segment);
    void apply_rem_segment(const segment& segment);

private:
    prefix_logger _logger;

    mutex _lock;

    cloud_storage::partition_manifest _manifest;
    model::offset _start_offset;
    model::offset _last_offset;

    cloud_storage::remote& _cloud_storage_api;
    ss::abort_source _download_as;
};

} // namespace cluster
