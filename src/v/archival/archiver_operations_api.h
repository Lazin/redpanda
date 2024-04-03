/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

namespace ss = seastar;

namespace archival {

/// The API provides operations that can be used by different
/// upload workflows. The object is not specific to any particular
/// NTP. Multiple workflows can use single API instance.
class archiver_operations_api {
public:
    archiver_operations_api() = default;
    archiver_operations_api(const archiver_operations_api&) = delete;
    archiver_operations_api(archiver_operations_api&&) noexcept = delete;
    archiver_operations_api& operator=(const archiver_operations_api&) = delete;
    archiver_operations_api& operator=(archiver_operations_api&&) noexcept
      = delete;
    virtual ~archiver_operations_api();

    // Segments
public:
    /// Reconciled upload candidate
    struct segment_upload_candidate_t {
        /// Partition
        model::ktp ntp;
        /// Stream of log data in cloud-storage format
        ss::input_stream<char> payload;
        /// Size of the data stream
        size_t size_bytes;
        /// Segment metadata
        cloud_storage::segment_meta metadata;

        // NOTE: the operator is needed for tests only, it doesn't check
        // equality of byte streams.
        bool operator==(const segment_upload_candidate_t& o) const noexcept;

        friend std::ostream&
        operator<<(std::ostream& o, const segment_upload_candidate_t& s);
    };
    using segment_upload_candidate_ptr
      = ss::lw_shared_ptr<segment_upload_candidate_t>;
    struct find_upload_candidates_arg {
        static constexpr auto default_initialized
          = std::numeric_limits<size_t>::max();
        model::ktp ntp;
        // Configuration parameter
        size_t target_size{default_initialized};
        // Configuration parameter
        size_t min_size{default_initialized};
        // Enforces strict upload cadence
        std::optional<std::chrono::seconds> upload_interval;
        bool compacted_reupload{false};
        bool inline_manifest{false};

        bool operator==(const find_upload_candidates_arg&) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const find_upload_candidates_arg& s);
    };

    struct find_upload_candidates_result {
        model::ktp ntp;
        std::deque<segment_upload_candidate_ptr> results;
        model::offset read_write_fence;

        bool operator==(const find_upload_candidates_result& rhs) const;

        friend std::ostream&
        operator<<(std::ostream& o, const find_upload_candidates_result& r);
    };

    /// Return upload candidate(s) if data is available or nullopt
    /// if there is not enough data to start an upload.
    virtual ss::future<result<find_upload_candidates_result>>
    find_upload_candidates(
      retry_chain_node&, find_upload_candidates_arg) noexcept
      = 0;

    struct schedule_upload_results {
        model::ktp ntp;
        std::deque<std::optional<cloud_storage::segment_record_stats>> stats;
        std::deque<cloud_storage::upload_result> results;

        // Insync offset of the uploaded manifest
        model::offset manifest_clean_offset;
        model::offset read_write_fence;

        size_t num_put_requests{0};
        size_t num_bytes_sent{0};

        bool operator==(const schedule_upload_results& o) const noexcept;

        friend std::ostream&
        operator<<(std::ostream& o, const schedule_upload_results& s);
    };

    /// Upload data to S3 and return results
    ///
    /// The method uploads segments with their corresponding tx-manifests and
    /// indexes and also the manifest. The result contains the insync offset of
    /// the uploaded manifest. The state of the uploaded manifest doesn't
    /// include uploaded segments because they're not admitted yet.
    virtual ss::future<result<schedule_upload_results>>
    schedule_uploads(retry_chain_node&, find_upload_candidates_result) noexcept
      = 0;

    struct admit_uploads_result {
        model::ktp ntp;
        size_t num_succeeded{0};
        size_t num_failed{0};

        // The in-sync offset of the manifest after update
        model::offset manifest_dirty_offset;

        auto operator<=>(const admit_uploads_result&) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const admit_uploads_result& s);
    };
    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    virtual ss::future<result<admit_uploads_result>>
    admit_uploads(retry_chain_node&, schedule_upload_results) noexcept = 0;

    struct manifest_upload_arg {
        model::ktp ntp;

        bool operator==(const manifest_upload_arg& other) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const manifest_upload_arg& arg);
    };

    struct manifest_upload_result {
        model::ktp ntp;
        size_t num_put_requests{0};
        size_t size_bytes{0};

        bool operator==(const manifest_upload_result& other) const = default;

        friend std::ostream&
        operator<<(std::ostream& o, const manifest_upload_result& arg);
    };

    /// Reupload manifest and replicate configuration batch
    virtual ss::future<result<manifest_upload_result>>
    upload_manifest(retry_chain_node&, manifest_upload_arg) noexcept = 0;
};

} // namespace archival
