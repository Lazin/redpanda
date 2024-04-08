/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "archival/archiver_operations_impl.h"

#include "archival/archiver_operations_api.h"
#include "archival/types.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment_index.h"
#include "cloud_storage/tx_range_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/partition_manager.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "storage/batch_consumer_utils.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include <exception>

namespace archival {
namespace detail {

// NOTE: the type parameters are needed so we could use gmock to test this code
// only some of our classes expose public abstract interface that can be mocked.
// The large chunk of the code can't be mocked directly. But we can use these
// type parameter to replace the types with the corresponding mocks.
class archiver_operations_impl : public archiver_operations_api {
    /// Combined result of several uploads
    struct aggregated_upload_result {
        cloud_storage::upload_result code{
          cloud_storage::upload_result::success};
        /// Populated on success
        std::optional<cloud_storage::segment_record_stats> stats;
        /// Total number of PUT requests used
        size_t put_requests{0};
        /// Total number of bytes sent
        size_t bytes_sent{0};

        // Combine aggregated results
        ///
        /// The error codes are combined using max function (the worst
        /// error wins). The order is: success < timeout < failure < cancelled.
        /// The 'stats' objects can't be combined so the series of aggregated
        /// results can't contains more than one 'stats' instances.
        void combine(const aggregated_upload_result& other) {
            put_requests += other.put_requests;
            bytes_sent += other.bytes_sent;
            if (!stats.has_value()) {
                stats = other.stats;
            }
            code = std::max(code, other.code);
        }
    };

public:
    /// C-tor
    archiver_operations_impl(
      ss::shared_ptr<cloud_storage_remote_api> api,
      ss::shared_ptr<cloud_storage_cache_put_api> cache,
      ss::shared_ptr<cluster_partition_manager_api> pm,
      ss::shared_ptr<segment_upload_builder_api> upl_builder,
      ss::scheduling_group sg,
      cloud_storage_clients::bucket_name bucket)
      : _rtc(_as)
      , _rtclog(archival_log, _rtc)
      , _api(std::move(api))
      , _cache(std::move(cache))
      , _upl_builder(std::move(upl_builder))
      , _pm(std::move(pm))
      , _read_buffer_size(
          config::shard_local_cfg().storage_read_buffer_size.bind())
      , _readahead(
          config::shard_local_cfg().storage_read_readahead_count.bind())
      , _bucket(std::move(bucket))
      , _sg(sg) {}

    /// Return upload candidate(s) if data is available or nullopt
    /// if there is not enough data to start an upload.
    ss::future<result<find_upload_candidates_result>> find_upload_candidates(
      retry_chain_node& workflow_rtc,
      find_upload_candidates_arg arg) noexcept override {
        vlog(_rtclog.debug, "find_upload_candidates {}", arg);
        try {
            auto partition = _pm->get_partition(arg.ntp);
            if (partition == nullptr) {
                // maybe race condition (partition was stopped or moved)
                vlog(
                  _rtclog.debug,
                  "find_upload_candidates - can't find partition {}",
                  arg.ntp);
                co_return error_outcome::unexpected_failure;
            }
            find_upload_candidates_result result{
              .ntp = arg.ntp,
              .read_write_fence = partition->get_applied_offset(),
            };
            auto base_offset = model::next_offset(
              partition->get_uploaded_offset());

            // If not limit is specified (quota is set to -1) we don't want to
            // upload unlimited amount of data in one iteration. The defaults in
            // this case are configured to allow only one segment to be
            // uploaded.
            auto size_quota = arg.upload_size_quota > 0 ? arg.upload_size_quota
                                                        : arg.target_size;
            constexpr size_t default_req_quota
              = 4; // up to 3 requests to upload one segment + manifest
            auto req_quota = arg.upload_requests_quota > 0
                               ? arg.upload_requests_quota
                               : default_req_quota;

            size_limited_offset_range range(
              base_offset, arg.target_size, arg.min_size);

            retry_chain_node op_rtc(&workflow_rtc);

            vlog(
              _rtclog.debug,
              "start collecting segments, base_offset {}, size quota {}, "
              "requests "
              "quota {}",
              base_offset,
              size_quota,
              req_quota);
            while (size_quota > 0 && req_quota > 0) {
                auto upload = co_await make_non_compacted_upload(
                  base_offset, partition, arg, _sg, op_rtc.get_deadline());

                if (
                  upload.has_error()
                  && upload.error() == error_outcome::not_enough_data) {
                    vlog(
                      _rtclog.debug,
                      "make_non_compacted_upload failed {}",
                      upload.error());
                    break;
                }
                if (upload.has_error()) {
                    vlog(
                      _rtclog.error,
                      "make_non_compacted_upload failed {}",
                      upload.error());
                    co_return upload.error();
                }
                vlog(
                  _rtclog.debug,
                  "make_non_compacted_upload success {}",
                  upload.value());
                base_offset = model::next_offset(
                  upload.value()->metadata.committed_offset);
                size_quota -= upload.value()->size_bytes;
                req_quota
                  -= 2; // 2 PUT requests for segment upload + index upload
                if (upload.value()->metadata.metadata_size_hint > 0) {
                    // another PUT request if tx-manifest is not empty
                    req_quota -= 1;
                }
                result.results.push_back(std::move(upload.value()));
            }

            vlog(
              _rtclog.debug,
              "find_upload_candidates completed with {} results, read-write "
              "fence "
              "{}",
              result.results.size(),
              result.read_write_fence);
            // TODO: compacted upload (if possible)
            // TODO: merge adjacent segments
            co_return result;
        } catch (...) {
            vlog(
              _rtclog.error,
              "Failed to create upload candidate: {}",
              std::current_exception());
        }
        co_return error_outcome::unexpected_failure;
    }

    /// Upload data to S3 and return results
    ///
    /// The method uploads segments with their corresponding tx-manifests and
    /// indexes and also the manifest. The result contains the insync offset of
    /// the uploaded manifest. The state of the uploaded manifest doesn't
    /// include uploaded segments because they're not admitted yet.
    ss::future<result<schedule_upload_results>> schedule_uploads(
      retry_chain_node&, find_upload_candidates_result) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    /// Add metadata to the manifest by replicating archival metadata
    /// configuration batch
    ss::future<result<admit_uploads_result>> admit_uploads(
      retry_chain_node&, schedule_upload_results) noexcept override {
        // TODO: set base_timestamp and max_timestamp
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    /// Reupload manifest and replicate configuration batch
    ss::future<result<manifest_upload_result>>
    upload_manifest(retry_chain_node&, manifest_upload_arg) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    ss::future<result<apply_archive_retention_result>> apply_archive_retention(
      retry_chain_node&, apply_archive_retention_arg) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    /// Remove segments from the backlog and replicate the command that clears
    /// the backlog.
    /// The result contains read-write fence offset that can be used to
    /// replicate next batch. It also contains number of 'delete' requests used
    /// by the operation.
    ss::future<result<garbage_collect_result>> garbage_collect_archive(
      retry_chain_node&, apply_archive_retention_result) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    /// Apply retention to the region of the log managed by the STM
    ss::future<result<apply_stm_retention_result>> apply_stm_retention(
      retry_chain_node&, apply_stm_retention_arg) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

    /// Remove segments from the backlog and replicate the command that clears
    /// the backlog.
    /// The result contains read-write fence offset that can be used to
    /// replicate next batch. It also contains number of 'delete' requests used
    /// by the operation.
    ss::future<result<garbage_collect_result>> garbage_collect(
      retry_chain_node&, apply_stm_retention_result) noexcept override {
        co_return error_outcome::unexpected_failure; // Not implemented
    }

private:
    ss::future<result<ss::lw_shared_ptr<segment_upload_candidate_t>>>
    make_non_compacted_upload(
      model::offset base_offset,
      ss::shared_ptr<cluster_partition_api> part,
      find_upload_candidates_arg arg,
      ss::scheduling_group sg,
      ss::lowres_clock::time_point deadline) noexcept {
        vlog(
          _rtclog.debug,
          "make_non_compacted_upload base_offset {}, arg {}",
          base_offset,
          arg);
        try {
            auto guard = _gate.hold();
            auto candidate = ss::make_lw_shared<segment_upload_candidate_t>();
            size_limited_offset_range range(
              base_offset, arg.target_size, arg.min_size);
            auto upload = co_await _upl_builder->prepare_segment_upload(
              part, range, _read_buffer_size(), sg, deadline);
            if (upload.has_error()) {
                vlog(
                  _rtclog.warn,
                  "prepare_segment_upload failed {}",
                  upload.error());
                co_return upload.error();
            }
            auto res = std::move(upload.value());
            vlog(
              _rtclog.debug,
              "prepare_segment_upload returned meta {}, offsets {}, payload "
              "size "
              "{}",
              res->meta,
              res->offsets,
              res->size_bytes);

            // Convert metadata
            auto delta_offset = part->offset_delta(res->offsets.base);

            auto delta_offset_next = part->offset_delta(
              model::next_offset(res->offsets.last));

            // FIXME: make sure that the upload doesn't cross the term boundary
            auto segment_term = part->get_offset_term(res->offsets.base);
            if (!segment_term.has_value()) {
                vlog(
                  _rtclog.error,
                  "Can't find term for offset {}",
                  res->offsets.base);
                co_return error_outcome::offset_not_found;
            }

            cloud_storage::segment_meta segm_meta{
              .is_compacted = res->is_compacted,
              .size_bytes = res->size_bytes,
              .base_offset = res->offsets.base,
              .committed_offset = res->offsets.last,
              // Timestamps will be populated during the upload
              .base_timestamp = {},
              .max_timestamp = {},
              .delta_offset = delta_offset,
              .ntp_revision = part->get_initial_revision(),
              .archiver_term = arg.archiver_term,
              /// Term of the segment (included in segment file name)
              .segment_term = segment_term.value(),
              .delta_offset_end = delta_offset_next,
              .sname_format = cloud_storage::segment_name_format::v3,
              /// Size of the tx-range (in v3 format)
              .metadata_size_hint = 0,
            };

            candidate->ntp = arg.ntp;
            candidate->size_bytes = res->size_bytes;
            candidate->metadata = segm_meta;

            // Generate payload
            candidate->payload = std::move(res->payload);

            // Generate tx-manifest data
            auto tx = co_await get_aborted_transactions(
              part, res->offsets.base, res->offsets.last);
            candidate->tx = std::move(tx);

            if (candidate->tx.size() > 0) {
                // This value has to be set to 0 by default to avoid
                // downloading the tx-manifest in case if there were
                // no transactions in the offset range.
                candidate->metadata.metadata_size_hint = candidate->tx.size();
            }

            co_return std::move(candidate);
        } catch (...) {
            vlog(
              _rtclog.error,
              "Failed to create non-compacted upload candidate {}",
              std::current_exception());
        }
        co_return error_outcome::unexpected_failure;
    }

    ss::future<fragmented_vector<model::tx_range>> get_aborted_transactions(
      ss::shared_ptr<cluster_partition_api> part,
      model::offset base,
      model::offset last) {
        auto guard = _gate.hold();
        co_return co_await part->aborted_transactions(base, last);
    }

    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _rtclog;
    ss::shared_ptr<cloud_storage_remote_api> _api;
    ss::shared_ptr<cloud_storage_cache_put_api> _cache
      [[maybe_unused]]; // TODO: remove unused
    ss::shared_ptr<segment_upload_builder_api> _upl_builder;
    ss::shared_ptr<cluster_partition_manager_api> _pm;
    config::binding<size_t> _read_buffer_size;
    config::binding<int16_t> _readahead;
    cloud_storage_clients::bucket_name _bucket;
    ss::scheduling_group _sg;
};

ss::shared_ptr<archiver_operations_api> make_archiver_operations_api(
  ss::shared_ptr<cloud_storage_remote_api> remote,
  ss::shared_ptr<cloud_storage_cache_put_api> cache,
  ss::shared_ptr<cluster_partition_manager_api> pm,
  ss::shared_ptr<segment_upload_builder_api> upl,
  cloud_storage_clients::bucket_name bucket) {
    return ss::make_shared<archiver_operations_impl>(
      std::move(remote),
      std::move(cache),
      std::move(pm),
      std::move(upl),
      ss::default_scheduling_group(), // TODO: use proper scheduling group
      std::move(bucket));
}

} // namespace detail

} // namespace archival
