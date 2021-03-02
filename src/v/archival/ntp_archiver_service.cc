/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"

#include "archival/logger.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>

#include <stdexcept>

namespace archival {

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    o << "{bucket_name:" << cfg.bucket_name()
      << ",upload_policy:" << cfg.upload_policy
      << ",delete_policy:" << cfg.delete_policy
      << ",interval:" << cfg.interval.count()
      << ",gc_interval:" << cfg.gc_interval.count()
      << ",client_config:" << cfg.client_config
      << ",connection_limit:" << cfg.connection_limit() << "}";
    return o;
}

ntp_archiver::ntp_archiver(
  const storage::ntp_config& ntp, const configuration& conf)
  : _ntp(ntp.ntp())
  , _rev(ntp.get_revision())
  , _client_conf(conf.client_config)
  , _policy(
      make_archival_policy(conf.upload_policy, conf.delete_policy, _ntp, _rev))
  , _bucket(conf.bucket_name)
  , _remote(_ntp, _rev)
  , _gate() {
    vlog(archival_log.debug, "Create ntp_archiver {}", _ntp.path());
}

ss::future<> ntp_archiver::stop() {
    _as.request_abort();
    return _gate.close();
}

const model::ntp& ntp_archiver::get_ntp() const { return _ntp; }

model::revision_id ntp_archiver::get_revision_id() const { return _rev; }

const ss::lowres_clock::time_point ntp_archiver::get_last_upload_time() const {
    return _last_upload_time;
}

const ss::lowres_clock::time_point ntp_archiver::get_last_delete_time() const {
    return _last_delete_time;
}

ss::future<download_manifest_result> ntp_archiver::download_manifest() {
    gate_guard guard{_gate};
    auto key = _remote.get_manifest_path();
    vlog(archival_log.debug, "Download manifest {}", key());
    auto path = s3::object_key(key());
    s3::client client(_client_conf, _as);
    auto result = download_manifest_result::success;
    try {
        auto resp = co_await client.get_object(_bucket, path);
        vlog(archival_log.debug, "Receive OK response from {}", path);
        co_await _remote.update(resp->as_input_stream());
    } catch (const s3::rest_error_response& err) {
        if (err.code() == s3::s3_error_code::no_such_key) {
            // This can happen when we're dealing with new partition for which
            // manifest wasn't uploaded. But also, this can appen if we uploaded
            // the first segment and crashed before we were able to upload the
            // manifest. This shouldn't be the problem though. We will just
            // re-upload this segment for once.
            vlog(archival_log.debug, "NoSuchKey response received {}", path);
            result = download_manifest_result::notfound;
        } else if (err.code() == s3::s3_error_code::slow_down) {
            // This can happen when we're dealing with high request rate to the
            // manifest's prefix. 
            vlog(archival_log.debug, "SlowDown response received {}", path);
            result = download_manifest_result::backoff;
        } else {
            throw;
        }
    }
    co_await client.shutdown();
    co_return result;
}

ss::future<> ntp_archiver::upload_manifest() {
    gate_guard guard{_gate};
    auto key = _remote.get_manifest_path();
    vlog(archival_log.debug, "Upload manifest {}", key());
    auto path = s3::object_key(key());
    auto [is, size] = _remote.serialize();
    s3::client client(_client_conf, _as);
    std::vector<s3::object_tag> tags = {{"rp-type", "partition-manifest"}};
    co_await client.put_object(_bucket, path, size, std::move(is), tags);
    co_await client.shutdown();
    co_return;
}

const manifest& ntp_archiver::get_remote_manifest() const { return _remote; }

/// Returns true if segment matches the metadata
static bool validate_metadata(
  const ss::lw_shared_ptr<storage::segment>& segment,
  const manifest::segment_meta& meta) {
    // If the metadata don't match the segment then the
    // local manifest needs to be updated. This can happen when
    // segment was compacted after updated_local_manifest was
    // called.
    //
    // We shouldn't upload this segment right away because it
    // might not match the policy.
    return meta.is_compacted == segment->is_compacted_segment()
           && meta.size_bytes == segment->size_bytes()
           && meta.base_offset == segment->offsets().base_offset
           && meta.committed_offset == segment->offsets().committed_offset;
}

ss::future<bool> ntp_archiver::upload_segment(
  manifest::segment_map::value_type target, storage::log_manager& lm) {
    vlog(archival_log.debug, "Upload {} segments", target.first);
    s3::client client(_client_conf, _as);
    const auto& [sname, meta] = target;
    auto segment = get_segment(sname, lm);
    if (segment) {
        // match segment with metadata
        if (!validate_metadata(segment, meta)) {
            co_return false;
        }
        auto stream = segment->offset_data_stream(
          meta.base_offset, ss::default_priority_class());
        auto s3path = _remote.get_remote_segment_path(sname);
        vlog(archival_log.debug, "Uploading segment \"{}\" to S3", s3path());
        try {
            std::vector<s3::object_tag> tags = {{"rp-type", "segment"}};
            co_await client.put_object(
              _bucket,
              s3::object_key(s3path()),
              segment->size_bytes(),
              std::move(stream),
              tags);
            vlog(
              archival_log.debug, "Completed segment \"{}\" to S3", s3path());
            _remote.add(sname, meta);
            co_await client.shutdown();
        } catch (...) {
            vlog(
              archival_log.error,
              "Failed to upload {} to S3. Reason: {}",
              sname,
              std::current_exception());
            co_return false;
        }
    } else {
        vlog(archival_log.error, "Can't find segment {}", sname());
    }
    co_return true;
}

ss::future<ntp_archiver::batch_result> ntp_archiver::upload_next_candidate(
  ss::semaphore& req_limit, storage::log_manager& lm) {
    vlog(archival_log.debug, "Uploading next candidate called");
    gate_guard guard{_gate};
    // calculate candidate set
    auto candidates = _policy->generate_upload_set(_remote, lm);
    if (!candidates) {
        vlog(archival_log.error, "Failed to generate upload candidates");
        co_return batch_result{};
    }
    auto num_candidates = std::min(candidates->size(), req_limit.max_counter());
    if (num_candidates == 0) {
        co_return batch_result{};
    }
    auto sunits = co_await ss::get_units(req_limit, num_candidates);
    std::vector<manifest::segment_map::value_type> upload_set;
    std::copy_n(
      candidates->begin(), num_candidates, std::back_inserter(upload_set));
    vlog(archival_log.debug, "Upload {} elements", upload_set.size());
    std::vector<ss::future<bool>> flist;
    flist.reserve(upload_set.size());
    for (auto target: upload_set) {
          flist.emplace_back(upload_segment(std::move(target), lm));
    }
    auto results = co_await ss::when_all_succeed(flist.begin(), flist.end());
    batch_result agg{};
    for (auto r: results) {
        if (r) {
            agg.num_succeded++;
        } else {
            agg.num_failed++;
        }
    }
    if (num_candidates) {
        vlog(archival_log.debug, "Completed S3 upload");
        co_await upload_manifest();
        _last_upload_time = ss::lowres_clock::now();
    }
    co_return agg;
}

ss::future<bool> ntp_archiver::delete_segment(
  manifest::segment_map::value_type target, storage::log_manager& lm) {
    const auto& [sname, meta] = target;
    auto segment = get_segment(sname, lm);
    if (!segment) {
        s3::client client(_client_conf, _as);
        auto s3path = _remote.get_remote_segment_path(sname);
        vlog(archival_log.debug, "Delete segment \"{}\" from S3", s3path());
        try {
            co_await client.delete_object(_bucket, s3::object_key(s3path()));
            _remote.delete_permanently(sname);
            co_await client.shutdown();
        } catch (...) {
            vlog(
              archival_log.error,
              "Failed to delete {} from S3. Reason: {}",
              sname,
              std::current_exception());
            co_return false;
        }
    }
    co_return true;
}

ss::future<ntp_archiver::batch_result> ntp_archiver::delete_next_candidate(
  ss::semaphore& req_limit, storage::log_manager& lm) {
    vlog(archival_log.debug, "Delete next candidate called");
    gate_guard guard{_gate};
    // calculate candidate set
    auto candidates = _policy->generate_delete_set(_remote, lm);
    if (!candidates) {
        vlog(archival_log.error, "Failed to generate candidates for deletion");
        co_return batch_result{};
    }
    auto num_candidates = std::min(
      candidates->size(), req_limit.max_counter());
    if (num_candidates == 0) {
        co_return batch_result{};
    }
    std::vector<manifest::segment_map::value_type> delete_set;
    std::copy_n(
      candidates->begin(), num_candidates, std::back_inserter(delete_set));
    vlog(archival_log.debug, "Delete {} elements", delete_set.size());
    // upload segments in parallel
    std::vector<ss::future<bool>> flist;
    flist.reserve(delete_set.size());
    for (auto target: delete_set) {
          flist.emplace_back(delete_segment(std::move(target), lm));
    }
    auto results = co_await ss::when_all_succeed(flist.begin(), flist.end());
    batch_result agg{};
    for (auto r: results) {
        if (r) {
            agg.num_succeded++;
        } else {
            agg.num_failed++;
        }
    }
    if (num_candidates) {
        vlog(archival_log.debug, "Completed S3 delete");
        co_await upload_manifest();
        _last_upload_time = ss::lowres_clock::now();
    }
    co_return agg;
}

ss::lw_shared_ptr<storage::segment>
ntp_archiver::get_segment(segment_name path, storage::log_manager& lm) {
    std::optional<storage::log> log = lm.get(_ntp);
    if (!log) {
        vlog(archival_log.debug, "log for {} not found", _ntp);
        return nullptr;
    }
    auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
    if (plog == nullptr) {
        return nullptr;
    }
    std::filesystem::path target_path(path());
    for (auto& segment : plog->segments()) {
        auto segment_path = std::filesystem::path(segment->reader().filename())
                              .filename()
                              .string();
        vlog(
          archival_log.debug,
          "comparing segment names {} and {}",
          segment_path,
          target_path);
        if (segment_path == target_path) {
            return segment;
        }
    }
    return nullptr;
}

} // namespace archival
