/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "arch/ntp_archiver_service.h"

#include "arch/error.h"
#include "arch/logger.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>

namespace arch {

class arch_policy_iface {
public:
    virtual std::optional<manifest>
    make_local_manifest(storage::log_manager& lm) = 0;
};

class arch_policy_non_compacted final : public arch_policy_iface {
public:
    arch_policy_non_compacted(model::ntp ntp, model::revision_id rev)
      : _ntp(ntp)
      , _rev(rev) {}

    std::optional<manifest>
    make_local_manifest(storage::log_manager& lm) final {
        manifest tmp(_ntp, _rev);
        std::optional<storage::log> log = lm.get(_ntp);
        if (!log) {
            return std::nullopt;
        }
        auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
        if (plog == nullptr) {
            return std::nullopt;
        }
        for (const auto& segment : plog->segments()) {
            bool closed = !segment->has_appender(); // is_closed();
            bool compacted = segment->is_compacted_segment();
            if (!closed || compacted) {
                continue;
            }
            manifest::segment_meta meta{
              .is_compacted = segment->is_compacted_segment(),
              .size_bytes = segment->size_bytes(),
              .base_offset = segment->offsets().base_offset,
              .committed_offset = segment->offsets().committed_offset,
              .is_deleted_locally = false,
            };
            auto path = std::filesystem::path(segment->reader().filename());
            auto seg_name = path.filename().string();
            // generate segment path
            tmp.add(segment_name(seg_name), meta);
        }
        return tmp;
    }

private:
    model::ntp _ntp;
    model::revision_id _rev;
};

ss::shared_ptr<arch_policy_iface> make_arch_policy(
  [[maybe_unused]] archiving_policy e, model::ntp ntp, model::revision_id rev) {
    return ss::make_shared<arch_policy_non_compacted>(ntp, rev);
}

ntp_archiver::ntp_archiver(storage::ntp_config ntp, const configuration& conf)
  : _ntpc(std::move(ntp))
  , _client_conf(conf.client_config)
  , _policy(make_arch_policy(conf.policy, _ntpc.ntp(), _ntpc.get_revision()))
  , _bucket(conf.bucket_name)
  , _local(_ntpc.ntp(), _ntpc.get_revision())
  , _remote(_ntpc.ntp(), _ntpc.get_revision())
  , _gate()
  , _abort() {
    vlog(arch_log.trace, "Create ntp_archiver {}", _ntpc.ntp().path());
}

ss::future<> ntp_archiver::download_manifest() {
    auto key = _remote.get_manifest_path();
    vlog(arch_log.trace, "Download manifest {}", key());
    auto path = s3::object_key(key());
    s3::client client(_client_conf);
    auto resp = co_await client.get_object(_bucket, path);
    co_await _remote.update(resp->as_input_stream());
    co_await client.shutdown();
    co_return;
}

ss::future<> ntp_archiver::upload_manifest() {
    auto key = _remote.get_manifest_path();
    vlog(arch_log.trace, "Upload manifest {}", key());
    auto path = s3::object_key(key());
    auto [is, size] = _remote.serialize();
    s3::client client(_client_conf);
    co_await client.put_object(_bucket, path, size, std::move(is));
    co_await client.shutdown();
    co_return;
}

void ntp_archiver::update_local_manifest(storage::log_manager& lm) {
    auto m = _policy->make_local_manifest(lm);
    if (m) {
        _local = *m;
    } else {
        throw manifest_error("policy error, can't build local manifest");
    }
}

const manifest& ntp_archiver::get_local_manifest() const { return _local; }

const manifest& ntp_archiver::get_remote_manifest() const { return _remote; }

ss::future<> ntp_archiver::upload_next_candidate(
  size_t max_segments, storage::log_manager& lm) {
    vlog(arch_log.trace, "Uploading next candidate called");
    // calculate candidate set
    auto candidates = _local.difference(_remote);
    std::vector<manifest::segments_map::value_type> upload_set;
    std::copy_n(
      candidates.begin(),
      std::min(candidates.size(), max_segments),
      std::back_inserter(upload_set));
    vlog(arch_log.trace, "Upload {} elements", upload_set.size());
    // upload segments in parallel
    co_await ss::parallel_for_each(
      upload_set,
      [this, &lm](
        const manifest::segments_map::value_type& kvpair) -> ss::future<void> {
          auto [sname, meta] = kvpair;
          auto segment = get_segment(sname, lm);
          if (meta.is_deleted_locally == false && segment) {
              // match segment with metadata
              if (
                meta.is_compacted != segment->is_compacted_segment()
                || meta.size_bytes != segment->size_bytes()
                || meta.base_offset != segment->offsets().base_offset
                || meta.committed_offset
                     != segment->offsets().committed_offset) {
                  // The metadata doesn't match the segment, which means hat the
                  // local manifest needs to be updated. This can happen when 
                  // segment was compacted after updated_local_manifest was called.
                  //
                  // We shouldn't upload this segment right away because it might not
                  // match the policy.
                  co_return;
              }

              auto stream = segment->offset_data_stream(
                meta.base_offset, ss::default_priority_class());
              auto s3path = _remote.get_remote_segment_path(sname);
              vlog(arch_log.trace, "Uploading segment \"{}\" to S3", s3path());
              s3::client client(_client_conf);
              co_await client.put_object(
                _bucket,
                s3::object_key(s3path()),
                segment->size_bytes(),
                std::move(stream));
              vlog(arch_log.trace, "Completed segment \"{}\" to S3", s3path());
              co_await client.shutdown();
              _remote.add(sname, meta);
          }
          co_return;
      });
    vlog(arch_log.trace, "Completed S3 upload");
    co_await upload_manifest();
    co_return;
}

ss::lw_shared_ptr<storage::segment>
ntp_archiver::get_segment(segment_name path, storage::log_manager& lm) {
    std::optional<storage::log> log = lm.get(_ntpc.ntp());
    if (!log) {
        vlog(arch_log.trace, "log for {} not found", _ntpc.ntp());
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
          arch_log.trace,
          "comparing segment names {} and {}",
          segment_path,
          target_path);
        if (segment_path == target_path) {
            return segment;
        }
    }
    return nullptr;
}

}  // namespace
