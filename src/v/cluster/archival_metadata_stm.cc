// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/archival_metadata_stm.h"

#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "raft/consensus.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "utils/named_type.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <boost/system/detail/errc.hpp>

namespace cluster {

namespace {

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

} // namespace

struct archival_metadata_stm::segment
  : public serde::
      envelope<segment, serde::version<0>, serde::compat_version<0>> {
    // ntp_revision is needed to reconstruct full remote path of
    // the segment. Deprecated because ntp_revision is now part of
    // segment_meta.
    model::initial_revision_id ntp_revision_deprecated;
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;
};

struct archival_metadata_stm::add_segment_cmd {
    static constexpr cmd_key key{0};

    using value = segment;
};

struct archival_metadata_stm::rem_segment_cmd {
    static constexpr cmd_key key{1};

    using value = segment;
};

struct archival_metadata_stm::snapshot
  : public serde::
      envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
    std::vector<segment> segments;
};

std::vector<archival_metadata_stm::segment>
archival_metadata_stm::segments_from_manifest(
  const cloud_storage::partition_manifest& manifest) {
    std::vector<segment> segments;
    segments.reserve(manifest.size());
    for (auto [key, meta] : manifest) {
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = manifest.get_revision_id();
        }
        auto name = cloud_storage::generate_segment_name(
          key.base_offset, key.term);
        segments.push_back(segment{
          .ntp_revision_deprecated = meta.ntp_revision,
          .name = std::move(name),
          .meta = meta});
    }

    std::sort(
      segments.begin(), segments.end(), [](const auto& s1, const auto& s2) {
          return s1.meta.base_offset < s2.meta.base_offset;
      });

    return segments;
}

archival_metadata_stm::archival_metadata_stm(
  raft::consensus* raft, cloud_storage::remote& remote, ss::logger& logger)
  : cluster::persisted_stm("archival_metadata.snapshot", logger, raft)
  , _logger(logger, ssx::sformat("ntp: {}", raft->ntp()))
  , _manifest(raft->ntp(), raft->log_config().get_initial_revision())
  , _cloud_storage_api(remote) {}

// todo: return result
ss::future<std::error_code> archival_metadata_stm::add_segments(
  const cloud_storage::partition_manifest& manifest,
  retry_chain_node& rc_node) {
    return _lock.with(rc_node.get_timeout(), [this, &manifest, &rc_node] {
        return do_add_segments(manifest, rc_node);
    });
}

// todo: return result
ss::future<std::error_code> archival_metadata_stm::do_add_segments(
  const cloud_storage::partition_manifest& new_manifest,
  retry_chain_node& rc_node) {
    if (!co_await sync(rc_node.get_timeout())) {
        co_return errc::timeout;
    }

    rc_node.check_abort();

    auto add_segments = segments_from_manifest(
      new_manifest.difference(_manifest));
    auto rem_segments = segments_from_manifest(
      _manifest.difference(new_manifest));
    if (add_segments.empty() && rem_segments.empty()) {
        co_return errc::success;
    }

    storage::record_batch_builder b(
      model::record_batch_type::archival_metadata, model::offset(0));
    for (const auto& segment : rem_segments) {
        iobuf key_buf = serde::to_iobuf(rem_segment_cmd::key);
        auto record_val = rem_segment_cmd::value{segment};
        iobuf val_buf = serde::to_iobuf(std::move(record_val));
        b.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }
    for (const auto& segment : add_segments) {
        iobuf key_buf = serde::to_iobuf(add_segment_cmd::key);
        auto record_val = add_segment_cmd::value{segment};
        iobuf val_buf = serde::to_iobuf(std::move(record_val));
        b.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }

    auto batch = std::move(b).build();
    auto fut = _raft->replicate(
      _insync_term,
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    auto result = co_await ss::with_timeout(
      rc_node.get_deadline(), std::move(fut));

    if (!result) {
        vlog(
          _logger.warn,
          "error on replicating remote segment metadata: {}",
          result.error());
        co_return result.error();
    }

    auto applied = co_await wait_no_throw(
      result.value().last_offset, rc_node.get_timeout());

    if (!applied) {
        co_return errc::replication_error;
    }

    for (const auto& segment : add_segments) {
        vlog(
          _logger.info,
          "new remote segment added (name: {}, base_offset: {} last_offset: "
          "{}), "
          "remote start_offset: {}, last_offset: {}",
          segment.name,
          segment.meta.base_offset,
          segment.meta.committed_offset,
          _start_offset,
          _last_offset);
    }

    co_return errc::success;
}

ss::future<> archival_metadata_stm::apply(model::record_batch b) {
    if (b.header().type != model::record_batch_type::archival_metadata) {
        _insync_offset = b.last_offset();
        co_return;
    }

    b.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<cmd_key>(r.release_key());
        if (key == add_segment_cmd::key) {
            auto value = serde::from_iobuf<add_segment_cmd::value>(
              r.release_value());
            apply_add_segment(value);
        } else if (key == rem_segment_cmd::key) {
            auto value = serde::from_iobuf<rem_segment_cmd::value>(
              r.release_value());
            apply_rem_segment(value);
        }
    });

    _insync_offset = b.last_offset();
}

ss::future<> archival_metadata_stm::handle_eviction() {
    cloud_storage::partition_manifest manifest;

    auto bucket = config::shard_local_cfg().cloud_storage_bucket.value();
    vassert(bucket, "configuration property cloud_storage_bucket must be set");

    auto timeout
      = config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms();
    auto backoff = config::shard_local_cfg().cloud_storage_initial_backoff_ms();

    retry_chain_node rc_node(_download_as, timeout, backoff);
    auto res = co_await _cloud_storage_api.download_manifest(
      s3::bucket_name{*bucket},
      _manifest.get_manifest_path(),
      manifest,
      rc_node);

    if (res == cloud_storage::download_result::notfound) {
        _insync_offset = raft::details::prev_offset(_raft->start_offset());
        set_next(_raft->start_offset());
        vlog(_logger.info, "handled log eviction, the manifest is absent");
        co_return;
    } else if (res != cloud_storage::download_result::success) {
        // sleep to the end of timeout to avoid calling handle_eviction in a
        // busy loop.
        co_await ss::sleep_abortable(rc_node.get_timeout(), _download_as);
        throw std::runtime_error{fmt::format(
          "couldn't download manifest {}: {}",
          _manifest.get_manifest_path(),
          res)};
    }

    _manifest = std::move(manifest);
    for (const auto& segment : _manifest) {
        if (
          _start_offset == model::offset{}
          || segment.second.base_offset < _start_offset) {
            _start_offset = segment.second.base_offset;
        }
    }
    _last_offset = _manifest.get_last_offset();

    // We can skip all offsets up to the _last_offset because we can be sure
    // that in the skipped batches there won't be any new remote segments.
    _insync_offset = _last_offset;
    auto next_offset = std::max(
      _raft->start_offset(), raft::details::next_offset(_insync_offset));
    set_next(next_offset);

    vlog(
      _logger.info,
      "handled log eviction, next offset: {}, remote start_offset: {}, "
      "last_offset: {}",
      next_offset,
      _start_offset,
      _last_offset);
}

ss::future<> archival_metadata_stm::apply_snapshot(
  stm_snapshot_header header, iobuf&& data) {
    auto snap = serde::from_iobuf<snapshot>(std::move(data));

    _manifest = cloud_storage::partition_manifest(
      _raft->ntp(), _raft->log_config().get_initial_revision());
    for (const auto& segment : snap.segments) {
        apply_add_segment(segment);
    }

    vlog(
      _logger.info,
      "applied snapshot at offset: {}, remote start_offset: {}, last_offset: "
      "{}",
      header.offset,
      _start_offset,
      _last_offset);

    _last_snapshot_offset = header.offset;
    _insync_offset = header.offset;
    co_return;
}

ss::future<stm_snapshot> archival_metadata_stm::take_snapshot() {
    auto segments = segments_from_manifest(_manifest);
    iobuf snap_data = serde::to_iobuf(
      snapshot{.segments = std::move(segments)});

    vlog(
      _logger.info,
      "creating snapshot at offset: {}, remote start_offset: {}, last_offset: "
      "{}",
      _insync_offset,
      _start_offset,
      _last_offset);
    co_return stm_snapshot::create(0, _insync_offset, std::move(snap_data));
}

model::offset archival_metadata_stm::max_collectible_offset() {
    if (
      !_raft->log_config().is_archival_enabled()
      && !config::shard_local_cfg().cloud_storage_enable_remote_write.value()) {
        // The archival is disabled but the state machine still exists so we
        // shouldn't stop eviction from happening.
        return model::offset::max();
    }
    return _last_offset;
}

void archival_metadata_stm::apply_add_segment(const segment& segment) {
    auto meta = segment.meta;
    if (meta.ntp_revision == model::initial_revision_id{}) {
        // metadata serialized by old versions of redpanda doesn't have the
        // ntp_revision field.
        meta.ntp_revision = segment.ntp_revision_deprecated;
    }
    _manifest.add(segment.name, segment.meta);

    // NOTE: here we don't take into account possibility of holes in the
    // remote offset range. Archival tries to upload segments in order, and
    // if for some reason is a hole, there are no mechanisms for correcting it.

    if (_start_offset == model::offset{} || meta.base_offset < _start_offset) {
        _start_offset = meta.base_offset;
    }

    if (meta.committed_offset > _last_offset) {
        if (meta.base_offset > raft::details::next_offset(_last_offset)) {
            // To ensure forward progress, we print a warning and skip over the
            // hole.

            vlog(
              _logger.warn,
              "hole in the remote offset range detected! previous last offset: "
              "{}, new segment base offset: {}",
              _last_offset,
              meta.base_offset);
        }

        _last_offset = meta.committed_offset;
    }
}

void archival_metadata_stm::apply_rem_segment(const segment& segment) {
    if (!_manifest.remove(segment.name, segment.meta)) {
        vlog(
          _logger.info,
          "Failed to remove segment from the manifiest. Segment base offset: "
          "{}",
          segment.meta.base_offset);
        return;
    }
    if (_manifest.size()) {
        _start_offset = _manifest.begin()->second.base_offset;
        _last_offset = _manifest.rbegin()->second.committed_offset;
    } else {
        _start_offset = model::offset{};
        _last_offset = model::offset{};
    }
    vlog(_logger.info, "segment {} deleted from the manifest", segment.name);
}

ss::future<> archival_metadata_stm::stop() {
    _download_as.request_abort();
    co_await raft::state_machine::stop();
}

} // namespace cluster
