/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archival_policy.h"

#include "archival/logger.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace archival {

using namespace std::chrono_literals;

std::ostream& operator<<(std::ostream& s, const upload_candidate& c) {
    fmt::print(
      s,
      "{{segment: {}, exposed_name: {}, starting_offset:{},  final_offset:{}, "
      "file_offset: {}, content_length: {}}}",
      *c.source,
      c.exposed_name,
      c.starting_offset,
      c.final_offset,
      c.file_offset,
      c.content_length);
    return s;
}

archival_policy::archival_policy(
  model::ntp ntp,
  service_probe& svc_probe,
  ntp_level_probe& ntp_probe,
  std::optional<segment_time_limit> limit)
  : _ntp(std::move(ntp))
  , _svc_probe(svc_probe)
  , _ntp_probe(ntp_probe)
  , _upload_limit(limit) {}

bool archival_policy::upload_limit_reached() {
    if (!_upload_limit.has_value()) {
        return false;
    }
    auto now = ss::lowres_clock::now();

    if (!_upload_deadline.has_value()) {
        _upload_deadline = now + (*_upload_limit)();
    }
    return _upload_deadline < now;
}

archival_policy::lookup_result archival_policy::find_segment(
  model::offset last_offset,
  model::offset high_watermark,
  storage::log_manager& lm) {
    vlog(
      archival_log.debug,
      "Upload policy for {} invoked, last-offset: {}",
      _ntp,
      last_offset);
    std::optional<storage::log> log = lm.get(_ntp);
    if (!log) {
        vlog(archival_log.warn, "Upload policy for {} no such ntp", _ntp);
        return {};
    }
    auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
    // NOTE: we need to break encapsulation here to access underlying
    // implementation because upload policy and archival subsystem needs to
    // access individual log segments (disk backed).
    if (plog == nullptr || plog->segment_count() == 0) {
        vlog(
          archival_log.debug,
          "Upload policy for {} no segments or in-memory log",
          _ntp);
        return {};
    }
    const auto& set = plog->segments();

    if (last_offset <= high_watermark) {
        _ntp_probe.upload_lag(high_watermark - last_offset);
    }

    const auto& ntp_conf = plog->config();
    auto it = set.lower_bound(last_offset);
    if (it == set.end() || (*it)->is_compacted_segment()) {
        // Skip forward if we hit a gap or compacted segment
        for (auto i = set.begin(); i != set.end(); i++) {
            const auto& sg = *i;
            if (last_offset < sg->offsets().base_offset) {
                // Move last offset forward
                it = i;
                auto offset = sg->offsets().base_offset;
                auto delta = offset - last_offset;
                last_offset = offset;
                _svc_probe.add_gap();
                _ntp_probe.gap_detected(delta);
                break;
            }
        }
    }
    if (it == set.end()) {
        vlog(
          archival_log.trace,
          "Upload policy for {}, upload candidate is not found",
          _ntp);
        return {};
    }
    // Invariant: it != set.end()
    bool closed = !(*it)->has_appender();
    bool force_upload = upload_limit_reached();
    if (!closed && !force_upload) {
        // Fast path, next upload candidate is not yet sealed. We may want to
        // optimize this case because it's expected to happen pretty often. This
        // can be done by saving weak_ptr to the segment inside the policy
        // object. The segment must be changed to derive from
        // ss::weakly_referencable.
        vlog(
          archival_log.debug,
          "Upload policy for {}, upload candidate is not closed",
          _ntp);
        return {};
    }
    auto dirty_offset = (*it)->offsets().dirty_offset;
    if (dirty_offset > high_watermark && !force_upload) {
        vlog(
          archival_log.debug,
          "Upload policy for {}, upload candidate offset {} is below high "
          "watermark {}",
          dirty_offset,
          high_watermark,
          _ntp);
        return {};
    }
    if (_upload_limit) {
        _upload_deadline = ss::lowres_clock::now() + _upload_limit.value()();
    }
    return {.segment = *it, .ntp_conf = &ntp_conf};
}

/// \brief Initializes upload_candidate structure taking into account
///        possible segment overlaps at 'off'
///
/// \param off_begin is a last_offset from manifest
/// \param off_end is a last offset to upload
/// \param segment is a segment that has this offset
/// \param ntp_conf is a ntp_config of the partition
///
/// \note Normally, the segments on a single node doesn't overlap,
///       but when leadership changes the new node will have to deal
///       with the situaiton when 'last_offset' doesn't match base
///       offset of any segment. In this situation we need to find
///       the segment that contains the 'last_offset' and find the
///       exact location of the 'last_offset' inside the segment (
///       or nearset offset, because index is sampled). Then we need
///       to compute file offset of the upload, content length and
///       new base offset (and new segment name for S3).
///       Example: last_offset is 1000, and we have segment with the
///       name '900-1-v1.log'. We should find offset 1000 inside it
///       and upload starting from it. The resulting file should have
///       a name '1000-1-v1.log'. If we were only able to find offset
///       990 instead of 1000, we will upload starting from it and
///       the name will be '990-1-v1.log'.
static upload_candidate create_upload_candidate(
  model::offset off_begin,
  model::offset off_end,
  const ss::lw_shared_ptr<storage::segment>& segment,
  const storage::ntp_config* ntp_conf) {
    auto term = segment->offsets().term;
    auto version = storage::record_version_type::v1;
    auto meta = storage::segment_path::parse_segment_filename(
      segment->reader().filename());
    if (meta) {
        version = meta->version;
    }
    size_t fsize = segment->reader().file_size();
    auto ix_begin = segment->index().find_nearest(off_begin);
    auto ix_end = segment->index().find_nearest(off_end);
    if (
      ix_begin.has_value() == false || ix_end.has_value() == false
      || (segment->offsets().base_offset == off_begin && segment->offsets().dirty_offset == off_end)) {
        // Fast path.
        // (unlikely) We're uploading full segment because there is no offset
        // index. Or we're uploading a full segment from the begining to the
        // end.
        auto orig_path = std::filesystem::path(segment->reader().filename());
        vlog(
          archival_log.debug,
          "Uploading full segment {}, last_offset: {}, located segment: {}, "
          "file size: {}",
          segment->reader().filename(),
          off_begin,
          segment->offsets(),
          fsize);
        return {
          .source = segment,
          .exposed_name = segment_name(orig_path.filename().string()),
          .starting_offset = segment->offsets().base_offset,
          .final_offset = segment->offsets().dirty_offset,
          .file_offset = 0,
          .content_length = fsize};
    }
    // This code path is responsible for partial upload.
    // The partial upload is either started from arbitrary offset or ends on
    // arbitrary offset or both.
    // Invariant: is_end can't be std::nullopt since we checked that
    vassert(
      ix_begin.has_value() && ix_end.has_value(),
      "Invariant is broken, the index can't be used.");
    size_t pos_begin = ix_begin->filepos;
    // The index lookup can undershoot, but if we know that the off_end is the
    // last offset of the segment, we can compute precise file position.
    size_t pos_end = off_end == segment->offsets().committed_offset
                       ? segment->size_bytes()
                       : ix_end->filepos;
    size_t clen = pos_end - pos_begin;
    auto path = storage::segment_path::make_segment_path(
      *ntp_conf, ix_begin->offset, term, version);
    vlog(
      archival_log.debug,
      "Uploading part of the segment {}, last_offset: {}, segment offsets: {}, "
      "upload size: {}, starting from the offset {} to {}",
      segment->reader().filename(),
      off_begin,
      segment->offsets(),
      clen,
      pos_begin,
      pos_end);
    return {
      .source = segment,
      .exposed_name = segment_name(path.filename().string()),
      .starting_offset = ix_begin->offset,
      .final_offset = ix_end->offset,
      .file_offset = pos_begin,
      .content_length = clen};
}

upload_candidate archival_policy::get_next_candidate(
  model::offset last_offset,
  model::offset high_watermark,
  storage::log_manager& lm) {
    auto [segment, ntp_conf] = find_segment(last_offset, high_watermark, lm);
    if (segment.get() == nullptr || ntp_conf == nullptr) {
        return {};
    }
    auto first = last_offset;
    auto last = std::min(segment->offsets().dirty_offset, high_watermark);
    return create_upload_candidate(first, last, segment, ntp_conf);
}

} // namespace archival
