/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/async_data_uploader.h"

#include "archival/logger.h"
#include "archival/types.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"
#include "storage/disk_log_impl.h"
#include "storage/offset_to_filepos.h"
#include "storage/parser.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_index.h"
#include "utils/retry_chain_node.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>

#include <exception>
#include <stdexcept>
#include <system_error>

namespace archival {

class reader_ds : public ss::data_source_impl {
    /// Consumer that reads data batches to the read buffer
    /// of the data source.
    class consumer {
    public:
        explicit consumer(reader_ds* ds, inclusive_offset_range range)
          : _parent(ds)
          , _range(range) {}

        /// This consumer accepts all batches if there is enough space in the
        /// read buffer.
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            vlog(
              _parent->_ctxlog.debug,
              "consume started, buffered {} bytes",
              _parent->_buffer.size_bytes());
            if (!_range.contains(batch.header())) {
                vlog(
                  _parent->_ctxlog.debug,
                  "skip batch {}-{}",
                  batch.base_offset(),
                  batch.last_offset());

                co_return ss::stop_iteration::no;
            }
            vlog(
              _parent->_ctxlog.debug,
              "consuming batch {}-{}",
              batch.base_offset(),
              batch.last_offset());
            auto hdr_iobuf = storage::disk_header_to_iobuf(batch.header());
            auto rec_iobuf = std::move(batch).release_data();
            _parent->_buffer.append(std::move(hdr_iobuf));
            _parent->_buffer.append(std::move(rec_iobuf));
            _parent->_num_batches++;
            bool max_bytes_reached = _parent->_buffer.size_bytes()
                                     > static_cast<size_t>(_parent->_max_bytes);
            vlog(
              _parent->_ctxlog.debug,
              "max_bytes_reached: {}",
              max_bytes_reached);
            co_return max_bytes_reached ? ss::stop_iteration::yes
                                        : ss::stop_iteration::no;
        }

        bool end_of_stream() const { return false; }

    private:
        reader_ds* _parent;
        inclusive_offset_range _range;
    };

    friend class consumer;

public:
    explicit reader_ds(
      model::ntp ntp,
      model::record_batch_reader r,
      size_t max_bytes,
      inclusive_offset_range range,
      model::timeout_clock::time_point deadline)
      : _reader(std::move(r))
      , _max_bytes((ssize_t)max_bytes)
      , _deadline(deadline)
      , _range(range)
      , _rtc(_as)
      , _ctxlog(archival_log, _rtc, ntp.path()) {}

    ss::future<ss::temporary_buffer<char>> get() override {
        // Consume using the reader until the reader is done
        if (_buffer.empty() && !_reader.is_end_of_stream()) {
            vlog(
              _ctxlog.trace, "Buffer is empty, pulling data from the reader");
            consumer c(this, _range);
            auto done = co_await _reader.consume(c, _deadline);
            vlog(
              _ctxlog.trace,
              "Consume returned {}, buffer has {} bytes",
              done,
              _buffer.size_bytes());
        }
        if (_buffer.empty()) {
            vlog(
              _ctxlog.trace,
              "Reader end-of-stream: {}",
              _reader.is_end_of_stream());
            ss::temporary_buffer<char> empty;
            co_return empty;
        }
        auto head = _buffer.begin();
        auto head_buf = std::move(*head).release();
        _buffer.pop_front();
        vlog(
          _ctxlog.debug,
          "get produced {} bytes, {} bytes buffered",
          head_buf.size(),
          _buffer.size_bytes());
        co_return head_buf;
    }
    ss::future<ss::temporary_buffer<char>> skip(uint64_t n) override {
        ss::temporary_buffer<char> empty;
        co_return empty;
    }
    ss::future<> close() override {
        co_await std::move(_reader).release()->finally();
    }

private:
    model::record_batch_reader _reader;
    iobuf _buffer;
    ssize_t _max_bytes;
    size_t _num_batches{};
    model::timeout_clock::time_point _deadline;
    inclusive_offset_range _range;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

ss::input_stream<char> make_reader_input_stream(
  model::ntp ntp,
  model::record_batch_reader r,
  size_t read_buffer_size,
  inclusive_offset_range range,
  model::timeout_clock::duration timeout) {
    auto deadline = model::timeout_clock::now() + timeout;
    auto ds = std::make_unique<reader_ds>(
      ntp, std::move(r), read_buffer_size, range, deadline);
    ss::input_stream<char> s(ss::data_source(std::move(ds)));
    return s;
}

segment_upload::segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  size_t read_buffer_size,
  ss::scheduling_group sg)
  : _ntp(part->get_ntp_config().ntp())
  , _part(part)
  , _rd_buffer_size(read_buffer_size)
  , _sg(sg)
  , _rtc(_as)
  , _ctxlog(archival_log, _rtc, _ntp.path()) {}

void segment_upload::throw_if_not_initialized(std::string_view caller) const {
    if (_stream.has_value() == false) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Segment upload is not initialized {} to {}",
          _ntp,
          caller));
    }
}

ss::future<result<std::unique_ptr<segment_upload>>>
segment_upload::make_segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  inclusive_offset_range range,
  size_t read_buffer_size,
  ss::scheduling_group sg,
  model::timeout_clock::time_point deadline) {
    std::unique_ptr<segment_upload> upl(
      new segment_upload(part, read_buffer_size, sg));

    auto res = co_await upl->initialize(range, deadline);
    if (res.has_failure()) {
        co_return res.as_failure();
    }
    co_return std::move(upl);
}

ss::future<result<std::unique_ptr<segment_upload>>>
segment_upload::make_segment_upload(
  ss::lw_shared_ptr<cluster::partition> part,
  size_limited_offset_range range,
  size_t read_buffer_size,
  ss::scheduling_group sg,
  model::timeout_clock::time_point deadline) {
    std::unique_ptr<segment_upload> upl(
      new segment_upload(part, read_buffer_size, sg));

    auto res = co_await upl->initialize(range, deadline);
    if (res.has_failure()) {
        co_return res.as_failure();
    }
    co_return std::move(upl);
}

ss::future<result<void>> segment_upload::initialize(
  inclusive_offset_range range, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    auto params = co_await compute_upload_parameters(range, deadline);
    if (params.has_failure()) {
        co_return params.as_failure();
    }
    _params = std::move(params.value());
    // Create a log reader config to scan the uploaded offset
    // range. We should skip the batch cache.
    storage::log_reader_config reader_cfg(
      range.base, range.last, priority_manager::local().archival_priority());
    reader_cfg.skip_batch_cache = true;
    reader_cfg.skip_readers_cache = true;
    vlog(_ctxlog.debug, "Creating log reader, config: {}", reader_cfg);
    auto reader = co_await _part->make_reader(reader_cfg);
    _stream = make_reader_input_stream(
      _ntp,
      std::move(reader),
      _rd_buffer_size,
      range,
      model::duration_to(deadline));
    co_return outcome::success();
}

ss::future<result<void>> segment_upload::initialize(
  size_limited_offset_range range, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    auto params = co_await compute_upload_parameters(range, deadline);
    if (params.has_failure()) {
        co_return params.as_failure();
    }
    _params = std::move(params.value());
    // Create a log reader config to scan the uploaded offset
    // range. We should skip the batch cache.
    storage::log_reader_config reader_cfg(
      params.value().offsets.base,
      params.value().offsets.last,
      priority_manager::local().archival_priority());
    reader_cfg.skip_batch_cache = true;
    reader_cfg.skip_readers_cache = true;
    vlog(_ctxlog.debug, "Creating log reader, config: {}", reader_cfg);
    auto reader = co_await _part->make_reader(reader_cfg);
    _stream = make_reader_input_stream(
      _ntp,
      std::move(reader),
      _rd_buffer_size,
      params.value().offsets,
      model::duration_to(deadline));
    co_return outcome::success();
}

ss::future<> segment_upload::close() {
    if (_stream.has_value()) {
        co_await _stream->close();
    }
    co_await _gate.close();

    // Return units
    _params.reset();
}

ss::future<result<segment_upload::subscan_res>> segment_upload::subscan_left(
  std::vector<ss::lw_shared_ptr<storage::segment>> segments,
  model::offset range_base,
  model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    subscan_res left_scan;
    auto segment = segments.front();
    auto ix_base = segment->index().find_nearest(range_base);
    model::offset target;
    if (!ix_base.has_value()) {
        // Scan from the beginning of the segment if we can't
        // use index.
        target = segment->offsets().base_offset;
    } else {
        target = ix_base.value().offset;
    }
    // NOTE: subscan can cross the segment boundary while looking for
    // the data timestamp
    auto subscan_res = co_await subscan(
      _part,
      inclusive_offset_range(target, range_base),
      ix_base->filepos,
      false,
      deadline);

    if (subscan_res.has_failure()) {
        // Failed to scan the beginning of the offset range.
        co_return subscan_res.as_failure();
    }

    vlog(
      _ctxlog.debug,
      "Left scan result, done: {}, offset range: {}-{}, file offset: "
      "{}, timestamps: {}-{}",
      left_scan.done,
      left_scan.base_offset,
      left_scan.last_offset,
      left_scan.filepos,
      left_scan.first_timestamp,
      left_scan.max_timestamp);

    if (!subscan_res.value().done) {
        // Subscan failed to find the data timestamp (this usually
        // requires some scanning past the target)
        co_return make_error_code(error_outcome::scan_failed);
    }
    left_scan = subscan_res.value();
    co_return left_scan;
}

ss::future<result<segment_upload::subscan_res>> segment_upload::subscan_right(
  std::vector<ss::lw_shared_ptr<storage::segment>> segments,
  model::offset range_last,
  model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    subscan_res right_scan;
    // The left and right might point to the same segment if we
    // have only one segment in the list.
    auto segment = segments.back();
    auto ix_last = segment->index().find_nearest(range_last);
    model::offset target;
    if (!ix_last.has_value()) {
        // Fallback to full segment scan
        target = segment->offsets().base_offset;
    } else {
        target = ix_last.value().offset;
    }
    auto subscan_res = co_await subscan(
      _part,
      inclusive_offset_range(target, range_last),
      ix_last->filepos,
      true,
      deadline);

    if (subscan_res.has_failure()) {
        // Failed to scan the beginning of the offset range.
        co_return subscan_res.as_failure();
    }

    vlog(
      _ctxlog.debug,
      "Right scan result, done: {}, offset range: {}-{}, file offset: "
      "{}, "
      "timestamps: {}-{}",
      right_scan.done,
      right_scan.base_offset,
      right_scan.last_offset,
      right_scan.filepos,
      right_scan.first_timestamp,
      right_scan.max_timestamp);

    if (!subscan_res.value().done) {
        // Subscan failed to find the data timestamp (this usually
        // requires some scanning past the target)
        co_return make_error_code(error_outcome::scan_failed);
    }
    right_scan = subscan_res.value();
    co_return right_scan;
}

ss::future<result<cloud_upload_parameters>>
segment_upload::compute_upload_parameters(
  inclusive_offset_range range, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    try {
        auto segments = [&] {
            auto log = _part->log();
            auto dli = dynamic_cast<const storage::disk_log_impl*>(log.get());
            auto base_it = dli->segments().lower_bound(range.base);
            std::vector<ss::lw_shared_ptr<storage::segment>> segments;
            for (auto it = base_it; it != dli->segments().end(); it++) {
                const auto& offsets = it->get()->offsets();
                if (offsets.committed_offset < range.base) {
                    continue;
                }
                if (offsets.base_offset > range.last) {
                    break;
                }
                segments.push_back(*it);
            }
            return segments;
        }();

        if (segments.empty()) {
            vlog(_ctxlog.error, "Can't find log segments to lock");
            co_return make_error_code(error_outcome::offset_not_found);
        }

        // Get locks first
        std::vector<ss::future<ss::rwlock::holder>> f_locks;
        f_locks.reserve(segments.size());
        for (auto& s : segments) {
            f_locks.emplace_back(s->read_lock());
        }

        // All scheduling points starts here
        auto holders = co_await ss::when_all_succeed(
          std::begin(f_locks), std::end(f_locks));

        // Left side
        auto ls_res = co_await subscan_left(segments, range.base, deadline);
        if (ls_res.has_failure()) {
            co_return ls_res.as_failure();
        }
        subscan_res left_scan = ls_res.value();

        // Right scan
        auto rs_res = co_await subscan_right(segments, range.last, deadline);
        if (rs_res.has_failure()) {
            co_return rs_res.as_failure();
        }
        subscan_res right_scan = rs_res.value();
        vassert(left_scan.done && right_scan.done, "Scan failed");

        // compute size
        size_t total_size = 0;
        bool is_compacted = false;
        if (segments.size() > 1) {
            size_t mid_size = 0;
            size_t ix_last = segments.size() - 1;
            size_t left_size = segments.front()->size_bytes()
                               - left_scan.filepos;
            size_t right_size = right_scan.filepos;
            for (size_t i = 0; i < segments.size(); i++) {
                mid_size += segments[i]->size_bytes()
                            * (i == 0 || i == ix_last ? 0 : 1);
                is_compacted = is_compacted
                               || segments[i]->finished_self_compaction();
            }
            total_size = left_size + mid_size + right_size;
            vlog(
              _ctxlog.debug,
              "Computed size components: {}-{}-{}, total: {}, is_compacted: {}",
              left_size,
              mid_size,
              right_size,
              total_size,
              is_compacted);
        } else {
            total_size = right_scan.filepos - left_scan.filepos;
            is_compacted = is_compacted
                           || segments.front()->finished_self_compaction();
            vlog(
              _ctxlog.debug,
              "Computed size components: {}-{}, total: {}, is_compacted: {}",
              left_scan.filepos,
              right_scan.filepos,
              total_size,
              is_compacted);
        }

        //
        cloud_upload_parameters result{
          .size_bytes = total_size,
          .is_compacted = is_compacted,
          .offsets = range,
          .first_real_offset = left_scan.base_offset,
          .first_data_timestamp = left_scan.first_timestamp,
          .last_real_offset = right_scan.last_offset,
          .last_data_timestamp = right_scan.max_timestamp,
          .segment_locks = std::move(holders),
        };

        co_return result;

    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(
              _ctxlog.debug,
              "Shutdown exception: {}",
              std::current_exception());
            co_return make_error_code(error_outcome::shutting_down);
        }
        vlog(
          _ctxlog.error, "Unexpected exception: {}", std::current_exception());
        co_return make_error_code(error_outcome::unexpected_failure);
    }
}

ss::future<result<cloud_upload_parameters>>
segment_upload::compute_upload_parameters(
  size_limited_offset_range range, model::timeout_clock::time_point deadline) {
    if (range.max_size < storage::segment_index::default_data_buffer_step) {
        co_return make_error_code(error_outcome::not_enough_data);
    }
    auto holder = _gate.hold();
    std::optional<inclusive_offset_range> discovered_offset_range;
    try {
        auto log = _part->log();
        auto dli = dynamic_cast<const storage::disk_log_impl*>(log.get());
        auto base_it = dli->segments().lower_bound(range.base);
        std::vector<ss::lw_shared_ptr<storage::segment>> segments;
        size_t current_size = 0;
        size_t base_file_pos = 0;

        for (auto it = base_it; it != dli->segments().end(); it++) {
            const auto& offsets = it->get()->offsets();
            if (offsets.committed_offset < range.base) {
                continue;
            }
            auto r_lock = co_await it->get()->read_lock();
            if (
              offsets.base_offset < range.base
              && offsets.committed_offset > range.base) {
                // The segment is first in the range and it only
                // fits inside the range partially. We're doing subscan here but
                // most of the time it won't require the actual scanning because
                // in this mode the upload will end on index entry or segment
                // end. When we subsequently creating uploads using this method
                // only the first upload will have to do scanning to find the
                // offset.
                auto subscan_res = co_await subscan_left(
                  {*it}, range.base, deadline);
                if (subscan_res.has_failure()) {
                    co_return subscan_res.as_failure();
                }
                auto sz = it->get()->file_size() - subscan_res.value().filepos;
                current_size += sz;
                base_file_pos = sz;
            } else {
                auto sz = it->get()->file_size();
                current_size += sz;
            }
            segments.push_back(*it);
            if (current_size > range.max_size) {
                // TODO: set limit on max_size
                // We accumulated enough segments to create the upload. There're
                // few cases:
                // - The beginning and the end of the upload is inside the same
                // segment. The 'segments' collection will have only one element
                // if this is the case.
                // - The beginning and the end are located in different
                // segments. In both cases we need to find the finish line. The
                // size calculation is affected by these two possibilities.
                vlog(
                  _ctxlog.debug,
                  "Segment upload size overshoot by {}, current segment size "
                  "{}",
                  current_size - range.max_size,
                  it->get()->file_size());
                size_t truncate_after = 0;
                if (segments.size() == 1) {
                    // At this point it's guaranteed that 'base_index_entry'
                    // contains value and the 'current_size' contains 'file_size
                    // - base_index_entry->file_pos'. The expression below is
                    // guaranteed to not underflow. The 'file_size - filepos' is
                    // greater than 'range.max_size' because 'current_size >
                    // 'range.max_size'.
                    truncate_after = it->get()->file_size() - base_file_pos
                                     - range.max_size;
                } else {
                    auto prev = current_size - it->get()->file_size();
                    auto delta = current_size - range.max_size;
                    truncate_after = prev + delta;
                }
                auto last_index_entry = it->get()->index().find_nearest(
                  truncate_after);
                if (
                  last_index_entry.has_value()
                  && model::prev_offset(last_index_entry->offset)
                       > range.base) {
                    vlog(
                      _ctxlog.debug,
                      "Updating offset range to {} - {}, {} bytes of the last "
                      "segments are included",
                      range.base,
                      last_index_entry->offset,
                      truncate_after);
                    discovered_offset_range = inclusive_offset_range(
                      range.base, model::prev_offset(last_index_entry->offset));
                } else {
                    vlog(
                      _ctxlog.debug,
                      "Updating offset range to {} - {} (end of segment)",
                      range.base,
                      it->get()->offsets().committed_offset);
                    discovered_offset_range = inclusive_offset_range(
                      range.base, it->get()->offsets().committed_offset);
                }
                break;
            } else if (current_size > range.min_size) {
                vlog(
                  _ctxlog.debug,
                  "Updating offset range to {} - {}",
                  range.base,
                  range.base,
                  it->get()->offsets().committed_offset);
                // We can include full segment to the list of uploaded segments
                discovered_offset_range = inclusive_offset_range(
                  range.base, it->get()->offsets().committed_offset);
                continue;
            }
        }
        if (current_size < range.min_size) {
            co_return make_error_code(error_outcome::offset_not_found);
        }

        if (segments.empty()) {
            vlog(_ctxlog.error, "Can't find log segments to lock");
            co_return make_error_code(error_outcome::offset_not_found);
        }

        if (!discovered_offset_range.has_value()) {
            vlog(_ctxlog.error, "Can't find enough segments to lock");
            co_return make_error_code(error_outcome::offset_not_found);
        }

        co_return co_await compute_upload_parameters(
          *discovered_offset_range, deadline);
    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(
              _ctxlog.debug,
              "Shutdown exception: {}",
              std::current_exception());
            co_return make_error_code(error_outcome::shutting_down);
        }
        vlog(
          _ctxlog.error, "Unexpected exception: {}", std::current_exception());
        co_return make_error_code(error_outcome::unexpected_failure);
    }
}

ss::future<result<segment_upload::subscan_res>> segment_upload::subscan(
  ss::lw_shared_ptr<cluster::partition> part,
  inclusive_offset_range range,
  size_t initial_offset,
  bool target_is_last_offset,
  model::timeout_clock::time_point deadline) noexcept {
    auto holder = _gate.hold();
    // Scan range and accumulate stats (size, etc)
    struct batch_meta_accumulator {
        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            auto offset = target_end_of_batch ? b.last_offset()
                                              : b.base_offset();
            if (offset < target) {
                // We're scanning up to the target
                result->filepos += model::packed_record_batch_header_size
                                   + b.data().size_bytes();
                result->num_batches += 1;
                result->num_records += b.record_count();
            } else {
                // We reached the target or overshooting it
                if (result->base_offset == model::offset{}) {
                    result->base_offset = b.base_offset();
                    result->last_offset = b.last_offset();
                    if (target_end_of_batch && target == offset) {
                        // We're looking for the end of the offset range. The
                        // end offset is inclusive so if we have an exact match
                        // we need to add the whole batch to the output. If the
                        // segment is compacted we may hit a gap and the batch
                        // that we will see here will have the offset which is
                        // larger than the target. In this case we don't need to
                        // add this batch to the output.
                        result->filepos
                          += model::packed_record_batch_header_size
                             + b.data().size_bytes();
                    }
                }
                if (
                  result->first_timestamp == model::timestamp{}
                  && b.header().type == model::record_batch_type::raft_data) {
                    result->first_timestamp = b.header().first_timestamp;
                    result->max_timestamp = b.header().max_timestamp;
                }
                if (
                  result->base_offset != model::offset{}
                  && result->first_timestamp != model::timestamp{}) {
                    // This means that we scanned up until we found a data
                    // timestamp.
                    result->done = true;
                    co_return ss::stop_iteration::yes;
                }
            }
            co_return ss::stop_iteration::no;
        }
        bool end_of_stream() const { return false; }

        ss::lw_shared_ptr<subscan_res> result;
        model::offset target;
        bool target_reached{false};
        bool target_end_of_batch;
    };

    try {
        auto scan_res = ss::make_lw_shared<subscan_res>();
        scan_res->filepos = initial_offset;
        batch_meta_accumulator acc{
          .result = scan_res,
          .target = range.last,
          .target_end_of_batch = target_is_last_offset,
        };

        storage::log_reader_config reader_cfg(
          range.base,
          model::offset::max(),
          priority_manager::local().archival_priority());
        reader_cfg.skip_batch_cache = true;
        reader_cfg.skip_readers_cache = true;
        auto reader = co_await part->make_reader(reader_cfg);
        std::exception_ptr last_err;
        try {
            co_await reader.consume(acc, deadline);
        } catch (...) {
            last_err = std::current_exception();
            vlog(_ctxlog.error, "Error detected while consuming {}", last_err);
        }
        try {
            co_await std::move(reader).release()->finally();
        } catch (...) {
            last_err = std::current_exception();
            vlog(
              _ctxlog.error,
              "Error detected while closing the reader {}",
              last_err);
        }
        if (last_err) {
            std::rethrow_exception(last_err);
        }
        co_return *scan_res;
    } catch (...) {
        if (ssx::is_shutdown_exception(std::current_exception())) {
            vlog(
              _ctxlog.debug,
              "Shutdown exception: {}",
              std::current_exception());
            co_return make_error_code(error_outcome::shutting_down);
        }
        vlog(
          _ctxlog.error, "Unexpected exception: {}", std::current_exception());
        co_return make_error_code(error_outcome::unexpected_failure);
    }
}

} // namespace archival
