/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "utils/retry_chain_node.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/outcome.hpp>
#include <boost/outcome/outcome.hpp>
#include <boost/outcome/policy/all_narrow.hpp>

#include <exception>
#include <utility>

namespace archival {

namespace detail {
inline model::offset cast_to_offset(int64_t i) noexcept {
    return model::offset(i);
}
} // namespace detail

/// Representation of the inclusive monotonic offset range
struct inclusive_offset_range {
    model::offset base;
    model::offset last;

    inclusive_offset_range() = default;

    inclusive_offset_range(model::offset base, model::offset last)
      : base(base)
      , last(last) {
        vassert(base <= last, "Invalid offset range {} - {}", base, last);
    }

    bool operator<=>(const inclusive_offset_range&) const = default;

    int64_t size() const noexcept { return last() - base() + 1; }

    bool contains(const model::record_batch_header& hdr) const noexcept {
        return base <= hdr.base_offset && hdr.last_offset() <= last;
    }

    auto begin() const noexcept {
        auto cit = boost::make_counting_iterator<int64_t>(base());
        return boost::make_transform_iterator(cit, &detail::cast_to_offset);
    }

    auto end() const noexcept {
        auto cit = boost::make_counting_iterator<int64_t>(last() + 1);
        return boost::make_transform_iterator(cit, &detail::cast_to_offset);
    }
};

/// Representation of the inclusive monotonic offset range limited by
/// on-disk size.
struct size_limited_offset_range {
    model::offset base;
    size_t min_size;
    size_t max_size;

    size_limited_offset_range(
      model::offset base,
      size_t max_size,
      std::optional<size_t> min_size = std::nullopt)
      : base(base)
      , min_size(min_size.value_or(0))
      , max_size(max_size) {}

    bool operator<=>(const size_limited_offset_range&) const = default;
};

inline std::ostream& operator<<(std::ostream& o, inclusive_offset_range r) {
    fmt::print(o, "[{}-{}]", r.base, r.last);
    return o;
}

/// Result of the upload size calculation.
/// Contains size of the region in bytes and locks.
struct cloud_upload_parameters {
    /// Size of the upload in bytes
    size_t size_bytes;
    /// True if at least one segment is compacted
    bool is_compacted;
    /// Offset range of the segment
    inclusive_offset_range offsets;
    /// First offset inside the uploaded segment (might be different from
    /// metadata offset if the segment is compacted)
    model::offset first_real_offset;
    /// Timestamp from the first data batch
    model::timestamp first_data_timestamp;
    /// Last offset inside the uploaded segment (might be different from
    /// metadata offset if the segment is compacted)
    model::offset last_real_offset;
    /// Timestamp from the last data batch
    model::timestamp last_data_timestamp;
    /// Segment level read locks
    std::vector<ss::rwlock::holder> segment_locks;
};

/// Individual segment upload
class segment_upload {
public:
    segment_upload(const segment_upload&) = delete;
    segment_upload(segment_upload&&) noexcept = delete;
    segment_upload& operator=(const segment_upload&) = delete;
    segment_upload& operator=(segment_upload&&) noexcept = delete;

    /// Close upload and free resources
    ss::future<> close();

    /// Get input stream or throw
    ///
    /// The method detaches the input stream and returns it. It's also closes
    /// the 'segment_upload' object (which should be a no-op at this point since
    /// all async operations are completed). If 'detach_stream' was called the
    /// caller no longer has to call 'close' on the 'segment_upload' object. If
    /// the method was never called the caller has to call 'close' explicitly.
    ss::future<ss::input_stream<char>> detach_stream() && {
        throw_if_not_initialized("get_stream");
        auto tmp = std::exchange(_stream, std::nullopt);
        co_await close();
        co_return std::move(tmp.value());
    }

    /// Get precise content length for the upload
    size_t get_size_bytes() const {
        throw_if_not_initialized("get_size_bytes");
        return _params->size_bytes;
    }

    /// Get upload metadata
    ///
    /// \returns cref of the metadata
    const cloud_upload_parameters& get_meta() const noexcept {
        throw_if_not_initialized("get_meta");
        return _params.value();
    }

    /// \brief Make new segment upload
    ///
    /// \note Make an upload that begins and ends at precise offsets.
    /// \param part is a partition
    /// \param range is an offset range to upload
    /// \param read_buffer_size is a buffer size used to upload data
    /// \param sg is a scheduling group used to upload the data
    /// \param deadline is a deadline for the upload object to be created (not
    ///                 for the actual upload to happen)
    /// \return initialized segment_upload object
    static ss::future<result<std::unique_ptr<segment_upload>>>
    make_segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      inclusive_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline);

    /// \brief Make new segment upload
    ///
    /// \note Make an upload that begins at certain offset and has certain size.
    /// This method is supposed to be used when the new upload is started.
    /// E.g. when we want to upload 1GiB segments we will use this code:
    ///
    /// \code
    /// auto upl = co_await segment_upload::make_segment_upload(
    ///                         _part,
    ///                         size_limited_offset_range(last_offset, 1_GiB),
    ///                         0x8000,
    ///                         _sg,
    ///                         10s);
    /// \endcode
    ///
    /// Note that the precision of this method is limited by index sampling
    /// rate. End of every upload created this way will be aligned with the
    /// index.
    ///
    /// \param part is a partition
    /// \param range is an offset range to upload
    /// \param read_buffer_size is a buffer size used to upload data
    /// \param sg is a scheduling group used to upload the data
    /// \param deadline is a deadline for the upload object to be created (not
    ///                 for the actual upload to happen)
    /// \return initialized segment_upload object
    static ss::future<result<std::unique_ptr<segment_upload>>>
    make_segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      size_limited_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline);

private:
    explicit segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      size_t read_buffer_size,
      ss::scheduling_group sg);

    /// Initialize segment upload using offset range
    ss::future<result<void>> initialize(
      inclusive_offset_range offsets,
      model::timeout_clock::time_point deadline);

    /// Initialize segment upload using offset range
    ss::future<result<void>> initialize(
      size_limited_offset_range range,
      model::timeout_clock::time_point deadline);

    void throw_if_not_initialized(std::string_view caller) const;

    /// Stores parameters of the target batch.
    /// The subscan returns this struct that contains
    /// base/last offsets of the target batch.
    struct subscan_res {
        size_t filepos{0};
        size_t num_batches{0};
        size_t num_records{0};
        model::timestamp first_timestamp;
        model::timestamp max_timestamp;
        model::offset base_offset;
        model::offset last_offset;
        bool done{false};
    };

    /// Scan partition to find target offset.
    /// Returned 'subscan_res' struct contains size of the
    /// scanned region on disk, number of batches/records,
    /// the timestamp and offset of the 'target'.
    /// The timestamp is a data timestamp (if the target is a config batch
    /// the subscan will scan until the data batch is found and use first data
    /// timestamp). The offset returned by the query is target or first offset >
    /// target if target was compacted.
    ss::future<result<subscan_res>> subscan(
      ss::lw_shared_ptr<cluster::partition> part,
      inclusive_offset_range range,
      size_t initial_offset,
      bool target_is_last_offset,
      model::timeout_clock::time_point deadline) noexcept;

    /// Calculate upload size using segment indexes
    ss::future<result<cloud_upload_parameters>> compute_upload_parameters(
      inclusive_offset_range range, model::timeout_clock::time_point deadline);

    /// Calculate upload size using segment indexes
    ss::future<result<cloud_upload_parameters>> compute_upload_parameters(
      size_limited_offset_range range,
      model::timeout_clock::time_point deadline);

    // Scan te beginning of the segment range
    ss::future<result<subscan_res>> subscan_left(
      std::vector<ss::lw_shared_ptr<storage::segment>> segments,
      model::offset range_base,
      model::timeout_clock::time_point deadline);

    /// Scan the end of the segment range
    ss::future<result<subscan_res>> subscan_right(
      std::vector<ss::lw_shared_ptr<storage::segment>> segments,
      model::offset range_last,
      model::timeout_clock::time_point deadline);

    model::ntp _ntp;
    ss::lw_shared_ptr<cluster::partition> _part;
    size_t _rd_buffer_size;
    ss::scheduling_group _sg;
    std::optional<ss::input_stream<char>> _stream;
    std::optional<cloud_upload_parameters> _params;
    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

} // namespace archival
