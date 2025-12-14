/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/materialized_extent.h"

#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "storage/record_batch_utils.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <exception>

namespace cloud_topics::l0 {

/// Map error codes from one type to another
template<class Src, class Dst>
struct errc_converter;

template<>
struct errc_converter<cloud_io::download_result, errc> {
    errc operator()(cloud_io::download_result r) {
        switch (r) {
        case cloud_io::download_result::notfound:
            return errc::download_not_found;
        case cloud_io::download_result::failed:
            return errc::download_failure;
        case cloud_io::download_result::timedout:
            return errc::timeout;
        case cloud_io::download_result::success:
            return errc::success;
        };
    }
};

/// Convert ready future to expected<> type
template<errc unexpected = errc::unexpected_failure, class T>
result<T> result_from_ready_future(ss::future<T>&& ready) {
    if (ready.failed()) {
        auto err = ready.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            return errc::shutting_down;
        }
        return unexpected;
    }
    return ready.get();
}

/// Convert ready future to result<> type, log unexpected
/// exception using provided functor
template<errc unexpected = errc::unexpected_failure, class T, class FormatFunc>
result<T> result_from_ready_future(ss::future<T>&& ready, FormatFunc fmt) {
    if (ready.failed()) {
        auto err = ready.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            return errc::shutting_down;
        }
        fmt(err);
        return unexpected;
    }
    return ready.get();
}

/// Convert result<> type to expected<>
///
/// The type of the error code should be known
template<class T, class E>
result<T> result_convert(result<T>&& res) {
    if (!res.has_value()) {
        errc_converter<E, errc> conv;
        return conv(res.error());
    }
    return res.value();
}

model::record_batch make_raft_data_batch(materialized_extent ext) {
    auto size = ext.meta.byte_range_size;
    vassert(
      size() > model::packed_record_batch_header_size,
      "L0 object is smaller ({}) than the batch header",
      size());
    // Since we now download only the relevant byte range, ext.object
    // contains data starting from position 0 (not from first_byte_offset)
    auto header_bytes = ext.object.share(
      0, model::packed_record_batch_header_size);
    auto records_bytes = ext.object.share(
      model::packed_record_batch_header_size,
      size() - model::packed_record_batch_header_size);
    auto header = storage::batch_header_from_disk_iobuf(
      std::move(header_bytes));
    // NOTE: the serialized raft_data batch doesn't have the offset set
    // so we need to populate it from the placeholder batch. We also need
    // to make sure that crc is correct.
    header.base_offset = kafka::offset_cast(ext.meta.base_offset);
    header.crc = model::crc_record_batch(header, records_bytes);
    crc::crc32c crc;
    model::crc_record_batch_header(crc, header);
    header.header_crc = crc.value();
    model::record_batch batch(
      header,
      std::move(records_bytes),
      model::record_batch::tag_ctor_ng{}); // TODO: fix compression
    return batch;
}

ss::future<result<bool>> materialize(
  materialized_extent* ext,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* /*cache*/,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe) {
    // Download only the relevant byte range directly from cloud storage
    // Skip cache reads and writes for materialization
    auto cache_file_name = std::filesystem::path(
      object_path_factory::level_zero_path(ext->meta.id));

    // Create byte range from extent metadata
    cloud_storage_clients::http_byte_range byte_range{
      ext->meta.first_byte_offset(),
      ext->meta.first_byte_offset() + ext->meta.byte_range_size() - 1};

    // Download directly from cloud storage without cache
    iobuf payload;

    cloud_io::transfer_details transfer_details{
      .bucket = bucket,
      .key = cloud_storage_clients::object_key(cache_file_name),
      .parent_rtc = *rtc,
      .success_cb =
        [probe, &payload] {
            probe->num_cloud_reads++;
            probe->cloud_read_bytes += payload.size_bytes();
        },
      .backoff_cb = [probe] { probe->num_cloud_reads++; },
    };

    auto consume_stream =
      [&payload](
        uint64_t /*content_length*/,
        ss::input_stream<char> stream) -> ss::future<uint64_t> {
        auto target = make_iobuf_ref_output_stream(payload);
        co_await ss::copy(stream, target);
        co_await stream.close();
        co_return payload.size_bytes();
    };

    auto dl_result = result_from_ready_future(
      co_await ss::coroutine::as_future(api->download_stream(
        std::move(transfer_details),
        consume_stream,
        "L0",
        false, // acquire_hydration_units
        byte_range)),
      [](std::exception_ptr e) {
          vlog(cd_log.error, "Unexpected error during L0 download: {}", e);
      });

    if (!dl_result.has_value()) {
        co_return dl_result.error();
    }

    if (dl_result.value() != cloud_io::download_result::success) {
        errc_converter<cloud_io::download_result, errc> conv;
        co_return conv(dl_result.value());
    }

    ext->object = std::move(payload);
    co_return true; // Always returns true since we're downloading from cloud
}

} // namespace cloud_topics::l0
