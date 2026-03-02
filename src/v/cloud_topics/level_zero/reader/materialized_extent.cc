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

#include "cloud_io/io_result.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/level_zero/reader/l0_object_cache.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "storage/record_batch_utils.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace cloud_topics::l0 {

model::record_batch make_raft_data_batch(materialized_extent ext) {
    auto offset = ext.meta.first_byte_offset;
    auto size = ext.meta.byte_range_size;
    vassert(
      size() > model::packed_record_batch_header_size,
      "L0 object is smaller ({}) than the batch header",
      size());
    auto header_bytes = ext.object.share(
      offset(), model::packed_record_batch_header_size);
    auto records_bytes = ext.object.share(
      offset() + model::packed_record_batch_header_size,
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
  l0_object_cache* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe) {
    // Try cache first
    auto cached = co_await cache->get_extent(
      ext->meta.id,
      ext->meta.first_byte_offset,
      ext->meta.byte_range_size,
      *rtc);
    if (cached.has_value()) {
        ext->object = std::move(cached.value());
        // Object now contains just the extent range, so reset offset to 0
        ext->meta.first_byte_offset = first_byte_offset_t{0};
        probe->num_cache_reads++;
        probe->cache_read_bytes += ext->object.size_bytes();
        co_return true;
    }

    // Cache miss — download from S3
    auto obj_key = cloud_storage_clients::object_key{
      object_path_factory::level_zero_path(ext->meta.id)};

    iobuf payload;
    cloud_io::download_request req{
      .transfer_details = {
        .bucket = bucket,
        .key = obj_key,
        .parent_rtc = *rtc,
        .success_cb =
          [probe, &payload] {
              probe->num_cloud_reads++;
              probe->cloud_read_bytes += payload.size_bytes();
          },
        .backoff_cb = [probe] { probe->num_cloud_reads++; },
      },
      .display_str = "L0",
      .payload = payload};

    auto fut = co_await ss::coroutine::as_future(
      api->download_object(std::move(req)));

    if (fut.failed()) {
        auto e = fut.get_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return errc::shutting_down;
        }
        vlog(cd_log.error, "Unexpected error during L0 download: {}", e);
        co_return errc::unexpected_failure;
    }

    auto dl_result = fut.get();
    if (dl_result != cloud_io::download_result::success) {
        switch (dl_result) {
        case cloud_io::download_result::notfound:
            co_return errc::download_not_found;
        case cloud_io::download_result::failed:
            co_return errc::download_failure;
        case cloud_io::download_result::timedout:
            co_return errc::timeout;
        case cloud_io::download_result::success:
            break;
        }
    }

    // Store in cache
    co_await cache->put(ext->meta.id, std::move(payload), *rtc);

    // Now read the extent from cache
    auto extent_data = co_await cache->get_extent(
      ext->meta.id,
      ext->meta.first_byte_offset,
      ext->meta.byte_range_size,
      *rtc);
    if (extent_data.has_value()) {
        ext->object = std::move(extent_data.value());
        // Object now contains just the extent range, so reset offset to 0
        ext->meta.first_byte_offset = first_byte_offset_t{0};
    } else {
        // This shouldn't normally happen — we just put the object.
        // But handle gracefully: memory pressure could evict between put
        // and get.
        co_return errc::download_failure;
    }

    co_return false;
}

} // namespace cloud_topics::l0
