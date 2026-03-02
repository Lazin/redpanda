/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/disk_l0_cache.h"

#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <filesystem>

namespace cloud_topics::l0 {

disk_l0_cache::disk_l0_cache(
  cloud_io::basic_cache_service_api<ss::lowres_clock>* cache,
  micro_probe* probe)
  : _cache(cache)
  , _probe(probe) {}

ss::future<std::optional<iobuf>> disk_l0_cache::get_extent(
  const object_id& id,
  first_byte_offset_t offset,
  byte_range_size_t size,
  basic_retry_chain_node<>& rtc) {
    auto cache_file_name = std::filesystem::path(
      object_path_factory::level_zero_path(id));

    // Poll is_cached() with retry backoff until the object is available,
    // not available, or we time out.
    std::optional<cloud_io::cache_element_status> status = std::nullopt;
    basic_retry_chain_node<> is_cached_rtc(retry_strategy::backoff, &rtc);
    retry_permit rp = is_cached_rtc.retry();
    while (rp.is_allowed && !status.has_value()) {
        auto is_cached_fut = co_await ss::coroutine::as_future(
          _cache->is_cached(cache_file_name));
        if (is_cached_fut.failed()) {
            auto e = is_cached_fut.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                co_return std::nullopt;
            }
            vlog(cd_log.warn, "is_cached check failed: {}", e);
            co_return std::nullopt;
        }

        auto cache_status = is_cached_fut.get();
        switch (cache_status) {
        case cloud_io::cache_element_status::available:
        case cloud_io::cache_element_status::not_available:
            status = cache_status;
            break;
        case cloud_io::cache_element_status::in_progress:
            if (rp.abort_source != nullptr) {
                co_await ss::sleep_abortable(rp.delay, *rp.abort_source);
            } else {
                co_await ss::sleep(rp.delay);
            }
            rp = is_cached_rtc.retry();
            continue;
        }
    }

    if (!rp.is_allowed || !status.has_value()
        || status.value() == cloud_io::cache_element_status::not_available) {
        co_return std::nullopt;
    }

    // Object is on disk — read the requested byte range.
    _probe->num_cache_reads++;
    auto buffer_size = config::shard_local_cfg().storage_read_buffer_size();
    constexpr unsigned int read_ahead = 0;
    auto fut = co_await ss::coroutine::as_future(_cache->get_stream_range(
      cache_file_name, offset(), size(), buffer_size, read_ahead));
    if (fut.failed()) {
        auto e = fut.get_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return std::nullopt;
        }
        vlog(cd_log.warn, "Failed to read from disk cache: {}", e);
        co_return std::nullopt;
    }
    auto sz_stream = std::move(fut.get());
    if (!sz_stream.has_value()) {
        co_return std::nullopt;
    }

    iobuf result_buf;
    auto target = make_iobuf_ref_output_stream(result_buf);
    _probe->cache_read_bytes += sz_stream->size;
    co_await ss::copy(sz_stream->body, target);
    co_await sz_stream->body.close();
    co_return result_buf;
}

ss::future<> disk_l0_cache::put(
  const object_id& id, iobuf data, basic_retry_chain_node<>& rtc) {
    auto cache_file_name = std::filesystem::path(
      object_path_factory::level_zero_path(id));

    auto sr_guard_fut = co_await ss::coroutine::as_future(
      _cache->reserve_space(data.size_bytes(), 1));

    if (sr_guard_fut.failed()) {
        auto e = sr_guard_fut.get_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return;
        }
        vlog(cd_log.warn, "Failed to reserve space in disk cache: {}", e);
        co_return;
    }

    auto sr_guard = std::move(sr_guard_fut.get());

    _probe->num_cache_writes++;
    auto buf_str = make_iobuf_input_stream(data.share());
    auto put_fut = co_await ss::coroutine::as_future(
      _cache->put(cache_file_name, buf_str, sr_guard));

    if (put_fut.failed()) {
        auto e = put_fut.get_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return;
        }
        vlog(
          cd_log.warn,
          "Failed to put L0 object into the disk cache: {}. "
          "The error will not be propagated to the client but "
          "Redpanda may use more resources.",
          e);
    } else {
        _probe->cache_write_bytes += data.size_bytes();
    }
}

} // namespace cloud_topics::l0
