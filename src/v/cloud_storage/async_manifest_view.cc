/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/async_manifest_view.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "resource_mgmt/io_priority.h"
#include "utils/retry_chain_node.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/outcome/success_failure.hpp>

#include <exception>

namespace cloud_storage {

async_manifest_view_cursor::async_manifest_view_cursor(
  async_manifest_view& view)
  : _view(view) {}

async_manifest_view::async_manifest_view(
  ss::sharded<remote>& remote,
  ss::sharded<cache>& cache,
  const partition_manifest& stm_manifest,
  cloud_storage_clients::bucket_name bucket)
  : _bucket(bucket)
  , _remote(remote)
  , _cache(cache)
  , _stm_manifest(stm_manifest)
  , _rtcnode(_as)
  , _ctxlog(cst_log, _rtcnode, _stm_manifest.get_ntp().path())
  , _timeout(
      config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms.bind())
  , _backoff(config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
  , _read_buffer_size(config::shard_local_cfg().storage_read_buffer_size.bind())
  , _readahead_size(
      config::shard_local_cfg().storage_read_readahead_count.bind()) {}

ss::future<> async_manifest_view::start() { co_return; }

ss::future<> async_manifest_view::stop() {
    _as.request_abort();
    co_await _gate.close();
    co_return;
}

static bool bucket_item_filter(
  const cloud_storage_clients::client::list_bucket_item& item) {
    return !boost::algorithm::ends_with(item.key, ".json");
}

static spillover_manifest_path_components
parse_spillover_manifest_path(const ss::sstring& path, int32_t index) {
    std::deque<ss::sstring> components;
    boost::split(components, path, boost::is_any_of(":"));
    spillover_manifest_path_components res{
      .base = model::offset(boost::lexical_cast<int64_t>(components[1])),
      .last = model::offset(boost::lexical_cast<int64_t>(components[2])),
      .base_kafka = kafka::offset(boost::lexical_cast<int64_t>(components[3])),
      .next_kafka = kafka::offset(boost::lexical_cast<int64_t>(components[4])),
      .base_ts = model::timestamp(boost::lexical_cast<int64_t>(components[5])),
      .last_ts = model::timestamp(boost::lexical_cast<int64_t>(components[6])),
      .index = index,
    };
    return res;
}

ss::future<result<async_manifest_view_cursor, error_outcome>>
async_manifest_view::get_cursor(model::offset base_offset) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (
          _stm_manifest.get_start_offset() != _last_stm_start_offset
          && _stm_manifest.get_archive_start_offset()
               != _last_stm_start_offset) {
            auto res = co_await scan_bucket();
            if (res.has_failure()) {
                vlog(
                  _ctxlog.error,
                  "Failed to re-scan the bucket: {}",
                  res.error());
                co_return res;
            }
            _manifests = res.value();
        }
        co_return async_manifest_view_cursor(*this);
    } catch (...) {
        co_return std::current_exception();
    }
}

bool async_manifest_view::is_empty() const noexcept {
    return _stm_manifest.size() == 0;
}

ss::future<> async_manifest_view::run_bg_loop() {
  ss::gate::holder h(_gate);
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::hydrate_manifest(
  remote_manifest_path path) const noexcept {
    spillover_manifest manifest(
      _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
    try {
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        auto res = co_await _remote.local().download_manifest(
          _bucket, path, manifest, fib);
        if (res != download_result::success) {
            vlog(
              cst_log.error,
              "failed to download manifest {}, object key: {}",
              res,
              path);
            co_return error_outcome::manifest_download_error;
        }
        auto [str, len] = co_await manifest.serialize();
        co_await _cache.local().put(
          manifest.get_manifest_path()(),
          str,
          priority_manager::local().shadow_indexing_priority());
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return manifest;
}

int32_t
async_manifest_view::search_spillover_manifests(search_query_t query) const {
    if (!_manifests.has_value()) {
        throw std::logic_error(fmt_with_ctx(
          fmt::format,
          "can't hydrate spillover manifest, bucket has to be scanned first"));
    }

    // Perform simple scan of the manifest list.
    auto it = std::find_if(
      _manifests->components.begin(),
      _manifests->components.end(),
      [query](const spillover_manifest_path_components& c) {
          return ss::visit(
            query,
            [&](model::offset o) { return o >= c.base && o <= c.last; },
            [&](kafka::offset k) {
                return k >= c.base_kafka && k < c.next_kafka;
            },
            [&](model::timestamp t) {
                return t >= c.base_ts && t <= c.last_ts;
            });
      });

    if (it == _manifests->components.end()) {
        return -1;
    }

    return it->index;
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::materialize_manifest(
  model::offset base_offset) const noexcept {
    spillover_manifest manifest(
      _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
    try {
        if (!_manifests.has_value()) {
            co_return std::make_exception_ptr(std::logic_error(fmt_with_ctx(
              fmt::format,
              "can't hydrate spillover manifest, bucket has to be scanned "
              "first")));
        }
        auto h = _gate.hold();
        // Perform simple scan of the manifest list
        auto index = search_spillover_manifests(base_offset);
        if (index < 0) {
            vlog(
              cst_log.error,
              "failed to find spillover manifest with base offset {}",
              base_offset);
            co_return error_outcome::manifest_not_found;
        }
        auto path = _manifests->manifests.at(index);
        // Probe cache. If not available or in case of race with cache eviction
        // hydrate manifest from the cloud.
        auto cache_status = co_await _cache.local().is_cached(path());
        switch (cache_status) {
        case cache_element_status::in_progress:
            vlog(_ctxlog.warn, "Concurrent manifest hydration, path {}", path);
            co_return error_outcome::repeat;
        case cache_element_status::not_available: {
            retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
            auto res = co_await _remote.local().download_manifest(
              _bucket, path, manifest, fib);
            if (res != download_result::success) {
                vlog(
                  cst_log.error,
                  "failed to download manifest {}, object key: {}",
                  res,
                  path);
                co_return error_outcome::manifest_download_error;
            }
            auto [str, len] = co_await manifest.serialize();
            co_await _cache.local().put(
              manifest.get_manifest_path()(),
              str,
              priority_manager::local().shadow_indexing_priority());
        }
        case cache_element_status::available: {
            auto res = co_await _cache.local().get(path());
            if (!res.has_value()) {
                vlog(
                  cst_log.warn,
                  "failed to read cached manifest {}, object key: {}",
                  res,
                  path);
                // Cache race removed the file after `is_cached` check, the
                // upper layer is supposed to retry the call.
                co_return error_outcome::repeat;
            }
            ss::file_input_stream_options options{
              .buffer_size = _read_buffer_size(),
              .read_ahead = _readahead_size(),
              .io_priority_class
              = priority_manager::local().shadow_indexing_priority()};
            auto data_stream = ss::make_file_input_stream(
              res->body, 0, std::move(options));
            co_await manifest.update(std::move(data_stream));
        }
        };
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return manifest;
}

ss::future<result<spillover_manifest_list, error_outcome>>
async_manifest_view::scan_bucket() noexcept {
    try {
        spillover_manifest_list result;
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        auto prefix = _stm_manifest.get_manifest_path();
        auto res = co_await _remote.local().list_objects(
          _bucket,
          fib,
          cloud_storage_clients::object_key(prefix().string()),
          std::nullopt,
          &bucket_item_filter);
        if (res.has_error()) {
            vlog(cst_log.error, "failed to list manifests: {}", res.error());
            co_return error_outcome::scan_bucket_error;
        }
        int32_t index = 0;
        for (const auto& it : res.value().contents) {
            result.manifests.emplace_back(it.key);
            result.components.push_back(
              parse_spillover_manifest_path(it.key, index));
            index++;
        }
        co_return result;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}
} // namespace cloud_storage