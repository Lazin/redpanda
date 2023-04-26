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

#include "cloud_storage/fwd.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/timestamp.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <exception>
#include <map>

namespace cloud_storage {

class async_manifest_view;

/// Result of the ListObjectsV2 scan
struct spillover_manifest_list {
    /// List of manifest paths
    std::deque<remote_manifest_path> manifests;
    /// List of decoded path components (offsets and timestamps)
    std::deque<spillover_manifest_path_components> components;
    /// List of manifest sizes (in binary format)
    std::deque<size_t> sizes;
};

/// The cursor can be used to traverse manifest
/// asynchronously. The full content of the manifest
/// can't b loaded into memory. Because of that the spillover
/// manifests has to be loaded into memory asynchronously while
/// the full manifest view is being traversed. This is done in
/// batches in the 'refresh' call.
class async_manifest_view_cursor {
public:
    explicit async_manifest_view_cursor(async_manifest_view& view);
    ss::future<> next();
    spillover_manifest::const_iterator begin();
    spillover_manifest::const_iterator end();

private:
    /// Manifest view ref
    async_manifest_view& _view;
    /// Full copy of the spillover manifests list (the assumption is that the
    /// list is relatively small)
    std::optional<spillover_manifest_list> _manifests;
};

/// Materialized spillover manifest
struct materialized_manifest {
    spillover_manifest manifest;
    ss::semaphore_units<> _units;
    intrusive_list_hook _hook;
};

/// Collection of materialized manifests
/// The cache stores manifests in memory (hence "materialized").
/// The memory limit is specified in bytes. The cache uses LRU
/// eviction policy and will try to evict least recently used
/// materialized manifest.
class materialized_manifest_cache {
public:
    materialized_manifest_cache(
      size_t capacity_bytes,
      retry_chain_logger& parent_logger,
      ss::abort_source& parent_as)
      : _capacity_bytes(capacity_bytes)
      , _ctxlog(parent_logger)
      , _parent_as(parent_as)
      , _sem(capacity_bytes) {}

    /// Reserve space to store next manifest. This method will
    /// wait until another manifest could be evicted.
    ss::future<ss::semaphore_units<>> prepare(size_t size_bytes) {
        if (size_bytes > _capacity_bytes) {
            // Oversized manifest handling. The manifest could be larger than
            // capacity. If we will not allow this manifest to be added to the
            // cache the subsystem will stall. The only possible solution is to
            // let the manifest into the cache and allow it to evict everything
            // else.
            size_bytes = _capacity_bytes;
        }
        auto maybe_units = ss::try_get_units(_sem, size_bytes);
        if (maybe_units) {
            // The cache is not full and can grant some capacity without
            // eviction
            co_return maybe_units;
        }
        // The cache is full, try to free up some space. Free at least
        // 'size_bytes' bytes.
        size_t bytes_evicted = 0;
        while (bytes_evicted < size_bytes && !_cache.empty()) {
            auto it = _access_order.begin();
            auto so = it->manifest.get_start_offset();
            vassert(
              so.has_value(),
              "Manifest can't be empty, ntp: {}",
              it->manifest.get_ntp());
            auto cit = _cache.find(so.value());
            if (cit != _cache.end()) {
                _cache.erase(cit);
                bytes_evicted += it->_units.count();
            } else {
                // This can happen if the 'shared_ptr<materialized_manifest>'
                // instance was evicted but still used by the cursor. In this
                // case it was evicted to satisfy different request and we
                // shouldn't take account for it.
                vlog(
                  _ctxlog.debug, "Manifest at {} already evicted", so.value());
            }
        }
        // Here the least recently used materialized manifests were evicted to
        // free up 'size_bytes' bytes. But these manifests could still be used
        // by some cursor. We need to wait for them to get released.
        auto u = co_await ss::get_units(_sem, size_bytes, _parent_as);
        co_return u;
    }

    /// Put manifest into the cache
    void put(ss::semaphore_units<> s, spillover_manifest manifest) {
        auto so = manifest.get_start_offset();
        vassert(
          so.has_value(),
          "Manifest can't be empty, ntp: {}",
          manifest.get_ntp());
        auto item = ss::make_shared<materialized_manifest>();
        item->manifest = std::move(manifest);
        item->_units = std::move(s);
        auto [it, ok] = _cache.insert(
          std::make_pair(so.value(), std::move(item)));
        if (ok) {
            // This may indicate a race, log a warning
            vlog(
              _ctxlog.error,
              "Manifest with base offset {} is already present",
              so);
            return;
        }
        _access_order.push_back(*it->second);
    }

    /// Find manifest by its base offset
    ss::shared_ptr<materialized_manifest> get(model::offset base_offset) {
        if (auto it = _cache.find(base_offset); it != _cache.end()) {
            elevate(it->second);
            return it->second;
        }
        return nullptr;
    }

    /// Move element forward to avoid its eviction
    void elevate(model::offset base_offset) {
        if (auto it = _cache.find(base_offset); it != _cache.end()) {
            elevate(it->second);
        }
    }
    void elevate(ss::shared_ptr<materialized_manifest>& manifest) {
        manifest->_hook.unlink();
        _access_order.push_back(*manifest);
    }

public:
    const size_t _capacity_bytes;
    retry_chain_logger& _ctxlog;
    std::map<model::offset, ss::shared_ptr<materialized_manifest>> _cache;
    ss::abort_source& _parent_as;
    ss::semaphore _sem;
    intrusive_list<materialized_manifest, &materialized_manifest::_hook>
      _access_order;
};

/// TODO
class async_manifest_view {
    friend class async_manifest_view_cursor;

public:
    explicit async_manifest_view(
      ss::sharded<remote>& remote,
      ss::sharded<cache>& cache,
      const partition_manifest& stm_manifest,
      cloud_storage_clients::bucket_name bucket);

    ss::future<> start();
    ss::future<> stop();

    ss::future<result<async_manifest_view_cursor, error_outcome>>
    get_cursor(model::offset base_offset) noexcept;

    bool is_empty() const noexcept;

private:
    ss::future<> run_bg_loop();

    ss::future<result<spillover_manifest_list, error_outcome>>
    scan_bucket() noexcept;

    /// Load manifest from the cloud
    ///
    /// On success put serialized copy into the cache. The method should only be
    /// called if the manifest is not available in the cache. The state of the
    /// view is not changed.
    ss::future<result<spillover_manifest, error_outcome>>
    hydrate_manifest(remote_manifest_path path) const noexcept;

    /// Load manifest from the cache
    ///
    /// The method reads manifest from the cache or downloads from the cloud.
    /// Local state is not changed. The returned manifest has to be stored
    /// in the view after the call.
    /// \throws
    ///     - not_found if the manifest doesn't exist
    ///     - repeat if the manifest is being downloaded already
    ///     - TODO
    ss::future<result<spillover_manifest, error_outcome>>
    materialize_manifest(model::offset base_offset) const noexcept;

    /// Search query type
    using search_query_t
      = std::variant<model::offset, kafka::offset, model::timestamp>;

    /// Find index of the spillover manifest
    ///
    /// \param query is a search query, either an offset, a kafka offset or a
    ///              timestamp
    /// \return index of the spillover manifest or -1 on error
    int32_t search_spillover_manifests(search_query_t query) const;

    mutable ss::gate _gate;
    ss::abort_source _as;
    cloud_storage_clients::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    ss::sharded<cache>& _cache;
    const partition_manifest& _stm_manifest;
    mutable retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
    config::binding<std::chrono::milliseconds> _timeout;
    config::binding<std::chrono::milliseconds> _backoff;
    config::binding<size_t> _read_buffer_size;
    config::binding<int16_t> _readahead_size;

    std::optional<spillover_manifest_list> _manifests;
    model::offset _last_archive_start_offset;
    model::offset _last_stm_start_offset;

    // Manifest in-memory storage
    std::unique_ptr<materialized_manifest_cache> _manifest_cache;

    // BG loop state
    struct materialization_request_t {
        search_query_t query;
        ss::promise<> promise;
    };
    ss::condition_variable _cvar;
};

} // namespace cloud_storage