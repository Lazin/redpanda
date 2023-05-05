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
#include "cloud_storage/materialized_segments.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/probe.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>

#include <algorithm>
#include <exception>
#include <map>

namespace cloud_storage {

/// Search query type
using async_view_search_query_t
  = std::variant<model::offset, kafka::offset, model::timestamp>;

/// Materialized spillover manifest
///
/// The object contains the manifest, semaphore units,
/// and it is also a part of an intrusive list.
struct materialized_manifest
  : ss::enable_shared_from_this<materialized_manifest> {
    materialized_manifest(
      const model::ntp& ntp,
      model::initial_revision_id rev,
      ss::semaphore_units<> u)
      : manifest(ntp, rev)
      , _units(std::move(u)) {}

    materialized_manifest(spillover_manifest manifest, ss::semaphore_units<> u)
      : manifest(std::move(manifest))
      , _units(std::move(u)) {}

    materialized_manifest() = delete;
    ~materialized_manifest() = default;
    materialized_manifest(const materialized_manifest&) = delete;
    materialized_manifest(materialized_manifest&&) = delete;
    materialized_manifest& operator=(const materialized_manifest&) = delete;
    materialized_manifest& operator=(materialized_manifest&&) = delete;

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
    using access_list_t
      = intrusive_list<materialized_manifest, &materialized_manifest::_hook>;

public:
    /// C-tor
    ///
    /// \param capacity_bytes max size of all manifests stored in the cache
    /// \param parent_logger logger to be used by the cache
    materialized_manifest_cache(
      size_t capacity_bytes, retry_chain_logger& parent_logger);

    /// Reserve space to store next manifest.
    ///
    /// \note This method will wait until another manifest could be evicted.
    ///       The size of the manifest has to be passed as a parameter. The
    ///       cache has fixed capacity. If the request is larger than the
    ///       capacity it will be clamped down to capacity. As result the
    ///       oversized request will have to evict everything else.
    /// \throw ss::timed_out_error if timeout expired. The evicted elements
    ///        are returned to the cache in this case.
    /// \param size_bytes is a size of the manifest in bytes
    /// \param timeout is a timeout for the operation
    /// \return future that result in acquired semaphore units
    ss::future<ss::semaphore_units<>> prepare(
      size_t size_bytes,
      std::optional<ss::lowres_clock::duration> timeout = std::nullopt);

    /// Return number of elements stored in the cache
    size_t size() const noexcept;

    /// Return total size of the cache content.
    /// The result can't be greater than capacity even if the
    /// content exceeds the capacity. This is the result of the oversized
    /// manifest handling in the cache. Oversized manifest assumes size which
    /// is equal to capacity of the cache to be able to get in.
    size_t size_bytes() const noexcept;

    /// Put manifest into the cache
    ///
    /// \param s is a semaphore units acquired using 'prepare' method
    /// \param manifest is a spillover manifest to store
    /// \note if the manifest with the same start-offset is already
    ///       present the manifest won't be added. Semaphore units
    ///       will be returned.
    void put(ss::semaphore_units<> s, spillover_manifest manifest);

    /// Find manifest by its base offset
    ss::shared_ptr<materialized_manifest> get(model::offset base_offset);

    /// Check manifest by its base offset
    bool contains(model::offset base_offset);

    /// Move element forward to avoid its eviction
    ///
    /// \param base is a start offset of the manifest
    /// \returns true on success, false if the manifest is evicted
    bool promote(model::offset base);

    /// Shift element one step forward to avoid eviction
    ///
    /// \param manifest is a manifest pointer
    /// \returns true on success, false if the manifest is evicted
    bool promote(ss::shared_ptr<materialized_manifest>& manifest);

    /// Remove element from cache
    ///
    /// \param base is a start offset of the manifest
    /// \return size of the removed manifest in bytes or 0 if manifest wasn't
    ///         found
    /// \note The method removes the manifest from the cache and releases
    ///       semaphore units so new  manifest can let into the cache. But the
    ///       method doesn't guarantee that the memory consumed by the removed
    ///       manifest is freed. The user might still have a pointer to the
    ///       manifest that will prevent it from being deleted.
    size_t remove(model::offset base);

    /// Starts the cache.
    ss::future<> start();

    /// Stop the cache.
    /// Ensure all async operations are completed.
    ss::future<> stop();

    /// Set new capacity
    ///
    /// \param new_size is a new size of the cache
    /// \param timeout is an optional timeout value for the
    ///                operation
    ss::future<> set_capacity(
      size_t new_size,
      std::optional<ss::lowres_clock::duration> timeout = std::nullopt);

public:
    using map_t
      = std::map<model::offset, ss::shared_ptr<materialized_manifest>>;

    /// Evict manifest pointed by the iterator
    ///
    /// \param it points to the elements to remove
    /// \param rollback is a temporary storage for the element used for rollback
    /// \return size of the evicted manifest in bytes
    /// \note the manifest could still be used by the cursor
    ///       after eviction.
    size_t evict(map_t::iterator it, access_list_t& rollback);

    access_list_t::iterator lookup_eviction_rollback_list(model::offset o);

    /// Restore manifest temporarily stored in the _eviction_rollback list.
    void rollback(model::offset so);

    /// Remove manifest from the _eviction_rollback list.
    void discard_rollback_manifest(model::offset so);

    size_t _capacity_bytes;
    retry_chain_logger& _ctxlog;
    map_t _cache;
    ss::gate _gate;
    ss::semaphore _sem;
    access_list_t _access_order;
    access_list_t _eviction_rollback;
    ss::semaphore_units<> _reserved;
};

class async_manifest_view;

/// Type that represents section of full metadata. It can either contain
/// a materialized spillover manifest or reference to main STM manifest.
using manifest_section_t = std::variant<
  ss::shared_ptr<materialized_manifest>,
  std::reference_wrapper<const partition_manifest>>;

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

    /// Seek to manifest that contains offset
    ss::future<result<bool, error_outcome>> seek(async_view_search_query_t q);

    /// Move to the next manifest or fail
    ss::future<result<bool, error_outcome>> next();

    /// Return current manifest
    ///
    /// \return pointer to the current manifest or nullopt
    std::optional<std::reference_wrapper<const spillover_manifest>>
    manifest() const;

private:
    /// Manifest view ref
    async_manifest_view& _view;

    manifest_section_t _current;

    /// Full copy of the spillover manifests list (the assumption is that the
    /// list is relatively small)
    std::optional<spillover_manifest_list> _manifests;
};

/// Service that maintains a view of the entire
/// partition metadata including STM manifest and
/// all spillover manifests. Spillover manifests are
/// materialized on demand.
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
    get_cursor(async_view_search_query_t q) noexcept;

    bool is_empty() const noexcept;

private:
    ss::future<> run_bg_loop();

    /// Scan the bucket and update the list of spillover manifests in
    /// the cloud. The scan is only performed if the STM state changed.
    /// \return true if the state was updated, false otherwise
    ss::future<result<bool, error_outcome>> maybe_scan_bucket() noexcept;

    /// Return true if the offset belongs to the archive
    bool is_archive(async_view_search_query_t o);

    /// Returns true if the offset belongs to the archival STM manifest
    bool is_stm(async_view_search_query_t o);

    /// Get spillover manifest by offset/timestamp
    ss::future<result<manifest_section_t, error_outcome>>
    get_materialized_manifest(async_view_search_query_t q) noexcept;

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
    materialize_manifest(async_view_search_query_t base_offset) const noexcept;

    /// Find index of the spillover manifest
    ///
    /// \param query is a search query, either an offset, a kafka offset or a
    ///              timestamp
    /// \return index of the spillover manifest or -1 on error
    int32_t search_spillover_manifests(async_view_search_query_t query) const;

    mutable ss::gate _gate;
    ss::abort_source _as;
    cloud_storage_clients::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    ss::sharded<cache>& _cache;
    partition_probe& _probe;

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

    /// Materialization request which is sent to background fiber
    struct materialization_request_t {
        spillover_manifest_path_components search_vec;
        ss::promise<
          result<ss::shared_ptr<materialized_manifest>, error_outcome>>
          promise;
        std::unique_ptr<hdr_hist::measurement> _measurement;
    };
    std::deque<materialization_request_t> _requests;
    ss::condition_variable _cvar;
};

} // namespace cloud_storage