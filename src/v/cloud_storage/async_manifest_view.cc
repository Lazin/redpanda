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
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "utils/human.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/outcome/success_failure.hpp>

#include <exception>
#include <functional>
#include <variant>

namespace cloud_storage {

static constexpr size_t max_cache_capacity_bytes = 1_GiB;

static ss::sstring to_string(const async_view_search_query_t& t) {
    return ss::visit(
      t,
      [&](model::offset ro) { return ssx::sformat("[offset: {}]", ro); },
      [&](kafka::offset ko) { return ssx::sformat("[kafka offset: {}]", ko); },
      [&](model::timestamp ts) { return ssx::sformat("[timestamp: {}]", ts); });
}

static bool contains(
  const spillover_manifest_path_components& c,
  const async_view_search_query_t& query) {
    return ss::visit(
      query,
      [&](model::offset o) { return o >= c.base && o <= c.last; },
      [&](kafka::offset k) { return k >= c.base_kafka && k < c.next_kafka; },
      [&](model::timestamp t) { return t >= c.base_ts && t <= c.last_ts; });
}

static bool
contains(const partition_manifest& m, const async_view_search_query_t& query) {
    return ss::visit(
      query,
      [&](model::offset o) {
          return o >= m.get_start_offset().value_or(model::offset::max())
                 && o <= m.get_last_offset();
      },
      [&](kafka::offset k) {
          return k >= m.get_start_kafka_offset()
                 && k < m.get_next_kafka_offset();
      },
      [&](model::timestamp t) {
          return m.size() > 0 && t >= m.begin()->base_timestamp
                 && t <= m.last_segment()->max_timestamp;
      });
}

materialized_manifest_cache::materialized_manifest_cache(
  size_t capacity_bytes, retry_chain_logger& parent_logger)
  : _capacity_bytes(capacity_bytes)
  , _ctxlog(parent_logger)
  , _sem(max_cache_capacity_bytes) {
    vassert(
      capacity_bytes > 0 && capacity_bytes < max_cache_capacity_bytes,
      "Invalid cache capacity {}, should be non-zero and below 1GiB",
      capacity_bytes);
}

ss::future<ss::semaphore_units<>> materialized_manifest_cache::prepare(
  size_t size_bytes, std::optional<ss::lowres_clock::duration> timeout) {
    ss::gate::holder h(_gate);
    if (size_bytes > _capacity_bytes) {
        vlog(
          _ctxlog.trace,
          "Oversized 'put' operation requested. Manifest size is {} bytes",
          size_bytes);
        // Oversized manifest handling. The manifest could be larger than
        // capacity. If we will not allow this manifest to be added to the
        // cache the subsystem will stall. The only possible solution is to
        // let the manifest into the cache and allow it to evict everything
        // else.
        size_bytes = _capacity_bytes;
    }
    auto maybe_units = ss::try_get_units(_sem, size_bytes);
    if (maybe_units.has_value()) {
        vlog(
          _ctxlog.trace,
          "{} units acquired without waiting, {} available",
          size_bytes,
          _sem.available_units());
        // The cache is not full and can grant some capacity without
        // eviction
        co_return std::move(maybe_units.value());
    }
    // The cache is full, try to free up some space. Free at least
    // 'size_bytes' bytes.
    size_t bytes_evicted = 0;
    std::deque<model::offset> evicted;
    while (bytes_evicted < size_bytes && !_cache.empty()) {
        auto it = _access_order.begin();
        auto so = it->manifest.get_start_offset();
        vassert(
          so.has_value(),
          "Manifest can't be empty, ntp: {}",
          it->manifest.get_ntp());
        auto cit = _cache.find(so.value());
        vassert(
          cit != _cache.end(), "Manifest at {} already evicted", so.value());
        evicted.push_back(so.value());
        // Invariant: the materialized_manifest is always linked to either
        // _access_order or _eviction_rollback list.
        bytes_evicted += evict(cit, _eviction_rollback);
    }
    // Here the least recently used materialized manifests were evicted to
    // free up 'size_bytes' bytes. But these manifests could still be used
    // by some cursor. We need to wait for them to get released.
    ss::semaphore_units<> u;
    try {
        if (timeout.has_value()) {
            u = co_await ss::get_units(_sem, size_bytes, timeout.value());
        } else {
            u = co_await ss::get_units(_sem, size_bytes);
        }
    } catch (const ss::timed_out_error& e) {
        // Operation timed out and we need to return elements stored in
        // the '_eviction_rollback' list back into '_cache'. Only
        // offsets from 'evicted' should be affected.
        vlog(
          _ctxlog.debug,
          "Prepare operation timed out, restoring {} spillover "
          "manifest",
          evicted.size());
        for (auto eso : evicted) {
            rollback(eso);
        }
        throw;
    } catch (...) {
        // In case of any other error the elements from
        // '_eviction_rollback' list should be evicted for real
        // (filtered by 'eviction' set).
        vlog(
          _ctxlog.error,
          "'{}' error detected, cleaning up eviction list",
          std::current_exception());
        for (auto eso : evicted) {
            discard_rollback_manifest(eso);
        }
        throw;
    }
    co_return u;
}

size_t materialized_manifest_cache::size() const noexcept {
    return _access_order.size() + _eviction_rollback.size();
}

size_t materialized_manifest_cache::size_bytes() const noexcept {
    size_t res = 0;
    for (const auto& m : _access_order) {
        res += m._units.count();
    }
    for (const auto& m : _eviction_rollback) {
        res += m._units.count();
    }
    return res;
}

void materialized_manifest_cache::put(
  ss::semaphore_units<> s, spillover_manifest manifest) {
    auto so = manifest.get_start_offset();
    vassert(
      so.has_value(), "Manifest can't be empty, ntp: {}", manifest.get_ntp());
    if (!_eviction_rollback.empty()) {
        auto it = lookup_eviction_rollback_list(so.value());
        if (it != _eviction_rollback.end()) {
            vlog(
              _ctxlog.error,
              "Manifest with base offset {} is being evicted from the "
              "cache",
              so);
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Manifest with start offset {} is being evicted from the "
              "cache",
              so));
        }
    }
    auto item = ss::make_shared<materialized_manifest>(
      std::move(manifest), std::move(s));
    auto [it, ok] = _cache.insert(std::make_pair(so.value(), std::move(item)));
    if (!ok) {
        // This may indicate a race, log a warning
        vlog(
          _ctxlog.error, "Manifest with base offset {} is already present", so);
        return;
    }
    _access_order.push_back(*it->second);
}

ss::shared_ptr<materialized_manifest>
materialized_manifest_cache::get(model::offset base_offset) {
    if (auto it = _cache.find(base_offset); it != _cache.end()) {
        if (promote(it->second)) {
            return it->second;
        }
    }
    if (!_eviction_rollback.empty()) {
        // Another fiber is waiting for the eviction of some elements.
        // These elements could be stored in the '_eviction_rollback' list
        // until there exist a copy of the shared pointer somewhere. We need
        // to search through the list and return matching manifest if
        // possible. Otherwise, the fiber may re-create the manifest and the
        // other fiber may restore evicted manifest (if the wait on a
        // semaphore will timeout) which will result in conflict.
        auto it = lookup_eviction_rollback_list(base_offset);
        if (it != _eviction_rollback.end()) {
            return it->shared_from_this();
        }
    }
    return nullptr;
}

bool materialized_manifest_cache::contains(model::offset base_offset) {
    return _cache.contains(base_offset);
}

bool materialized_manifest_cache::promote(model::offset base) {
    if (auto it = _cache.find(base); it != _cache.end()) {
        return promote(it->second);
    }
    return false;
}

bool materialized_manifest_cache::promote(
  ss::shared_ptr<materialized_manifest>& manifest) {
    if (!manifest->evicted) {
        manifest->_hook.unlink();
        _access_order.push_back(*manifest);
        return true;
    }
    return false;
}

size_t materialized_manifest_cache::remove(model::offset base) {
    access_list_t rollback;
    size_t evicted_bytes = 0;
    if (auto it = _cache.find(base); it != _cache.end()) {
        evicted_bytes = evict(it, rollback);
    }
    for (auto& m : rollback) {
        vlog(
          _ctxlog.debug,
          "Offloaded spillover manifest with offset {} from memory",
          m.manifest.get_start_offset());
        m._units.return_all();
    }
    return evicted_bytes;
}

ss::future<> materialized_manifest_cache::start() {
    auto num_reserved = max_cache_capacity_bytes - _capacity_bytes;
    if (ss::this_shard_id() == 0) {
        vlog(
          _ctxlog.info,
          "Starting materialized manifest cache, capacity: {}, reserved: {}",
          human::bytes(_capacity_bytes),
          human::bytes(num_reserved));
    }
    // Should be ready immediately since all units are available
    // before the cache is started.
    _reserved = co_await ss::get_units(_sem, num_reserved);
}

ss::future<> materialized_manifest_cache::stop() {
    _sem.broken();
    return _gate.close();
}

ss::future<> materialized_manifest_cache::set_capacity(
  size_t new_size, std::optional<ss::lowres_clock::duration> timeout) {
    if (new_size == 0 || new_size > max_cache_capacity_bytes) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "Invalid capacity value {}", new_size));
    }
    if (new_size == _capacity_bytes) {
        co_return;
    } else if (new_size < _capacity_bytes) {
        // Cache shrinks, we need to evict some elements from the cache
        // if there is not enough space to shrink. We need to acquire
        // the units and add them to reserved semaphore units.
        auto delta = _capacity_bytes - new_size;
        vlog(
          _ctxlog.debug,
          "Shrinking materialized manifest cache capacity from {} to {}",
          _capacity_bytes,
          new_size);
        auto u = co_await prepare(delta, timeout);
        _reserved.adopt(std::move(u));
    } else {
        vlog(
          _ctxlog.debug,
          "Increasing materialized manifest cache capacity from {} to {}",
          _capacity_bytes,
          new_size);
        // Cache grows so we need to release some reserved units.
        auto u = _reserved.split(new_size - _capacity_bytes);
        u.return_all();
    }
    _capacity_bytes = new_size;
    co_return;
}

size_t materialized_manifest_cache::evict(
  map_t::iterator it, access_list_t& rollback) {
    vlog(
      _ctxlog.trace,
      "Requested to evict manifest with start offset: {}, use count: {}, "
      "units: {}",
      it->first,
      it->second.use_count(),
      it->second->_units.count());
    auto sz = it->second->_units.count();
    it->second->_hook.unlink();
    it->second->evicted = true;
    rollback.push_back(*it->second);
    _cache.erase(it);
    return sz;
}

materialized_manifest_cache::access_list_t::iterator
materialized_manifest_cache::lookup_eviction_rollback_list(model::offset o) {
    return std::find_if(
      _eviction_rollback.begin(),
      _eviction_rollback.end(),
      [o](const materialized_manifest& m) {
          return m.manifest.get_start_offset() == o;
      });
}

void materialized_manifest_cache::rollback(model::offset so) {
    auto it = lookup_eviction_rollback_list(so);
    if (it == _eviction_rollback.end()) {
        vlog(
          _ctxlog.debug,
          "Can't rollback eviction of the manifest with start offset {}",
          so);
        return;
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    auto [_, ok] = _cache.insert(std::make_pair(so, ptr));
    if (!ok) {
        vlog(
          _ctxlog.error,
          "Manifest with base offset {} has a duplicate in the log",
          so);
        return;
    }
    ptr->evicted = false;
    _access_order.push_front(*ptr);
    vlog(
      _ctxlog.debug,
      "Successful rollback of the manifest with start offset {}",
      so);
}

void materialized_manifest_cache::discard_rollback_manifest(model::offset so) {
    auto it = lookup_eviction_rollback_list(so);
    if (it == _eviction_rollback.end()) {
        vlog(
          _ctxlog.error,
          "Can't find manifest with start offset {} in the rollback list",
          so);
    }
    auto ptr = it->shared_from_this();
    ptr->_hook.unlink();
    vlog(
      _ctxlog.debug,
      "Manifest with start offset {} removed from rollback list",
      so);
}

async_manifest_view_cursor::async_manifest_view_cursor(
  async_manifest_view& view, ss::lowres_clock::duration timeout)
  : _view(view)
  , _current(std::monostate())
  , _idle_timeout(timeout) {
    _timer.set_callback([this] { on_timeout(); });
}

ss::future<result<bool, error_outcome>>
async_manifest_view_cursor::seek(async_view_search_query_t q) {
    auto satisfies_query = ss::visit(
      _current,
      [](std::monostate) { return false; },
      [](stale_manifest) { return false; },
      [q](std::reference_wrapper<const partition_manifest> p) {
          return contains(p, q);
      },
      [q](const ss::shared_ptr<materialized_manifest>& m) {
          return contains(m->manifest, q);
      });
    if (satisfies_query) {
        // The seek is to the same manifest so no need to go through the churns
        // of hydrating/materializing/fetching the manifest
        co_return true;
    }
    auto res = co_await _view.get_materialized_manifest(q);
    if (res.has_failure()) {
        vlog(
          _view._ctxlog.error,
          "Failed to seek async_manifest_view_cursor: {}",
          res.error());
        co_return res.as_failure();
    }
    _current = res.value();
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return true;
}

ss::future<result<bool, error_outcome>> async_manifest_view_cursor::next() {
    static constexpr auto EOS = model::offset{};
    auto next_base_offset = ss::visit(
      _current,
      [](std::monostate) { return EOS; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) { return EOS; },
      [](const ss::shared_ptr<materialized_manifest>& m) {
          return model::next_offset(m->manifest.get_last_offset());
      });

    if (next_base_offset == EOS) {
        co_return false;
    }
    auto manifest = co_await _view.get_materialized_manifest(next_base_offset);
    if (manifest.has_failure()) {
        co_return manifest.as_failure();
    }
    _current = manifest.value();
    _timer.rearm(_idle_timeout + ss::lowres_clock::now());
    co_return true;
}

std::optional<std::reference_wrapper<const partition_manifest>>
async_manifest_view_cursor::manifest() const {
    using ret_t
      = std::optional<std::reference_wrapper<const partition_manifest>>;
    return ss::visit(
      _current,
      [](std::monostate) -> ret_t { return std::nullopt; },
      [this](stale_manifest) -> ret_t {
          auto errc = make_error_code(error_outcome::timed_out);
          throw std::system_error(
            errc,
            fmt_with_ctx(
              fmt::format,
              "{} manifest was evicted from the cache",
              _view.get_ntp()));
      },
      [](std::reference_wrapper<const partition_manifest> m) -> ret_t {
          return m;
      },
      [](const ss::shared_ptr<materialized_manifest>& m) -> ret_t {
          return std::ref(m->manifest);
      });
}

void async_manifest_view_cursor::on_timeout() {
    auto next = ss::visit(
      _current,
      [](std::monostate) { return model::offset{}; },
      [](stale_manifest sm) { return sm.next_offset; },
      [](std::reference_wrapper<const partition_manifest>) {
          return model::offset{};
      },
      [](const ss::shared_ptr<materialized_manifest>& m) {
          if (m->evicted) {
              return model::next_offset(m->manifest.get_last_offset());
          } else {
              return model::offset{};
          }
      });
    if (next != model::offset{}) {
        _current = stale_manifest{.next_offset = next};
    } else {
        _timer.arm(_idle_timeout);
    }
}

async_manifest_view::async_manifest_view(
  ss::sharded<remote>& remote,
  ss::sharded<cache>& cache,
  const partition_manifest& stm_manifest,
  cloud_storage_clients::bucket_name bucket,
  partition_probe& probe)
  : _bucket(bucket)
  , _remote(remote)
  , _cache(cache)
  , _probe(probe)
  , _stm_manifest(stm_manifest)
  , _rtcnode(_as)
  , _ctxlog(cst_log, _rtcnode, _stm_manifest.get_ntp().path())
  , _timeout(
      config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms.bind())
  , _backoff(config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
  , _read_buffer_size(config::shard_local_cfg().storage_read_buffer_size.bind())
  , _readahead_size(
      config::shard_local_cfg().storage_read_readahead_count.bind()) {}

ss::future<> async_manifest_view::start() {
    ssx::background = run_bg_loop();
    co_return;
}

ss::future<> async_manifest_view::stop() {
    _as.request_abort();
    co_await _gate.close();
}

ss::future<> async_manifest_view::run_bg_loop() {
    std::exception_ptr exc_ptr;
    try {
        ss::gate::holder h(_gate);
        while (!_as.abort_requested()) {
            co_await _cvar.when(
              [&] { return !_requests.empty() || _as.abort_requested(); });
            _as.check();
            if (!_requests.empty()) {
                auto front = std::move(_requests.front());
                _requests.pop_front();
                try {
                    vlog(
                      _ctxlog.debug,
                      "Processing spillover manifest request {}, path: {}",
                      front.search_vec,
                      front.path);
                    if (is_stm(front.search_vec.base)) {
                        vlog(
                          _ctxlog.warn,
                          "Request {} refers to STM manifest",
                          front.search_vec);
                        // Normally, the request shouldn't contain the STM
                        // request but nothing prevents us from handling this
                        // just in case.
                        front.promise.set_value(std::ref(_stm_manifest));
                        continue;
                    }
                    if (!_manifest_cache->contains(front.search_vec.base)) {
                        // Manifest is not cached and has to be hydrated and/or
                        // materialized.
                        auto u = co_await _manifest_cache->prepare(
                          front.size_bytes); // TODO: use timeout
                        // At this point we have free memory to download the
                        // spillover manifest.
                        auto m_res = co_await materialize_manifest(front.path);
                        if (m_res.has_failure()) {
                            vlog(
                              _ctxlog.error,
                              "Failed to materialize manifest {}, vec: {}, "
                              "error: "
                              "{}",
                              front.path,
                              front.search_vec,
                              m_res.error());
                            front.promise.set_value(m_res.as_failure());
                            continue;
                        }
                        // Put newly materialized manifest into the cache
                        _manifest_cache->put(
                          std::move(u), std::move(m_res.value()));
                        _probe.set_spillover_manifest_bytes(
                          static_cast<int64_t>(_manifest_cache->size_bytes()));
                        _probe.set_spillover_manifest_instances(
                          static_cast<int32_t>(_manifest_cache->size()));
                    }
                    auto cached = _manifest_cache->get(front.search_vec.base);
                    front.promise.set_value(cached);
                    vlog(
                      _ctxlog.debug,
                      "Spillover manifest request {} processed successfully",
                      front.search_vec);
                } catch (...) {
                    vlog(
                      _ctxlog.error,
                      "Failed processing request {}, exception: {}",
                      front.search_vec,
                      std::current_exception());
                    front.promise.set_to_current_exception();
                }
            }
        }
    } catch (const ss::broken_condition_variable&) {
        vlog(_ctxlog.debug, "Broken condition variable exception");
        exc_ptr = std::current_exception();
    } catch (const ss::abort_requested_exception&) {
        vlog(_ctxlog.debug, "Abort requested exception");
        exc_ptr = std::current_exception();
    } catch (const ss::gate_closed_exception&) {
        vlog(_ctxlog.debug, "Gate closed exception");
        exc_ptr = std::current_exception();
    } catch (...) {
        vlog(
          _ctxlog.debug, "Unexpected exception: {}", std::current_exception());
        exc_ptr = std::current_exception();
    }
    if (exc_ptr) {
        // Unblock all readers in case of error
        for (auto& req : _requests) {
            req.promise.set_exception(exc_ptr);
        }
    }
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

ss::future<result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
async_manifest_view::get_cursor(async_view_search_query_t query) noexcept {
    try {
        ss::gate::holder h(_gate);
        auto cursor = std::make_unique<async_manifest_view_cursor>(
          *this, 10s); // TODO: use configured value
        // This calls 'get_materialized_manifest' internally which
        // could potentially schedule manifest hydration/materialization
        // in the background fiber.
        auto result = co_await cursor->seek(query);
        if (result.has_error()) {
            vlog(
              _ctxlog.error,
              "failed to seek to offset {}, error: {}",
              query,
              result.error());
            co_return result.as_failure();
        }
        co_return cursor;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to create a cursor: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

bool async_manifest_view::is_empty() const noexcept {
    return _stm_manifest.size() == 0;
}

bool async_manifest_view::is_archive(async_view_search_query_t o) {
    return ss::visit(
      o,
      [this](model::offset ro) {
          return ro < _stm_manifest.get_start_offset().value_or(
                   model::offset::max());
      },
      [this](kafka::offset ko) {
          return ko < _stm_manifest.get_start_kafka_offset().value_or(
                   kafka::offset::max());
      },
      [this](model::timestamp ts) {
          auto bt = _stm_manifest.begin()->base_timestamp;
          return ts < bt;
      });
}

bool async_manifest_view::is_stm(async_view_search_query_t o) {
    return ss::visit(
      o,
      [this](model::offset ro) {
          return ro >= _stm_manifest.get_start_offset().value_or(
                   model::offset::max());
      },
      [this](kafka::offset ko) {
          return ko >= _stm_manifest.get_start_kafka_offset().value_or(
                   kafka::offset::max());
      },
      [this](model::timestamp ts) {
          auto sm = _stm_manifest.timequery(ts);
          return sm.has_value();
      });
}

ss::future<result<manifest_section_t, error_outcome>>
async_manifest_view::get_materialized_manifest(
  async_view_search_query_t q) noexcept {
    try {
        ss::gate::holder h(_gate);
        if (is_stm(q)) {
            // Fast path for STM reads
            co_return std::ref(_stm_manifest);
        }
        // The scan is not performed every time. Nevertheless, it can take long
        // time to re-scan. But in some cases it's unavoidable (because the data
        // is outdated). The scan could be moved to the background fiber but the
        // current fiber will still have to wait for it, so it doesn't make much
        // difference. To avoid stalls from bucket scans the metadata will be
        // updated on every archival stm spillover event.
        auto scan_res = co_await maybe_scan_bucket();
        if (scan_res.has_failure()) {
            vlog(_ctxlog.debug, "Can't scan the bucket, {}", scan_res.error());
            co_return scan_res.as_failure();
        }
        vassert(
          _manifests.has_value(),
          "Manifest list is not loaded, {}",
          to_string(q));
        auto index = search_spillover_manifests(q);
        if (index < 0) {
            vlog(
              _ctxlog.debug, "Can't find requested manifest, {}", to_string(q));
            co_return error_outcome::manifest_not_found;
        }
        auto vec = _manifests->components[index];
        auto loc = _manifests->manifests[index];
        auto szb = _manifests->sizes[index];
        auto res = _manifest_cache->get(vec.base);
        if (res) {
            co_return res;
        }
        // Send materialization request to background loop
        materialization_request_t request{
          .search_vec = vec,
          .path = std::move(loc),
          .size_bytes = szb,
          ._measurement = _probe.spillover_manifest_latency(),
        };
        auto fut = request.promise.get_future();
        _requests.emplace_back(std::move(request));
        _cvar.signal();
        auto m = co_await std::move(fut);
        if (m.has_failure()) {
            vlog(
              _ctxlog.error,
              "Failed to materialize spillover manifest: {}",
              m.error());
            co_return m.as_failure();
        }
        co_return m.value();
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize spillover manifest: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::hydrate_manifest(
  remote_manifest_path path) const noexcept {
    try {
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        retry_chain_node fib(_timeout(), _backoff(), &_rtcnode);
        auto res = co_await _remote.local().download_manifest(
          _bucket, path, manifest, fib);
        if (res != download_result::success) {
            vlog(
              _ctxlog.error,
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
        _probe.on_spillover_manifest_hydration();
        co_return std::move(manifest);
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

int32_t async_manifest_view::search_spillover_manifests(
  async_view_search_query_t query) const {
    if (!_manifests.has_value()) {
        throw std::logic_error(fmt_with_ctx(
          fmt::format,
          "can't hydrate spillover manifest, bucket has to be scanned first"));
    }
    if (
      _last_stm_start_offset
      < _stm_manifest.get_start_offset().value_or(model::offset{})) {
        auto errc = make_error_code(error_outcome::repeat);
        throw std::system_error(
          errc,
          fmt_with_ctx(
            fmt::format,
            "list of spillover manifests is outdated, current "
            "start_offset: {}, cached: {}",
            _stm_manifest.get_start_offset().value_or(model::offset{}),
            _last_stm_start_offset));
    }
    if (_last_archive_start_offset < _stm_manifest.get_archive_start_offset()) {
        auto errc = make_error_code(error_outcome::repeat);
        throw std::system_error(
          errc,
          fmt_with_ctx(
            fmt::format,
            "list of spillover manifests is outdated, current "
            "archive_start_offset: {}, cached: {}",
            _stm_manifest.get_archive_start_offset(),
            _last_archive_start_offset));
    }

    // Perform simple scan of the manifest list.
    auto it = std::find_if(
      _manifests->components.begin(),
      _manifests->components.end(),
      [query](const spillover_manifest_path_components& c) {
          return contains(c, query);
      });

    if (it == _manifests->components.end()) {
        return -1;
    }

    return it->index;
}

ss::future<result<spillover_manifest, error_outcome>>
async_manifest_view::materialize_manifest(
  remote_manifest_path path) const noexcept {
    try {
        auto h = _gate.hold();
        spillover_manifest manifest(
          _stm_manifest.get_ntp(), _stm_manifest.get_revision_id());
        // Perform simple scan of the manifest list
        // Probe cache. If not available or in case of race with cache eviction
        // hydrate manifest from the cloud.
        auto cache_status = co_await _cache.local().is_cached(path());
        switch (cache_status) {
        case cache_element_status::in_progress:
            vlog(_ctxlog.warn, "Concurrent manifest hydration, path {}", path);
            co_return error_outcome::repeat;
        case cache_element_status::not_available: {
            auto res = co_await hydrate_manifest(path);
            if (res.has_failure()) {
                vlog(
                  _ctxlog.error,
                  "failed to download manifest, object key: {}, error: {}",
                  path,
                  res.error());
                co_return error_outcome::manifest_download_error;
            }
            auto manifest = std::move(res.value());
            auto [str, len] = co_await manifest.serialize();
            co_await _cache.local().put(
              manifest.get_manifest_path()(),
              str,
              priority_manager::local().shadow_indexing_priority());
        } break;
        case cache_element_status::available: {
            auto res = co_await _cache.local().get(path());
            if (!res.has_value()) {
                vlog(
                  _ctxlog.warn,
                  "failed to read cached manifest, object key: {}",
                  path);
                // Cache race removed the file after `is_cached` check, the
                // upper layer is supposed to retry the call.
                co_return error_outcome::repeat;
            }
            ss::file_input_stream_options options{
              .buffer_size = _read_buffer_size(),
              .read_ahead = static_cast<uint32_t>(_readahead_size()),
              .io_priority_class
              = priority_manager::local().shadow_indexing_priority()};
            auto data_stream = ss::make_file_input_stream(
              res->body, 0, std::move(options));
            co_await manifest.update(std::move(data_stream));
        } break;
        }
        _probe.on_spillover_manifest_materialization();
        co_return manifest;
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
}

bool async_manifest_view::scan_needed() const noexcept {
    return _stm_manifest.get_start_offset() > _last_stm_start_offset
           || _last_stm_start_offset == model::offset{};
}

ss::future<result<bool, error_outcome>>
async_manifest_view::maybe_scan_bucket() noexcept {
    try {
        if (
          _stm_manifest.get_start_offset() == _last_stm_start_offset
          || _stm_manifest.get_archive_start_offset() == model::offset{}) {
            // In this case no new spillover manifests could be uploaded.
            co_return false;
        }
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
            vlog(_ctxlog.error, "failed to list manifests: {}", res.error());
            co_return error_outcome::scan_bucket_error;
        }
        int32_t index = 0;
        for (const auto& it : res.value().contents) {
            result.manifests.emplace_back(it.key);
            result.components.push_back(
              parse_spillover_manifest_path(it.key, index));
            result.sizes.push_back(it.size_bytes), index++;
        }
        _manifests.emplace(std::move(result));
    } catch (...) {
        vlog(
          _ctxlog.error,
          "Failed to materialize segment: {}",
          std::current_exception());
        co_return error_outcome::failure;
    }
    co_return true;
}
} // namespace cloud_storage