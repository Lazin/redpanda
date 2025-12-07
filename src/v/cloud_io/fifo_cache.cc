/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/fifo_cache.h"

#include "bytes/iostream.h"
#include "cloud_io/logger.h"
#include "utils/directory_walker.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/file.hh>

#include <fmt/core.h>

#include <algorithm>
#include <regex>
#include <stdexcept>

namespace cloud_io {

namespace {
struct chunk_file_info {
    std::filesystem::path file_path;
    uint64_t chunk_id;
};

struct chunk_walker {
    ss::future<> operator()(ss::directory_entry entry) {
        if (entry.type != ss::directory_entry_type::regular) {
            co_return;
        }

        std::string filename = entry.name;
        std::smatch match;

        if (std::regex_match(filename, match, chunk_pattern)) {
            // Extract the numeric ID from the filename
            uint64_t chunk_id = std::stoull(match[1].str());
            auto file_path = cache_dir / filename;

            vlog(
              log.info,
              "fifo_cache: found chunk file: {}, id={}",
              file_path.string(),
              chunk_id);

            chunk_files->push_back(
              chunk_file_info{.file_path = file_path, .chunk_id = chunk_id});
        }
        co_return;
    }

    std::filesystem::path cache_dir;
    chunked_vector<chunk_file_info>* chunk_files;
    // Regular expression to match cache_<numeric-id>.chunk
    std::regex chunk_pattern{R"(cache_(\d+)\.chunk)"};
};
} // namespace

fifo_cache::fifo_cache(
  std::filesystem::path cache_dir, fifo_cache_config config)
  : _cache_dir(std::move(cache_dir))
  , _chunk_size(config.chunk_size)
  , _cache_size(config.cache_size)
  , _max_objects_per_chunk(
      config.max_objects
      / std::max(size_t{1}, config.cache_size / config.chunk_size))
  , _space_sem(config.cache_size, "fifo_cache/space")
  , _objects_sem(config.max_objects, "fifo_cache/objects") {
    vlog(
      log.info,
      "fifo_cache created: cache_dir={}, cache_size={}, chunk_size={}, "
      "max_objects={}, max_objects_per_chunk={}, initial_capacity={}",
      _cache_dir.string(),
      config.cache_size,
      _chunk_size,
      config.max_objects,
      _max_objects_per_chunk,
      config.cache_size);
}

ss::future<> fifo_cache::start() {
    vlog(
      log.info,
      "fifo_cache starting: enumerating chunks in {}",
      _cache_dir.string());

    chunked_vector<chunk_file_info> chunk_files;
    chunk_walker walker{.cache_dir = _cache_dir, .chunk_files = &chunk_files};

    // Enumerate all files in the cache directory and collect matching files
    co_await directory_walker::walk(_cache_dir.string(), std::move(walker));

    for (const auto& chunk_file : chunk_files) {
        // Check if index file exists
        auto index_path = chunk_file.file_path;
        index_path.replace_extension(".index");

        // Check if index file exists using file_exists
        bool index_exists = co_await ss::file_exists(index_path.string());

        if (!index_exists) {
            vlog(
              log.warn,
              "fifo_cache: chunk file {} has no index file, skipping",
              chunk_file.file_path.string());
            continue;
        }

        // Read the index file
        vlog(
          log.debug, "fifo_cache: reading index from {}", index_path.string());

        auto index_file = co_await ss::open_file_dma(
          index_path.string(), ss::open_flags::ro);
        auto index_size = co_await index_file.size();
        auto index_stream = ss::make_file_input_stream(index_file);
        auto index_buf = co_await read_iobuf_exactly(index_stream, index_size);
        co_await index_stream.close();

        vlog(
          log.debug,
          "fifo_cache: read index from {}, size={}",
          index_path.string(),
          index_buf.size_bytes());

        // Open the chunk file
        auto file = co_await ss::open_file_dma(
          chunk_file.file_path.string(), ss::open_flags::rw);

        auto file_size = co_await file.size();

        vlog(
          log.debug,
          "fifo_cache: opened chunk file: {}, size={}",
          chunk_file.file_path.string(),
          file_size);

        auto chunk = std::make_unique<fifo_chunk>(
          std::move(file), fifo_chunk::status_t::primary, file_size);

        // Install the index
        chunk->install_index(std::move(index_buf));

        vlog(
          log.debug,
          "fifo_cache: installed index for chunk_id={}",
          chunk_file.chunk_id);

        // Store the chunk info
        _chunks.push_back(
          chunk_info{
            .chunk_id = chunk_file.chunk_id,
            .chunk = std::move(chunk),
            .file_path = chunk_file.file_path,
          });
    }

    std::ranges::sort(
      _chunks, [](const chunk_info& lhs, const chunk_info& rhs) {
          return lhs.chunk_id < rhs.chunk_id;
      });

    // Account for space used by loaded chunks
    for (const auto& chunk_info : _chunks) {
        auto usage = chunk_info.chunk->usage_bytes();
        auto num_keys = chunk_info.chunk->get_index_entries().size();

        // Consume semaphore units for the used space and objects
        if (usage > 0) {
            auto space_units = co_await ss::get_units(_space_sem, usage);
            space_units.return_all();
            _current_cache_size += usage;
        }
        if (num_keys > 0) {
            auto object_units = co_await ss::get_units(_objects_sem, num_keys);
            object_units.return_all();
            _current_cache_objects += num_keys;
        }

        vlog(
          log.debug,
          "fifo_cache: accounted for chunk_id={}, usage={}, num_keys={}",
          chunk_info.chunk_id,
          usage,
          num_keys);
    }

    vlog(
      log.info,
      "fifo_cache started: loaded {} chunks, current_size={}, "
      "current_objects={}, available_space={}",
      _chunks.size(),
      _current_cache_size,
      _current_cache_objects,
      _space_sem.available_units());

    // Check if disk usage overshoots and evict chunks if necessary
    auto disk_usage = calculate_disk_usage();
    if (disk_usage > _cache_size) {
        vlog(
          log.warn,
          "fifo_cache::start: disk usage {} exceeds cache size {}, evicting "
          "chunks",
          disk_usage,
          _cache_size);

        // Evict chunks until we're under the limit
        while (!_chunks.empty() && calculate_disk_usage() > _cache_size) {
            co_await remove_oldest_chunk();
        }

        vlog(
          log.info,
          "fifo_cache::start: after eviction, {} chunks remaining, disk_usage={}",
          _chunks.size(),
          calculate_disk_usage());
    }

    co_return;
}

ss::future<> fifo_cache::stop() {
    vlog(log.info, "fifo_cache stopping: closing {} chunks", _chunks.size());

    for (auto& chunk_info : _chunks) {
        co_await chunk_info.chunk->stop();
        vlog(
          log.debug,
          "fifo_cache: stopped chunk_id={}",
          chunk_info.chunk_id);
    }

    vlog(log.info, "fifo_cache stopped");
    co_return;
}

ss::future<std::optional<cache_item_stream>> fifo_cache::get_stream(
  std::filesystem::path key,
  size_t read_buffer_size,
  unsigned int read_ahead) {
    vlog(
      log.debug,
      "fifo_cache::get_stream: key={}, read_buffer_size={}, read_ahead={}",
      key.string(),
      read_buffer_size,
      read_ahead);

    ss::sstring key_str = key.string();

    // Search through all chunks for the key
    for (const auto& chunk_info : _chunks) {
        auto read_slot = chunk_info.chunk->find(key_str);
        if (read_slot.has_value()) {
            vlog(
              log.debug,
              "fifo_cache::get_stream: found key={} in chunk_id={}",
              key_str,
              chunk_info.chunk_id);

            auto stream = chunk_info.chunk->stream_at(
              *read_slot, read_buffer_size, read_ahead);
            co_return cache_item_stream{
              .body = std::move(stream),
              .size = read_slot->payload_size_bytes,
            };
        }
    }

    vlog(log.debug, "fifo_cache::get_stream: key={} not found", key_str);
    co_return std::nullopt;
}

ss::future<> fifo_cache::put(
  std::filesystem::path key,
  ss::input_stream<char>& data,
  basic_space_reservation_guard<ss::lowres_clock>& reservation,
  size_t write_buffer_size,
  unsigned int write_behind) {
    // Convert path to sstring for fifo_chunk API
    ss::sstring key_str = key.string();

    vlog(
      log.debug,
      "fifo_cache::put: key={}, write_buffer_size={}, write_behind={}",
      key_str,
      write_buffer_size,
      write_behind);

    // Get fields from the reservation guard (populated by reserve_space)
    if (!reservation.id().has_value() || !reservation.offset().has_value()
        || !reservation.payload_size().has_value()) {
        throw std::runtime_error(fmt::format(
          "Reservation guard missing required fields for key: {}. "
          "id={}, offset={}, payload_size={}",
          key_str,
          reservation.id().has_value() ? "set" : "unset",
          reservation.offset().has_value() ? "set" : "unset",
          reservation.payload_size().has_value() ? "set" : "unset"));
    }

    uint64_t chunk_id = *reservation.id();
    uint64_t offset = *reservation.offset();
    uint64_t payload_size = *reservation.payload_size();
    uint64_t slot_size_bytes = reservation.reserved_bytes();

    // Find the chunk by id
    auto chunk_it = std::ranges::find_if(
      _chunks, [chunk_id](const chunk_info& info) {
          return info.chunk_id == chunk_id;
      });

    if (chunk_it == _chunks.end()) {
        throw std::runtime_error(
          fmt::format("Chunk with id {} not found for key: {}", chunk_id, key_str));
    }

    auto* target_chunk = chunk_it->chunk.get();

    // Reconstruct the write_slot from the stored fields
    fifo_chunk::write_slot write_slot{
      .offset = offset,
      .slot_size_bytes = slot_size_bytes,
    };

    // Write to the target chunk using the prepared slot
    co_await target_chunk->put(
      key_str, write_slot, payload_size, std::move(data), write_buffer_size, write_behind);
    target_chunk->mark_clean(key_str);
    // TODO: rollback allocated chunk slot in case of error

    // Serialize and write the index to disk
    auto index_path = chunk_it->file_path;
    index_path.replace_extension(".index");

    auto index_buf = target_chunk->serialize_index();

    vlog(
      log.debug,
      "fifo_cache: writing index to {}, size={}",
      index_path.string(),
      index_buf.size_bytes());

    co_await ss::recursive_touch_directory(_cache_dir.string());
    auto index_file = co_await ss::open_file_dma(
      index_path.string(),
      ss::open_flags::wo | ss::open_flags::create | ss::open_flags::truncate);

    auto out = co_await ss::make_file_output_stream(index_file);
    co_await write_iobuf_to_output_stream(std::move(index_buf), out);
    co_await out.flush();
    co_await out.close();

    vlog(
      log.debug,
      "fifo_cache: successfully wrote key={}, size={}",
      key_str,
      payload_size);
}

seastar::coroutine::experimental::generator<ss::sstring>
fifo_cache::scan_keys(std::optional<std::filesystem::path> prefix) const {
    struct key_iterator {
        ss::sstring key;
        absl::btree_map<ss::sstring, detail::fifo_index_entry>::const_iterator
          current;
        absl::btree_map<ss::sstring, detail::fifo_index_entry>::const_iterator
          end;

        bool operator>(const key_iterator& other) const {
            return key > other.key;
        }
    };

    std::optional<ss::sstring> prefix_str = prefix.transform(
      [](const auto& p) { return p.string(); });

    // Priority queue with greater comparator (min-heap)
    // NOTE: it is possible to do better by using different
    // queue type (from boost.priority_queue). We can also
    // select a type of the queue based on cardinality (different
    // queues will be faster or slower depending on number of
    // fifo_chunk instances).
    std::
      priority_queue<key_iterator, chunked_vector<key_iterator>, std::greater<>>
        pq;

    // Initialize priority queue with first key from each chunk
    for (const auto& chunk_info : _chunks) {
        auto& entries = chunk_info.chunk->get_index_entries();
        auto begin = prefix_str.has_value() ? entries.lower_bound(*prefix_str)
                                            : entries.begin();
        auto end = entries.end();

        if (begin != end) {
            pq.push(
              key_iterator{.key = begin->first, .current = begin, .end = end});
        }
    }

    // N-way merge
    while (!pq.empty()) {
        auto top = pq.top();
        pq.pop();

        // It is possible to have duplicates. We're not filtering
        // them out here. Instead, we will be preventing them upon
        // ingestion. So no special handling of duplicates is required.
        co_yield top.key;

        // Advance iterator for this chunk
        auto next_it = top.current;
        ++next_it;
        if (next_it != top.end) {
            pq.push(
              key_iterator{
                .key = next_it->first, .current = next_it, .end = top.end});
        }
    }
    co_return;
}

ss::future<cache_element_status>
fifo_cache::is_cached(const std::filesystem::path& key) {
    ss::sstring key_str = key.string();

    vlog(log.debug, "fifo_cache::is_cached: checking key={}", key_str);

    // Search through all chunks for the key
    for (const auto& chunk_info : _chunks) {
        auto status = chunk_info.chunk->is_cached(key_str);
        if (status != cache_element_status::not_available) {
            vlog(
              log.debug,
              "fifo_cache::is_cached: key={} found in chunk_id={}, status={}",
              key_str,
              chunk_info.chunk_id,
              status);
            co_return status;
        }
    }

    vlog(
      log.debug,
      "fifo_cache::is_cached: key={} not found",
      key_str);
    co_return cache_element_status::not_available;
}

ss::future<basic_space_reservation_guard<ss::lowres_clock>>
fifo_cache::reserve_space(uint64_t bytes, size_t objects) {
    vlog(
      log.debug,
      "fifo_cache::reserve_space: requesting bytes={}, objects={}",
      bytes,
      objects);

    // Get the chunk to write to (may roll to a new chunk if needed)
    auto target_chunk = co_await get_or_roll_chunk();

    // Prepare a write slot in the target chunk
    auto write_slot = target_chunk->prepare(bytes);
    if (!write_slot) {
        throw std::runtime_error(
          fmt::format("Failed to prepare write slot for {} bytes", bytes));
    }

    // Find the chunk_id for the target chunk
    uint64_t chunk_id = 0;
    for (const auto& chunk_info : _chunks) {
        if (chunk_info.chunk.get() == target_chunk) {
            chunk_id = chunk_info.chunk_id;
            break;
        }
    }

    vlog(
      log.debug,
      "fifo_cache::reserve_space: granted reservation bytes={}, objects={}, "
      "chunk_id={}, offset={}, slot_size={}",
      bytes,
      objects,
      chunk_id,
      write_slot->offset,
      write_slot->slot_size_bytes);

    // Create reservation guard with the slot_size as reserved_bytes
    auto guard = basic_space_reservation_guard<ss::lowres_clock>(
      *this, write_slot->slot_size_bytes, objects);

    // Populate the optional fields with the write_slot information
    guard.set_id(chunk_id);
    guard.set_offset(write_slot->offset);
    guard.set_payload_size(bytes);

    co_return guard;
}

void fifo_cache::reserve_space_release(
  uint64_t reserved_bytes,
  size_t reserved_objects,
  uint64_t used_bytes,
  size_t used_objects,
  std::optional<uint64_t> /*id*/,
  std::optional<uint64_t> /*offset*/,
  std::optional<uint64_t> /*payload_size*/) {
    vlog(
      log.debug,
      "fifo_cache::reserve_space_release: reserved_bytes={}, "
      "reserved_objects={}, used_bytes={}, used_objects={}",
      reserved_bytes,
      reserved_objects,
      used_bytes,
      used_objects);

    // Semaphore units are managed by chunk allocation/eviction in get_or_roll_chunk
    // and remove_oldest_chunk, so we don't need to update anything here
}

uint64_t fifo_cache::calculate_disk_usage() const {
    uint64_t total = _chunks.size() * _chunk_size;
    vlog(
      log.debug,
      "fifo_cache::calculate_disk_usage: {} chunks, total={}",
      _chunks.size(),
      total);
    return total;
}

ss::future<bool> fifo_cache::evict_chunks(uint64_t required_space) {
    vlog(
      log.debug,
      "fifo_cache::evict_chunks_for_space: required_space={}",
      required_space);

    auto current_usage = calculate_disk_usage();

    // Check if we have enough space
    if (current_usage + required_space <= _cache_size) {
        vlog(
          log.debug,
          "fifo_cache::evict_chunks_for_space: no eviction needed, "
          "current_usage={}, required_space={}, cache_size={}",
          current_usage,
          required_space,
          _cache_size);
        co_return true;
    }

    // Evict chunks until we have enough space
    while (!_chunks.empty()) {
        current_usage = calculate_disk_usage();
        if (current_usage + required_space <= _cache_size) {
            vlog(
              log.info,
              "fifo_cache::evict_chunks_for_space: eviction successful, "
              "current_usage={}, required_space={}",
              current_usage,
              required_space);
            co_return true;
        }

        vlog(
          log.info,
          "fifo_cache::evict_chunks_for_space: evicting oldest chunk, "
          "current_usage={}, required_space={}, cache_size={}",
          current_usage,
          required_space,
          _cache_size);

        co_await remove_oldest_chunk();
    }

    // Could not free enough space
    vlog(
      log.error,
      "fifo_cache::evict_chunks_for_space: failed to free space, "
      "required_space={}, cache_size={}",
      required_space,
      _cache_size);
    co_return false;
}

ss::future<> fifo_cache::remove_oldest_chunk() {
    if (_chunks.empty()) {
        vlog(log.warn, "fifo_cache::remove_oldest_chunk: no chunks to remove");
        co_return;
    }

    // Remove the first chunk (oldest)
    auto& oldest = _chunks.front();

    vlog(
      log.info,
      "fifo_cache::remove_oldest_chunk: removing chunk_id={}, file={}",
      oldest.chunk_id,
      oldest.file_path.string());

    // Get usage statistics before removal
    auto usage = oldest.chunk->usage_bytes();
    auto num_keys = oldest.chunk->get_index_entries().size();

    // Stop the chunk
    co_await oldest.chunk->stop();

    // Delete chunk file
    try {
        co_await ss::remove_file(oldest.file_path.string());
        vlog(
          log.debug,
          "fifo_cache::remove_oldest_chunk: deleted chunk file {}",
          oldest.file_path.string());
    } catch (const std::exception& e) {
        vlog(
          log.warn,
          "fifo_cache::remove_oldest_chunk: failed to delete chunk file {}: {}",
          oldest.file_path.string(),
          e.what());
    }

    // Delete index file
    auto index_path = oldest.file_path;
    index_path.replace_extension(".index");
    try {
        co_await ss::remove_file(index_path.string());
        vlog(
          log.debug,
          "fifo_cache::remove_oldest_chunk: deleted index file {}",
          index_path.string());
    } catch (const std::exception& e) {
        vlog(
          log.warn,
          "fifo_cache::remove_oldest_chunk: failed to delete index file {}: {}",
          index_path.string(),
          e.what());
    }

    // Update cache statistics
    _current_cache_size -= usage;
    _current_cache_objects -= num_keys;

    // Return space to semaphores
    // Signal full chunk capacity since that's what was reserved when allocated
    _space_sem.signal(_chunk_size);
    _objects_sem.signal(_max_objects_per_chunk);

    // Remove from vector by shifting all elements
    // chunked_vector doesn't support erase, so we need to rebuild
    chunked_vector<chunk_info> new_chunks;
    for (size_t i = 1; i < _chunks.size(); ++i) {
        new_chunks.push_back(std::move(_chunks[i]));
    }
    _chunks = std::move(new_chunks);

    vlog(
      log.info,
      "fifo_cache::remove_oldest_chunk: removed chunk, freed usage={}, "
      "num_keys={}, remaining_chunks={}",
      usage,
      num_keys,
      _chunks.size());

    co_return;
}

ss::future<fifo_chunk*> fifo_cache::get_or_roll_chunk() {
    // Acquire mutex to protect chunk modifications
    auto units = co_await _chunks_mutex.get_units();

    fifo_chunk* target_chunk = nullptr;
    bool need_new_chunk = false;
    std::string roll_reason;

    if (_chunks.empty()) {
        need_new_chunk = true;
        roll_reason = "no chunks exist";
    } else {
        auto& last_chunk = _chunks.back().chunk;
        auto current_objects = last_chunk->get_index_entries().size();

        // Check if adding one more object would exceed the limit
        if (current_objects + 1 > _max_objects_per_chunk) {
            need_new_chunk = true;
            roll_reason = fmt::format(
              "object limit reached (current={}, limit={})",
              current_objects,
              _max_objects_per_chunk);

            // Log warning if chunk has space but we're rolling due to object
            // count
            if (!last_chunk->is_full()) {
                vlog(
                  log.warn,
                  "fifo_cache: rolling chunk due to object limit, chunk has "
                  "space but current_objects={}, max_objects_per_chunk={}",
                  current_objects,
                  _max_objects_per_chunk);
            }
        } else if (last_chunk->is_full()) {
            need_new_chunk = true;
            roll_reason = "chunk is full";
        } else {
            target_chunk = last_chunk.get();
        }
    }

    if (need_new_chunk) {
        vlog(
          log.debug,
          "fifo_cache: rolling chunk, reason: {}",
          roll_reason);

        // Need to create a new chunk
        // First check if we need to evict old chunks to make room
        auto eviction_success = co_await evict_chunks(_chunk_size);
        if (!eviction_success) {
            throw std::runtime_error(fmt::format(
              "Failed to allocate new chunk: insufficient space, cache_size={}, "
              "chunk_size={}",
              _cache_size,
              _chunk_size));
        }

        uint64_t chunk_id = 0;
        if (!_chunks.empty()) {
            chunk_id = _chunks.back().chunk_id + 1;
        }

        auto file_path = _cache_dir / fmt::format("cache_{}.chunk", chunk_id);

        vlog(
          log.info,
          "fifo_cache: creating new chunk file: {}, size={}",
          file_path.string(),
          _chunk_size);

        // Allocate the disk space
        auto file = co_await ss::open_file_dma(
          file_path.string(),
          ss::open_flags::rw | ss::open_flags::create
            | ss::open_flags::truncate);
        co_await file.allocate(0, _chunk_size);

        auto chunk = std::make_unique<fifo_chunk>(
          std::move(file), fifo_chunk::status_t::primary, _chunk_size);

        target_chunk = chunk.get();

        _chunks.push_back(
          chunk_info{
            .chunk_id = chunk_id,
            .chunk = std::move(chunk),
            .file_path = file_path,
          });

        // Consume semaphore units for the newly allocated chunk
        co_await _space_sem.wait(_chunk_size);
        co_await _objects_sem.wait(_max_objects_per_chunk);

        vlog(
          log.debug,
          "fifo_cache: created new chunk with id={}, consumed space={}, "
          "objects={}",
          chunk_id,
          _chunk_size,
          _max_objects_per_chunk);
    }

    co_return target_chunk;
}

} // namespace cloud_io
