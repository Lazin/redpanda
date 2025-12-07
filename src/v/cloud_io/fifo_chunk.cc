#include "cloud_io/fifo_chunk.h"

#include "cloud_io/cache_service.h"
#include "cloud_io/logger.h"
#include "serde/envelope.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/defer.hh>

#include <system_error>

namespace cloud_io {
static constexpr size_t min_slot_size = 128_KiB;

fifo_chunk::fifo_chunk(
  ss::file f, fifo_chunk::status_t status, size_t file_size)
  : _file(std::move(f))
  , _status(status) {
    _index.total_bytes = file_size;
    vlog(
      log.debug,
      "fifo_chunk created: status={}, file_size={}",
      status == status_t::primary ? "primary" : "secondary",
      file_size);
}

ss::future<> fifo_chunk::stop() {
    vlog(log.debug, "fifo_chunk stopping");
    co_await _file.close();
    co_await _gate.close();
    vlog(log.debug, "fifo_chunk stopped");
}

uint64_t fifo_chunk::usage_bytes() { return _index.allocated; }

void fifo_chunk::require_primary() {
    vassert(
      _status == status_t::primary,
      "Method that can only be invoked on a primary is invoked on a secondary "
      "chunk instance");
}

void fifo_chunk::require_secondary() {
    vassert(
      _status == status_t::secondary,
      "Method that can only be invoked on a secondary is invoked on a primary "
      "chunk instance");
}

std::optional<fifo_chunk::write_slot>
fifo_chunk::prepare(size_t payload_size) {
    vlog(log.debug, "fifo_chunk::prepare: payload_size={}", payload_size);
    require_primary();
    auto to_allocate = ss::align_up(payload_size, min_slot_size);
    if (_index.allocated + to_allocate > _index.total_bytes) {
        vlog(
          log.debug,
          "fifo_chunk::prepare failed: insufficient space (need {}, "
          "have {})",
          to_allocate,
          _index.total_bytes - _index.allocated);
        return std::nullopt;
    }
    auto offset = _index.allocated;
    _index.allocated += to_allocate;
    write_slot slot{
      .offset = offset,
      .slot_size_bytes = to_allocate,
    };
    vlog(
      log.debug,
      "fifo_chunk::prepare succeeded: slot={{offset={}, slot_size={}}}",
      slot.offset,
      slot.slot_size_bytes);
    return slot;
}

ss::future<> fifo_chunk::put(
  const ss::sstring& key,
  fifo_chunk::write_slot slot,
  uint64_t payload_size,
  ss::input_stream<char> payload,
  size_t write_buffer_size,
  unsigned int write_behind) {
    vlog(
      log.debug,
      "fifo_chunk::put: key={}, slot={{offset={}, slot_size={}}}, "
      "payload_size={}, write_buffer_size={}, write_behind={}",
      key,
      slot.offset,
      slot.slot_size_bytes,
      payload_size,
      write_buffer_size,
      write_behind);
    vassert(
      slot.slot_size_bytes % min_slot_size == 0,
      "Incorrect slot size {}",
      slot.slot_size_bytes);

    // Check if key already exists
    auto cached = is_cached(key);
    switch (cached) {
    case cache_element_status::available:
    case cache_element_status::in_progress:
        vlog(
          log.error,
          "fifo_chunk::put failed: key={}, already cached (status={})",
          key,
          cached);
        throw std::runtime_error(
          fmt::format("Key {} is already cached", key));
    case cache_element_status::not_available:
        break;
    }

    auto h = _gate.hold();
    // This is a simplistic approach which I think will work well in practice
    // since we don't need to overly optimize a single cache write. The payload
    // size is expected to be relatively small (16MiB for TS, 4MiB for CT).
    size_t alignment = _file.memory_dma_alignment();
    size_t disk_alignment = _file.disk_read_dma_alignment();
    size_t pos = slot.offset;
    size_t end = slot.offset + slot.slot_size_bytes;

    while (!payload.eof()) {
        size_t remaining = end - pos;
        size_t to_read = std::min(write_buffer_size * write_behind, remaining);
        auto write_buffer = ss::temporary_buffer<char>::aligned(
          alignment, ss::align_up(to_read, disk_alignment));
        // Here we should assume that the buf is not aligned and not always
        // contains write_buffer_size bytes.
        auto buf = co_await payload.read_exactly(write_buffer_size);
        std::memcpy(write_buffer.get_write(), buf.get(), buf.size());
        if (buf.size() < write_buffer.size()) {
            auto slack = write_buffer.size() - buf.size();
            auto ptr = write_buffer.get_write();
            std::advance(ptr, buf.size());
            std::memset(ptr, 0, slack);
        }
        auto num_bytes = co_await _file.dma_write(
          pos, write_buffer.get(), write_buffer.size());
        pos += buf.size();
        if (num_bytes < write_buffer.size()) {
            // Short write may happen due to I/O error. It's better to fail
            // the whole cache put then to attempt to continue writing.
            vlog(
              log.error,
              "fifo_chunk::put: short write at offset={}, expected={}, "
              "actual={}",
              pos,
              write_buffer.size(),
              num_bytes);
            throw std::runtime_error("Short write");
        }
    }

    // Add to index after the data is flushed
    auto new_element = detail::fifo_index_entry{
      .offset = slot.offset,
      .payload_size = payload_size,
      .slot_size = slot.slot_size_bytes,
    };
    auto [_, ok] = _index.entries.insert(std::make_pair(key, new_element));
    vassert(ok, "Key {} is already added", key);

    vlog(
      log.debug,
      "fifo_chunk::put completed: key={}, slot={{offset={}, slot_size={}}}",
      key,
      slot.offset,
      slot.slot_size_bytes);
}

ss::input_stream<char> fifo_chunk::stream_at(
  read_slot slot, size_t read_buffer_size, unsigned int read_ahead) {
    vlog(
      log.debug,
      "fifo_chunk::stream_at: slot={{offset={}, payload_size={}, "
      "slot_size={}}}, read_buffer_size={}, read_ahead={}",
      slot.offset,
      slot.payload_size_bytes,
      slot.slot_size_bytes,
      read_buffer_size,
      read_ahead);
    ss::file_input_stream_options opts;
    opts.buffer_size = read_buffer_size;
    opts.read_ahead = read_ahead;
    return ss::make_file_input_stream(
      _file, slot.offset, slot.payload_size_bytes, opts);
}

cache_element_status fifo_chunk::is_cached(const ss::sstring& key) const {
    auto it = _index.entries.find(key);
    cache_element_status result;
    if (it != _index.entries.end()) {
        result = cache_element_status::available;
    } else {
        result = cache_element_status::not_available;
    }
    vlog(log.debug, "fifo_chunk::is_cached: key={}, result={}", key, result);
    return result;
}

std::optional<fifo_chunk::read_slot> fifo_chunk::find(const ss::sstring& key) {
    vlog(log.debug, "fifo_chunk::find: key={}", key);
    // This method can be used on both primary and secondary.
    // On a secondary it can be used only if the index was installed previously.
    auto it = _index.entries.find(key);
    if (it == _index.entries.end()) {
        vlog(log.debug, "fifo_chunk::find: key={} not found", key);
        return std::nullopt;
    }
    read_slot slot{
      .offset = it->second.offset,
      .payload_size_bytes = it->second.payload_size,
      .slot_size_bytes = it->second.slot_size,
    };
    vlog(
      log.debug,
      "fifo_chunk::find succeeded: key={}, slot={{offset={}, payload_size={}, "
      "slot_size={}}}",
      key,
      slot.offset,
      slot.payload_size_bytes,
      slot.slot_size_bytes);
    return slot;
}

iobuf fifo_chunk::serialize_index() const {
    vlog(
      log.debug,
      "fifo_chunk::serialize_index: entries={}, allocated={}, complete={}",
      _index.entries.size(),
      _index.allocated,
      _index.complete);
    return serde::to_iobuf(_index);
}

void fifo_chunk::install_index(iobuf buf) {
    vlog(log.debug, "fifo_chunk::install_index: buf_size={}", buf.size_bytes());
    auto index = serde::from_iobuf<detail::fifo_index>(std::move(buf));
    vlog(
      log.debug,
      "fifo_chunk::install_index: installed entries={}, allocated={}, "
      "complete={}",
      index.entries.size(),
      index.allocated,
      index.complete);
    _index = std::move(index);
}

void fifo_chunk::set_index_complete(bool c) noexcept {
    vlog(log.debug, "fifo_chunk::set_index_complete: complete={}", c);
    _index.complete = c;
}

bool fifo_chunk::is_index_complete() const noexcept {
    vlog(
      log.debug, "fifo_chunk::is_index_complete: complete={}", _index.complete);
    return _index.complete;
}

} // namespace cloud_io
