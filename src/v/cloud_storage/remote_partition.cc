/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_partition.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "storage/parser_errc.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/temporary_buffer.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace cloud_storage {

using data_t = model::record_batch_reader::data_t;
using foreign_data_t = model::record_batch_reader::foreign_data_t;
using storage_t = model::record_batch_reader::storage_t;

class record_batch_reader_impl final : public model::record_batch_reader::impl {
    using remote_segment_list_t = std::vector<std::unique_ptr<remote_segment>>;
    using remote_segment_iterator = remote_segment_list_t::iterator;

public:
    /// Accepts list of segments ordered by base_offset
    explicit record_batch_reader_impl(
      const storage::log_reader_config& config,
      ss::lw_shared_ptr<offset_translator> t,
      std::vector<std::unique_ptr<remote_segment>> s) noexcept
      : _config(config)
      , _segments(std::move(s))
      , _it(_segments.begin())
      , _translator(std::move(t)) {
        if (!is_end_of_stream()) {
            _current = ss::make_lw_shared<remote_segment_batch_reader>(
              *_it->get(), _config, *_translator);
        }
    }

    ~record_batch_reader_impl() override = default;
    record_batch_reader_impl(record_batch_reader_impl&& o) noexcept = delete;
    record_batch_reader_impl&
    operator=(record_batch_reader_impl&& o) noexcept = delete;
    record_batch_reader_impl(const record_batch_reader_impl& o) = delete;
    record_batch_reader_impl& operator=(const record_batch_reader_impl& o)
      = delete;

    bool is_end_of_stream() const override { return _it == _segments.end(); }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point deadline) override {
        while (_current /*&& !is_end_of_stream()*/) {
            // TODO: stop early if we reach max offset
            if (!_current) {
                break;
            }
            vlog(cst_log.debug, "Invoking 'read_some' on current log reader");
            auto result = co_await _current->read_some(deadline);
            if (
              !result
              && result.error() == storage::parser_errc::end_of_stream) {
                vlog(
                  cst_log.debug,
                  "Error while reading from stream, reset stream");
                co_await reset_stream();
                continue;
            } else if (!result) {
                vlog(cst_log.debug, "Unexpected error");
                throw std::system_error(result.error());
            }
            data_t d = std::move(result.value());
            co_return storage_t{std::move(d)};
        }
        vlog(
          cst_log.debug,
          "EOS reached {} {}",
          static_cast<bool>(_current),
          is_end_of_stream());
        co_return storage_t{};
    }

    void print(std::ostream& o) override {
        o << "cloud_storage_record_batch_reader";
    }

private:
    ss::future<> reset_stream() {
        vlog(cst_log.debug, "reset_stream called");
        if (_current) {
            vlog(cst_log.debug, "closing existing log reader");
            co_await _current->close();
        }
        vlog(cst_log.debug, "initializing new log reader");
        _it++;
        if (_it != _segments.end()) {
            _current = ss::make_lw_shared<remote_segment_batch_reader>(
              *_it->get(), _config, *_translator);
        } else {
            _current = {};
        }
        vlog(
          cst_log.debug,
          "reset_stream completed {} {}",
          static_cast<bool>(_current),
          is_end_of_stream());
    }

    storage::log_reader_config _config;
    remote_segment_list_t _segments;
    remote_segment_iterator _it;
    ss::lw_shared_ptr<remote_segment_batch_reader> _current;
    ss::lw_shared_ptr<offset_translator> _translator;
};

remote_partition::remote_partition(
  const manifest& m, remote& api, s3::bucket_name bucket)
  : _api(api)
  , _manifest(m)
  , _translator(ss::make_lw_shared<offset_translator>())
  , _bucket(std::move(bucket))
  , _rtc(5s, 200ms) // TODO: pass timeout value via c-tor
  , _ctxlog(cst_log, _rtc, _manifest.get_ntp().path()) {
    _translator->update(_manifest);
}

ss::future<model::record_batch_reader> remote_partition::make_reader(
  storage::log_reader_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    // pt.1 (re)hydrate translator
    _translator->update(_manifest);
    // pt.2 search the segments that match the request
    std::vector<const manifest::segment_map::value_type*> items;
    for (const auto& meta : _manifest) {
        auto b = meta.second.base_offset;
        auto e = meta.second.committed_offset;
        if (config.max_offset < b || e < config.start_offset) {
            continue;
        }
        items.push_back(&meta);
    }
    // pt.3 create record_batch_reader
    std::sort(
      items.begin(),
      items.end(),
      [](
        const manifest::segment_map::value_type* lhs,
        const manifest::segment_map::value_type* rhs) {
          return lhs->second.base_offset < rhs->second.base_offset;
      });
    std::vector<std::unique_ptr<remote_segment>> segments;
    for (const auto& m : items) {
        auto s = std::make_unique<remote_segment>(
          _api, _bucket, _manifest, m->first, _rtc);
        segments.emplace_back(std::move(s));
    }
    vlog(
      cst_log.debug,
      "Creating new record_batch_reader, {} segments found",
      segments.size());
    auto impl = std::make_unique<record_batch_reader_impl>(
      config, _translator, std::move(segments));
    model::record_batch_reader rdr(std::move(impl));
    co_return rdr;
}

} // namespace cloud_storage
