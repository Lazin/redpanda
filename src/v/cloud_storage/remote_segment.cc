/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_segment.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "storage/parser.h"
#include "utils/gate_guard.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/temporary_buffer.hh>

#include <exception>

namespace cloud_storage {

download_exception::download_exception(download_result r)
  : result(r) {
    vassert(
      r != download_result::success,
      "Exception created with successful error code");
}

const char* download_exception::what() const noexcept {
    switch (result) {
    case download_result::failed:
        return "Failed";
    case download_result::notfound:
        return "NotFound";
    case download_result::timedout:
        return "TimedOut";
    case download_result::success:
        return "Success";
    }
    __builtin_unreachable();
}

// stream impl
struct dl_data_source_impl : ss::data_source_impl {
    dl_data_source_impl() = delete;
    dl_data_source_impl(
      size_t pos,
      remote& api,
      s3::bucket_name bucket,
      const manifest& manifest,
      manifest::key name,
      retry_chain_node& rtc,
      size_t queue_size)
      : _queue(queue_size)
      , _bytes_consumed(pos)
      , _rtc(rtc) {
        // Background job that pushes data from input stream to the queue
        (void)ss::with_gate(
          _gate,
          [this, &api, bucket, &manifest, name]() -> ss::future<uint64_t> {
              retry_chain_logger ctxlog(cst_log, _rtc);
              auto result = co_await api.download_segment(
                bucket,
                name,
                manifest,
                [this, &ctxlog](uint64_t full_size, ss::input_stream<char> s)
                  -> ss::future<uint64_t> {
                    vlog(
                      ctxlog.debug,
                      "remote api callback invoked, size: {}, already consumed "
                      "{} bytes",
                      full_size,
                      _bytes_consumed);
                    try {
                        co_await s.skip(_bytes_consumed);
                        while (!_done) {
                            auto tmp = co_await s.read();
                            auto size = tmp.size();
                            vlog(
                              cst_log.debug,
                              "dl_data_source_impl::bg-job, received {} bytes, "
                              "_done {}, _queue size {}",
                              size,
                              _done,
                              _queue.size());
                            _done = size == 0;
                            co_await _queue.push_eventually(std::move(tmp));
                            _bytes_consumed += size;
                        }
                        vlog(
                          cst_log.debug,
                          "dl_data_source_impl::bg-job, stop, done: {}, "
                          "consumed {}",
                          _done,
                          _bytes_consumed);
                    } catch (const ss::abort_requested_exception&) {
                        _done = true;
                    } catch (...) {
                        /// TODO: if the error is 'connection reset by peer' or
                        // a timeout but the stream is not closed we should
                        // restart the request by calling 'download_segment'
                        // again and skipping the prefix.
                        vlog(
                          ctxlog.error,
                          "Error while consuming input_stream: {}",
                          std::current_exception());
                        if (!_done) {
                            _queue.abort(std::current_exception());
                            co_return 0;
                        }
                    }
                    vlog(
                      cst_log.debug,
                      "Done consuming the log segment, bytes consumed: {}, "
                      "total size: {}",
                      _bytes_consumed,
                      full_size);
                    co_return full_size;
                },
                _rtc);
              if (result != download_result::success) {
                  auto e = std::make_exception_ptr(download_exception(result));
                  _queue.abort(e);
              }
              co_return _bytes_consumed;
          });
    }

    ss::future<ss::temporary_buffer<char>> get() override {
        gate_guard g(_gate);
        vlog(
          cst_log.debug,
          "dl_data_source_impl::get, _done {}, _queue size {}",
          _done,
          _queue.size());
        try {
            auto tmp = co_await _queue.pop_eventually();
            co_return tmp;
        } catch (const ss::abort_requested_exception&) {
            // ignore since it's used to unblock the consumer
        }
        co_return ss::temporary_buffer<char>();
    }

    ss::future<> close() override {
        vlog(
          cst_log.debug,
          "dl_data_source_impl::close has blocked consumer {}, done {}",
          _queue.has_blocked_consumer(),
          _done);
        _done = true;
        auto e = std::make_exception_ptr(ss::abort_requested_exception());
        _queue.abort(e);
        return _gate.close();
    }

    ss::gate _gate;
    ss::queue<ss::temporary_buffer<char>> _queue;
    uint64_t _bytes_consumed{0};
    bool _done{false};
    retry_chain_node& _rtc;
};

remote_segment::remote_segment(
  remote& r,
  s3::bucket_name bucket,
  const manifest& m,
  manifest::key path,
  retry_chain_node& parent)
  : _api(r)
  , _bucket(std::move(bucket))
  , _manifest(m)
  , _path(std::move(path))
  , _rtc(&parent) {}

const model::ntp& remote_segment::get_ntp() const {
    return _manifest.get_ntp();
}

ss::input_stream<char>
remote_segment::data_stream(size_t pos, const offset_translator& ot) {
    auto src = std::make_unique<dl_data_source_impl>(
      pos, _api, _bucket, _manifest, _path, _rtc, 1);
    return ot.patch_stream(
      ss::input_stream<char>(ss::data_source(std::move(src))), _rtc);
}

/// Record batch consumer that consumes a single record batch
class single_record_consumer : public storage::batch_consumer {
public:
    using consume_result = storage::batch_consumer::consume_result;
    using stop_parser = storage::batch_consumer::stop_parser;

    single_record_consumer(
      storage::log_reader_config& conf, remote_segment_batch_reader& parent)
      : _config(conf)
      , _parent(parent) {}

    consume_result accept_batch_start(
      const model::record_batch_header& header) const override {
        vlog(
          cst_log.debug,
          "single_record_consumer::accept_batch_start {}",
          header);
        if (header.base_offset() > _config.max_offset) {
            vlog(
              cst_log.debug,
              "single_record_consumer::accept_batch_start stop parser because "
              "{} > {}",
              header.base_offset(),
              _config.max_offset);
            return batch_consumer::consume_result::stop_parser;
        }

        // The segment can be scanned from the begining so we should skip
        // irrelevant batches.
        if (unlikely(header.last_offset() < _config.start_offset)) {
            vlog(
              cst_log.debug,
              "single_record_consumer::accept_batch_start skip becuse {} < {}",
              header.last_offset(),
              _config.start_offset);
            return batch_consumer::consume_result::skip_batch;
        }

        if (
          (_config.strict_max_bytes || _config.bytes_consumed)
          && (_config.bytes_consumed + header.size_bytes) > _config.max_bytes) {
            vlog(
              cst_log.debug,
              "single_record_consumer::accept_batch_start stop because "
              "overbudget");
            // signal to log reader to stop (see log_reader::is_done)
            _config.over_budget = true;
            return batch_consumer::consume_result::stop_parser;
        }

        if (_config.type_filter && _config.type_filter != header.type) {
            vlog(
              cst_log.debug,
              "single_record_consumer::accept_batch_start skip because "
              "of filter");
            return batch_consumer::consume_result::skip_batch;
        }

        if (_config.first_timestamp > header.first_timestamp) {
            // kakfa needs to guarantee that the returned record is >=
            // first_timestamp
            vlog(
              cst_log.debug,
              "single_record_consumer::accept_batch_start skip because "
              "of timestamp");
            return batch_consumer::consume_result::skip_batch;
        }
        // we want to consume the batch
        return batch_consumer::consume_result::accept_batch;
    }

    /**
     * unconditionally consumes batch start
     */
    void consume_batch_start(
      model::record_batch_header header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        _header = header;
        _header.ctx.term = model::term_id(); // TODO: use correct term!
    }

    /**
     * unconditionally skip batch
     */
    void skip_batch_start(
      model::record_batch_header,
      size_t /*physical_base_offset*/,
      size_t /*size_on_disk*/) override {
        // TODO: use this to check invariant
    }

    void consume_records(iobuf&& ib) override { _records = std::move(ib); }

    stop_parser consume_batch_end() override {
        auto batch = model::record_batch{
          _header, std::move(_records), model::record_batch::tag_ctor_ng{}};
        _parent.produce(std::move(batch));
        return stop_parser::no;
    }

    void print(std::ostream& o) const override {
        o << "single_record_consumer";
    }

private:
    storage::log_reader_config& _config;
    remote_segment_batch_reader& _parent;
    model::record_batch_header _header;
    iobuf _records;
};

remote_segment_batch_reader::remote_segment_batch_reader(
  remote_segment& s,
  storage::log_reader_config& config,
  offset_translator& trans) noexcept
  : _seg(s)
  , _config(config)
  , _translator(trans) {}

ss::future<result<ss::circular_buffer<model::record_batch>>>
remote_segment_batch_reader::read_some(
  model::timeout_clock::time_point deadline) {
    vlog(
      cst_log.debug,
      "remote_segment_batch_reader::read_some(1) - done={}, ringbuf size={}",
      _done,
      _ringbuf.size());
    if (_done) {
        co_return storage::parser_errc::end_of_stream;
    }
    if (_ringbuf.empty()) {
        if (!_parser && !_done) {
            _parser = init_parser();
        }
        // TODO: produce as many items as limits allows
        auto result = co_await _parser->consume_one();
        if (!result) {
            co_return result.error();
        }
        if (result.value() == storage::batch_consumer::stop_parser::yes) {
            _done = true;
        }
    }
    vlog(
      cst_log.debug,
      "remote_segment_batch_reader::read_some(2) - done={}, ringbuf size={}",
      _done,
      _ringbuf.size());
    co_return std::move(_ringbuf);
}

std::unique_ptr<storage::continuous_batch_parser>
remote_segment_batch_reader::init_parser() {
    vlog(cst_log.debug, "remote_segment_batch_reader::init_parser");
    // TODO: turn single_record_consumer into normal consumer
    auto stream = _seg.data_stream(0, _translator);
    auto parser = std::make_unique<storage::continuous_batch_parser>(
      std::make_unique<single_record_consumer>(_config, *this),
      std::move(stream));
    return parser;
}

void remote_segment_batch_reader::produce(model::record_batch batch) {
    vlog(cst_log.debug, "remote_segment_batch_reader::produce");
    _ringbuf.push_back(std::move(batch));
}

ss::future<> remote_segment_batch_reader::close() {
    vlog(cst_log.debug, "remote_segment_batch_reader::close");
    if (_parser) {
        vlog(
          cst_log.debug, "remote_segment_batch_reader::close - parser-close");
        return _parser->close();
    }
    return ss::now();
}

} // namespace cloud_storage
