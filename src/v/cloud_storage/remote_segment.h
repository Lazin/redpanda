/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "model/record.h"
#include "s3/client.h"
#include "storage/parser.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

namespace cloud_storage {

class download_exception : public std::exception {
public:
    explicit download_exception(download_result r);

    const char* what() const noexcept override;

    const download_result result;
};

class remote_segment final {
public:
    remote_segment(
      remote& r,
      s3::bucket_name bucket,
      const manifest& m,
      manifest::key path,
      retry_chain_node& parent);

    const model::ntp& get_ntp() const;

    /// create an input stream _sharing_ the underlying file handle
    /// starting at position @pos
    ss::input_stream<char> data_stream(size_t pos, const offset_translator& t);

private:
    remote& _api;
    s3::bucket_name _bucket;
    const manifest& _manifest;
    manifest::key _path;
    retry_chain_node _rtc;
    // TBD: add cache reference
};

class single_record_consumer;

/// The segment reader that can be used to fetch data from cloud storage
///
/// Instead of populating an internal buffer as storage::log_reader
/// this mechanism creates a stream which is consumed by the parser
/// lazily.
/// The difference is due to the fact that skipping_consumer in storage
/// can easily skip the prefix of the segment but the 'remote' couldn't
/// do this as easy. It can be implemented but still there're limitations
/// that won't allow us to create an input stream for every chunk of data.
/// Instead, the reader is creating an input_stream<char> which is consumed
/// lazily as client consumes record batches. S3 can reset the connection
/// if client waits for too long. In this case the stream is re-created.
/// This component is supposed to be intgrated with the cache.
class remote_segment_batch_reader final {
public:
    // TODO: pass batch-cache
    remote_segment_batch_reader(
      remote_segment&,
      storage::log_reader_config& config,
      offset_translator& translator) noexcept;

    remote_segment_batch_reader(
      remote_segment_batch_reader&&) noexcept = default;
    remote_segment_batch_reader&
    operator=(remote_segment_batch_reader&&) noexcept = delete;
    remote_segment_batch_reader(const remote_segment_batch_reader&) = delete;
    remote_segment_batch_reader& operator=(const remote_segment_batch_reader&)
      = delete;
    ~remote_segment_batch_reader() noexcept = default;

    ss::future<result<ss::circular_buffer<model::record_batch>>>
      read_some(model::timeout_clock::time_point);

    ss::future<> close();

private:
    friend class single_record_consumer;
    std::unique_ptr<storage::continuous_batch_parser> init_parser();
    void produce(model::record_batch batch);

    remote_segment& _seg;
    storage::log_reader_config& _config;
    offset_translator& _translator;
    std::unique_ptr<storage::continuous_batch_parser> _parser;
    bool _done{false};
    ss::circular_buffer<model::record_batch> _ringbuf;
};

} // namespace cloud_storage
