/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_io/remote.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "container/fragmented_vector.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "storage/record_batch_builder.h"
#include "utils/retry_chain_node.h"
#include "utils/uuid.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <absl/container/btree_map.h>

#include <chrono>

namespace cloud_topics {

struct batcher_result {
    uuid_t uuid;
    // Reader that contains placeholder batches. Batches
    // should map to original batches 1:1 but have different
    // content.
    std::unique_ptr<model::record_batch_reader> reader;
};

using batcher_req_index = named_type<int64_t, struct batcher_req_index_tag>;

namespace details {


template<class Clock>
using write_request_ptr = ss::lw_shared_ptr<write_request<Clock>>;

/// Object stores references to aggregated write requests
template<class Clock>
struct aggregated_write_request {
    using write_request_ptr = write_request_ptr<Clock>;

    using ntp_map_t
      = absl::btree_map<model::ntp, chunked_vector<write_request_ptr>>;

    // This map contains data that has to be uploaded to the
    // cloud storage.
    ntp_map_t to_upload;

    size_t size_bytes{0};
};

template<class Clock>
using aggregated_write_request_ptr
  = ss::lw_shared_ptr<aggregated_write_request<Clock>>;

struct serializing_consumer_stats {
    size_t header_bytes{0};
    size_t record_bytes{0};
    // TODO(Lazin): add more stats
};

class data_layout {
public:
    struct batch_ref {
        object_id id;
        first_byte_offset_t physical_offset{0};
        byte_range_size_t size_bytes{0};
        ss::circular_buffer<model::record_batch> placeholders;
    };

    batch_ref& get_batch_ref(const model::ntp& ntp) {
        return std::ref(_refs[ntp]);
    }

    model::record_batch make_dl_placeholder_batch(
      object_id id,
      size_t physical_offset,
      size_t size_bytes,
      const model::record_batch& src) {
        vassert(
          src.header().type == model::record_batch_type::raft_data,
          "Expected raft_data batch type, got {}",
          src.header());
        dl_placeholder placeholder{
          .id = id,
          .offset = first_byte_offset_t(physical_offset),
          .size_bytes = byte_range_size_t(size_bytes),
        };
        storage::record_batch_builder builder(
          model::record_batch_type::
            version_fence /*TODO: use dl_placeholder batch type*/,
          src.base_offset());
        // TX data (producer id, control flag) are not copied from 'src' yet.

        // Put the payload
        builder.add_raw_kv(
          serde::to_iobuf(dl_placeholder_record_key::payload),
          serde::to_iobuf(placeholder));
        // Align produced placeholder with the original raft_data batch
        for (auto i = 1; i < src.record_count(); i++) {
            iobuf empty;
            builder.add_raw_kv(
              serde::to_iobuf(dl_placeholder_record_key::empty),
              std::move(empty));
        }
        return std::move(builder).build();
    }

    void register_bytes(
      model::ntp ntp,
      object_id id,
      iobuf* serialized_header,
      iobuf* serialized_payload,
      model::record_batch batch) { // NOLINT
        byte_range_size_t batch_size{0};
        auto offset = _current_pos;
        batch_size += byte_range_size_t{serialized_header->size_bytes()};
        batch_size += byte_range_size_t{serialized_payload->size_bytes()};
        _current_pos += batch_size;
        // Temporary logic (until placeholder batches are implemented)
        iobuf tmp;
        tmp.append(serialized_header->copy());
        tmp.append(serialized_payload->copy());
        auto it = _refs.find(ntp);
        if (it == _refs.end()) {
            ss::circular_buffer<model::record_batch> ph;
            ph.push_back(
              make_dl_placeholder_batch(id, offset, batch_size, batch));
            _refs.insert(std::make_pair(
              ntp,
              batch_ref{
                .id = id,
                .physical_offset = offset,
                .size_bytes = batch_size,
                // Currently, we're storing copy of the data here.
                // It will be replaced by the placeholder.
                .placeholders = std::move(ph),
              }));
        } else {
            // Ditto, this will use batch passed as a parameter but
            // we will have to replace this with the placeholder
            it->second.placeholders.push_back(std::move(batch));
        }
    }
    // TODO: add register delimiter method that should propagate _current_pos
    // without adding new refs
private:
    first_byte_offset_t _current_pos{0};
    std::map<model::ntp, batch_ref> _refs;
};

class serializing_consumer {
public:
    explicit serializing_consumer(
      std::optional<model::ntp> ntp,
      object_id id,
      iobuf* output,
      data_layout* layout);

    // Accept all batches unconditionally
    ss::future<ss::stop_iteration> operator()(model::record_batch batch);

    bool end_of_stream() const { return false; }

private:
    std::optional<model::ntp> _ntp;
    object_id _id;
    iobuf* _output;
    data_layout* _layout;
};

template<class Clock>
struct concatenating_stream_data_source : ss::data_source_impl {
    using aggregated_write_request_ptr = aggregated_write_request_ptr<Clock>;

    explicit concatenating_stream_data_source(
      object_id id, aggregated_write_request_ptr& p, data_layout& layout)
      : _id(id)
      , _layout(&layout) {
        linearize(p);
    }

    ss::future<ss::temporary_buffer<char>> get() {
        if (_buff.empty()) {
            if (_refill_ix == _readers.size()) {
                // Signal EOS
                co_return ss::temporary_buffer<char>();
            }
            // This iobuf stores placeholders that reference the data
            data_layout layout;
            // Convert reader to iobuf
            auto& [ntp, rdr] = _readers.at(_refill_ix++);
            co_await rdr.consume(
              serializing_consumer(ntp, _id, &_buff, _layout),
              model::timeout_clock::now() + std::chrono::seconds(10));
            vassert(!_buff.empty(), "Reader returned empty buffer");
        }
        // Invariant: _buff has some data
        auto b = _buff.begin()->share();
        _buff.pop_front();
        co_return std::move(b);
    }

private:
    // Move readers from the input map (ord. by ntp) into the _readers
    // collection.
    // Data is not copied. Only the record_batch_reader instances are
    // reshuffled.
    void linearize(aggregated_write_request_ptr& inp) {
        for (auto& [ntp, requests] : inp->to_upload) {
            // TODO: add delimiter here
            // create a model::record_batch_reader for an individual record
            // batch that contains the delimiter (introduce the new batch type)
            for (auto& req : requests) {
                _readers.push_back(std::make_tuple(
                  std::make_optional(ntp), std::move(req->reader)));
            }
        }
    }

    object_id _id;
    // We're storing ntp + reader for normal batches and nullopt + reader for
    // delimiters and metadata batches.
    chunked_vector<
      std::tuple<std::optional<model::ntp>, model::record_batch_reader>>
      _readers;
    size_t _refill_ix{0};
    iobuf _buff;
    data_layout* _layout;
};

/// Access point for unit-tests
class batcher_accessor;

} // namespace details


/// The data path uploader
///
/// The batcher collects a list of write_request instances in
/// memory. Periodically, the data is uploaded to the cloud storage
/// and removed from memory.
template<class Clock = ss::lowres_clock>
class batcher {
    using clock_t = Clock;
    using timestamp_t = typename Clock::time_point;
    using write_request = details::write_request<Clock>;
    using write_request_ptr = details::write_request_ptr<Clock>;
    using aggregated_write_request_ptr
      = details::aggregated_write_request_ptr<Clock>;

public:
    explicit batcher(
      cloud_storage_clients::bucket_name bucket,
      cloud_io::remote_api<Clock>& remote_api);

    /// Upload data to the cloud storage
    ///
    /// Wait until the record batch is uploaded and return result.
    /// Always consumes everything from the reader.
    ss::future<result<model::record_batch_reader>> write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader&& r,
      std::chrono::milliseconds timeout);

    ss::future<> start();
    ss::future<> stop();

private:
    template<class C>
    friend struct upload_data_flow_controller;

    using request_list = intrusive_list<write_request, &write_request::_hook>;

    /// Background fiber responsible for merging
    /// aggregated log data and sending it to the
    /// cloud storage
    ///
    /// The method should only be invoked on shard 0
    ss::future<> bg_controller_loop();

    /// List of write requests which are ready to be uploaded
    struct size_limited_write_req_list {
        /// Write requests list which are ready for
        /// upload or expired.
        request_list ready;
        /// If the batcher contains more write requests which
        /// were not included because the size limit was reached
        /// this field will be set to false.
        bool complete{true};
        /// Total size of all listed write requests.
        size_t size_bytes{0};
    };

    /// Get write requests atomically. 
    /// Return requests which are either ready to be uploaded or expired. Limit
    /// the total size of all returned write requests by 'max_bytes'.
    size_limited_write_req_list
    get_write_requests(size_t max_bytes);

    /// Wait until upload interval elapses or until
    /// enough bytes are accumulated
    ss::future<> wait_for_next_upload();

    /// Upload L0 object based on placeholders
    ///
    /// Collect data from every shard and upload stream of data to S3.
    /// Inform every shard about the completed upload so they could evict the
    /// data.
    ///
    /// \return size of the uploaded object or error code
    ss::future<result<size_t>> upload_object(aggregated_write_request_ptr);

    cloud_io::remote_api<Clock>& _remote;
    cloud_storage_clients::bucket_name _bucket;
    config::binding<std::chrono::milliseconds> _upload_timeout;
    config::binding<std::chrono::milliseconds> _upload_interval;

    ss::gate _gate;
    ss::abort_source _as;

    // List of new write requests
    request_list _pending;

    static constexpr size_t max_buffer_size = 16_MiB;
    static constexpr size_t max_cardinality = 1000;

    batcher_req_index _index{0};
    size_t _current_size{0};
    basic_retry_chain_node<Clock> _rtc;
    basic_retry_chain_logger<Clock> _logger;

    ss::condition_variable _cv;
};
} // namespace cloud_topics
