/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/cloud_topic_partition_reader.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/dl_placeholder.h"
#include "cluster/partition.h"
#include "logger.h"
#include "model/timeout_clock.h"

#include <chrono>
#include <exception>
#include <iterator>
#include <utility>

namespace kafka {

// TODO: add config
static constexpr size_t L0_max_bytes_per_metadata_fetch = 4_KiB;

cloud_topic_partition_reader_impl::cloud_topic_partition_reader_impl(
  storage::log_reader_config& cfg,
  ss::lw_shared_ptr<cluster::partition> underlying,
  ss::shared_ptr<experimental::cloud_topics::data_plane_api> ct_api)
  : _config(cfg)
  , _underlying(std::move(underlying))
  , _ct_api(std::move(ct_api)) {}

ss::future<model::record_batch_reader::storage_t>
cloud_topic_partition_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    chunked_circular_buffer<model::record_batch> res;
    switch (_current) {
    case state::empty_state:
        co_await fetch_metadata(deadline);
        [[fallthrough]];
    case state::ready_state:
        co_await materialize_batches(deadline);
        [[fallthrough]];
    case state::materialized_state:
    case state::end_of_stream_state:
        // Handled in the next switch statement
        break;
    }

    // Invariant: in case of success the state will be either materialized
    // or end_of_stream. In case of error the state will be end_of_stream.
    switch (_current) {
    case state::empty_state:
    case state::ready_state:
        _current = state::end_of_stream_state;
        throw std::runtime_error("Invalid reader state (ready/empty)");
    case state::materialized_state:
        consume_materialized_batches(&res);
        [[fallthrough]];
    case state::end_of_stream_state:
        break;
    }
    co_return res;
}

ss::future<cloud_topic_partition_reader_impl::state>
cloud_topic_partition_reader_impl::fetch_metadata(
  model::timeout_clock::time_point deadline) {
    vassert(
      _current == state::empty_state || _current == state::materialized_state,
      "Invalid state transition, unexpected current state: {}",
      std::to_underlying(_current));
    if (_meta.size() > 0) {
        // If we already have metadata, we can skip fetching it again.
        _current = state::ready_state;
        co_return;
    }
    try {
        // Fetch metadata from the _underlying
        auto ot_state = _underlying->get_offset_translator_state();
        storage::log_reader_config cfg(_config);
        cfg.start_offset = ot_state->to_log_offset(cfg.start_offset);
        cfg.max_offset = ot_state->to_log_offset(cfg.max_offset);
        cfg.translate_offsets = storage::translate_offsets::yes;
        cfg.type_filter = {model::record_batch_type::dl_placeholder};
        // This parameter defines how many bytes we want to fetch
        // from the underlying partition in one go.
        // The L0 meta batches are small, so we can fetch a lot of them in a
        // single request and then gradually materialize them.
        // The 'cfg.max_bytes' doesn't limit the size of the materialized
        // batches, because it is fetching L0 meta batches, which have different
        // size. In order to know the size of the materialized batches we need
        // to fetch L0 meta batches first and then parse them.
        cfg.max_bytes = L0_max_bytes_per_metadata_fetch;

        auto reader = co_await _underlying->make_reader(cfg);
        auto placeholders = co_await model::consume_reader_to_chunked_vector(
          std::move(reader), deadline);

        // Convert L0 meta batches to extent_meta structures.
        chunked_circular_buffer<experimental::cloud_topics::extent_meta> meta;
        chunked_circular_buffer<model::record_batch_header> headers;
        for (auto&& batch : placeholders) {
            headers.push_back(batch.header());
            experimental::cloud_topics::extent_meta e{
              .base_offset = model::offset_cast(batch.base_offset()),
              .last_offset = model::offset_cast(batch.last_offset()),
            };
            iobuf payload = std::move(batch).release_data();
            iobuf_parser parser(std::move(payload));
            auto record = model::parse_one_record_from_buffer(parser);
            iobuf value = std::move(record).release_value();
            auto placeholder
              = serde::from_iobuf<experimental::cloud_topics::dl_placeholder>(
                std::move(value));
            e.id = placeholder.id;
            e.first_byte_offset = placeholder.offset;
            e.byte_range_size = placeholder.size_bytes;
            meta.push_back(e);
        }
        vassert(
          meta.size() == headers.size(),
          "Expected the same number of headers and meta batches, got {} and {}",
          meta.size(),
          headers.size());
        _meta = std::move(meta);
        _headers = std::move(headers);
        if (!_meta.empty()) {
            vlog(
              kdlog.debug,
              "Fetched {} L0 meta batches from the underlying partition, "
              "first byte offset: {}, last byte offset: {}",
              _meta.size(),
              _meta.front().first_byte_offset,
              _meta.back().last_offset);
        } else {
            vlog(
              kdlog.debug,
              "No L0 meta batches fetched from the underlying partition, "
              "start offset: {}, max offset: {}",
              cfg.start_offset,
              cfg.max_offset);
        }

    } catch (...) {
        vlog(
          kdlog.info,
          "Failed to fetch metadata from the underlying partition: {}",
          std::current_exception());
        _current = state::end_of_stream_state;
        co_return;
    }
    _current = _meta.empty() ? state::end_of_stream_state : state::ready_state;
}

ss::future<> cloud_topic_partition_reader_impl::materialize_batches(
  model::timeout_clock::time_point deadline) {
    if (_current == state::end_of_stream_state) {
        _current = state::end_of_stream_state;
        co_return;
    }
    vassert(
      _current == state::ready_state || _current == state::materialized_state,
      "Invalid state transition, unexpected current state: {}",
      std::to_underlying(_current));

    if (_batches.size() > 0) {
        // We're already materialized.
        _current = state::materialized_state;
        co_return;
    }

    // Cherry-pick enough L0 meta batches to materialize.
    vassert(
      _meta.size() == _headers.size(),
      "Expected the same number of headers and meta batches, got {} and {}",
      _meta.size(),
      _headers.size());
    try {
        chunked_vector<experimental::cloud_topics::extent_meta> to_materialize;
        chunked_vector<model::record_batch_header> to_materialize_headers;
        size_t materialize_bytes = 0;
        while (_config.bytes_consumed < _config.max_bytes && !_meta.empty()) {
            auto meta = _meta.front();
            auto header = _headers.front();
            if (
              (_config.strict_max_bytes || _config.bytes_consumed > 0)
              && (_config.bytes_consumed + meta.byte_range_size)
                   > _config.max_bytes) {
                // If the next meta batch exceeds the max bytes limit, we stop
                // materializing. The only exception is if we didn't collect any
                // batches yet, in which case we still materialize the next
                // batch. This could happen if the first meta batch is larger
                // than the max bytes limit (oversized batch or too small
                // limit). In this case we don't want to stall the reader
                // completely.
                break;
            }
            _meta.pop_front();
            _headers.pop_front();
            _config.bytes_consumed += meta.byte_range_size;
            materialize_bytes += meta.byte_range_size;
            to_materialize.push_back(meta);
            to_materialize_headers.push_back(header);
        }

        // Ask data layer to bring data from the cloud storage.
        auto mat_res = co_await _ct_api->materialize(
          _underlying->ntp(),
          materialize_bytes,
          std::move(to_materialize),
          std::chrono::duration_cast<std::chrono::milliseconds>(
            model::time_until(deadline)));
        if (mat_res.has_error()) {
            vlog(
              kdlog.info,
              "Failed to materialize batches from the cloud storage: {}",
              mat_res.error().message());
            _current = state::end_of_stream_state;
            co_return;
        }

        // Patch materialized record batches
        auto batches = std::move(mat_res.value());
        for (size_t i = 0; i < batches.size(); i++) {
            auto& data_hdr = batches.at(i).header();
            auto size = data_hdr.size_bytes;
            auto crc = data_hdr.crc;
            data_hdr = to_materialize_headers.at(i);
            data_hdr.type = model::record_batch_type::raft_data;
            data_hdr.size_bytes = size;
            data_hdr.crc = crc;
            // Recalculate the header crc
            data_hdr.header_crc = model::internal_header_only_crc(data_hdr);
        }

        _batches = std::move(batches);
        // Materialize batches from the L0 meta batches.
        vlog(
          kdlog.debug,
          "Materialized {} batches from the L0 meta batches",
          _batches.size());
    } catch (...) {
        vlog(
          kdlog.info,
          "Failed to materialize batches {}",
          std::current_exception());
        _current = state::end_of_stream_state;
        co_return;
    }

    _current = state::materialized_state;
}

cloud_topic_partition_reader_impl::state
cloud_topic_partition_reader_impl::consume_materialized_batches(
  chunked_circular_buffer<model::record_batch>* dest) {
    vlog(
      kdlog.debug,
      "consuming {} materialized batches, cached {} extents",
      _batches.size());
    std::move(_batches.begin(), _batches.end(), std::back_inserter(*dest));
    _batches.clear();
    _config.start_offset = model::next_offset(dest->back().last_offset());
    _current = _meta.empty() ? state::empty_state : state::ready_state;
}

void cloud_topic_partition_reader_impl::print(std::ostream& o) {
    o << "cloud_topics_reader";
}

bool cloud_topic_partition_reader_impl::is_end_of_stream() const {
    return _current == state::end_of_stream_state;
}

} // namespace kafka
