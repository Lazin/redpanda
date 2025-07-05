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
#pragma once

#include "cloud_topics/extent_meta.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/log_reader.h"
#include <chrono>

namespace cluster {
class partition;
}

namespace experimental::cloud_topics {
class data_plane_api;
class app;
} // namespace experimental::cloud_topics

namespace kafka {

class cloud_topic_partition_reader_impl
  : public model::record_batch_reader::impl {
public:
    cloud_topic_partition_reader_impl(
      storage::log_reader_config& cfg,
      ss::lw_shared_ptr<cluster::partition> underlying,
      ss::shared_ptr<experimental::cloud_topics::data_plane_api> ct_api);

    bool is_end_of_stream() const final;

    ss::future<model::record_batch_reader::storage_t>
      do_load_slice(model::timeout_clock::time_point) final;

    void print(std::ostream& o) final;

private:
    // States
    enum class state {
        empty_state,
        ready_state,
        materialized_state,
        end_of_stream_state,
    };

    // Fetch L0 meta batches from the underlying partition
    ss::future<state> fetch_metadata(model::timeout_clock::time_point deadline);
    ss::future<state> materialize_batches(model::timeout_clock::time_point deadline);
    state consume_materialized_batches(chunked_circular_buffer<model::record_batch>* dest);

    state _current{state::empty_state};

    chunked_circular_buffer<experimental::cloud_topics::extent_meta> _meta;
    chunked_circular_buffer<model::record_batch_header> _headers;
    chunked_vector<model::record_batch> _batches;
    storage::log_reader_config _config;
    ss::lw_shared_ptr<cluster::partition> _underlying;
    ss::shared_ptr<experimental::cloud_topics::data_plane_api> _ct_api;
};

} // namespace kafka
