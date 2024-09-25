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

#include "base/seastarx.h"
#include "cloud_topics/batcher/write_request.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/dl_placeholder.h"
#include "model/record.h"

#include <seastar/core/iostream.hh>

#include <absl/container/btree_map.h>

namespace cloud_topics::details {

/// List of placeholder batches that has to be propagated
/// to the particular write request.
template<class Clock>
struct batches_for_req {
    /// Generated placeholder batches
    ss::circular_buffer<model::record_batch> placeholders;
    /// Source write request. The list has to have size = 1
    write_request_list<Clock> ref;
};

// This component aggregates a bunch of write
// requests and produces single serialized object.
template<class Clock>
class aggregator {
public:
    explicit aggregator(object_id id = object_id{uuid_t::create()});
    aggregator(const aggregator&) = delete;
    aggregator(aggregator&&) = delete;
    aggregator& operator=(const aggregator&) = delete;
    aggregator& operator=(aggregator&&) = delete;
    ~aggregator();

    void add(write_request_list<Clock>& pending);
    size_t size_bytes() const noexcept;

    ss::input_stream<char> prepare();

    void ack();
    void ack_error(errc);

private:

    /// Generate placeholders.
    /// This method should be invoked before 'get_result'
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>>
    get_placeholders();

    /// Produce L0 object payload.
    /// The method messes up the state so it can only
    /// be called once.
    ss::input_stream<char> get_stream();

    object_id _id;
    /// Source data for the aggregator
    absl::btree_map<model::ntp, write_request_list<Clock>> _staging;
    /// Prepared placeholders
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>> _aggregated;
    bool _acknowledged{false};
    size_t _size_bytes{0};
};

} // namespace cloud_topics::details
