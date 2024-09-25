/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/batcher/aggregator.h"
#include "cloud_topics/batcher/write_request.h"
#include "cloud_topics/batcher/serializer.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

cloud_topics::details::serialized_chunk
get_random_serialized_chunk(int num_batches, int num_records_per_batch) { // NOLINT
    ss::circular_buffer<model::record_batch> batches;
    model::offset o{0};
    for (int ix_batch = 0; ix_batch < num_batches; ix_batch++) {
        auto batch = model::test::make_random_batch(
          o, num_records_per_batch, false);
        o = model::next_offset(batch.last_offset());
        batches.push_back(std::move(batch));
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    auto fut = cloud_topics::details::serialize_in_memory_record_batch_reader(std::move(reader));
    return fut.get();
}

TEST(aggregator, single_request) {
    constexpr static auto timeout = 10s;
    auto chunk = get_random_serialized_chunk(10, 10);
    cloud_topics::details::write_request<ss::manual_clock> request(model::controller_ntp, cloud_topics::details::batcher_req_index(0), std::move(chunk), timeout);
    cloud_topics::details::write_request_list<ss::manual_clock> list;
    list.push_back(request);

    // The aggregator produces single L0 object
    cloud_topics::details::aggregator<ss::manual_clock> aggregator;

    aggregator.add(list);
    auto in_stream = aggregator.prepare();
    // iobuf dest;
    // auto out_stream = make_iobuf_ref_output_stream(dest);
    // ss::copy(in_stream, out_stream).get();
}