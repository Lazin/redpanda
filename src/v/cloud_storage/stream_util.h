/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/iostream.hh>

#include <absl/container/btree_map.h>

namespace cloud_storage {

class stream_util final {
public:
    struct stream_stats {
        model::offset min_offset;
        model::offset max_offset;
        uint64_t size_bytes{};
    };

    /// Copy source stream into the destination stream
    ///
    /// Return stream stats (min/max offset and size)
    static ss::future<stream_stats> copy_stream(
      ss::input_stream<char> src,
      ss::output_stream<char> dst,
      retry_chain_node& fib);
};

} // namespace cloud_storage
