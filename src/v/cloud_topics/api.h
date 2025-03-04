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

#include "base/outcome.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>

namespace experimental::cloud_topics {

struct reader_with_tx {
    model::record_batch_reader reader;
    fragmented_vector<model::tx_range> tx;
};

class api {
public:
    api() = default;

    api(const api&) = delete;
    api& operator=(const api&) = delete;
    api(api&&) noexcept = delete;
    api& operator=(api&&) noexcept = delete;
    virtual ~api() = default;

    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;

    virtual ss::future<result<model::record_batch_reader>> write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout)
      = 0;

    virtual ss::future<result<reader_with_tx>> make_reader(
      model::ntp ntp,
      storage::log_reader_config cfg,
      std::chrono::milliseconds timeout) = 0;
};

} // namespace experimental::cloud_topics
