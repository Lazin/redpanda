/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cloud_topics/api.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace experimental::cloud_topics {

class app {
public:
    explicit app(ss::shared_ptr<api>);

    app(const app&) = delete;
    app& operator=(const app&) = delete;
    app(app&&) noexcept = delete;
    app& operator=(app&&) noexcept = delete;

    seastar::future<> start();
    seastar::future<> stop();

    ss::future<result<model::record_batch_reader>> write_and_debounce(
      model::ntp ntp,
      model::record_batch_reader r,
      std::chrono::milliseconds timeout);

    ss::future<result<reader_with_tx>> make_reader(
      model::ntp ntp,
      storage::log_reader_config cfg,
      std::chrono::milliseconds timeout);

private:
    ss::shared_ptr<api> _impl;
};

} // namespace experimental::cloud_topics
