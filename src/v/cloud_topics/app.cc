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
#include "cloud_topics/app.h"

#include <memory>

namespace experimental::cloud_topics {

app::app(ss::shared_ptr<api> ptr)
  : _impl(std::move(ptr)) {}

seastar::future<> app::start() { return _impl->start(); }

seastar::future<> app::stop() { return _impl->stop(); }

ss::future<result<model::record_batch_reader>> app::write_and_debounce(
  model::ntp ntp,
  model::record_batch_reader r,
  std::chrono::milliseconds timeout) {
    return _impl->write_and_debounce(std::move(ntp), std::move(r), timeout);
}

ss::future<result<reader_with_tx>> app::make_reader(
  model::ntp ntp,
  storage::log_reader_config cfg,
  std::chrono::milliseconds timeout) {
    return _impl->make_reader(std::move(ntp), cfg, timeout);
}

} // namespace experimental::cloud_topics
