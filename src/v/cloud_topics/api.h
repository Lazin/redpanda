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

#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <memory>

namespace experimental::cloud_topics {

class api {
public:
    api() = default;

    api(const api&) = delete;
    api& operator=(const api&) = delete;
    api(api&&) noexcept = delete;
    api& operator=(api&&) noexcept = delete;
    virtual ~api() = default;
};

} // namespace experimental::cloud_topics
