/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"

#include <seastar/core/rwlock.hh>

namespace experimental::cloud_topics {

struct [[nodiscard]] cluster_epoch_fence {
    // Units protecting the epoch state.
    // If it's set to nullopt the batch has to be discarded
    // because of the out of order epoch.
    std::optional<ss::rwlock::holder> unit;
    // Term in which the batch is replicated.
    model::term_id term;
};
} // namespace experimental::cloud_topics
