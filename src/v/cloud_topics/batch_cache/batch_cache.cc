/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/batch_cache.h"

#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"

namespace experimental::cloud_topics {

batch_cache::batch_cache(storage::log_manager* log_manager)
  : _lm(log_manager) {}

void batch_cache::put(const model::ntp& ntp, const model::record_batch& b) {
    if (_lm == nullptr) {
        return;
    }
    auto it = _index.find(ntp);
    if (it == _index.end()) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        auto [new_it, ok] = _index.insert(std::make_pair(
          ntp,
          std::make_unique<storage::batch_cache_index>(std::move(*cache_ix))));
        if (ok) {
            it = new_it;
        } else {
            return;
        }
    }
    it->second->put(b, storage::batch_cache::is_dirty_entry::no);
}

std::optional<model::record_batch>
batch_cache::get(const model::ntp& ntp, model::offset o) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    if (auto it = _index.find(ntp); it != _index.end()) {
        return it->second->get(o);
    }
    return std::nullopt;
}

} // namespace experimental::cloud_topics
