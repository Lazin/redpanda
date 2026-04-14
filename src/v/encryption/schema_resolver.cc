/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/schema_resolver.h"

#include <seastar/core/coroutine.hh>

namespace encryption {

void schema_resolver::register_rules(
  model::topic topic, topic_encryption_config config) {
    // Invalidate any cached result for this topic when rules change.
    _cache.erase(topic);
    _configs.insert_or_assign(std::move(topic), std::move(config));
}

ss::future<std::optional<encryption_schema>>
schema_resolver::resolve(model::topic topic) const {
    // Check the resolved cache first.
    if (auto it = _cache.find(topic); it != _cache.end()) {
        auto& cached = it->second;
        co_return encryption_schema{
          .format = cached.format,
          .handle = cached.handle,
          .tagged_fields = cached.tagged_fields,
        };
    }

    // Look up pre-configured rules.
    auto cfg_it = _configs.find(topic);
    if (cfg_it == _configs.end()) {
        co_return std::nullopt;
    }

    auto result = build_schema(cfg_it->second);
    if (!result.has_value()) {
        co_return std::nullopt;
    }

    // Cache the resolved schema. const_cast is safe here because we are
    // populating a lazy cache that does not change observable state.
    auto& mutable_cache
      = const_cast<chunked_hash_map<model::topic, encryption_schema>&>(_cache);
    mutable_cache.emplace(
      std::move(topic),
      encryption_schema{
        .format = result->format,
        .handle = result->handle,
        .tagged_fields = result->tagged_fields,
      });
    co_return std::move(result);
}

bool schema_resolver::has_encryption_rules_cached(
  const model::topic& topic) const {
    return _cache.contains(topic) || _configs.contains(topic);
}

std::optional<encryption_schema>
schema_resolver::build_schema(const topic_encryption_config& config) {
    if (config.rules.empty() || config.field_tags.empty()) {
        return std::nullopt;
    }

    std::vector<tagged_field> tagged_fields;
    tagged_fields.reserve(config.field_tags.size());

    for (const auto& field : config.field_tags) {
        // Find the first rule whose tag matches this field's tag.
        for (const auto& rule : config.rules) {
            if (rule.tag == field.tag) {
                tagged_fields.push_back(tagged_field{
                  .path = field.path,
                  .tag = field.tag,
                  .kek_name = rule.kek_name,
                });
                break;
            }
        }
    }

    if (tagged_fields.empty()) {
        return std::nullopt;
    }

    return encryption_schema{
      .format = config.format,
      .handle = config.handle,
      .tagged_fields = std::move(tagged_fields),
    };
}

} // namespace encryption
