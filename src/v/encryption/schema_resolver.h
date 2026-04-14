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

#pragma once

#include "encryption/field_transformer.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <optional>
#include <vector>

namespace encryption {

/// An encryption rule describes which tag to match and which KEK to use for
/// encryption. Rules are evaluated in order; the first matching rule for a
/// given tag wins.
struct encryption_rule {
    ss::sstring tag;
    ss::sstring kek_name;
};

/// A pre-configured description of which fields carry which tags for a topic's
/// value schema. This is the input that drives resolution: for each field,
/// the resolver matches its tag against the ordered rule list to determine
/// the KEK.
struct field_tag_mapping {
    std::vector<ss::sstring> path;
    ss::sstring tag;
};

/// Configuration for a topic's encryption: the ordered rule list and the
/// field-to-tag mappings extracted from the schema.
struct topic_encryption_config {
    schema_format format{schema_format::avro};
    schema_handle handle;
    std::vector<encryption_rule> rules;
    std::vector<field_tag_mapping> field_tags;
};

/// Resolves encryption schemas for topics.
///
/// Since the schema registry does not yet support ruleSet/domainRules, this
/// implementation accepts pre-configured rules at construction or via
/// register_rules(). Once full schema registry integration is available, the
/// resolve() method can be extended to fetch rules from the registry.
class schema_resolver {
public:
    schema_resolver() = default;

    /// Register encryption configuration for a topic. This is the test-friendly
    /// API; production code will eventually derive this from the schema
    /// registry's ruleSet.
    void register_rules(model::topic topic, topic_encryption_config config);

    /// Resolve encryption schema for a topic. Returns nullopt if no encryption
    /// rules exist for the topic.
    ss::future<std::optional<encryption_schema>>
    resolve(model::topic topic) const;

    /// Check if any encryption rules exist for a topic (synchronous cache
    /// check).
    bool has_encryption_rules_cached(const model::topic& topic) const;

private:
    /// Resolved cache: topic -> encryption_schema. Populated on first
    /// resolve() call for each topic.
    chunked_hash_map<model::topic, encryption_schema> _cache;

    /// Pre-configured rules per topic.
    chunked_hash_map<model::topic, topic_encryption_config> _configs;

    /// Build an encryption_schema from a topic_encryption_config by matching
    /// field tags against the ordered rule list.
    static std::optional<encryption_schema>
    build_schema(const topic_encryption_config& config);
};

} // namespace encryption
