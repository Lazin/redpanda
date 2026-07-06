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

#include "model/metadata.h"

#include <gtest/gtest.h>

using model::cloud_storage_default_mode;
using model::redpanda_storage_mode;

// Full matrix: user input string x cloud_storage_default_mode -> parsed enum
// (nullopt = rejected). Only local/tiered/cloud/unset are valid mode values;
// the variant names and the internal 'tiered_cloud' spelling are rejected --
// a variant is selected with redpanda.storage.mode.version instead.
TEST(storage_mode_alias, from_user_string_matrix) {
    struct {
        std::string_view input;
        std::optional<redpanda_storage_mode> under_v1;
        std::optional<redpanda_storage_mode> under_v2;
    } cases[] = {
      {"local", redpanda_storage_mode::local, redpanda_storage_mode::local},
      {"tiered",
       redpanda_storage_mode::tiered,
       redpanda_storage_mode::tiered_cloud},
      {"cloud", redpanda_storage_mode::cloud, redpanda_storage_mode::cloud},
      {"unset", redpanda_storage_mode::unset, redpanda_storage_mode::unset},
      {"tiered_v1", std::nullopt, std::nullopt},
      {"tiered_v2", std::nullopt, std::nullopt},
      {"tiered_cloud", std::nullopt, std::nullopt},
      {"bogus", std::nullopt, std::nullopt},
    };
    for (const auto& c : cases) {
        EXPECT_EQ(
          model::redpanda_storage_mode_from_user_string(
            c.input, cloud_storage_default_mode::tiered_v1),
          c.under_v1)
          << c.input << " under tiered_v1";
        EXPECT_EQ(
          model::redpanda_storage_mode_from_user_string(
            c.input, cloud_storage_default_mode::tiered_v2),
          c.under_v2)
          << c.input << " under tiered_v2";
    }
}

// Both tiered variants display as 'tiered'; the variant is exposed through
// redpanda.storage.mode.version (storage_mode_version).
TEST(storage_mode_alias, user_name) {
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::tiered),
      "tiered");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(
        redpanda_storage_mode::tiered_cloud),
      "tiered");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::local),
      "local");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::cloud),
      "cloud");
    EXPECT_STREQ(
      model::redpanda_storage_mode_user_name(redpanda_storage_mode::unset),
      "unset");
}

TEST(storage_mode_alias, storage_mode_version) {
    EXPECT_EQ(
      model::storage_mode_version(redpanda_storage_mode::tiered),
      cloud_storage_default_mode::tiered_v1);
    EXPECT_EQ(
      model::storage_mode_version(redpanda_storage_mode::tiered_cloud),
      cloud_storage_default_mode::tiered_v2);
    EXPECT_EQ(
      model::storage_mode_version(redpanda_storage_mode::local), std::nullopt);
    EXPECT_EQ(
      model::storage_mode_version(redpanda_storage_mode::cloud), std::nullopt);
    EXPECT_EQ(
      model::storage_mode_version(redpanda_storage_mode::unset), std::nullopt);
}

TEST(storage_mode_alias, storage_mode_with_version) {
    EXPECT_EQ(
      model::storage_mode_with_version(cloud_storage_default_mode::tiered_v1),
      redpanda_storage_mode::tiered);
    EXPECT_EQ(
      model::storage_mode_with_version(cloud_storage_default_mode::tiered_v2),
      redpanda_storage_mode::tiered_cloud);
}

// The context-free parser keeps the static aliases and the internal spelling
// (used for cluster-config round-trips and the shadow-link sync fallback).
TEST(storage_mode_alias, from_string_static_aliases) {
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_v1"),
      redpanda_storage_mode::tiered);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_v2"),
      redpanda_storage_mode::tiered_cloud);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered_cloud"),
      redpanda_storage_mode::tiered_cloud);
    EXPECT_EQ(
      model::redpanda_storage_mode_from_string("tiered"),
      redpanda_storage_mode::tiered);
}

TEST(storage_mode_alias, cloud_storage_default_mode_round_trip) {
    for (auto m :
         {cloud_storage_default_mode::tiered_v1,
          cloud_storage_default_mode::tiered_v2}) {
        EXPECT_EQ(
          model::cloud_storage_default_mode_from_string(
            model::cloud_storage_default_mode_to_string(m)),
          m);
    }
    EXPECT_EQ(
      model::cloud_storage_default_mode_from_string("tiered"), std::nullopt);
}
