/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_version.h"
#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace ct = experimental::cloud_topics;

TEST(ctp_stm_epochs_test, add_first_epoch) {
    // Test adding the first epoch to an empty epochs container
    ct::ctp_stm_epochs epochs;
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50}));
    EXPECT_EQ(epochs.get_max_epoch(), ct::cluster_epoch{1});
}

TEST(ctp_stm_epochs_test, add_epoch_monotonic) {
    // Test adding multiple epochs in monotonically increasing order
    ct::ctp_stm_epochs epochs;
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50}));
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60}));
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{3}, kafka::offset{300}, model::offset{70}));
    EXPECT_EQ(epochs.get_max_epoch(), ct::cluster_epoch{3});
}

TEST(ctp_stm_epochs_test, add_epoch_non_monotonic_epoch) {
    // Test that adding epochs with non-monotonic epoch values fails
    ct::ctp_stm_epochs epochs;
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{100}, model::offset{50}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{200}, model::offset{60}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60}));
}

TEST(ctp_stm_epochs_test, add_epoch_non_monotonic_insync_offset) {
    // Test that adding epochs with non-monotonic insync offset values fails
    ct::ctp_stm_epochs epochs;
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{40}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{50}));
}

TEST(ctp_stm_epochs_test, add_epoch_non_monotonic_kafka_offset) {
    // Test that adding epochs with non-monotonic kafka offset values fails
    ct::ctp_stm_epochs epochs;
    EXPECT_TRUE(epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{90}, model::offset{60}));
    EXPECT_FALSE(epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{100}, model::offset{60}));
}

TEST(ctp_stm_epochs_test, get_max_epoch_empty) {
    // Test getting max epoch from an empty epochs container returns nullopt
    ct::ctp_stm_epochs epochs;
    EXPECT_EQ(epochs.get_max_epoch(), std::nullopt);
}

TEST(ctp_stm_epochs_test, find_first_unused_empty) {
    // Test finding first unused epoch in an empty epochs container returns
    // nullopt
    ct::ctp_stm_epochs epochs;
    EXPECT_EQ(epochs.find_first_epoch(kafka::offset{100}), std::nullopt);
}

TEST(ctp_stm_epochs_test, find_first_single_epoch) {
    // Test finding first epoch with a single epoch entry
    ct::ctp_stm_epochs epochs;
    epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50});

    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{100})->epoch, ct::cluster_epoch{1});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{150})->epoch, ct::cluster_epoch{1});
}

TEST(ctp_stm_epochs_test, find_first_unused_multiple_epochs) {
    // Test finding first unused epoch with multiple epoch entries
    ct::ctp_stm_epochs epochs;
    epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50});
    epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60});
    epochs.add_epoch(
      ct::cluster_epoch{3}, kafka::offset{300}, model::offset{70});

    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{100})->epoch, ct::cluster_epoch{1});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{150})->epoch, ct::cluster_epoch{1});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{200})->epoch, ct::cluster_epoch{2});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{250})->epoch, ct::cluster_epoch{2});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{300})->epoch, ct::cluster_epoch{3});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{400})->epoch, ct::cluster_epoch{3});
}

TEST(ctp_stm_epochs_test, truncate_to_log_offset) {
    // Test truncating epochs based on log offset
    ct::ctp_stm_epochs epochs;
    epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50});
    epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60});
    epochs.add_epoch(
      ct::cluster_epoch{3}, kafka::offset{300}, model::offset{70});

    epochs.truncate_to(model::offset{60});
    EXPECT_EQ(epochs.get_max_epoch(), ct::cluster_epoch{3});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{200})->epoch, ct::cluster_epoch{2});
}

TEST(ctp_stm_epochs_test, get_epochs_at) {
    // Test getting a snapshot of epochs at a specific log offset
    ct::ctp_stm_epochs epochs;
    epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50});
    epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60});
    epochs.add_epoch(
      ct::cluster_epoch{3}, kafka::offset{300}, model::offset{70});

    auto snapshot = epochs.get_epochs_at(model::offset{55});
    EXPECT_EQ(snapshot.get_max_epoch(), ct::cluster_epoch{1});
    EXPECT_EQ(
      snapshot.find_first_epoch(kafka::offset{100})->epoch,
      ct::cluster_epoch{1});
    EXPECT_EQ(
      snapshot.find_first_epoch(kafka::offset{200})->epoch,
      ct::cluster_epoch{1});

    snapshot = epochs.get_epochs_at(model::offset{65});
    EXPECT_EQ(snapshot.get_max_epoch(), ct::cluster_epoch{2});
    EXPECT_EQ(
      snapshot.find_first_epoch(kafka::offset{200})->epoch,
      ct::cluster_epoch{2});
    EXPECT_EQ(
      snapshot.find_first_epoch(kafka::offset{300})->epoch,
      ct::cluster_epoch{2});
}

TEST(ctp_stm_epochs_test, ctp_stm_epochs_idempotency) {
    // This test checks that adding epochs multiple times does not change the
    // state

    struct batch_element {
        model::offset base_offset;
        ct::cluster_epoch cluster_epoch;
    };

    std::vector<batch_element> log = {
      {model::offset{100}, ct::cluster_epoch{1}},
      {model::offset{101}, ct::cluster_epoch{1}},
      {model::offset{102}, ct::cluster_epoch{1}},
      {model::offset{103}, ct::cluster_epoch{2}},
      {model::offset{104}, ct::cluster_epoch{2}},
      {model::offset{105}, ct::cluster_epoch{2}},
      {model::offset{106}, ct::cluster_epoch{3}},
      {model::offset{107}, ct::cluster_epoch{3}},
      {model::offset{108}, ct::cluster_epoch{3}},
      {model::offset{109}, ct::cluster_epoch{4}}};

    ct::ctp_stm_epochs epochs;

    for (const auto& element : log) {
        // The goal here is to have idempotent behavior when applying the same
        // elements multiple times.

        // If current max_epoch is smaller than element epoch, invoke add_epoch
        auto current_max = epochs.get_max_epoch();
        if (!current_max || current_max.value() < element.cluster_epoch) {
            bool added = epochs.add_epoch(
              element.cluster_epoch,
              model::offset_cast(element.base_offset),
              element.base_offset);
            EXPECT_TRUE(added);
        }
        if (current_max && current_max.value() == element.cluster_epoch) {
            bool added = epochs.add_epoch(
              element.cluster_epoch,
              model::offset_cast(element.base_offset),
              element.base_offset);
            EXPECT_FALSE(added);
        }
    }

    EXPECT_EQ(epochs.get_max_epoch(), ct::cluster_epoch{4});

    // Second pass to check idempotency
    for (const auto& element : log) {
        auto current_max = epochs.get_max_epoch();
        if (!current_max || current_max.value() < element.cluster_epoch) {
            // This should not add any new epochs since they already exist
            bool added = epochs.add_epoch(
              element.cluster_epoch,
              model::offset_cast(element.base_offset),
              element.base_offset);

            EXPECT_FALSE(added);
        }
    }

    EXPECT_EQ(epochs.get_max_epoch(), ct::cluster_epoch{4});
}

TEST(ctp_stm_state_test, ctp_stm_state_idempotency) {
    // Same as previous test, but for ctp_stm_state.
    struct batch_element {
        model::offset base_offset;
        ct::cluster_epoch cluster_epoch;
        bool skip{false};
    };

    std::vector<batch_element> log = {
      {model::offset{195}, ct::cluster_epoch{1}},
      {model::offset{196}, ct::cluster_epoch{1}, true},
      {model::offset{197}, ct::cluster_epoch{1}},
      {model::offset{198}, ct::cluster_epoch{1}, true},
      {model::offset{199}, ct::cluster_epoch{1}},
      {model::offset{200}, ct::cluster_epoch{1}},
      {model::offset{201}, ct::cluster_epoch{1}},
      {model::offset{202}, ct::cluster_epoch{2}},
      {model::offset{203}, ct::cluster_epoch{2}},
      {model::offset{204}, ct::cluster_epoch{3}},
      // This simulates second application
      {model::offset{197}, ct::cluster_epoch{1}},
      {model::offset{198}, ct::cluster_epoch{1}, true},
      {model::offset{199}, ct::cluster_epoch{1}},
      {model::offset{200}, ct::cluster_epoch{1}},
    };

    ct::ctp_stm_state state;

    for (const auto& element : log) {
        // NOTE: this mimics the behavior of ctp_stm::do_apply method.
        // In this method we first advance insync offset, then check if the
        // current max epoch is smaller than element epoch, and if so, invoke
        // add_epoch, and finally advance applied offset. We're also checking
        // if the batch can be applied using 'can_apply' method.

        state.advance_insync_offset(element.base_offset);

        if (element.skip) {
            // Skip this element, simulating a scenario where the batch is not
            // applied because it has different type.
            continue;
        }

        if (!state.can_apply(element.base_offset)) {
            continue;
        }

        // If current max_epoch is smaller than element epoch, invoke add_epoch
        auto current_max = state.get_max_epoch();
        if (!current_max || current_max.value() < element.cluster_epoch) {
            state.advance_insync_offset(element.base_offset);
            bool added = state.add_epoch(
              element.cluster_epoch, model::offset_cast(element.base_offset));
            EXPECT_TRUE(added);
            state.advance_applied_offset();
        }

        // Then call advance_applied_offset
        state.advance_applied_offset();

        // Verify state consistency
        EXPECT_EQ(state.get_insync_offset(), element.base_offset);
        EXPECT_EQ(state.get_applied_offset(), element.base_offset);
    }

    EXPECT_EQ(state.get_max_epoch(), ct::cluster_epoch{3});
    EXPECT_EQ(state.get_insync_offset(), model::offset{204});
    EXPECT_EQ(state.get_applied_offset(), model::offset{204});
}

TEST(ctp_stm_epochs_test, find_first_epoch_multiple_scenarios) {
    // Test finding first epoch with multiple epochs and different
    // last_reconciled values
    ct::ctp_stm_epochs epochs;

    // Add multiple epochs with known kafka offsets
    epochs.add_epoch(
      ct::cluster_epoch{1}, kafka::offset{100}, model::offset{50});
    epochs.add_epoch(
      ct::cluster_epoch{2}, kafka::offset{200}, model::offset{60});
    epochs.add_epoch(
      ct::cluster_epoch{3}, kafka::offset{300}, model::offset{70});
    epochs.add_epoch(
      ct::cluster_epoch{4}, kafka::offset{400}, model::offset{80});
    epochs.add_epoch(
      ct::cluster_epoch{5}, kafka::offset{500}, model::offset{90});

    // Test with last_reconciled exactly at epoch boundaries
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{100})->epoch, ct::cluster_epoch{1});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{200})->epoch, ct::cluster_epoch{2});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{300})->epoch, ct::cluster_epoch{3});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{400})->epoch, ct::cluster_epoch{4});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{500})->epoch, ct::cluster_epoch{5});

    // Test with last_reconciled between epochs
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{150})->epoch, ct::cluster_epoch{1});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{250})->epoch, ct::cluster_epoch{2});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{350})->epoch, ct::cluster_epoch{3});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{450})->epoch, ct::cluster_epoch{4});

    // Test with last_reconciled after all epochs
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{600})->epoch, ct::cluster_epoch{5});
}

TEST(ctp_stm_epochs_test, find_first_epoch_edge_cases) {
    // Test finding first epoch with edge cases
    ct::ctp_stm_epochs epochs;

    // Add epochs with consecutive kafka offsets
    epochs.add_epoch(
      ct::cluster_epoch{10}, kafka::offset{1000}, model::offset{100});
    epochs.add_epoch(
      ct::cluster_epoch{11}, kafka::offset{1001}, model::offset{101});
    epochs.add_epoch(
      ct::cluster_epoch{12}, kafka::offset{1002}, model::offset{102});

    // Test with consecutive offsets
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{1000})->epoch,
      ct::cluster_epoch{11});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{1001})->epoch,
      ct::cluster_epoch{12});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{1002})->epoch,
      ct::cluster_epoch{12});
    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{1003})->epoch,
      ct::cluster_epoch{12});
}

TEST(ctp_stm_epochs_test, broken_invariant_assert) {
    // Test that the assertion is triggered when the wrong last reconciled
    // offset is passed to find_first_epoch.
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    ct::ctp_stm_epochs epochs;
    epochs.add_epoch(
      ct::cluster_epoch{10}, kafka::offset{1000}, model::offset{100});

    EXPECT_EQ(
      epochs.find_first_epoch(kafka::offset{999})->epoch,
      ct::cluster_epoch{10});

    ASSERT_DEATH(
      epochs.find_first_epoch(kafka::offset{998}),
      "'false' ctp_stm_epochs::find_first_epoch: first epoch has "
      "first_epoch_offset greater than LRO: 998, first_epoch_offset: 1000, "
      "epoch: 10");
}

TEST(ctp_stm_state_test, get_max_collectible_offset_empty) {
    // Test get_max_collectible_offset with empty state
    ct::ctp_stm_state state;
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset::max());
}

TEST(ctp_stm_state_test, get_max_collectible_offset_epochs_only) {
    // Test get_max_collectible_offset with epochs but no offsets (nothing is
    // reconciled)
    ct::ctp_stm_state state;

    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    // No last reconciled offset set, should return first epoch's changed_at
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset{99});
}

TEST(ctp_stm_state_test, get_max_collectible_offset_with_reconciled) {
    // Test get_max_collectible_offset with last reconciled offset
    ct::ctp_stm_state state;

    // Add epochs
    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{300});
    state.add_epoch(ct::cluster_epoch{3}, kafka::offset{250});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{400});
    state.advance_last_reconciled_offset(kafka::offset{175});
    state.advance_applied_offset();

    // Should return changed_at of epoch 2 (first epoch after last reconciled)
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset{199});
}

TEST(ctp_stm_state_test, get_max_collectible_offset_at_boundary) {
    // Test get_max_collectible_offset with last reconciled at epoch boundary
    ct::ctp_stm_state state;

    // Add epochs
    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{300});
    state.add_epoch(ct::cluster_epoch{3}, kafka::offset{250});
    state.advance_applied_offset();

    // Set last reconciled offset exactly at epoch 2 boundary
    state.advance_insync_offset(model::offset{400});
    state.advance_last_reconciled_offset(kafka::offset{150});
    state.advance_applied_offset();

    // Should return changed_at of epoch 2
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset{199});
}

TEST(ctp_stm_state_test, get_max_collectible_offset_beyond_epochs) {
    // Test get_max_collectible_offset with last reconciled offset in the
    // last epoch. We only store first kafka offset of the epoch so anything
    // that surpasses the last epoch belongs to the last epoch.
    ct::ctp_stm_state state;

    // Add epochs
    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    // Set last reconciled offset beyond all epochs
    state.advance_insync_offset(model::offset{300});
    state.advance_last_reconciled_offset(kafka::offset{300});
    state.advance_applied_offset();

    EXPECT_EQ(state.get_max_collectible_offset(), model::offset{199});
}

TEST(ctp_stm_state_test, get_max_collectible_offset_single_epoch) {
    // Test get_max_collectible_offset with single epoch
    ct::ctp_stm_state state;

    // Add single epoch
    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    // Set last reconciled offset
    state.advance_insync_offset(model::offset{200});
    state.advance_last_reconciled_offset(kafka::offset{75});
    state.advance_applied_offset();

    // We should retain the entire epoch in the local log until it's fully
    // reconciled. This is a great limitation of the current design.
    // The L0 metadata batch is evicted from the local log only if the
    // epoch that it references is fully reconciled.
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset{99});
}

TEST(ctp_stm_state_test, truncate_to_log_offset) {
    // Test truncating ctp_stm_state based on log offset
    ct::ctp_stm_state state;

    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{300});
    state.add_epoch(ct::cluster_epoch{3}, kafka::offset{250});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{400});
    state.advance_last_reconciled_offset(kafka::offset{175});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{500});
    state.advance_last_reconciled_offset(kafka::offset{275});
    state.advance_applied_offset();

    state.truncate_to(model::offset{250});

    EXPECT_EQ(state.get_max_epoch(), ct::cluster_epoch{3});
    EXPECT_EQ(state.find_first_epoch(), ct::cluster_epoch{3});
    EXPECT_EQ(state.get_last_reconciled_offset(), kafka::offset{275});
}

TEST(ctp_stm_state_test, truncate_to_empty_state) {
    // Test truncating ctp_stm_state with empty state. This will happen
    // on a regular basis when the state is empty and prefix_truncate
    // command is applied (nothing should happen really).
    ct::ctp_stm_state state;

    // Truncating empty state should not crash
    state.truncate_to(model::offset{100});

    EXPECT_EQ(state.get_max_epoch(), std::nullopt);
    EXPECT_EQ(state.get_last_reconciled_offset(), kafka::offset{0});
}

TEST(ctp_stm_state_test, truncate_to_removes_all_epochs) {
    // Test truncating ctp_stm_state that removes all epochs
    ct::ctp_stm_state state;

    // Add epochs at early offsets
    state.advance_insync_offset(model::offset{50});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{25});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{75});
    state.advance_applied_offset();

    // Truncate to a very high offset
    state.truncate_to(model::offset{1000});

    // All epochs should be removed
    EXPECT_EQ(state.get_max_epoch(), std::nullopt);
    EXPECT_EQ(state.find_first_epoch(), std::nullopt);
}

TEST(
  ctp_stm_state_test, truncate_to_max_collectible_preserves_last_reconciled) {
    // Test truncating to max collectible offset preserves the ability to
    // compute last reconciled offset.
    // With the ctp_stm and the max_collectible offset we shouldn't actually
    // truncate all epochs. We will always leave the last epoch in the state
    // because of the way max_collectible offset is computed. We will also leave
    // at least one last reconciled offset in the state for the same reason.
    ct::ctp_stm_state state;

    // Add multiple epochs and follow with the truncations limited by the max
    // collectible offset.
    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{300});
    state.add_epoch(ct::cluster_epoch{3}, kafka::offset{250});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{400});
    state.add_epoch(ct::cluster_epoch{4}, kafka::offset{350});
    state.advance_applied_offset();

    // Advance last reconciled offset to last offset of epoch 1
    state.advance_insync_offset(model::offset{500});
    state.advance_last_reconciled_offset(
      kafka::offset{149}); // epoch 2 starts at 150, so 149 is last of epoch 1
    state.advance_applied_offset();

    // Advance last reconciled offset to last offset of epoch 2
    state.advance_insync_offset(model::offset{600});
    state.advance_last_reconciled_offset(
      kafka::offset{249}); // epoch 3 starts at 250, so 249 is last of epoch 2
    state.advance_applied_offset();

    // Advance last reconciled offset to last offset of epoch 3
    state.advance_insync_offset(model::offset{700});
    state.advance_last_reconciled_offset(
      kafka::offset{349}); // epoch 4 starts at 350, so 349 is last of epoch 3
    state.advance_applied_offset();

    // Get max collectible offset and verify it's as expected
    auto max_collectible = state.get_max_collectible_offset();
    EXPECT_EQ(max_collectible, model::offset{399}); // changed_at of epoch 4

    // Store last reconciled offset before truncation
    auto last_reconciled_before = state.get_last_reconciled_offset();
    EXPECT_EQ(last_reconciled_before, kafka::offset{349});

    // Truncate to max collectible offset
    state.truncate_to(max_collectible);

    // Verify that we can still compute last reconciled offset
    auto last_reconciled_after = state.get_last_reconciled_offset();
    EXPECT_EQ(last_reconciled_before, last_reconciled_after);

    // Verify that epochs before max collectible are removed but epoch 4 remains
    EXPECT_EQ(state.get_max_epoch(), ct::cluster_epoch{4});
    EXPECT_EQ(state.find_first_epoch(), ct::cluster_epoch{4});

    // Verify max collectible offset is still computable
    auto new_max_collectible = state.get_max_collectible_offset();
    EXPECT_EQ(new_max_collectible, model::offset{399});
}

TEST(
  ctp_stm_state_test, truncate_to_max_collectible_preserves_last_reconciled2) {
    ct::ctp_stm_state state;

    state.advance_insync_offset(model::offset{100});
    state.add_epoch(ct::cluster_epoch{1}, kafka::offset{50});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{200});
    state.add_epoch(ct::cluster_epoch{2}, kafka::offset{150});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{300});
    state.add_epoch(ct::cluster_epoch{3}, kafka::offset{250});
    state.advance_applied_offset();

    state.advance_insync_offset(model::offset{400});
    state.add_epoch(ct::cluster_epoch{4}, kafka::offset{350});
    state.advance_applied_offset();

    // Reconcile below the max_collectible_offset
    state.advance_insync_offset(model::offset{700});
    state.advance_last_reconciled_offset(
      kafka::offset{349}); // last offset of epoch 3
    state.advance_applied_offset();

    // Now reconcile above the max_collectible_offset
    state.advance_insync_offset(model::offset{800});
    state.advance_last_reconciled_offset(
      kafka::offset{600}); // middle of epoch 4
    state.advance_applied_offset();

    // This is still set to 400 because the epoch 4 is not fully
    // reconciled yet. It can only be fully reconciled when epoch 5
    // is added. At this point the boundary of epoch 4 is determined.
    auto max_collectible = state.get_max_collectible_offset();
    EXPECT_EQ(max_collectible, model::offset{399});

    // Store last reconciled offset before truncation
    auto last_reconciled_before = state.get_last_reconciled_offset();
    EXPECT_EQ(last_reconciled_before, kafka::offset{600});

    // Truncate to max collectible offset
    state.truncate_to(max_collectible);

    // Verify that we can still compute last reconciled offset
    auto last_reconciled_after = state.get_last_reconciled_offset();
    EXPECT_EQ(last_reconciled_before, last_reconciled_after);
    EXPECT_EQ(state.get_max_epoch(), ct::cluster_epoch{4});
    EXPECT_EQ(state.find_first_epoch(), ct::cluster_epoch{4});
    auto new_max_collectible = state.get_max_collectible_offset();
    EXPECT_EQ(new_max_collectible, model::offset{399});

    // close epoch 4 by adding epoch 5, now the max_collectible offset
    // should move forward.
    state.advance_insync_offset(model::offset{900});
    state.add_epoch(ct::cluster_epoch{5}, kafka::offset{550});
    state.advance_applied_offset();

    auto final_max_collectible = state.get_max_collectible_offset();
    EXPECT_EQ(final_max_collectible, model::offset{899});
}
