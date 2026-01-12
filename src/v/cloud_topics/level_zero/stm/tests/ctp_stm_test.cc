/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "ssx/when_all.h"
#include "test_utils/async.h"

#include <optional>

namespace ct = cloud_topics;
using namespace std::chrono_literals;

struct ctp_stm_api_accessor {
    ss::future<std::expected<model::offset, ct::ctp_stm_api_errc>>
    replicated_apply(model::record_batch rb, ss::abort_source& as) {
        // This function is used to access the private method of ctp_stm_api
        return api.replicated_apply(std::move(rb), model::no_timeout, as);
    }
    ct::ctp_stm_api api;
};

namespace cloud_topics {
struct ctp_stm_accessor {
    auto take_snapshot(ctp_stm& stm) { return stm.take_local_snapshot({}); }

    auto install_snapshot(ctp_stm& stm, raft::stm_snapshot snapshot) {
        return stm.apply_local_snapshot(
          snapshot.header, std::move(snapshot.data));
    }

    bool epoch_cv_has_waiters(ctp_stm& stm) {
        return stm._epoch_updated_cv.has_waiters();
    }

    auto get_main_state_min_epoch(const ctp_stm& stm) {
        return stm._mn_state.estimate_min_epoch();
    }

    auto get_oo_state_min_epoch(const ctp_stm& stm) {
        return stm._oo_state.estimate_min_epoch();
    }

    auto get_main_state_max_epoch(const ctp_stm& stm) {
        return stm._mn_state.get_max_epoch();
    }

    auto get_oo_state_max_epoch(const ctp_stm& stm) {
        return stm._oo_state.get_max_epoch();
    }
};
} // namespace cloud_topics

class ctp_stm_fixture : public raft::stm_raft_fixture<ct::ctp_stm> {
public:
    ss::future<> start() {
        enable_offset_translation();
        co_await initialize_state_machines();
    }

    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<ct::ctp_stm>(ct::cd_log, node.raft().get());
    }

    ct::ctp_stm_api api(raft::raft_node_instance& node) {
        return ct::ctp_stm_api(get_stm<0>(node));
    }

    model::record_batch make_record_batch(
      ct::cluster_epoch e,
      model::offset base_offset,
      int32_t seq,
      std::optional<int> size = std::nullopt) {
        ct::object_id id = ct::object_id::create(e);
        ct::ctp_placeholder placeholder{
          .id = id,
          .offset = ct::first_byte_offset_t{0},
          .size_bytes = ct::byte_range_size_t{0},
        };

        storage::record_batch_builder builder(
          model::record_batch_type::ctp_placeholder, base_offset);

        auto first_value = serde::to_iobuf(placeholder);

        builder.add_raw_kv(std::nullopt, std::move(first_value));
        if (size.has_value()) {
            for (int i = 1; i < size.value(); ++i) {
                builder.add_raw_kv(std::nullopt, std::nullopt);
            }
        }

        auto ph = std::move(builder).build();
        ph.header().first_timestamp = model::timestamp::now();
        ph.header().max_timestamp = model::timestamp::now();
        ph.header().base_sequence = seq;
        ph.header().header_crc = model::internal_header_only_crc(ph.header());
        return ph;
    }

    ss::future<std::expected<model::offset, ct::ctp_stm_api_errc>>
    replicate_record_batch(
      raft::raft_node_instance& node, model::record_batch rb) {
        ctp_stm_api_accessor accessor{.api = api(node)};
        co_return co_await accessor.replicated_apply(std::move(rb), as);
    }

    ss::abort_source as;
};

TEST_F_CORO(ctp_stm_fixture, test_basic) {
    // Test replicates L0 metadata batch and checks that the epoch is updated
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch.value().has_value());

    auto b = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{1});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{1});

    gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_TRUE_CORO(gc_epoch.value().has_value());
    ASSERT_EQ_CORO(gc_epoch.value().value(), ct::cluster_epoch{0});
}

TEST_F_CORO(ctp_stm_fixture, test_fencing) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{2}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    // Acquire the fence for epoch 2 (should succeed)
    {
        auto fence
          = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{2});
        ASSERT_TRUE_CORO(fence.has_value());
    }

    // Acquire the fence for epoch 1 (should fail - it's < the first replicated
    // epoch which is 2)
    {
        auto fence
          = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{1});
        ASSERT_FALSE_CORO(fence.has_value());
    }

    // Advance max_seen_epoch to 3 by fencing it (without replicating epoch 3)
    auto write_fence
      = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{3});
    ASSERT_TRUE_CORO(write_fence.has_value());

    // Out of order fence for epoch 2 (should fail - once we've fenced epoch 3,
    // we can't go back and fence epoch 2 even though it was replicated)
    auto leader_api = api(node(*get_leader()));
    auto fut = leader_api.fence_epoch(ct::cluster_epoch{2});
    co_await ss::sleep(100ms);

    write_fence = {};

    auto read_fence = co_await std::move(fut);
    // The fence succeeds because fencing doesn't change min_epoch, only
    // reconciliation does Since no reconciliation has happened, min_epoch is
    // still nullopt or 0, so epoch 2 can still be fenced
    ASSERT_TRUE_CORO(read_fence.has_value());
}

TEST_F_CORO(ctp_stm_fixture, test_last_reconciled_offset) {
    // This test checks reconciliation in the ctp_stm in case if
    // epoch spans a single offset.
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res1 = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res1.has_value());

    auto b2 = make_record_batch(ct::cluster_epoch{2}, model::offset{1}, 1);
    auto res2 = co_await replicate_record_batch(
      node(*get_leader()), std::move(b2));
    ASSERT_TRUE_CORO(res2.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    auto gc_epoch_before
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_before);
    ASSERT_TRUE_CORO(gc_epoch_before->has_value());
    ASSERT_EQ_CORO(gc_epoch_before->value(), ct::cluster_epoch{0});

    // Advance reconciled offset to the first batch (b1),
    // now b1 is reconciled and can be removed alongside its epoch (1).
    // First referenced epoch is now 2.
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset{0}, model::no_timeout, as);

    // Check that max and max_seen_epochs remain the same
    auto max_epoch_after = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch_after = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch_after.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch_after.has_value());
    ASSERT_EQ_CORO(max_epoch_after.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch_after.value(), ct::cluster_epoch{2});

    // Check that first epoch to remove has moved forward
    auto gc_epoch_after
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_after.has_value());
    ASSERT_TRUE_CORO(gc_epoch_after->has_value());
    ASSERT_EQ_CORO(gc_epoch_after->value(), ct::cluster_epoch{1});

    // Advance reconciled offset to the b2 batch.
    // Now all epochs can be discarded.
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset{1}, model::no_timeout, as);

    max_epoch_after = api(node(*get_leader())).get_max_epoch();
    max_seen_epoch_after = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch_after.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch_after.has_value());

    // We know that b2 started epoch 2 but we don't yet know where it ends
    gc_epoch_after = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_after.has_value());
    ASSERT_FALSE_CORO(gc_epoch_after->has_value());
}

TEST_F_CORO(ctp_stm_fixture, test_truncate_all_epochs) {
    // This test gradually adds epochs and removes them by advancing the
    // reconciled offset. It checks that the epochs are removed correctly and
    // that the state is consistent. Then it adds more epochs and checks that
    // the state is still consistent.
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    model::offset last_offset = model::offset{0};
    ct::cluster_epoch last_epoch = ct::cluster_epoch{0};
    for (int i = 0; i < 100; i += 10) {
        last_offset = model::offset(i);
        last_epoch = ct::cluster_epoch(i / 2);
        auto b = make_record_batch(last_epoch, last_offset, i, 10);
        auto res = co_await replicate_record_batch(
          node(*get_leader()), std::move(b));
        ASSERT_TRUE_CORO(res.has_value());
    }

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), last_epoch);
    ASSERT_EQ_CORO(max_seen_epoch.value(), last_epoch);
    // Nothing yet reconciled
    auto inactive_epoch
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_FALSE_CORO(inactive_epoch->has_value());

    // Advance reconciled offset to the middle of the first epoch
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset(50), model::no_timeout, as);
    ss::abort_source as;
    co_await api(node(*get_leader())).sync_in_term(model::no_timeout, as);
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(inactive_epoch->value(), ct::cluster_epoch{24});

    // Advance reconciled offset exactly to the end of the first epoch
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset(99), model::no_timeout, as);
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    max_epoch = api(node(*get_leader())).get_max_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(max_epoch);
    ASSERT_FALSE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(max_epoch.value(), last_epoch);
}

TEST_F_CORO(ctp_stm_fixture, test_start_offset) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(*get_leader());
    auto leader_api = api(leader);
    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res1 = co_await replicate_record_batch(leader, std::move(b1));
    ASSERT_TRUE_CORO(res1.has_value());
    auto b2 = make_record_batch(ct::cluster_epoch{2}, model::offset{1}, 1);
    auto res2 = co_await replicate_record_batch(leader, std::move(b2));
    ASSERT_TRUE_CORO(res2.has_value());

    auto start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{0});

    co_await leader_api.set_start_offset(
      kafka::offset{1}, model::no_timeout, as);

    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{1});

    co_await leader_api.set_start_offset(
      kafka::offset{2}, model::no_timeout, as);
    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{2});

    co_await leader_api.set_start_offset(
      kafka::offset{1}, model::no_timeout, as);
    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{2});
}

TEST_F_CORO(ctp_stm_fixture, truncates_below_lro) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(*get_leader());
    EXPECT_EQ(leader.raft()->last_snapshot_index(), model::offset::min());
    auto leader_api = api(leader);
    // Write some data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Wait for all nodes to replicate up to offset 1023 before rolling.
    co_await wait_for_committed_offset(model::offset{1023}, 10s);
    // Segment roll on all the nodes so we can take a snapshot.
    for (auto& vnode : all_vnodes()) {
        co_await node(vnode.id()).raft()->log()->force_roll();
    }
    // Write some more data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Advance the LRO
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{2000}, model::no_timeout, as);
    // Wait for the snapshot to be created
    for (auto& vnode : all_vnodes()) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &vnode]() {
            return node(vnode.id()).raft()->last_snapshot_index()
                   == model::offset{1024};
        });
    }
}

TEST_F_CORO(ctp_stm_fixture, can_replay_truncated_log) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(get_leader().value());
    EXPECT_EQ(leader.raft()->last_snapshot_index(), model::offset::min());
    auto leader_api = api(leader);
    // Write some data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Wait for all nodes to replicate up to offset 1023 before rolling.
    co_await wait_for_committed_offset(model::offset{1023}, 10s);
    // Segment roll on all the nodes so we can take a snapshot.
    for (auto& vnode : all_vnodes()) {
        co_await node(vnode.id()).raft()->log()->force_roll();
    }
    // Write some more data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Advance the LRO to a low value that will be truncated away
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{1}, model::no_timeout, as);
    // Advance the LRO to truncate what the previous batch pointed too
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{2000}, model::no_timeout, as);
    // Wait for the snapshot to be created
    for (auto& vnode : all_vnodes()) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &vnode]() {
            return node(vnode.id()).raft()->last_snapshot_index()
                   == model::offset{1024};
        });
    }
    auto follower_id = random_follower_id().value();
    vlog(ct::cd_log.info, "restarting node {}", follower_id);
    auto dirty_offset = leader.raft()->dirty_offset();
    co_await restart_node_and_delete_data(follower_id);
    co_await wait_for_committed_offset(dirty_offset, 10s);
    auto follower_stm = get_stm<0>(node(follower_id));
    co_await follower_stm->wait(dirty_offset, model::no_timeout);
    vlog(ct::cd_log.info, "recovery done: {}", follower_id);
}

TEST_F_CORO(ctp_stm_fixture, test_snapshot) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{2}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    auto& leader = node(*get_leader());
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor a;
    auto snapshot = co_await a.take_snapshot(*stm);

    co_await a.install_snapshot(*stm, std::move(snapshot));

    // Acquire the fence for epoch 1 (should fail)
    {
        auto fence = co_await api(leader).fence_epoch(ct::cluster_epoch{1});
        ASSERT_FALSE_CORO(fence.has_value());
    }
}

TEST_F_CORO(ctp_stm_fixture, test_fence_epoch_concurrent_new_epoch) {
    // This test verifies the optimization in fence_epoch() where multiple
    // concurrent requests for a new epoch only require one write lock.
    // The first request acquires the epoch_update_lock and write lock, updates
    // the epoch, then signals waiters. The remaining requests wake up and take
    // the read-lock path since the epoch has been updated.
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto& leader = node(*get_leader());
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor accessor;

    // First, establish epoch 1 by replicating a batch
    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(leader, std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    // Verify epoch 1 is established
    auto max_epoch = api(leader).get_max_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{1});

    // Launch multiple concurrent fence_epoch calls for epoch 2 (a new epoch).
    // All of these will initially see max_seen_epoch=1 and need to bump to 2.
    // With the optimization:
    // - One request acquires _epoch_update_lock, gets write lock, updates epoch
    // - Others wait on condition variable, then take read-lock path
    constexpr size_t num_concurrent_requests = 10;
    using expected_t
      = std::expected<ct::cluster_epoch_fence, ct::stale_cluster_epoch>;
    std::vector<ss::future<expected_t>> futures;
    futures.reserve(num_concurrent_requests);

    {
        auto leader_api = api(leader);
        // Make a single request which fences the current epoch (thus holding a
        // read lock)
        auto initial_fence = co_await leader_api.fence_epoch(
          ct::cluster_epoch{1});
        ASSERT_TRUE_CORO(initial_fence.has_value());
        // Push back a number of futures which will not yet be able to be
        // resolved since a read lock is outstanding, forcing a number of
        // requests to become waiters while a single request waits for a
        // write lock- previously, this would have resulted in all requests
        // waiting on a write lock sequentially.
        for (size_t i = 0; i < num_concurrent_requests; ++i) {
            futures.push_back(leader_api.fence_epoch(ct::cluster_epoch{2}));
        }
        // All requests but one should be waiting on cv.
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [&] { return accessor.epoch_cv_has_waiters(*stm); });
        // Let `initial_fence` go out of scope.
    }

    // Wait for all fences to be acquired - they should all be able to succeed
    // without hanging because only one request (the first request) had to
    // obtain a write lock, which then downgraded to a read lock, and the rest
    // of the requests could obtain read locks for the current epoch.
    auto fences = co_await ssx::when_all_succeed<std::vector<expected_t>>(
      std::move(futures));
    for (auto& fence : fences) {
        ASSERT_TRUE_CORO(fence.has_value())
          << "All fence requests should succeed";
        ASSERT_EQ_CORO(fence->unit.count(), 1);
    }

    ASSERT_FALSE_CORO(accessor.epoch_cv_has_waiters(*stm));

    // Verify epoch 2 is now established
    auto max_seen = api(leader).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_seen.has_value());
    ASSERT_EQ_CORO(max_seen.value(), ct::cluster_epoch{2});
}

TEST_F_CORO(ctp_stm_fixture, test_out_of_order_epoch_handling) {
    // This test checks that out-of-order epochs are handled correctly:
    // 1. Out-of-order batches are routed to _oo_state
    // 2. Out-of-order epochs can be fenced if they're >= oo_state min_epoch
    // 3. Out-of-order epochs are rejected if they're < oo_state min_epoch
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto& leader = node(*get_leader());
    auto leader_api = api(leader);
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor accessor;

    // Step 1: Apply in-order batches with epochs 5, 10, 15 to establish main
    // state
    auto b5 = make_record_batch(ct::cluster_epoch{5}, model::offset{0}, 0);
    auto res5 = co_await replicate_record_batch(leader, std::move(b5));
    ASSERT_TRUE_CORO(res5.has_value());

    auto b10 = make_record_batch(ct::cluster_epoch{10}, model::offset{1}, 1);
    auto res10 = co_await replicate_record_batch(leader, std::move(b10));
    ASSERT_TRUE_CORO(res10.has_value());

    auto b15 = make_record_batch(ct::cluster_epoch{15}, model::offset{2}, 2);
    auto res15 = co_await replicate_record_batch(leader, std::move(b15));
    ASSERT_TRUE_CORO(res15.has_value());

    // Verify main state has max epoch 15
    auto main_max = accessor.get_main_state_max_epoch(*stm);
    ASSERT_TRUE_CORO(main_max.has_value());
    ASSERT_EQ_CORO(main_max.value(), ct::cluster_epoch{15});

    // Verify oo_state has no epochs yet
    auto oo_max = accessor.get_oo_state_max_epoch(*stm);
    ASSERT_FALSE_CORO(oo_max.has_value());

    // Step 2: Apply out-of-order batch with epoch 8 (< main_max)
    // This should be routed to oo_state
    auto b8 = make_record_batch(ct::cluster_epoch{8}, model::offset{3}, 3);
    auto res8 = co_await replicate_record_batch(leader, std::move(b8));
    ASSERT_TRUE_CORO(res8.has_value());

    // Verify main state still has max epoch 15 (unchanged)
    main_max = accessor.get_main_state_max_epoch(*stm);
    ASSERT_TRUE_CORO(main_max.has_value());
    ASSERT_EQ_CORO(main_max.value(), ct::cluster_epoch{15});

    // Verify oo_state now has max epoch 8
    oo_max = accessor.get_oo_state_max_epoch(*stm);
    ASSERT_TRUE_CORO(oo_max.has_value());
    ASSERT_EQ_CORO(oo_max.value(), ct::cluster_epoch{8});

    // Step 3: Try to fence epoch 8 (should succeed - it's in oo_state and not
    // reconciled)
    auto fence8 = co_await leader_api.fence_epoch(ct::cluster_epoch{8});
    ASSERT_TRUE_CORO(fence8.has_value());
    fence8 = {}; // Release fence

    // Step 4: Partially reconcile - advance LRO past epoch 5 but not past 8
    // This will update both states' min_epoch_lower_bound
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{0}, model::no_timeout, as);

    // Verify the min epochs after partial reconciliation
    // The relaxed algorithm doesn't assume min_epoch moves to 10 just because
    // we saw epoch 5 at offset 0 - there could be more epoch 5 batches later
    auto main_min = accessor.get_main_state_min_epoch(*stm);
    auto oo_min = accessor.get_oo_state_min_epoch(*stm);
    ASSERT_TRUE_CORO(main_min.has_value());
    ASSERT_TRUE_CORO(oo_min.has_value());
    ASSERT_EQ_CORO(main_min.value(), ct::cluster_epoch{5});
    ASSERT_EQ_CORO(oo_min.value(), ct::cluster_epoch{8});

    // Step 5: Try to fence epoch 8 again (should still succeed - epoch 8 not
    // fully reconciled)
    fence8 = co_await leader_api.fence_epoch(ct::cluster_epoch{8});
    ASSERT_TRUE_CORO(fence8.has_value());
    fence8 = {}; // Release fence

    // Step 6: Try to fence epoch 7 (should succeed - it's >= main_min even
    // though it was never applied)
    auto fence7 = co_await leader_api.fence_epoch(ct::cluster_epoch{7});
    ASSERT_TRUE_CORO(fence7.has_value());
    fence7 = {}; // Release fence

    // Step 6b: Try to fence epoch 4 (should fail - it's < main_min)
    auto fence4 = co_await leader_api.fence_epoch(ct::cluster_epoch{4});
    ASSERT_FALSE_CORO(fence4.has_value());

    // Step 7: Apply another out-of-order batch with epoch 12
    auto b12 = make_record_batch(ct::cluster_epoch{12}, model::offset{4}, 4);
    auto res12 = co_await replicate_record_batch(leader, std::move(b12));
    ASSERT_TRUE_CORO(res12.has_value());

    // Verify oo_state now has max epoch 12
    oo_max = accessor.get_oo_state_max_epoch(*stm);
    ASSERT_TRUE_CORO(oo_max.has_value());
    ASSERT_EQ_CORO(oo_max.value(), ct::cluster_epoch{12});

    // Step 8: Fence epoch 12 (should succeed)
    auto fence12 = co_await leader_api.fence_epoch(ct::cluster_epoch{12});
    ASSERT_TRUE_CORO(fence12.has_value());
    fence12 = {}; // Release fence

    // Step 9: Fully reconcile all batches - advance LRO past all epochs
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{4}, model::no_timeout, as);

    // After this, both states' min_epoch_lower_bound should have advanced past
    // all applied epochs

    // Step 10: Try to fence epoch 8 again (should fail - fully reconciled)
    fence8 = co_await leader_api.fence_epoch(ct::cluster_epoch{8});
    ASSERT_FALSE_CORO(fence8.has_value());

    // Step 11: Try to fence epoch 12 (should fail - fully reconciled)
    fence12 = co_await leader_api.fence_epoch(ct::cluster_epoch{12});
    ASSERT_FALSE_CORO(fence12.has_value());

    // Step 12: Verify epoch 15 can still be fenced (in main state, not fully
    // reconciled)
    auto fence15 = co_await leader_api.fence_epoch(ct::cluster_epoch{15});
    ASSERT_TRUE_CORO(fence15.has_value());
}

TEST_F_CORO(ctp_stm_fixture, test_sequential_operations_fuzz) {
    // Fuzz test that validates correctness under various operation orderings:
    // - Bump cluster epoch
    // - Replicate with current epoch
    // - Replicate with stale epoch (below min_epoch) - should be rejected
    // - Replicate with stale epoch (above min_epoch) - should be accepted
    // - Advance LRO - should be accepted or rejected based on validity

    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto& leader = node(*get_leader());
    auto leader_api = api(leader);
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor accessor;

    // Test state tracking
    struct test_state {
        ct::cluster_epoch current_epoch{0};
        model::offset last_replicated_offset{-1};
        kafka::offset last_reconciled_offset{-1};

        // Statistics
        size_t epoch_bumps{0};
        size_t successful_replications{0};
        size_t rejected_stale_below_min{0};
        size_t accepted_stale_above_min{0};
        size_t successful_lro_advances{0};
        size_t rejected_lro_advances{0};

        // Track epochs we've replicated for validation
        std::vector<std::pair<model::offset, ct::cluster_epoch>>
          replicated_batches;
    };

    test_state state;

    // Random number generator
    std::random_device rd;
    std::mt19937 gen(rd());

    // Run fuzzing iterations
    constexpr int num_iterations = 5000;

    for (int iteration = 0; iteration < num_iterations; ++iteration) {
        // Choose random operation (0-4)
        std::uniform_int_distribution<> op_dist(0, 4);
        int op = op_dist(gen);

        if (op == 0) {
            // Operation 1: Bump cluster epoch
            state.current_epoch = ct::cluster_epoch{state.current_epoch() + 1};
            state.epoch_bumps++;

            vlog(
              ct::cd_log.trace,
              "Iteration {}: Bumped epoch to {}",
              iteration,
              state.current_epoch);

        } else if (op == 1 && state.current_epoch() > 0) {
            // Operation 2: Replicate with current (up-to-date) epoch

            // Acquire fence first
            auto fence_result = co_await leader_api.fence_epoch(
              state.current_epoch);

            if (!fence_result.has_value()) {
                vlog(
                  ct::cd_log.trace,
                  "Iteration {}: Failed to fence current epoch {} (stale: {})",
                  iteration,
                  state.current_epoch,
                  fence_result.error().latest_seen);
                continue;
            }

            // Replicate batch
            auto batch = make_record_batch(
              state.current_epoch,
              model::offset{state.last_replicated_offset() + 1},
              iteration);

            auto result = co_await replicate_record_batch(
              leader, std::move(batch));

            ASSERT_TRUE_CORO(result.has_value());
            state.last_replicated_offset = result.value();
            state.successful_replications++;
            state.replicated_batches.emplace_back(
              result.value(), state.current_epoch);

            vlog(
              ct::cd_log.trace,
              "Iteration {}: Replicated current epoch {} at offset {}",
              iteration,
              state.current_epoch,
              result.value());

        } else if (op == 2 && !state.replicated_batches.empty()) {
            // Operation 3: Try to replicate with stale epoch BELOW min_epoch
            // This should be rejected by fence_epoch

            auto main_min = accessor.get_main_state_min_epoch(*stm);

            if (!main_min.has_value()) {
                // No min_epoch set yet, skip this operation
                continue;
            }

            if (main_min.value()() == 0) {
                // min_epoch is 0, can't go below
                continue;
            }

            // Choose an epoch below min_epoch
            std::uniform_int_distribution<> epoch_dist(
              0, main_min.value()() - 1);
            auto stale_epoch = ct::cluster_epoch{epoch_dist(gen)};

            // Try to acquire fence - this should fail
            auto fence_result = co_await leader_api.fence_epoch(stale_epoch);

            ASSERT_FALSE_CORO(fence_result.has_value());
            state.rejected_stale_below_min++;

            vlog(
              ct::cd_log.trace,
              "Iteration {}: Correctly rejected stale epoch {} (below "
              "min_epoch {})",
              iteration,
              stale_epoch,
              main_min.value());

        } else if (
          op == 3 && !state.replicated_batches.empty()
          && state.current_epoch() > 1) {
            // Operation 4: Try to replicate with stale epoch ABOVE OR EQUAL TO
            // min_epoch This should be accepted if the epoch is within valid
            // range

            auto main_min = accessor.get_main_state_min_epoch(*stm);

            ct::cluster_epoch min_epoch_val{0};
            if (main_min.has_value()) {
                min_epoch_val = main_min.value();
            }

            // Choose an epoch between min_epoch and current_epoch - 1
            if (state.current_epoch() <= min_epoch_val()) {
                continue;
            }

            std::uniform_int_distribution<> epoch_dist(
              min_epoch_val(), state.current_epoch() - 1);
            auto stale_epoch = ct::cluster_epoch{epoch_dist(gen)};

            // Try to acquire fence - this should succeed
            auto fence_result = co_await leader_api.fence_epoch(stale_epoch);

            if (!fence_result.has_value()) {
                // Fence failed - might be because this epoch is now beyond
                // max_seen
                vlog(
                  ct::cd_log.trace,
                  "Iteration {}: Stale epoch {} failed fence (latest_seen: {})",
                  iteration,
                  stale_epoch,
                  fence_result.error().latest_seen);
                continue;
            }

            // Replicate batch with stale epoch
            auto batch = make_record_batch(
              stale_epoch,
              model::offset{state.last_replicated_offset() + 1},
              iteration);

            auto result = co_await replicate_record_batch(
              leader, std::move(batch));

            ASSERT_TRUE_CORO(result.has_value());
            state.last_replicated_offset = result.value();
            state.accepted_stale_above_min++;
            state.replicated_batches.emplace_back(result.value(), stale_epoch);

            vlog(
              ct::cd_log.trace,
              "Iteration {}: Replicated stale epoch {} at offset {} "
              "(min_epoch: {})",
              iteration,
              stale_epoch,
              result.value(),
              min_epoch_val);

        } else if (op == 4 && state.last_replicated_offset() >= 0) {
            // Operation 5: Advance LRO
            // Choose a random offset up to last replicated, or slightly beyond

            std::uniform_int_distribution<> offset_dist(
              std::max(state.last_reconciled_offset() + 1, 0L),
              state.last_replicated_offset() + 2);
            auto new_lro = kafka::offset{offset_dist(gen)};

            // Advancing LRO should always succeed (it's idempotent and can go
            // forward)
            co_await leader_api.advance_reconciled_offset(
              new_lro, model::no_timeout, as);

            if (new_lro > state.last_reconciled_offset) {
                state.last_reconciled_offset = new_lro;
                state.successful_lro_advances++;

                vlog(
                  ct::cd_log.trace,
                  "Iteration {}: Advanced LRO to {}",
                  iteration,
                  new_lro);
            } else {
                vlog(
                  ct::cd_log.trace,
                  "Iteration {}: LRO already at or beyond {}",
                  iteration,
                  new_lro);
            }
        }
    }

    // Final validation
    vlog(
      ct::cd_log.info,
      "Fuzz test completed: {} epoch bumps, {} successful replications, "
      "{} rejected stale (below min), {} accepted stale (above min), "
      "{} LRO advances",
      state.epoch_bumps,
      state.successful_replications,
      state.rejected_stale_below_min,
      state.accepted_stale_above_min,
      state.successful_lro_advances);

    // Verify STM invariants
    auto max_epoch = leader_api.get_max_epoch();
    auto max_seen_epoch = leader_api.get_max_seen_epoch();

    if (max_epoch.has_value() && max_seen_epoch.has_value()) {
        // max_epoch should be <= max_seen_epoch
        ASSERT_LE_CORO(max_epoch.value()(), max_seen_epoch.value()());

        vlog(
          ct::cd_log.info,
          "Final state: max_epoch={}, max_seen_epoch={}",
          max_epoch.value(),
          max_seen_epoch.value());
    }

    // Verify min_epoch <= max_epoch invariant
    auto main_min = accessor.get_main_state_min_epoch(*stm);
    auto main_max = accessor.get_main_state_max_epoch(*stm);

    if (main_min.has_value() && main_max.has_value()) {
        ASSERT_LE_CORO(main_min.value()(), main_max.value()());

        vlog(
          ct::cd_log.info,
          "Final state: main_min={}, main_max={}",
          main_min.value(),
          main_max.value());
    }

    // Verify out-of-order state invariants
    auto oo_min = accessor.get_oo_state_min_epoch(*stm);
    auto oo_max = accessor.get_oo_state_max_epoch(*stm);

    if (oo_min.has_value() && oo_max.has_value()) {
        ASSERT_LE_CORO(oo_min.value()(), oo_max.value()());

        vlog(
          ct::cd_log.info,
          "Final state: oo_min={}, oo_max={}",
          oo_min.value(),
          oo_max.value());
    }

    // Verify we had some activity
    ASSERT_GT_CORO(state.epoch_bumps, 0);

    vlog(ct::cd_log.info, "Sequential fuzz test validation passed");
}
