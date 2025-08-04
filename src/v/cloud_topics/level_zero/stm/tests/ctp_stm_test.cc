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
#include "cloud_topics/level_zero/stm/ctp_stm_factory.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "cloud_topics/dl_placeholder.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

namespace ct = experimental::cloud_topics;

class ctp_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ctp_stm_fixture() 
    : rtc(as)
    {}


    ~ctp_stm_fixture() override {
        for (auto& entry : api_by_vnode) {
            entry.second->stop().get();
        }
    };

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;

            experimental::cloud_topics::ctp_stm_factory stm_factory;
            stm_factory.create(
              builder, &*node->raft(), cluster::stm_instance_config{nullptr});

            vlog(ct::cd_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));

            stm_by_vnode[node->get_vnode()]
              = node->raft()->stm_manager()->get<ct::ctp_stm>();

            api_by_vnode.emplace(
              node->get_vnode(),
              ss::make_shared<ct::ctp_stm_api>(
                rtc, node->raft()->stm_manager()->get<ct::ctp_stm>()));
        }
    }

    ct::ctp_stm_api& api(raft::raft_node_instance& node) {
        return *api_by_vnode[node.get_vnode()];
    }

    model::record_batch make_record_batch(ct::cluster_epoch e, model::offset base_offset, int32_t seq) {
        ct::object_id id = ct::object_id::create(e);
        ct::dl_placeholder placeholder{
          .id = id,
          .offset = ct::first_byte_offset_t{0},
          .size_bytes = ct::byte_range_size_t{0},
        };

        storage::record_batch_builder builder(
          model::record_batch_type::dl_placeholder, base_offset);

        auto first_key = serde::to_iobuf(
          experimental::cloud_topics::dl_placeholder_record_key::payload);

        auto first_value = serde::to_iobuf(placeholder);

        builder.add_raw_kv(std::move(first_key), std::move(first_value));
        builder.add_raw_kv(std::nullopt, std::nullopt);

        auto ph = std::move(builder).build();
        ph.header().first_timestamp = model::timestamp::now();
        ph.header().max_timestamp = model::timestamp::now();
        ph.header().base_sequence = seq;
        ph.header().header_crc = model::internal_header_only_crc(ph.header());
        return ph;
    }


    ss::abort_source as;
    retry_chain_node rtc;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm>> stm_by_vnode;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm_api>>
      api_by_vnode;
};

TEST_F_CORO(ctp_stm_fixture, test_basic) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = api(node(*get_leader())).get_gc_epoch();

    ASSERT_FALSE_CORO(gc_epoch.has_value());

    // Replicate L0 metadata batch with epoch set to some value and check
    auto res = co_await this->with_leader(1s, [this] (raft::raft_node_instance& leader) {
      auto b = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
      raft::replicate_options opt(raft::consistency_level::quorum_ack);
      return leader.raft()->replicate(std::move(b), opt);
    });
    ASSERT_FALSE_CORO(res.has_error());

    api(node(*get_leader())).get_max_epoch();

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto projected_epoch = api(node(*get_leader())).get_projected_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(projected_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{1});
    ASSERT_EQ_CORO(projected_epoch.value(), ct::cluster_epoch{1});
}
