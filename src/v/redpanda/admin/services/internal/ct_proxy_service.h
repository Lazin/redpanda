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

#include "cluster/cluster_epoch_service.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/ct_proxy.proto.h"

#include <seastar/core/sharded.hh>

namespace admin {

class ct_proxy_service_impl : public proto::admin::ct_proxy::ct_proxy_service {
public:
    explicit ct_proxy_service_impl(
      ss::sharded<cluster::partition_manager>* pm,
      ss::sharded<cluster::topic_table>* tt,
      ss::sharded<cluster::shard_table>* st,
      ss::sharded<cluster::cluster_epoch_service<>>* epoch_service)
      : _partition_manager(pm)
      , _topic_table(tt)
      , _shard_table(st)
      , _epoch_service(epoch_service) {}

    seastar::future<proto::admin::ct_proxy::get_cluster_epoch_response>
      get_cluster_epoch(
        serde::pb::rpc::context,
        proto::admin::ct_proxy::get_cluster_epoch_request) override;

    seastar::future<proto::admin::ct_proxy::replicate_placeholders_response>
      replicate_placeholders(
        serde::pb::rpc::context,
        proto::admin::ct_proxy::replicate_placeholders_request) override;

    seastar::future<proto::admin::ct_proxy::read_placeholders_response>
      read_placeholders(
        serde::pb::rpc::context,
        proto::admin::ct_proxy::read_placeholders_request) override;

    seastar::future<
      proto::admin::ct_proxy::list_cloud_topic_partitions_response>
      list_cloud_topic_partitions(
        serde::pb::rpc::context,
        proto::admin::ct_proxy::list_cloud_topic_partitions_request) override;

    seastar::future<proto::admin::ct_proxy::read_l1_metadata_response>
      read_l1_metadata(
        serde::pb::rpc::context,
        proto::admin::ct_proxy::read_l1_metadata_request) override;

private:
    std::optional<ss::shard_id> get_partition_shard(const model::ntp& ntp);

    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::cluster_epoch_service<>>* _epoch_service;
};

} // namespace admin
