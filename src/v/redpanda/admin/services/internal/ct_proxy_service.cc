/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/ct_proxy_service.h"

#include "bytes/iobuf.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/types.h"
#include "cluster/partition.h"
#include "container/chunked_vector.h"
#include "model/namespace.h"
#include "model/record.h"
#include "serde/protobuf/rpc.h"
#include "storage/log_reader.h"
#include "storage/parser_utils.h"

#include <seastar/core/coroutine.hh>
#include <regex>

namespace admin {

namespace {

// Helper to get ctp_stm_api from a partition
ss::lw_shared_ptr<cloud_topics::ctp_stm_api>
make_ctp_stm_api(ss::lw_shared_ptr<cluster::partition> p) {
    auto stm = p->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    if (!stm) {
        throw serde::pb::rpc::invalid_argument_exception(
          "partition is not a cloud topic");
    }
    return ss::make_lw_shared<cloud_topics::ctp_stm_api>(stm);
}

// Helper to convert proto PlaceholderData to extent_meta
cloud_topics::extent_meta proto_to_extent_meta(
  const proto::admin::ct_proxy::placeholder_data& proto_ph) {
    cloud_topics::extent_meta meta;

    // Convert object_id from proto
    const auto& uuid_iobuf = proto_ph.get_object_id_uuid();
    if (uuid_iobuf.size_bytes() != uuid_t::length) {
        throw serde::pb::rpc::invalid_argument_exception(
          "object_id_uuid must be 16 bytes");
    }

    // Linearize iobuf to get UUID bytes
    auto uuid_str = uuid_iobuf.linearize_to_string();
    std::vector<uint8_t> uuid_bytes(uuid_str.begin(), uuid_str.end());
    uuid_t uuid(uuid_bytes);

    meta.id.name = uuid;
    meta.id.epoch = cloud_topics::cluster_epoch(proto_ph.get_cluster_epoch());
    meta.id.prefix = proto_ph.get_object_id_prefix();

    meta.first_byte_offset = cloud_topics::first_byte_offset_t(
      proto_ph.get_first_byte_offset());
    meta.byte_range_size = cloud_topics::byte_range_size_t(
      proto_ph.get_byte_range_size());
    meta.base_offset = kafka::offset(proto_ph.get_base_offset());
    meta.last_offset = kafka::offset(proto_ph.get_last_offset());

    return meta;
}

} // namespace

ss::lw_shared_ptr<cluster::partition>
ct_proxy_service_impl::get_partition(const model::ntp& ntp) {
    // TODO: Cross-shard partition access not yet supported.
    // For now, only access partitions on the current shard.
    auto partition = _partition_manager->local().get(ntp);
    if (!partition) {
        throw serde::pb::rpc::not_found_exception(
          "partition not found or on different shard");
    }

    return partition;
}

seastar::future<proto::admin::ct_proxy::get_cluster_epoch_response>
ct_proxy_service_impl::get_cluster_epoch(
  serde::pb::rpc::context,
  proto::admin::ct_proxy::get_cluster_epoch_request req) {
    // Get topic_id from topic_table
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});

    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }

    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }

    // Build NTP and get partition
    model::ntp ntp{
      model::kafka_namespace,
      model::topic{req.get_partition().get_topic()},
      model::partition_id{req.get_partition().get_partition()}};

    auto partition = get_partition(ntp);

    // Get cluster epoch from topic revision ID
    auto epoch = partition->get_topic_revision_id();

    proto::admin::ct_proxy::get_cluster_epoch_response response;
    response.set_cluster_epoch(epoch());
    co_return response;
}

seastar::future<proto::admin::ct_proxy::replicate_placeholders_response>
ct_proxy_service_impl::replicate_placeholders(
  serde::pb::rpc::context,
  proto::admin::ct_proxy::replicate_placeholders_request req) {
    // Get topic_id and partition
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});

    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }

    model::ntp ntp{
      model::kafka_namespace,
      model::topic{req.get_partition().get_topic()},
      model::partition_id{req.get_partition().get_partition()}};

    auto partition = get_partition(ntp);

    // Get ctp_stm_api for fencing
    auto ctp_api = make_ctp_stm_api(partition);

    // Perform RW-fence with expected cluster epoch
    auto fence_result = co_await ctp_api->fence_epoch(
      cloud_topics::cluster_epoch(req.get_expected_cluster_epoch()));

    if (!fence_result) {
        throw serde::pb::rpc::invalid_argument_exception(
          "fencing failed - cluster epoch mismatch");
    }

    // Extract term from fence before moving
    auto term = fence_result.value().term;

    // Convert proto placeholders to extent_meta and encode as batches
    chunked_vector<model::record_batch> batches;

    for (const auto& proto_ph : req.get_placeholders()) {
        auto extent = proto_to_extent_meta(proto_ph);

        // Create minimal batch header - encode_placeholder_batch will fill in the rest
        model::record_batch_header header{
          .base_offset = model::offset(extent.base_offset()),
          .type = model::record_batch_type::ctp_placeholder,
          .last_offset_delta = static_cast<int32_t>(
            extent.last_offset() - extent.base_offset()),
          .record_count = 1,
        };

        // Encode placeholder batch
        auto batch = cloud_topics::encode_placeholder_batch(header, extent);
        batches.push_back(std::move(batch));
    }

    // Replicate batches directly
    auto result = co_await partition->replicate(
      std::move(batches),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    if (!result) {
        throw serde::pb::rpc::unavailable_exception(
          "replication failed");
    }

    proto::admin::ct_proxy::replicate_placeholders_response response;
    response.set_last_offset(result.value().last_offset());
    response.set_term(term());
    co_return response;
}

seastar::future<proto::admin::ct_proxy::read_placeholders_response>
ct_proxy_service_impl::read_placeholders(
  serde::pb::rpc::context,
  proto::admin::ct_proxy::read_placeholders_request req) {
    // Get partition
    model::ntp ntp{
      model::kafka_namespace,
      model::topic{req.get_partition().get_topic()},
      model::partition_id{req.get_partition().get_partition()}};

    auto partition = get_partition(ntp);

    // Create log reader config
    size_t max_bytes = req.get_max_bytes() > 0
      ? req.get_max_bytes()
      : std::numeric_limits<size_t>::max();

    storage::local_log_reader_config reader_cfg(
      model::offset(req.get_start_offset()),
      model::offset(req.get_max_offset()),
      max_bytes,
      model::record_batch_type::ctp_placeholder,
      std::nullopt, // time
      std::nullopt  // abort_source
    );

    // Create reader
    auto reader = co_await partition->log()->make_reader(reader_cfg);

    proto::admin::ct_proxy::read_placeholders_response response;

    // Read batches
    model::record_batch_reader::storage_t batches_storage = co_await
      model::consume_reader_to_memory(
        std::move(reader),
        model::no_timeout);

    // Extract data_t from variant
    auto& batches = std::get<model::record_batch_reader::data_t>(batches_storage);

    // Parse each batch and convert to proto
    for (auto& batch : batches) {
        if (batch.header().type != model::record_batch_type::ctp_placeholder) {
            continue;
        }

        // Parse placeholder from batch
        auto placeholder = cloud_topics::parse_placeholder_batch(std::move(batch));

        // Create proto placeholder batch
        proto::admin::ct_proxy::placeholder_batch proto_batch;

        // Set placeholder data
        auto& proto_ph = proto_batch.get_placeholder();

        // Convert object_id - uuid_t to iobuf
        iobuf uuid_buf;
        const auto& uuid_data = placeholder.id.name.uuid();
        uuid_buf.append(
          reinterpret_cast<const uint8_t*>(uuid_data.data),
          uuid_t::length);
        proto_ph.set_object_id_uuid(std::move(uuid_buf));

        proto_ph.set_cluster_epoch(placeholder.id.epoch());
        proto_ph.set_object_id_prefix(placeholder.id.prefix);
        proto_ph.set_first_byte_offset(placeholder.offset());
        proto_ph.set_byte_range_size(placeholder.size_bytes());

        // Calculate base/last offsets from batch header
        auto base_offset = batch.base_offset();
        auto last_offset = batch.last_offset();
        proto_ph.set_base_offset(model::offset_cast(base_offset)());
        proto_ph.set_last_offset(model::offset_cast(last_offset)());

        // Set batch metadata
        proto_batch.set_base_offset(model::offset_cast(base_offset)());
        proto_batch.set_last_offset(model::offset_cast(last_offset)());
        proto_batch.set_record_count(batch.record_count());

        // Producer metadata
        if (batch.header().attrs.is_transactional()) {
            proto_batch.set_is_transactional(true);
            proto_batch.set_producer_id(batch.header().producer_id);
            proto_batch.set_producer_epoch(batch.header().producer_epoch);
        }

        // Add to response
        response.get_batches().push_back(std::move(proto_batch));
    }

    co_return response;
}

seastar::future<proto::admin::ct_proxy::list_cloud_topic_partitions_response>
ct_proxy_service_impl::list_cloud_topic_partitions(
  serde::pb::rpc::context,
  proto::admin::ct_proxy::list_cloud_topic_partitions_request req) {
    proto::admin::ct_proxy::list_cloud_topic_partitions_response response;

    // Get all topic namespaces
    auto all_topic_namespaces = _topic_table->local().all_topics();

    // Iterate through all topics
    for (const auto& tp_ns : all_topic_namespaces) {
        // Filter for Kafka namespace only
        if (tp_ns.ns != model::kafka_namespace) {
            continue;
        }

        // Get topic metadata
        const auto& metadata_opt = _topic_table->local().get_topic_metadata_ref(tp_ns);
        if (!metadata_opt) {
            continue;
        }

        const auto& metadata = metadata_opt->get();

        // Check if this is a cloud topic
        if (!metadata.get_configuration().properties.cloud_topic_enabled) {
            continue;
        }

        // Apply topic filter if specified
        const auto& topic_filter = req.get_topic_filter();
        if (!topic_filter.empty()) {
            try {
                std::string filter_str{topic_filter};
                std::regex filter_regex{filter_str};
                std::string topic_str{tp_ns.tp()};
                if (!std::regex_match(topic_str, filter_regex)) {
                    continue;
                }
            } catch (...) {
                // Invalid regex, skip filtering
            }
        }

        // Iterate through all partitions of this topic
        for (int i = 0; i < metadata.get_configuration().partition_count; ++i) {
            model::partition_id partition_id(i);
            model::ntp ntp{model::kafka_namespace, tp_ns.tp, partition_id};

            // Get partition if it's on this shard
            // TODO: Support cross-shard calls
            auto partition = _partition_manager->local().get(ntp);
            if (!partition) {
                continue;
            }

            proto::admin::ct_proxy::cloud_topic_partition_info part_info;
            part_info.set_topic(ss::sstring(tp_ns.tp()));
            part_info.set_partition(partition_id());

            // Get offsets from partition
            try {
                auto ctp_api = make_ctp_stm_api(partition);
                part_info.set_start_offset(ctp_api->get_start_offset()());

                // High watermark and log end offset
                auto committed_kafka = model::offset_cast(partition->committed_offset());
                part_info.set_high_watermark(committed_kafka() + 1);

                auto dirty_kafka = model::offset_cast(partition->dirty_offset());
                part_info.set_log_end_offset(dirty_kafka() + 1);

                // Check if L1 data exists (LRO > min means L1 has data)
                auto lro = ctp_api->get_last_reconciled_offset();
                part_info.set_has_l1_data(lro > kafka::offset::min());

                // Add to response
                response.get_partitions().push_back(std::move(part_info));
            } catch (...) {
                // If we can't get metadata, skip this partition
                continue;
            }
        }
    }

    co_return response;
}

seastar::future<proto::admin::ct_proxy::read_l1_metadata_response>
ct_proxy_service_impl::read_l1_metadata(
  serde::pb::rpc::context,
  [[maybe_unused]] proto::admin::ct_proxy::read_l1_metadata_request req) {
    // TODO: This requires access to the L1 metastore, which is not currently
    // injected into this service. For now, return an unimplemented error.
    // To implement this, we would need to:
    // 1. Inject ss::sharded<cloud_topics::l1::replicated_metastore>* into the constructor
    // 2. Query the metastore for extent metadata in the offset range
    // 3. Convert L1 extent metadata to proto format

    throw serde::pb::rpc::unimplemented_exception(
      "read_l1_metadata not yet implemented");
}

} // namespace admin
