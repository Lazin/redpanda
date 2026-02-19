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

#include "base/vlog.h"
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
#include <seastar/util/log.hh>

#include <regex>

namespace {
static ss::logger ctplog("ct_proxy_service");
}

namespace admin {

namespace {

// Helper to convert proto PlaceholderData to extent_meta
cloud_topics::extent_meta
proto_to_extent_meta(const proto::admin::ct_proxy::placeholder_data& proto_ph) {
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

std::optional<ss::shard_id>
ct_proxy_service_impl::get_partition_shard(const model::ntp& ntp) {
    vlog(ctplog.debug, "get_partition_shard: looking up ntp={}", ntp);

    // Look up which shard owns this partition
    auto shard = _shard_table->local().shard_for(ntp);
    if (!shard) {
        vlog(
          ctplog.warn,
          "get_partition_shard: ntp={} not found in shard table",
          ntp);
    } else {
        vlog(
          ctplog.debug,
          "get_partition_shard: ntp={} is on shard {}",
          ntp,
          *shard);
    }
    return shard;
}

seastar::future<proto::admin::ct_proxy::get_cluster_epoch_response>
ct_proxy_service_impl::get_cluster_epoch(
  serde::pb::rpc::context, proto::admin::ct_proxy::get_cluster_epoch_request) {
    vlog(ctplog.info, "get_cluster_epoch");

    // Get the current cluster epoch from the epoch service
    ss::abort_source as;
    auto epoch_result = co_await _epoch_service->local().get_cached_epoch(&as);

    if (!epoch_result) {
        vlog(
          ctplog.warn,
          "get_cluster_epoch: failed to get epoch: {}",
          epoch_result.error().message());
        throw serde::pb::rpc::unavailable_exception(
          "failed to get cluster epoch");
    }

    auto epoch = epoch_result.value();
    vlog(ctplog.info, "get_cluster_epoch: epoch={}", epoch);

    proto::admin::ct_proxy::get_cluster_epoch_response response;
    response.set_cluster_epoch(epoch);
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

    auto shard = get_partition_shard(ntp);
    if (!shard) {
        throw serde::pb::rpc::not_found_exception(
          "partition not found in shard table");
    }

    // Convert proto placeholders to extent_meta before crossing shard boundary
    chunked_vector<cloud_topics::extent_meta> extents;
    for (const auto& proto_ph : req.get_placeholders()) {
        extents.push_back(proto_to_extent_meta(proto_ph));
    }

    auto expected_epoch = cloud_topics::cluster_epoch(
      req.get_expected_cluster_epoch());

    // Result type for cross-shard invocation
    struct replicate_result {
        kafka::offset last_offset;
        model::term_id term;
        bool success{false};
        ss::sstring error_msg;
    };

    // Do all partition work on the correct shard
    auto result = co_await _partition_manager->invoke_on(
      *shard,
      [ntp, extents = std::move(extents), expected_epoch](
        cluster::partition_manager& pm) mutable
        -> ss::future<replicate_result> {
          auto partition = pm.get(ntp);
          if (!partition) {
              co_return replicate_result{
                .success = false, .error_msg = "partition not found"};
          }

          // Get ctp_stm_api for fencing
          auto stm
            = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
          if (!stm) {
              co_return replicate_result{
                .success = false,
                .error_msg = "partition is not a cloud topic"};
          }
          auto ctp_api = ss::make_lw_shared<cloud_topics::ctp_stm_api>(stm);

          // Perform RW-fence with expected cluster epoch
          auto fence_result = co_await ctp_api->fence_epoch(expected_epoch);

          if (!fence_result) {
              co_return replicate_result{
                .success = false,
                .error_msg = "fencing failed - cluster epoch mismatch"};
          }

          // Extract term from fence
          auto term = fence_result.value().term;

          // Convert extents to batches
          chunked_vector<model::record_batch> batches;
          for (const auto& extent : extents) {
              // Create a properly initialized header
              auto now = model::timestamp::now();
              auto record_count = static_cast<int32_t>(
                extent.last_offset() - extent.base_offset() + 1);

              vlog(
                ctplog.info,
                "Creating placeholder batch: base_offset={}, last_offset={}, "
                "record_count={}",
                extent.base_offset(),
                extent.last_offset(),
                record_count);

              model::record_batch_header header{
                .header_crc = 0,
                .size_bytes = 0, // Will be set by reset_size_checksum_metadata
                .base_offset = model::offset(extent.base_offset()),
                .type = model::record_batch_type::ctp_placeholder,
                .crc = 0,
                .attrs = model::record_batch_attributes{},
                .last_offset_delta = record_count - 1,
                .first_timestamp = now,
                .max_timestamp = now,
                .producer_id = model::producer_id{-1},
                .producer_epoch = int16_t{-1},
                .base_sequence = int32_t{-1},
                .record_count = record_count,
              };
              auto batch = cloud_topics::encode_placeholder_batch(
                header, extent);

              vlog(
                ctplog.info,
                "Encoded placeholder batch: size_bytes={}, data_size={}",
                batch.header().size_bytes,
                batch.data().size_bytes());

              batches.push_back(std::move(batch));
          }

          // Replicate batches
          auto repl_result = co_await partition->replicate(
            std::move(batches),
            raft::replicate_options{raft::consistency_level::quorum_ack});

          if (!repl_result) {
              co_return replicate_result{
                .success = false, .error_msg = "replication failed"};
          }

          co_return replicate_result{
            .last_offset = repl_result.value().last_offset,
            .term = term,
            .success = true};
      });

    if (!result.success) {
        if (
          result.error_msg == "partition not found"
          || result.error_msg == "partition is not a cloud topic") {
            throw serde::pb::rpc::not_found_exception(result.error_msg);
        } else if (
          result.error_msg == "fencing failed - cluster epoch mismatch") {
            throw serde::pb::rpc::invalid_argument_exception(result.error_msg);
        } else {
            throw serde::pb::rpc::unavailable_exception(result.error_msg);
        }
    }

    proto::admin::ct_proxy::replicate_placeholders_response response;
    response.set_last_offset(result.last_offset());
    response.set_term(result.term());
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

    auto shard = get_partition_shard(ntp);
    if (!shard) {
        throw serde::pb::rpc::not_found_exception(
          "partition not found in shard table");
    }

    // Create log reader config
    size_t max_bytes = req.has_max_bytes()
                         ? req.get_max_bytes()
                         : std::numeric_limits<size_t>::max();

    auto start_offset = model::offset(req.get_start_offset());
    auto max_offset = model::offset(req.get_max_offset());

    // Do all partition work on the correct shard
    auto response = co_await _partition_manager->invoke_on(
      *shard,
      [ntp, start_offset, max_offset, max_bytes](cluster::partition_manager& pm)
        -> ss::future<proto::admin::ct_proxy::read_placeholders_response> {
          auto partition = pm.get(ntp);
          if (!partition) {
              throw serde::pb::rpc::not_found_exception("partition not found");
          }

          storage::local_log_reader_config reader_cfg(
            start_offset,
            max_offset,
            max_bytes,
            model::record_batch_type::ctp_placeholder,
            std::nullopt, // time
            std::nullopt  // abort_source
          );

          // Create reader
          auto reader = co_await partition->log()->make_reader(reader_cfg);

          proto::admin::ct_proxy::read_placeholders_response response;

          // Read batches
          model::record_batch_reader::storage_t batches_storage
            = co_await model::consume_reader_to_memory(
              std::move(reader), model::no_timeout);

          // Extract data_t from variant
          auto& batches = std::get<model::record_batch_reader::data_t>(
            batches_storage);

          // Parse each batch and convert to proto
          for (auto& batch : batches) {
              if (
                batch.header().type
                != model::record_batch_type::ctp_placeholder) {
                  continue;
              }

              // Save batch header info before moving batch
              auto base_offset = batch.base_offset();
              auto last_offset = batch.last_offset();
              auto record_count = batch.record_count();
              auto is_transactional
                = batch.header().attrs.is_transactional();
              auto producer_id = batch.header().producer_id;
              auto producer_epoch = batch.header().producer_epoch;

              // Parse placeholder from batch (consumes batch)
              auto placeholder = cloud_topics::parse_placeholder_batch(
                std::move(batch));

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

              proto_ph.set_base_offset(model::offset_cast(base_offset)());
              proto_ph.set_last_offset(model::offset_cast(last_offset)());

              // Set batch metadata
              proto_batch.set_base_offset(model::offset_cast(base_offset)());
              proto_batch.set_last_offset(model::offset_cast(last_offset)());
              proto_batch.set_record_count(record_count);

              // Producer metadata
              if (is_transactional) {
                  proto_batch.set_is_transactional(true);
                  proto_batch.set_producer_id(producer_id);
                  proto_batch.set_producer_epoch(producer_epoch);
              }

              // Add to response
              response.get_batches().push_back(std::move(proto_batch));
          }

          co_return response;
      });

    co_return response;
}

seastar::future<proto::admin::ct_proxy::list_cloud_topic_partitions_response>
ct_proxy_service_impl::list_cloud_topic_partitions(
  serde::pb::rpc::context,
  proto::admin::ct_proxy::list_cloud_topic_partitions_request req) {
    vlog(
      ctplog.info,
      "list_cloud_topic_partitions: has_filter={}",
      req.has_topic_filter());
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
        const auto& metadata_opt = _topic_table->local().get_topic_metadata_ref(
          tp_ns);
        if (!metadata_opt) {
            continue;
        }

        const auto& metadata = metadata_opt->get();

        // Check if this is a cloud topic
        if (!metadata.get_configuration().is_cloud_topic()) {
            vlog(
              ctplog.debug,
              "list_cloud_topic_partitions: topic {} is not a cloud topic",
              tp_ns.tp);
            continue;
        }

        vlog(
          ctplog.info,
          "list_cloud_topic_partitions: found cloud topic {}, partitions={}",
          tp_ns.tp,
          metadata.get_configuration().partition_count);

        // Apply topic filter if specified
        if (req.has_topic_filter()) {
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
        }

        // Iterate through all partitions of this topic
        for (int i = 0; i < metadata.get_configuration().partition_count; ++i) {
            model::partition_id partition_id(i);
            model::ntp ntp{model::kafka_namespace, tp_ns.tp, partition_id};

            // Look up which shard owns this partition
            auto shard = _shard_table->local().shard_for(ntp);
            if (!shard) {
                vlog(
                  ctplog.debug,
                  "list_cloud_topic_partitions: ntp={} not in shard table",
                  ntp);
                continue;
            }

            // Get partition info from the correct shard
            // Capture topic name by value for cross-shard safety
            ss::sstring topic_name(tp_ns.tp());
            try {
                auto part_info_opt = co_await _partition_manager->invoke_on(
                  *shard,
                  [ntp, topic_name = std::move(topic_name), partition_id](
                    cluster::partition_manager& pm) mutable {
                      auto partition = pm.get(ntp);
                      if (!partition) {
                          return std::optional<
                            proto::admin::ct_proxy::cloud_topic_partition_info>{
                            std::nullopt};
                      }

                      proto::admin::ct_proxy::cloud_topic_partition_info
                        part_info;
                      part_info.set_topic(std::move(topic_name));
                      part_info.set_partition(partition_id());

                      // Get offsets from partition
                      try {
                          auto stm = partition->raft()
                                       ->stm_manager()
                                       ->get<cloud_topics::ctp_stm>();
                          if (!stm) {
                              return std::optional<
                                proto::admin::ct_proxy::
                                  cloud_topic_partition_info>{std::nullopt};
                          }
                          auto ctp_api
                            = ss::make_lw_shared<cloud_topics::ctp_stm_api>(
                              stm);
                          part_info.set_start_offset(
                            ctp_api->get_start_offset()());

                          // High watermark and log end offset
                          auto committed_kafka = model::offset_cast(
                            partition->committed_offset());
                          part_info.set_high_watermark(committed_kafka() + 1);

                          auto dirty_kafka = model::offset_cast(
                            partition->dirty_offset());
                          part_info.set_log_end_offset(dirty_kafka() + 1);

                          // Check if L1 data exists (LRO > min means L1 has
                          // data)
                          auto lro = ctp_api->get_last_reconciled_offset();
                          part_info.set_has_l1_data(lro > kafka::offset::min());

                          return std::optional<
                            proto::admin::ct_proxy::cloud_topic_partition_info>{
                            std::move(part_info)};
                      } catch (...) {
                          return std::optional<
                            proto::admin::ct_proxy::cloud_topic_partition_info>{
                            std::nullopt};
                      }
                  });

                if (part_info_opt) {
                    response.get_partitions().push_back(
                      std::move(*part_info_opt));
                }
            } catch (...) {
                // If we can't get metadata, skip this partition
                vlog(
                  ctplog.debug,
                  "list_cloud_topic_partitions: failed to get info for ntp={}",
                  ntp);
                continue;
            }
        }
    }

    vlog(
      ctplog.info,
      "list_cloud_topic_partitions: returning {} partitions",
      response.get_partitions().size());
    co_return response;
}

seastar::future<proto::admin::ct_proxy::read_l1_metadata_response>
ct_proxy_service_impl::read_l1_metadata(
  serde::pb::rpc::context,
  [[maybe_unused]] proto::admin::ct_proxy::read_l1_metadata_request req) {
    // TODO: This requires access to the L1 metastore, which is not currently
    // injected into this service. For now, return an unimplemented error.
    // To implement this, we would need to:
    // 1. Inject ss::sharded<cloud_topics::l1::replicated_metastore>* into the
    // constructor
    // 2. Query the metastore for extent metadata in the offset range
    // 3. Convert L1 extent metadata to proto format

    throw serde::pb::rpc::unimplemented_exception(
      "read_l1_metadata not yet implemented");
}

} // namespace admin
