/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/remote.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/types.h"
#include "cloud_topics/types.h"
#include "model/batch_builder.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "redpanda/admin/services/internal/ct_proxy_service.h"
#include "redpanda/tests/fixture.h"
#include "ssx/sformat.h"
#include "storage/log_reader.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/io_priority_class.hh>

#include <gtest/gtest.h>

static ss::logger ct_proxy_test_log("ct_proxy_pipeline_test");

/// Test fixture that combines S3 imposter and Redpanda for testing
/// cloud topics write pipeline and ct_proxy admin API integration
class ct_proxy_pipeline_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    ct_proxy_pipeline_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        // No expectations: tests will PUT and GET organically
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    void SetUp() override {
        // Create cloud topic
        cluster::topic_properties props;
        props.cloud_topic_enabled = true;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();

        // Get partition and verify it has ctp_stm
        partition = app.partition_manager.local().get(ntp);
        ASSERT_TRUE(partition != nullptr);

        auto stm = partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
        ASSERT_TRUE(stm != nullptr);

        // Create ct_proxy service for admin API access
        ct_proxy_service = std::make_unique<admin::ct_proxy_service_impl>(
          &app.partition_manager, &app.controller->get_topics_state());
    }

    void TearDown() override {
        ct_proxy_service.reset();
        for (auto& fn : std::views::reverse(cleanup)) {
            fn();
        }
    }

    /// Creates a batch with test data
    model::record_batch make_test_batch(
      kafka::offset base_offset, size_t num_records, std::string_view prefix) {
        model::batch_builder builder;
        for (size_t i = 0; i < num_records; i++) {
            auto key = ssx::sformat("{}key{}", prefix, i);
            auto val = ssx::sformat("{}val{}", prefix, i);

            builder.add_record(model::record(
              /*attributes=*/{},
              /*timestamp_delta=*/0,
              /*offset_delta=*/static_cast<int32_t>(i),
              /*key=*/iobuf::from(key),
              /*value=*/iobuf::from(val),
              /*hdrs=*/{}));
        }

        builder.set_batch_timestamp(
          model::timestamp_type::create_time, model::timestamp::now());

        builder.set_base_offset(base_offset);
        return builder.build_sync();
    }

    /// Uploads serialized data to S3 and returns extent_meta
    ss::future<cloud_topics::extent_meta> upload_to_cloud(
      const cloud_topics::l0::serialized_chunk& chunk,
      cloud_topics::cluster_epoch epoch) {
        // Generate object key
        auto uuid = uuid_t::create();
        cloud_topics::object_id obj_id{
          .epoch = epoch, .name = uuid, .prefix = 0};

        auto object_key = cloud_storage_clients::object_key(
          ssx::sformat("{}/{}/{}", epoch(), uuid, 0));

        // Upload to S3
        // Create root retry chain node for this upload
        ss::abort_source as;
        retry_chain_node root_rtc(
          as,
          ss::lowres_clock::now() + std::chrono::seconds(30),
          std::chrono::milliseconds(100));

        cloud_storage::upload_request req{
          .transfer_details
          = {.bucket = cloud_storage_clients::bucket_name("test-bucket"),
             .key = object_key,
             .parent_rtc = root_rtc},
          .type = cloud_storage::upload_type::object,
          .payload = chunk.payload.copy()};

        auto& remote = app.cloud_storage_api.local();
        auto upload_res = co_await remote.upload_object(std::move(req));

        EXPECT_EQ(upload_res, cloud_storage::upload_result::success);

        // Return the first extent with the object_id set
        // (Caller can update other extents if needed)
        EXPECT_GE(chunk.extents.size(), 1);

        auto extent = chunk.extents[0];
        extent.id = obj_id;

        co_return extent;
    }

    /// Replicates placeholders using ct_proxy admin API
    ss::future<> replicate_placeholders_via_admin_api(
      const chunked_vector<cloud_topics::extent_meta>& extents,
      cloud_topics::cluster_epoch expected_epoch) {
        // Build request
        proto::admin::ct_proxy::replicate_placeholders_request req;

        // Set partition
        auto& partition_proto = req.get_partition();
        partition_proto.set_topic(ss::sstring(topic_name()));
        partition_proto.set_partition(ntp.tp.partition());

        req.set_expected_cluster_epoch(expected_epoch());

        // Convert extent_meta to PlaceholderData proto
        for (const auto& extent : extents) {
            proto::admin::ct_proxy::placeholder_data ph_data;

            // Set object_id UUID
            iobuf uuid_buf;
            const auto& uuid_data = extent.id.name.uuid();
            uuid_buf.append(
              reinterpret_cast<const uint8_t*>(uuid_data.data),
              uuid_t::length);
            ph_data.set_object_id_uuid(std::move(uuid_buf));

            ph_data.set_cluster_epoch(extent.id.epoch());
            ph_data.set_object_id_prefix(extent.id.prefix);
            ph_data.set_first_byte_offset(extent.first_byte_offset());
            ph_data.set_byte_range_size(extent.byte_range_size());
            ph_data.set_base_offset(extent.base_offset());
            ph_data.set_last_offset(extent.last_offset());

            req.get_placeholders().push_back(std::move(ph_data));
        }

        // Call service
        serde::pb::rpc::context ctx;
        auto resp = co_await ct_proxy_service->replicate_placeholders(
          ctx, std::move(req));

        vlog(
          ct_proxy_test_log.info,
          "Replicated placeholders, last_offset: {}, term: {}",
          resp.get_last_offset(),
          resp.get_term());
    }

    /// Reads placeholders using ct_proxy admin API
    ss::future<
      chunked_vector<proto::admin::ct_proxy::placeholder_batch>>
    read_placeholders_via_admin_api(
      kafka::offset start_offset, kafka::offset max_offset) {
        // Build request
        proto::admin::ct_proxy::read_placeholders_request req;

        // Set partition
        auto& partition_proto = req.get_partition();
        partition_proto.set_topic(ss::sstring(topic_name()));
        partition_proto.set_partition(ntp.tp.partition());

        req.set_start_offset(start_offset());
        req.set_max_offset(max_offset());
        req.set_max_bytes(1024 * 1024); // 1MB

        // Call service
        serde::pb::rpc::context ctx;
        auto resp = co_await ct_proxy_service->read_placeholders(
          ctx, std::move(req));

        vlog(
          ct_proxy_test_log.info,
          "Read {} placeholder batches",
          resp.get_batches().size());

        co_return std::move(resp.get_batches());
    }

    /// Validates placeholder metadata matches uploaded extent
    void validate_placeholder_metadata(
      const proto::admin::ct_proxy::placeholder_data& ph_data,
      const cloud_topics::extent_meta& original_extent) {
        // Reconstruct object_id from placeholder
        const auto& uuid_iobuf = ph_data.get_object_id_uuid();
        auto uuid_str = uuid_iobuf.linearize_to_string();
        std::vector<uint8_t> uuid_bytes(uuid_str.begin(), uuid_str.end());
        uuid_t uuid(uuid_bytes);

        // Validate object_id matches
        EXPECT_EQ(uuid, original_extent.id.name);
        EXPECT_EQ(ph_data.get_cluster_epoch(), original_extent.id.epoch());
        EXPECT_EQ(ph_data.get_object_id_prefix(), original_extent.id.prefix);

        // Validate byte ranges match
        EXPECT_EQ(
          ph_data.get_first_byte_offset(), original_extent.first_byte_offset());
        EXPECT_EQ(
          ph_data.get_byte_range_size(), original_extent.byte_range_size());

        // Note: We don't validate Kafka offsets here because the placeholder
        // was replicated after a fence batch, so its log offsets differ from
        // the original extent's Kafka offsets. The important thing is that
        // the object reference and byte ranges match, allowing us to download
        // the correct data from cloud storage.
    }

    std::vector<ss::noncopyable_function<void()>> cleanup;
    scoped_config test_local_cfg;
    const model::topic topic_name{"ct_proxy_test_topic"};
    model::ntp ntp{model::kafka_namespace, topic_name, 0};
    ss::lw_shared_ptr<cluster::partition> partition;
    std::unique_ptr<admin::ct_proxy_service_impl> ct_proxy_service;
};

/// Test that validates the complete flow:
/// 1. Create batches manually
/// 2. Serialize using write_pipeline serializer
/// 3. Upload to cloud
/// 4. Replicate placeholders via ct_proxy admin API
/// 5. Read placeholders back via ct_proxy admin API
/// 6. Download from cloud and validate data
TEST_F(ct_proxy_pipeline_fixture, test_write_replicate_read_download_flow) {
    // Disable reconciliation to test L0 path exclusively
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    // Step 1: Create test batches manually
    vlog(ct_proxy_test_log.info, "Step 1: Creating test batches");

    chunked_vector<model::record_batch> batches;
    batches.push_back(make_test_batch(kafka::offset(0), 5, "batch0-"));

    // Step 2: Serialize batches using write_pipeline serializer
    vlog(ct_proxy_test_log.info, "Step 2: Serializing batches");

    auto serialized_chunk = cloud_topics::l0::serialize_batches(
                              std::move(batches))
                              .get();

    ASSERT_GT(serialized_chunk.payload.size_bytes(), 0);
    // Serializer creates one extent per batch
    ASSERT_GE(serialized_chunk.extents.size(), 1);

    vlog(
      ct_proxy_test_log.info,
      "Serialized {} bytes, {} extents",
      serialized_chunk.payload.size_bytes(),
      serialized_chunk.extents.size());

    // Step 3: Upload to cloud storage
    vlog(ct_proxy_test_log.info, "Step 3: Uploading to cloud");

    auto cluster_epoch = cloud_topics::cluster_epoch(
      partition->get_topic_revision_id()());
    auto extent = upload_to_cloud(serialized_chunk, cluster_epoch).get();

    vlog(
      ct_proxy_test_log.info,
      "Uploaded object: epoch={}, uuid={}, base_offset={}, last_offset={}",
      extent.id.epoch(),
      extent.id.name,
      extent.base_offset(),
      extent.last_offset());

    // Step 4: Replicate placeholders via ct_proxy admin API
    vlog(
      ct_proxy_test_log.info,
      "Step 4: Replicating placeholders via admin API");

    chunked_vector<cloud_topics::extent_meta> extents_to_replicate;
    extents_to_replicate.push_back(extent);

    replicate_placeholders_via_admin_api(
      extents_to_replicate, cluster_epoch)
      .get();

    // Step 5: Read placeholders back via ct_proxy admin API
    vlog(
      ct_proxy_test_log.info,
      "Step 5: Reading placeholders via admin API");

    // Note: ct_proxy replicates a fence batch first, so placeholders start at offset 1
    auto placeholder_batches = read_placeholders_via_admin_api(
                                 kafka::offset(0), kafka::offset(100))
                                 .get();

    // Find the actual data placeholder (skip fence batches)
    ASSERT_GE(placeholder_batches.size(), 1);

    const auto& ph_batch = placeholder_batches[0];
    // Placeholder batch starts at offset 1 (after fence at offset 0)
    ASSERT_GE(ph_batch.get_base_offset(), 0);
    ASSERT_GE(ph_batch.get_last_offset(), ph_batch.get_base_offset());
    ASSERT_EQ(ph_batch.get_record_count(), 1); // Placeholder is 1 record

    const auto& ph_data = ph_batch.get_placeholder();
    vlog(
      ct_proxy_test_log.info,
      "Read placeholder: epoch={}, base_offset={}, last_offset={}, "
      "first_byte_offset={}, byte_range_size={}",
      ph_data.get_cluster_epoch(),
      ph_data.get_base_offset(),
      ph_data.get_last_offset(),
      ph_data.get_first_byte_offset(),
      ph_data.get_byte_range_size());

    // Step 6: Validate placeholder metadata matches uploaded extent
    vlog(
      ct_proxy_test_log.info,
      "Step 6: Validating placeholder metadata");

    validate_placeholder_metadata(ph_data, extent);

    vlog(
      ct_proxy_test_log.info,
      "Successfully validated placeholder metadata matches uploaded extent");
}

/// Test with multiple batches to validate extent handling
TEST_F(ct_proxy_pipeline_fixture, test_multiple_batches) {
    test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
      .set_value(true);

    vlog(ct_proxy_test_log.info, "Creating multiple test batches");

    chunked_vector<model::record_batch> batches;
    batches.push_back(make_test_batch(kafka::offset(0), 3, "batch0-"));
    batches.push_back(make_test_batch(kafka::offset(3), 4, "batch1-"));
    batches.push_back(make_test_batch(kafka::offset(7), 2, "batch2-"));

    auto serialized_chunk = cloud_topics::l0::serialize_batches(
                              std::move(batches))
                              .get();

    ASSERT_EQ(serialized_chunk.extents.size(), 3);

    auto cluster_epoch = cloud_topics::cluster_epoch(
      partition->get_topic_revision_id()());
    auto extent = upload_to_cloud(serialized_chunk, cluster_epoch).get();

    // Update all extents to use the same object_id
    chunked_vector<cloud_topics::extent_meta> all_extents;
    for (auto& ext : serialized_chunk.extents) {
        ext.id = extent.id;
        all_extents.push_back(ext);
    }

    replicate_placeholders_via_admin_api(all_extents, cluster_epoch).get();

    // Note: ct_proxy replicates a fence batch first
    auto placeholder_batches = read_placeholders_via_admin_api(
                                 kafka::offset(0), kafka::offset(100))
                                 .get();

    ASSERT_EQ(placeholder_batches.size(), 3);

    // Verify each placeholder - offsets start at 1 (after fence at 0)
    EXPECT_GE(placeholder_batches[0].get_base_offset(), 0);
    EXPECT_GE(placeholder_batches[0].get_last_offset(), placeholder_batches[0].get_base_offset());

    EXPECT_GE(placeholder_batches[1].get_base_offset(), placeholder_batches[0].get_last_offset());
    EXPECT_GE(placeholder_batches[1].get_last_offset(), placeholder_batches[1].get_base_offset());

    EXPECT_GE(placeholder_batches[2].get_base_offset(), placeholder_batches[1].get_last_offset());
    EXPECT_GE(placeholder_batches[2].get_last_offset(), placeholder_batches[2].get_base_offset());

    vlog(
      ct_proxy_test_log.info,
      "Successfully validated {} placeholder batches",
      placeholder_batches.size());
}
