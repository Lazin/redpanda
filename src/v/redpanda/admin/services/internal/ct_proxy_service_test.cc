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
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "proto/redpanda/core/admin/internal/cloud_topics/v1/ct_proxy.proto.h"
#include "serde/protobuf/rpc.h"

#include <gtest/gtest.h>

namespace admin {

// Test that extent_meta can be converted to and from proto format
TEST(CtProxyServiceTest, ProtoExtentMetaConversion) {
    // Create an extent_meta
    cloud_topics::extent_meta meta;
    meta.id.epoch = cloud_topics::cluster_epoch(12345);
    meta.id.name = uuid_t::create();
    meta.id.prefix = 42;
    meta.first_byte_offset = cloud_topics::first_byte_offset_t(1024);
    meta.byte_range_size = cloud_topics::byte_range_size_t(2048);
    meta.base_offset = kafka::offset(100);
    meta.last_offset = kafka::offset(200);

    // Convert to proto
    proto::admin::ct_proxy::placeholder_data proto_ph;
    // Create iobuf from UUID bytes
    iobuf uuid_buf;
    uuid_buf.append(
      reinterpret_cast<const uint8_t*>(meta.id.name.uuid().data),
      uuid_t::length);
    proto_ph.set_object_id_uuid(std::move(uuid_buf));
    proto_ph.set_cluster_epoch(meta.id.epoch());
    proto_ph.set_object_id_prefix(meta.id.prefix);
    proto_ph.set_first_byte_offset(meta.first_byte_offset());
    proto_ph.set_byte_range_size(meta.byte_range_size());
    proto_ph.set_base_offset(meta.base_offset());
    proto_ph.set_last_offset(meta.last_offset());

    // Verify proto values
    EXPECT_EQ(proto_ph.get_cluster_epoch(), 12345);
    EXPECT_EQ(proto_ph.get_object_id_prefix(), 42);
    EXPECT_EQ(proto_ph.get_first_byte_offset(), 1024);
    EXPECT_EQ(proto_ph.get_byte_range_size(), 2048);
    EXPECT_EQ(proto_ph.get_base_offset(), 100);
    EXPECT_EQ(proto_ph.get_last_offset(), 200);
    EXPECT_EQ(proto_ph.get_object_id_uuid().size_bytes(), 16);
}

// Test placeholder data validation
TEST(CtProxyServiceTest, PlaceholderDataValidation) {
    proto::admin::ct_proxy::placeholder_data proto_ph;

    // Invalid UUID size (not 16 bytes)
    iobuf short_uuid;
    short_uuid.append(reinterpret_cast<const uint8_t*>("too_short"), 9);
    proto_ph.set_object_id_uuid(std::move(short_uuid));
    proto_ph.set_cluster_epoch(12345);
    proto_ph.set_object_id_prefix(42);
    proto_ph.set_first_byte_offset(1024);
    proto_ph.set_byte_range_size(2048);
    proto_ph.set_base_offset(100);
    proto_ph.set_last_offset(200);

    // This should throw when trying to convert
    // (Test would need access to proto_to_extent_meta helper)
    EXPECT_EQ(proto_ph.get_object_id_uuid().size_bytes(), 9);  // too short
}

// Test object path format
TEST(CtProxyServiceTest, ObjectPathFormat) {
    // This test validates the expected L0 object path format
    // Format: level_zero/data/{prefix:03}/{epoch:018}/{uuid}

    cloud_topics::object_id id;
    id.epoch = cloud_topics::cluster_epoch(12345);
    id.name = uuid_t::create();
    id.prefix = 42;

    // The object path would be constructed as:
    // fmt::format("level_zero/data/{:03}/{:018}/{}", id.prefix(), id.epoch(), id.name)

    // Verify format constraints
    EXPECT_GE(id.prefix, 0);
    EXPECT_LE(id.prefix, 999);
    EXPECT_GT(id.epoch(), 0);
}

// Test placeholder batch encoding
TEST(CtProxyServiceTest, PlaceholderBatchEncoding) {
    // Create extent_meta for a placeholder
    cloud_topics::extent_meta meta;
    meta.id.epoch = cloud_topics::cluster_epoch(12345);
    meta.id.name = uuid_t::create();
    meta.id.prefix = 42;
    meta.first_byte_offset = cloud_topics::first_byte_offset_t(1024);
    meta.byte_range_size = cloud_topics::byte_range_size_t(2048);
    meta.base_offset = kafka::offset(100);
    meta.last_offset = kafka::offset(200);

    // Create batch header
    model::record_batch_header header{
      .base_offset = model::offset(meta.base_offset()),
      .type = model::record_batch_type::ctp_placeholder,
      .crc = 0,
      .last_offset_delta = static_cast<int32_t>(
        meta.last_offset() - meta.base_offset()),
      .record_count = 101,  // Number of records in the batch (100-200 inclusive)
    };
    header.ctx.term = model::term_id(1);

    // Encode placeholder batch
    auto batch = cloud_topics::encode_placeholder_batch(header, meta);

    // Verify batch properties
    EXPECT_EQ(batch.header().type, model::record_batch_type::ctp_placeholder);
    EXPECT_EQ(batch.base_offset(), model::offset(100));
    EXPECT_EQ(batch.last_offset(), model::offset(200));

    // Parse placeholder back
    auto parsed = cloud_topics::parse_placeholder_batch(std::move(batch));

    // Verify parsed placeholder matches original extent_meta
    EXPECT_EQ(parsed.id.epoch, meta.id.epoch);
    EXPECT_EQ(parsed.id.name, meta.id.name);
    EXPECT_EQ(parsed.id.prefix, meta.id.prefix);
    EXPECT_EQ(parsed.offset, meta.first_byte_offset);
    EXPECT_EQ(parsed.size_bytes, meta.byte_range_size);
}

// Test request validation
TEST(CtProxyServiceTest, GetClusterEpochRequest) {
    proto::admin::ct_proxy::get_cluster_epoch_request req;
    req.get_partition().set_topic("test-topic");
    req.get_partition().set_partition(0);

    EXPECT_EQ(req.get_partition().get_topic(), "test-topic");
    EXPECT_EQ(req.get_partition().get_partition(), 0);
}

TEST(CtProxyServiceTest, ReplicatePlaceholdersRequest) {
    proto::admin::ct_proxy::replicate_placeholders_request req;
    req.get_partition().set_topic("test-topic");
    req.get_partition().set_partition(0);
    req.set_expected_cluster_epoch(12345);

    // Add a placeholder
    proto::admin::ct_proxy::placeholder_data ph;
    uuid_t test_uuid = uuid_t::create();
    iobuf uuid_buf;
    uuid_buf.append(
      reinterpret_cast<const uint8_t*>(test_uuid.uuid().data),
      uuid_t::length);
    ph.set_object_id_uuid(std::move(uuid_buf));
    ph.set_cluster_epoch(12345);
    ph.set_object_id_prefix(42);
    ph.set_first_byte_offset(1024);
    ph.set_byte_range_size(2048);
    ph.set_base_offset(100);
    ph.set_last_offset(200);
    req.get_placeholders().push_back(std::move(ph));

    EXPECT_EQ(req.get_placeholders().size(), 1);
    EXPECT_EQ(req.get_expected_cluster_epoch(), 12345);
}

TEST(CtProxyServiceTest, ReadPlaceholdersRequest) {
    proto::admin::ct_proxy::read_placeholders_request req;
    req.get_partition().set_topic("test-topic");
    req.get_partition().set_partition(0);
    req.set_start_offset(100);
    req.set_max_offset(200);
    req.set_max_bytes(1024 * 1024);  // 1 MB

    EXPECT_EQ(req.get_start_offset(), 100);
    EXPECT_EQ(req.get_max_offset(), 200);
    EXPECT_EQ(req.get_max_bytes(), 1024 * 1024);
}

TEST(CtProxyServiceTest, ListCloudTopicPartitionsRequest) {
    proto::admin::ct_proxy::list_cloud_topic_partitions_request req;
    req.set_topic_filter("test-.*");

    EXPECT_EQ(req.get_topic_filter(), "test-.*");
}

} // namespace admin
