// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package proxy

import (
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/storage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// ProducerHandler handles Kafka produce requests.
type ProducerHandler struct {
	adminClient *admin.Client
	s3Client    *storage.S3Client
	cfg         *config.Config
	logger      *zap.Logger
}

// NewProducerHandler creates a new producer handler.
func NewProducerHandler(
	adminClient *admin.Client,
	s3Client *storage.S3Client,
	cfg *config.Config,
	logger *zap.Logger,
) *ProducerHandler {
	return &ProducerHandler{
		adminClient: adminClient,
		s3Client:    s3Client,
		cfg:         cfg,
		logger:      logger,
	}
}

// HandleProduce handles a Kafka produce request.
func (h *ProducerHandler) HandleProduce(
	ctx context.Context,
	req *kmsg.ProduceRequest,
) (*kmsg.ProduceResponse, error) {
	resp := &kmsg.ProduceResponse{}

	// Check for transactions (not supported)
	if req.TransactionalID != nil && *req.TransactionalID != "" {
		h.logger.Warn("rejecting transactional produce request",
			zap.String("transactional_id", *req.TransactionalID))
		return h.errorResponse("transactional produce not supported"), nil
	}

	// Process each topic
	for _, topicReq := range req.Topics {
		if !h.isAllowedTopic(topicReq.Topic) {
			h.logger.Warn("rejecting produce to disallowed topic",
				zap.String("topic", topicReq.Topic))
			continue
		}

		topicResp := kmsg.ProduceResponseTopic{Topic: topicReq.Topic}

		// Process each partition
		for _, partReq := range topicReq.Partitions {
			partResp := h.handlePartitionProduce(ctx, topicReq.Topic, partReq)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

// handlePartitionProduce handles produce for a single partition.
func (h *ProducerHandler) handlePartitionProduce(
	ctx context.Context,
	topic string,
	req kmsg.ProduceRequestTopicPartition,
) kmsg.ProduceResponseTopicPartition {
	logger := h.logger.With(
		zap.String("topic", topic),
		zap.Int32("partition", req.Partition))

	// Parse records from the request
	records, err := h.parseRecords(req.Records)
	if err != nil {
		logger.Error("failed to parse records", zap.Error(err))
		return h.partitionError(req.Partition, kmsg.ErrorCode(1)) // OFFSET_OUT_OF_RANGE
	}

	if len(records) == 0 {
		logger.Warn("no records in produce request")
		return h.partitionError(req.Partition, kmsg.ErrorCode(0))
	}

	// Check for idempotent producer (not supported without aggregation)
	if h.hasProducerID(records) {
		logger.Warn("rejecting idempotent producer request")
		return h.partitionError(req.Partition, kmsg.ErrorCode(1))
	}

	// 1. Get cluster epoch from admin API
	epoch, err := h.adminClient.GetClusterEpoch(ctx, topic, req.Partition)
	if err != nil {
		logger.Error("failed to get cluster epoch", zap.Error(err))
		return h.partitionError(req.Partition, kmsg.ErrorCode(6)) // NOT_LEADER_FOR_PARTITION
	}

	logger.Debug("got cluster epoch", zap.Int64("epoch", epoch))

	// 2. Generate object ID with epoch
	objectID := l0.GenerateObjectID(epoch)

	// 3. Create L0 object from records
	objectData, extent, err := l0.CreateL0Object(records)
	if err != nil {
		logger.Error("failed to create L0 object", zap.Error(err))
		return h.partitionError(req.Partition, kmsg.ErrorCode(1))
	}
	extent.ID = objectID

	logger.Debug("created L0 object",
		zap.String("object_id", objectID.Name.String()),
		zap.Int("size_bytes", len(objectData)))

	// 4. Upload to S3
	objectPath := l0.GetObjectPath(objectID)
	if err := h.s3Client.Upload(ctx, objectPath, objectData); err != nil {
		logger.Error("failed to upload to S3", zap.Error(err))
		return h.partitionError(req.Partition, kmsg.ErrorCode(1))
	}

	logger.Debug("uploaded to S3", zap.String("path", objectPath))

	// 5. Replicate placeholder via admin API
	placeholder := admin.ExtentMetaToPlaceholderData(extent)
	lastOffset, term, err := h.adminClient.ReplicatePlaceholders(
		ctx,
		topic,
		req.Partition,
		[]*admin.PlaceholderData{placeholder},
		epoch,
	)
	if err != nil {
		logger.Error("failed to replicate placeholder", zap.Error(err))
		return h.partitionError(req.Partition, kmsg.ErrorCode(1))
	}

	logger.Info("replicated placeholder",
		zap.Int64("last_offset", lastOffset),
		zap.Int64("term", term))

	// 6. Return success response
	return kmsg.ProduceResponseTopicPartition{
		Partition:   req.Partition,
		ErrorCode:   0,
		BaseOffset:  lastOffset,
		LogAppendTime: -1, // Not used
	}
}

// parseRecords parses records from the Kafka batch format.
func (h *ProducerHandler) parseRecords(recordsBytes []byte) ([]*kgo.Record, error) {
	if len(recordsBytes) == 0 {
		return nil, nil
	}

	// Parse Kafka record batch using franz-go
	// The recordsBytes contain one or more record batches in Kafka wire format
	var records []*kgo.Record

	// TODO: Implement full Kafka batch parsing
	// For now, return empty slice as placeholder
	// In production, this would:
	// 1. Parse the record batch header (length, magic, crc, attributes, etc.)
	// 2. Extract individual records from the batch
	// 3. Decompress if needed
	// 4. Convert to kgo.Record format

	return records, nil
}

// hasProducerID checks if any records have a producer ID set.
func (h *ProducerHandler) hasProducerID(records []*kgo.Record) bool {
	// Check if records are part of an idempotent/transactional producer
	// This would be indicated by producer metadata in the batch header
	return false // Placeholder
}

// isAllowedTopic checks if a topic is in the allowed topics list.
func (h *ProducerHandler) isAllowedTopic(topic string) bool {
	for _, allowed := range h.cfg.CloudTopics.AllowedTopics {
		if topic == allowed {
			return true
		}
	}
	return false
}

// partitionError creates an error response for a partition.
func (h *ProducerHandler) partitionError(
	partition int32,
	errorCode kmsg.ErrorCode,
) kmsg.ProduceResponseTopicPartition {
	return kmsg.ProduceResponseTopicPartition{
		Partition:   partition,
		ErrorCode:   errorCode,
		BaseOffset:  -1,
		LogAppendTime: -1,
	}
}

// errorResponse creates a generic error response.
func (h *ProducerHandler) errorResponse(msg string) *kmsg.ProduceResponse {
	// Return an empty response with error
	return &kmsg.ProduceResponse{}
}
