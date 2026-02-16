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

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// ConsumerHandler handles Kafka fetch requests.
type ConsumerHandler struct {
	adminClient *admin.Client
	s3Client    *storage.S3Client
	cfg         *config.Config
	logger      *zap.Logger
}

// NewConsumerHandler creates a new consumer handler.
func NewConsumerHandler(
	adminClient *admin.Client,
	s3Client *storage.S3Client,
	cfg *config.Config,
	logger *zap.Logger,
) *ConsumerHandler {
	return &ConsumerHandler{
		adminClient: adminClient,
		s3Client:    s3Client,
		cfg:         cfg,
		logger:      logger,
	}
}

// HandleFetch handles a Kafka fetch request.
func (h *ConsumerHandler) HandleFetch(
	ctx context.Context,
	req *kmsg.FetchRequest,
) (*kmsg.FetchResponse, error) {
	// Set response version to match request for correct serialization
	resp := &kmsg.FetchResponse{
		Version: req.Version,
	}

	// Process each topic
	for _, topicReq := range req.Topics {
		if !h.isAllowedTopic(topicReq.Topic) {
			h.logger.Warn("rejecting fetch from disallowed topic",
				zap.String("topic", topicReq.Topic))
			continue
		}

		topicResp := kmsg.FetchResponseTopic{Topic: topicReq.Topic}

		// Process each partition
		for _, partReq := range topicReq.Partitions {
			partResp := h.handlePartitionFetch(ctx, topicReq.Topic, partReq)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

// handlePartitionFetch handles fetch for a single partition.
func (h *ConsumerHandler) handlePartitionFetch(
	ctx context.Context,
	topic string,
	req kmsg.FetchRequestTopicPartition,
) kmsg.FetchResponseTopicPartition {
	logger := h.logger.With(
		zap.String("topic", topic),
		zap.Int32("partition", req.Partition),
		zap.Int64("fetch_offset", req.FetchOffset))

	// 1. Read placeholders from admin API
	// Use a reasonable max offset (e.g., fetch_offset + 1000 batches)
	maxOffset := req.FetchOffset + 1000

	placeholders, err := h.adminClient.ReadPlaceholders(
		ctx,
		topic,
		req.Partition,
		req.FetchOffset,
		maxOffset,
	)
	if err != nil {
		logger.Error("failed to read placeholders", zap.Error(err))
		return h.partitionError(req.Partition, 1)
	}

	logger.Debug("read placeholders", zap.Int("count", len(placeholders)))

	if len(placeholders) == 0 {
		// No data available at this offset
		return kmsg.FetchResponseTopicPartition{
			Partition:     req.Partition,
			ErrorCode:     0,
			HighWatermark: req.FetchOffset,
		}
	}

	// 2. Fetch L0 objects from S3 and materialize records
	var allRecordsBytes []byte

	for _, ph := range placeholders {
		// Build object ID from placeholder
		objectID := h.placeholderToObjectID(ph.Placeholder)
		objectPath := l0.GetObjectPath(objectID)

		logger.Debug("downloading L0 object",
			zap.String("path", objectPath),
			zap.Int64("base_offset", ph.BaseOffset))

		// Download from S3
		objectData, err := h.s3Client.Download(ctx, objectPath)
		if err != nil {
			logger.Error("failed to download from S3",
				zap.Error(err),
				zap.String("path", objectPath))
			continue
		}

		// Create placeholder serde for deserialization
		placeholderSerde := &l0.PlaceholderSerde{
			ID:        objectID,
			Offset:    ph.Placeholder.FirstByteOffset,
			SizeBytes: ph.Placeholder.ByteRangeSize,
		}

		// Deserialize L0 object to records
		records, err := l0.DeserializeL0Object(objectData, placeholderSerde)
		if err != nil {
			logger.Error("failed to deserialize L0 object", zap.Error(err))
			continue
		}

		logger.Debug("deserialized records", zap.Int("count", len(records)))

		// Encode records back to Kafka wire format
		// Re-encode the records as a batch
		batchBytes, _, err := l0.CreateL0Object(records)
		if err != nil {
			logger.Error("failed to encode records", zap.Error(err))
			continue
		}

		allRecordsBytes = append(allRecordsBytes, batchBytes...)
	}

	// 3. Build fetch response
	// Get high watermark (would need admin API call or cache)
	highWatermark := req.FetchOffset + int64(len(placeholders))

	return kmsg.FetchResponseTopicPartition{
		Partition:     req.Partition,
		ErrorCode:     0,
		HighWatermark: highWatermark,
		RecordBatches: allRecordsBytes,
	}
}

// placeholderToObjectID converts a placeholder to an object ID.
func (h *ConsumerHandler) placeholderToObjectID(ph *admin.PlaceholderData) l0.ObjectID {
	var objectID l0.ObjectID
	objectID.Epoch = ph.ClusterEpoch
	objectID.Prefix = uint16(ph.ObjectIDPrefix)
	objectID.Name.UnmarshalBinary(ph.ObjectIDUUID)
	return objectID
}

// isAllowedTopic checks if a topic is in the allowed topics list.
func (h *ConsumerHandler) isAllowedTopic(topic string) bool {
	for _, allowed := range h.cfg.CloudTopics.AllowedTopics {
		if topic == allowed {
			return true
		}
	}
	return false
}

// partitionError creates an error response for a partition.
func (h *ConsumerHandler) partitionError(
	partition int32,
	errorCode int16,
) kmsg.FetchResponseTopicPartition {
	return kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		ErrorCode:     errorCode,
		HighWatermark: -1,
	}
}
