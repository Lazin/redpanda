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
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// MetadataHandler handles Kafka metadata requests.
type MetadataHandler struct {
	adminClient *admin.Client
	cfg         *config.Config
	logger      *zap.Logger
	serverID    int32 // Always 0 for single broker presentation
	serverHost  string
	serverPort  int32
}

// NewMetadataHandler creates a new metadata handler.
func NewMetadataHandler(
	adminClient *admin.Client,
	cfg *config.Config,
	logger *zap.Logger,
) *MetadataHandler {
	// Parse host and port from kafka_listen_address
	// For simplicity, assume format "host:port"
	host := "localhost"
	port := int32(9092)

	// TODO: Parse actual host and port from cfg.Server.KafkaListenAddress

	return &MetadataHandler{
		adminClient: adminClient,
		cfg:         cfg,
		logger:      logger,
		serverID:    0,
		serverHost:  host,
		serverPort:  port,
	}
}

// HandleMetadata handles a Kafka metadata request.
func (h *MetadataHandler) HandleMetadata(
	ctx context.Context,
	req *kmsg.MetadataRequest,
) (*kmsg.MetadataResponse, error) {
	// Get list of cloud topic partitions from admin API
	partitions, err := h.adminClient.ListCloudTopicPartitions(ctx, "")
	if err != nil {
		h.logger.Error("failed to list cloud topic partitions", zap.Error(err))
		return h.errorResponse(), nil
	}

	// Filter by allowed topics
	allowedPartitions := h.filterAllowed(partitions)

	h.logger.Debug("listing cloud topic partitions",
		zap.Int("total", len(partitions)),
		zap.Int("allowed", len(allowedPartitions)))

	// Build response
	resp := &kmsg.MetadataResponse{
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: h.serverID,
				Host:   h.serverHost,
				Port:   h.serverPort,
			},
		},
	}

	// Group partitions by topic
	topicMap := make(map[string][]*admin.PartitionInfo)
	for _, p := range allowedPartitions {
		topicMap[p.Topic] = append(topicMap[p.Topic], p)
	}

	// Build topic responses
	for topic, parts := range topicMap {
		topicResp := kmsg.MetadataResponseTopic{
			Topic:     kmsg.StringPtr(topic),
			ErrorCode: 0,
		}

		for _, p := range parts {
			topicResp.Partitions = append(topicResp.Partitions,
				kmsg.MetadataResponseTopicPartition{
					Partition:       p.Partition,
					Leader:          h.serverID, // We are the leader
					LeaderEpoch:     0,
					Replicas:        []int32{h.serverID},
					ISR:             []int32{h.serverID},
					OfflineReplicas: []int32{},
				})
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return resp, nil
}

// filterAllowed filters partitions to only allowed topics.
func (h *MetadataHandler) filterAllowed(partitions []*admin.PartitionInfo) []*admin.PartitionInfo {
	var allowed []*admin.PartitionInfo

	allowedMap := make(map[string]bool)
	for _, topic := range h.cfg.CloudTopics.AllowedTopics {
		allowedMap[topic] = true
	}

	for _, p := range partitions {
		if allowedMap[p.Topic] {
			allowed = append(allowed, p)
		}
	}

	return allowed
}

// errorResponse creates a generic error response.
func (h *MetadataHandler) errorResponse() *kmsg.MetadataResponse {
	return &kmsg.MetadataResponse{
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: h.serverID,
				Host:   h.serverHost,
				Port:   h.serverPort,
			},
		},
		Topics: []kmsg.MetadataResponseTopic{},
	}
}
