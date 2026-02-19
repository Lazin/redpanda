// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	pb "github.com/redpanda-data/redpanda/proto/redpanda/core/admin/internal/cloud_topics/v1"
	pbcommon "github.com/redpanda-data/redpanda/proto/redpanda/core/common/v1"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0"
	"google.golang.org/protobuf/proto"
)

// Client wraps the Redpanda admin API client for cloud topics operations.
// Uses ConnectRPC protocol (HTTP + protobuf) to communicate with Redpanda.
type Client struct {
	baseURLs   []string
	httpClient *http.Client
}

// PlaceholderData represents a placeholder to be replicated.
type PlaceholderData struct {
	ObjectIDUUID    []byte
	ClusterEpoch    int64
	ObjectIDPrefix  uint32
	FirstByteOffset uint64
	ByteRangeSize   uint64
	BaseOffset      int64
	LastOffset      int64
}

// PlaceholderBatch represents a batch of placeholders read from the partition.
type PlaceholderBatch struct {
	Placeholder     *PlaceholderData
	BaseOffset      int64
	LastOffset      int64
	RecordCount     int32
	ProducerID      int64
	ProducerEpoch   int32
	IsTransactional bool
}

// PartitionInfo represents metadata about a cloud topic partition.
type PartitionInfo struct {
	Topic         string
	Partition     int32
	StartOffset   int64
	HighWatermark int64
	LogEndOffset  int64
	HasL1Data     bool
}

// ConnectRPC service path
const servicePath = "redpanda.core.admin.internal.cloud_topics.v1.CtProxyService"

// NewClient creates a new admin API client using ConnectRPC protocol.
func NewClient(ctx context.Context, cfg *config.Config) (*Client, error) {
	if len(cfg.Redpanda.AdminAPI.Addresses) == 0 {
		return nil, fmt.Errorf("no admin API addresses configured")
	}

	scheme := "http"
	if cfg.Redpanda.AdminAPI.TLS.Enabled {
		scheme = "https"
		// TODO: Implement TLS configuration
	}

	// Build base URLs for all admin API addresses
	baseURLs := make([]string, len(cfg.Redpanda.AdminAPI.Addresses))
	for i, address := range cfg.Redpanda.AdminAPI.Addresses {
		baseURLs[i] = fmt.Sprintf("%s://%s", scheme, address)
	}

	return &Client{
		baseURLs:   baseURLs,
		httpClient: &http.Client{},
	}, nil
}

// Close closes the admin API client connection.
func (c *Client) Close() error {
	// HTTP client doesn't need explicit closing
	return nil
}

// callRPC makes a ConnectRPC call to the server.
// It tries all configured admin API endpoints until one succeeds.
func (c *Client) callRPC(ctx context.Context, method string, req proto.Message, resp proto.Message) error {
	// Serialize request
	reqBody, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	var lastErr error
	for _, baseURL := range c.baseURLs {
		// Build URL: baseURL/servicePath/methodName
		url := fmt.Sprintf("%s/%s/%s", baseURL, servicePath, method)

		// Create HTTP request
		httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		// Set ConnectRPC headers
		httpReq.Header.Set("Content-Type", "application/proto")
		httpReq.Header.Set("Connect-Protocol-Version", "1")

		// Execute request
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("request to %s failed: %w", baseURL, err)
			continue
		}

		// Read response body
		respBody, err := io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response from %s: %w", baseURL, err)
			continue
		}

		// Check for errors - retry on 404 (partition on different node),
		// 500 (internal error, e.g. not leader), 503 (unavailable/not leader)
		if httpResp.StatusCode == http.StatusNotFound ||
			httpResp.StatusCode == http.StatusInternalServerError ||
			httpResp.StatusCode == http.StatusServiceUnavailable {
			lastErr = fmt.Errorf("RPC to %s failed with status %d: %s", baseURL, httpResp.StatusCode, string(respBody))
			continue
		}

		// For other non-OK status codes, return error without retry
		if httpResp.StatusCode != http.StatusOK {
			return fmt.Errorf("RPC failed with status %d: %s", httpResp.StatusCode, string(respBody))
		}

		// Deserialize response
		if err := proto.Unmarshal(respBody, resp); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("all admin API endpoints failed, last error: %w", lastErr)
}

// GetClusterEpoch gets the current cluster epoch.
// The cluster epoch is a global value for the entire cluster.
func (c *Client) GetClusterEpoch(ctx context.Context) (int64, error) {
	req := &pb.GetClusterEpochRequest{}
	resp := &pb.GetClusterEpochResponse{}

	if err := c.callRPC(ctx, "GetClusterEpoch", req, resp); err != nil {
		return 0, fmt.Errorf("GetClusterEpoch RPC failed: %w", err)
	}

	return resp.ClusterEpoch, nil
}

// ReplicatePlaceholders replicates placeholders to a partition.
func (c *Client) ReplicatePlaceholders(
	ctx context.Context,
	topic string,
	partition int32,
	placeholders []*PlaceholderData,
	expectedEpoch int64,
) (int64, int64, error) {
	// Convert placeholders to proto format
	protoPlaceholders := make([]*pb.PlaceholderData, len(placeholders))
	for i, ph := range placeholders {
		protoPlaceholders[i] = &pb.PlaceholderData{
			ObjectIdUuid:    ph.ObjectIDUUID,
			ClusterEpoch:    ph.ClusterEpoch,
			ObjectIdPrefix:  ph.ObjectIDPrefix,
			FirstByteOffset: ph.FirstByteOffset,
			ByteRangeSize:   ph.ByteRangeSize,
			BaseOffset:      ph.BaseOffset,
			LastOffset:      ph.LastOffset,
		}
	}

	req := &pb.ReplicatePlaceholdersRequest{
		Partition: &pbcommon.TopicPartition{
			Topic:     topic,
			Partition: partition,
		},
		Placeholders:         protoPlaceholders,
		ExpectedClusterEpoch: expectedEpoch,
	}
	resp := &pb.ReplicatePlaceholdersResponse{}

	if err := c.callRPC(ctx, "ReplicatePlaceholders", req, resp); err != nil {
		return 0, 0, fmt.Errorf("ReplicatePlaceholders RPC failed: %w", err)
	}

	return resp.LastOffset, resp.Term, nil
}

// ReadPlaceholders reads placeholders from a partition.
func (c *Client) ReadPlaceholders(
	ctx context.Context,
	topic string,
	partition int32,
	startOffset int64,
	maxOffset int64,
) ([]*PlaceholderBatch, error) {
	req := &pb.ReadPlaceholdersRequest{
		Partition: &pbcommon.TopicPartition{
			Topic:     topic,
			Partition: partition,
		},
		StartOffset: startOffset,
		MaxOffset:   maxOffset,
	}
	resp := &pb.ReadPlaceholdersResponse{}

	if err := c.callRPC(ctx, "ReadPlaceholders", req, resp); err != nil {
		return nil, fmt.Errorf("ReadPlaceholders RPC failed: %w", err)
	}

	// Convert proto batches to PlaceholderBatch
	batches := make([]*PlaceholderBatch, len(resp.Batches))
	for i, protoBatch := range resp.Batches {
		batches[i] = &PlaceholderBatch{
			Placeholder: &PlaceholderData{
				ObjectIDUUID:    protoBatch.Placeholder.ObjectIdUuid,
				ClusterEpoch:    protoBatch.Placeholder.ClusterEpoch,
				ObjectIDPrefix:  protoBatch.Placeholder.ObjectIdPrefix,
				FirstByteOffset: protoBatch.Placeholder.FirstByteOffset,
				ByteRangeSize:   protoBatch.Placeholder.ByteRangeSize,
				BaseOffset:      protoBatch.Placeholder.BaseOffset,
				LastOffset:      protoBatch.Placeholder.LastOffset,
			},
			BaseOffset:      protoBatch.BaseOffset,
			LastOffset:      protoBatch.LastOffset,
			RecordCount:     protoBatch.RecordCount,
			ProducerID:      protoBatch.ProducerId,
			ProducerEpoch:   protoBatch.ProducerEpoch,
			IsTransactional: protoBatch.IsTransactional,
		}
	}

	return batches, nil
}

// ListCloudTopicPartitions lists all cloud topic partitions.
func (c *Client) ListCloudTopicPartitions(ctx context.Context, topicFilter string) ([]*PartitionInfo, error) {
	req := &pb.ListCloudTopicPartitionsRequest{}
	if topicFilter != "" {
		req.TopicFilter = &topicFilter
	}
	resp := &pb.ListCloudTopicPartitionsResponse{}

	if err := c.callRPC(ctx, "ListCloudTopicPartitions", req, resp); err != nil {
		return nil, fmt.Errorf("ListCloudTopicPartitions RPC failed: %w", err)
	}

	// Convert proto partitions to PartitionInfo
	partitions := make([]*PartitionInfo, len(resp.Partitions))
	for i, protoPartition := range resp.Partitions {
		partitions[i] = &PartitionInfo{
			Topic:         protoPartition.Topic,
			Partition:     protoPartition.Partition,
			StartOffset:   protoPartition.StartOffset,
			HighWatermark: protoPartition.HighWatermark,
			LogEndOffset:  protoPartition.LogEndOffset,
			HasL1Data:     protoPartition.HasL1Data,
		}
	}

	return partitions, nil
}

// ExtentMetaToPlaceholderData converts l0.ExtentMeta to PlaceholderData.
func ExtentMetaToPlaceholderData(meta *l0.ExtentMeta) *PlaceholderData {
	uuidBytes, _ := meta.ID.Name.MarshalBinary()
	return &PlaceholderData{
		ObjectIDUUID:    uuidBytes,
		ClusterEpoch:    meta.ID.Epoch,
		ObjectIDPrefix:  uint32(meta.ID.Prefix),
		FirstByteOffset: meta.FirstByteOffset,
		ByteRangeSize:   meta.ByteRangeSize,
		BaseOffset:      meta.BaseOffset,
		LastOffset:      meta.LastOffset,
	}
}
