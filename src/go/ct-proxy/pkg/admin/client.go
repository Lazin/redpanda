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
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the Redpanda admin API client for cloud topics operations.
type Client struct {
	conn *grpc.ClientConn
	// TODO: Add generated proto client when proto is compiled
	// client pb.CtProxyServiceClient
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
	Placeholder      *PlaceholderData
	BaseOffset       int64
	LastOffset       int64
	RecordCount      int32
	ProducerID       int64
	ProducerEpoch    int32
	IsTransactional  bool
}

// PartitionInfo represents metadata about a cloud topic partition.
type PartitionInfo struct {
	Topic          string
	Partition      int32
	StartOffset    int64
	HighWatermark  int64
	LogEndOffset   int64
	HasL1Data      bool
}

// NewClient creates a new admin API client.
func NewClient(ctx context.Context, cfg *config.Config) (*Client, error) {
	if len(cfg.Redpanda.AdminAPI.Addresses) == 0 {
		return nil, fmt.Errorf("no admin API addresses configured")
	}

	// Connect to first admin API address
	// TODO: Implement retry logic and failover
	address := cfg.Redpanda.AdminAPI.Addresses[0]

	var opts []grpc.DialOption
	if cfg.Redpanda.AdminAPI.TLS.Enabled {
		// TODO: Implement TLS configuration
		return nil, fmt.Errorf("TLS not yet implemented")
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin API: %w", err)
	}

	return &Client{
		conn: conn,
		// client: pb.NewCtProxyServiceClient(conn),
	}, nil
}

// Close closes the admin API client connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetClusterEpoch gets the current cluster epoch for a partition.
func (c *Client) GetClusterEpoch(ctx context.Context, topic string, partition int32) (int64, error) {
	// TODO: Implement using generated proto client
	// req := &pb.GetClusterEpochRequest{
	//     Partition: &pb.TopicPartition{
	//         Topic:     topic,
	//         Partition: partition,
	//     },
	// }
	// resp, err := c.client.GetClusterEpoch(ctx, req)
	// if err != nil {
	//     return 0, err
	// }
	// return resp.ClusterEpoch, nil
	return 0, fmt.Errorf("not yet implemented")
}

// ReplicatePlaceholders replicates placeholders to a partition.
func (c *Client) ReplicatePlaceholders(
	ctx context.Context,
	topic string,
	partition int32,
	placeholders []*PlaceholderData,
	expectedEpoch int64,
) (int64, int64, error) {
	// TODO: Implement using generated proto client
	// Returns: lastOffset, term, error
	return 0, 0, fmt.Errorf("not yet implemented")
}

// ReadPlaceholders reads placeholders from a partition.
func (c *Client) ReadPlaceholders(
	ctx context.Context,
	topic string,
	partition int32,
	startOffset int64,
	maxOffset int64,
) ([]*PlaceholderBatch, error) {
	// TODO: Implement using generated proto client
	return nil, fmt.Errorf("not yet implemented")
}

// ListCloudTopicPartitions lists all cloud topic partitions.
func (c *Client) ListCloudTopicPartitions(ctx context.Context, topicFilter string) ([]*PartitionInfo, error) {
	// TODO: Implement using generated proto client
	return nil, fmt.Errorf("not yet implemented")
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
