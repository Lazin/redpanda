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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/l0"
)

func TestExtentMetaToPlaceholderData(t *testing.T) {
	objectID := l0.GenerateObjectID(12345)
	meta := &l0.ExtentMeta{
		ID:              objectID,
		FirstByteOffset: 1024,
		ByteRangeSize:   2048,
		BaseOffset:      100,
		LastOffset:      200,
	}

	placeholder := ExtentMetaToPlaceholderData(meta)

	if placeholder.ClusterEpoch != objectID.Epoch {
		t.Errorf("epoch mismatch: %d != %d", placeholder.ClusterEpoch, objectID.Epoch)
	}

	if placeholder.ObjectIDPrefix != uint32(objectID.Prefix) {
		t.Errorf("prefix mismatch: %d != %d", placeholder.ObjectIDPrefix, objectID.Prefix)
	}

	if placeholder.FirstByteOffset != meta.FirstByteOffset {
		t.Errorf("offset mismatch: %d != %d", placeholder.FirstByteOffset, meta.FirstByteOffset)
	}

	if placeholder.ByteRangeSize != meta.ByteRangeSize {
		t.Errorf("size mismatch: %d != %d", placeholder.ByteRangeSize, meta.ByteRangeSize)
	}

	if placeholder.BaseOffset != meta.BaseOffset {
		t.Errorf("base offset mismatch: %d != %d", placeholder.BaseOffset, meta.BaseOffset)
	}

	if placeholder.LastOffset != meta.LastOffset {
		t.Errorf("last offset mismatch: %d != %d", placeholder.LastOffset, meta.LastOffset)
	}

	// Verify UUID bytes
	if len(placeholder.ObjectIDUUID) != 16 {
		t.Errorf("UUID should be 16 bytes, got %d", len(placeholder.ObjectIDUUID))
	}
}

// Additional tests would include:
// - TestNewClient
// - TestGetClusterEpoch (with mock gRPC server)
// - TestReplicatePlaceholders (with mock gRPC server)
// - TestReadPlaceholders (with mock gRPC server)
// - TestListCloudTopicPartitions (with mock gRPC server)
