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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"go.uber.org/zap"
)

func TestIsAllowedTopic(t *testing.T) {
	cfg := &config.Config{
		CloudTopics: config.CloudTopicsConfig{
			AllowedTopics: []string{"topic1", "topic2", "topic3"},
		},
	}

	logger := zap.NewNop()
	handler := NewProducerHandler(nil, nil, cfg, logger)

	tests := []struct {
		topic   string
		allowed bool
	}{
		{"topic1", true},
		{"topic2", true},
		{"topic3", true},
		{"topic4", false},
		{"", false},
		{"topic1-suffix", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			result := handler.isAllowedTopic(tt.topic)
			if result != tt.allowed {
				t.Errorf("isAllowedTopic(%q) = %v, want %v", tt.topic, result, tt.allowed)
			}
		})
	}
}

// Additional tests would include:
// - TestHandleProduce with mock admin client and S3 client
// - TestHandleProduceTransactionRejection
// - TestHandleProduceIdempotentRejection
// - TestPartitionProduceFlow
