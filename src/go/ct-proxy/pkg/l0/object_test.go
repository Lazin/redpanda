// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package l0

import (
	"regexp"
	"testing"
)

func TestGenerateObjectID(t *testing.T) {
	epoch := int64(12345)
	objectID := GenerateObjectID(epoch)

	if objectID.Epoch != epoch {
		t.Errorf("expected epoch %d, got %d", epoch, objectID.Epoch)
	}

	if objectID.Name.String() == "" {
		t.Error("UUID should not be empty")
	}

	if objectID.Prefix > MaxPrefix {
		t.Errorf("prefix %d exceeds max %d", objectID.Prefix, MaxPrefix)
	}
}

func TestGetObjectPath(t *testing.T) {
	objectID := GenerateObjectID(12345)
	objectID.Prefix = 42

	path := GetObjectPath(objectID)

	// Expected format: level_zero/data/{prefix:03}/{epoch:018}/{uuid}
	pattern := `^level_zero/data/042/000000000000012345/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	matched, err := regexp.MatchString(pattern, path)
	if err != nil {
		t.Fatalf("failed to compile regex: %v", err)
	}

	if !matched {
		t.Errorf("path %q does not match expected format", path)
	}
}

func TestObjectPathFormat(t *testing.T) {
	tests := []struct {
		name     string
		epoch    int64
		prefix   uint16
		expected string
	}{
		{
			name:     "zero prefix",
			epoch:    0,
			prefix:   0,
			expected: "level_zero/data/000/000000000000000000/",
		},
		{
			name:     "max prefix",
			epoch:    999999999999999999,
			prefix:   999,
			expected: "level_zero/data/999/999999999999999999/",
		},
		{
			name:     "typical values",
			epoch:    12345,
			prefix:   123,
			expected: "level_zero/data/123/000000000000012345/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objectID := ObjectID{
				Epoch:  tt.epoch,
				Prefix: tt.prefix,
			}
			path := GetObjectPath(objectID)

			if !regexp.MustCompile("^" + regexp.QuoteMeta(tt.expected)).MatchString(path) {
				t.Errorf("path %q does not start with expected prefix %q", path, tt.expected)
			}
		})
	}
}
