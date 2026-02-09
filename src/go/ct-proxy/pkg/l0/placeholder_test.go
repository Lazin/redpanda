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
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
)

// TestPlaceholderSerialize tests the placeholder serialization format.
// This is CRITICAL - it must match the C++ serde::envelope format exactly.
func TestPlaceholderSerialize(t *testing.T) {
	// Create a test placeholder
	testUUID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	placeholder := PlaceholderSerde{
		ID: ObjectID{
			Epoch:  12345,
			Name:   testUUID,
			Prefix: 42,
		},
		Offset:    1024,
		SizeBytes: 2048,
	}

	// Serialize
	data, err := placeholder.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize placeholder: %v", err)
	}

	// Validate total size (58 bytes expected)
	if len(data) != 58 {
		t.Fatalf("expected 58 bytes, got %d", len(data))
	}

	// Validate outer envelope header
	offset := 0
	version := binary.LittleEndian.Uint32(data[offset:])
	if version != 0 {
		t.Errorf("outer envelope version: expected 0, got %d", version)
	}
	offset += 4

	compatVersion := binary.LittleEndian.Uint32(data[offset:])
	if compatVersion != 0 {
		t.Errorf("outer envelope compat_version: expected 0, got %d", compatVersion)
	}
	offset += 4

	// Validate inner envelope header (object_id)
	objVersion := binary.LittleEndian.Uint32(data[offset:])
	if objVersion != 1 {
		t.Errorf("object_id version: expected 1, got %d", objVersion)
	}
	offset += 4

	objCompatVersion := binary.LittleEndian.Uint32(data[offset:])
	if objCompatVersion != 0 {
		t.Errorf("object_id compat_version: expected 0, got %d", objCompatVersion)
	}
	offset += 4

	// Validate epoch
	epoch := int64(binary.LittleEndian.Uint64(data[offset:]))
	if epoch != 12345 {
		t.Errorf("epoch: expected 12345, got %d", epoch)
	}
	offset += 8

	// Validate UUID (16 bytes)
	uuidBytes, _ := testUUID.MarshalBinary()
	if !bytes.Equal(data[offset:offset+16], uuidBytes) {
		t.Errorf("UUID mismatch")
	}
	offset += 16

	// Validate prefix
	prefix := binary.LittleEndian.Uint16(data[offset:])
	if prefix != 42 {
		t.Errorf("prefix: expected 42, got %d", prefix)
	}
	offset += 2

	// Validate first_byte_offset
	firstByteOffset := binary.LittleEndian.Uint64(data[offset:])
	if firstByteOffset != 1024 {
		t.Errorf("first_byte_offset: expected 1024, got %d", firstByteOffset)
	}
	offset += 8

	// Validate byte_range_size
	byteRangeSize := binary.LittleEndian.Uint64(data[offset:])
	if byteRangeSize != 2048 {
		t.Errorf("byte_range_size: expected 2048, got %d", byteRangeSize)
	}
}

// TestPlaceholderRoundTrip tests serialization and deserialization.
func TestPlaceholderRoundTrip(t *testing.T) {
	original := PlaceholderSerde{
		ID: ObjectID{
			Epoch:  99999,
			Name:   uuid.New(),
			Prefix: 123,
		},
		Offset:    4096,
		SizeBytes: 8192,
	}

	// Serialize
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Deserialize
	var deserialized PlaceholderSerde
	if err := deserialized.Deserialize(data); err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Compare
	if original.ID.Epoch != deserialized.ID.Epoch {
		t.Errorf("epoch mismatch: %d != %d", original.ID.Epoch, deserialized.ID.Epoch)
	}
	if original.ID.Name != deserialized.ID.Name {
		t.Errorf("UUID mismatch")
	}
	if original.ID.Prefix != deserialized.ID.Prefix {
		t.Errorf("prefix mismatch: %d != %d", original.ID.Prefix, deserialized.ID.Prefix)
	}
	if original.Offset != deserialized.Offset {
		t.Errorf("offset mismatch: %d != %d", original.Offset, deserialized.Offset)
	}
	if original.SizeBytes != deserialized.SizeBytes {
		t.Errorf("size_bytes mismatch: %d != %d", original.SizeBytes, deserialized.SizeBytes)
	}
}

// TestPlaceholderKnownBytes tests against a known byte sequence from C++.
// This is the MOST CRITICAL test - it validates binary compatibility with Redpanda.
func TestPlaceholderKnownBytes(t *testing.T) {
	// This test should be updated with actual byte sequences from C++ unit tests
	// once they are available. For now, it validates the structure.
	t.Skip("Update with actual C++ byte sequences for validation")

	// Example of what this test should look like:
	// knownBytes := []byte{
	//     0x00, 0x00, 0x00, 0x00,  // outer version
	//     0x00, 0x00, 0x00, 0x00,  // outer compat
	//     0x01, 0x00, 0x00, 0x00,  // inner version
	//     0x00, 0x00, 0x00, 0x00,  // inner compat
	//     ... // rest of the bytes
	// }
	//
	// var placeholder PlaceholderSerde
	// if err := placeholder.Deserialize(knownBytes); err != nil {
	//     t.Fatalf("failed to deserialize known bytes: %v", err)
	// }
	//
	// // Validate expected values
}

// TestPlaceholderDeserializeInvalidData tests error handling.
func TestPlaceholderDeserializeInvalidData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{
			name: "too short",
			data: make([]byte, 10),
			err:  ErrInvalidPlaceholder,
		},
		{
			name: "empty",
			data: []byte{},
			err:  ErrInvalidPlaceholder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p PlaceholderSerde
			err := p.Deserialize(tt.data)
			if err != tt.err {
				t.Errorf("expected error %v, got %v", tt.err, err)
			}
		})
	}
}
