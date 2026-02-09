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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCreateL0ObjectEmpty(t *testing.T) {
	// Test with empty records
	records := []*kgo.Record{}
	_, _, err := CreateL0Object(records)
	if err != ErrInvalidPlaceholder {
		t.Errorf("expected ErrInvalidPlaceholder, got %v", err)
	}
}

func TestCreateL0ObjectSingleRecord(t *testing.T) {
	// Create a single record
	records := []*kgo.Record{
		{
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Offset:    0,
			Timestamp: time.Unix(1000, 0),
		},
	}

	objectBytes, meta, err := CreateL0Object(records)
	if err != nil {
		t.Fatalf("CreateL0Object failed: %v", err)
	}

	if meta.BaseOffset != 0 {
		t.Errorf("expected base offset 0, got %d", meta.BaseOffset)
	}
	if meta.LastOffset != 0 {
		t.Errorf("expected last offset 0, got %d", meta.LastOffset)
	}
	if meta.FirstByteOffset != 0 {
		t.Errorf("expected first byte offset 0, got %d", meta.FirstByteOffset)
	}
	if meta.ByteRangeSize != uint64(len(objectBytes)) {
		t.Errorf("expected byte range size %d, got %d", len(objectBytes), meta.ByteRangeSize)
	}

	// Verify we can deserialize it back
	placeholder := &PlaceholderSerde{
		Offset:    meta.FirstByteOffset,
		SizeBytes: meta.ByteRangeSize,
	}

	decodedRecords, err := DeserializeL0Object(objectBytes, placeholder)
	if err != nil {
		t.Fatalf("DeserializeL0Object failed: %v", err)
	}

	if len(decodedRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decodedRecords))
	}

	// Verify record contents
	rec := decodedRecords[0]
	if string(rec.Key) != "key1" {
		t.Errorf("expected key 'key1', got '%s'", string(rec.Key))
	}
	if string(rec.Value) != "value1" {
		t.Errorf("expected value 'value1', got '%s'", string(rec.Value))
	}
	if rec.Offset != 0 {
		t.Errorf("expected offset 0, got %d", rec.Offset)
	}
}

func TestCreateL0ObjectMultipleRecords(t *testing.T) {
	// Create multiple records
	records := []*kgo.Record{
		{
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Offset:    100,
			Timestamp: time.Unix(1000, 0),
		},
		{
			Key:       []byte("key2"),
			Value:     []byte("value2"),
			Offset:    101,
			Timestamp: time.Unix(1001, 0),
		},
		{
			Key:       []byte("key3"),
			Value:     []byte("value3"),
			Offset:    102,
			Timestamp: time.Unix(1002, 0),
		},
	}

	objectBytes, meta, err := CreateL0Object(records)
	if err != nil {
		t.Fatalf("CreateL0Object failed: %v", err)
	}

	if meta.BaseOffset != 100 {
		t.Errorf("expected base offset 100, got %d", meta.BaseOffset)
	}
	if meta.LastOffset != 102 {
		t.Errorf("expected last offset 102, got %d", meta.LastOffset)
	}

	// Deserialize and verify
	placeholder := &PlaceholderSerde{
		Offset:    meta.FirstByteOffset,
		SizeBytes: meta.ByteRangeSize,
	}

	decodedRecords, err := DeserializeL0Object(objectBytes, placeholder)
	if err != nil {
		t.Fatalf("DeserializeL0Object failed: %v", err)
	}

	if len(decodedRecords) != 3 {
		t.Fatalf("expected 3 records, got %d", len(decodedRecords))
	}

	// Verify each record
	for i, rec := range decodedRecords {
		expectedKey := []byte("key" + string(rune('1'+i)))
		expectedValue := []byte("value" + string(rune('1'+i)))
		expectedOffset := int64(100 + i)

		if string(rec.Key) != string(expectedKey) {
			t.Errorf("record %d: expected key '%s', got '%s'", i, string(expectedKey), string(rec.Key))
		}
		if string(rec.Value) != string(expectedValue) {
			t.Errorf("record %d: expected value '%s', got '%s'", i, string(expectedValue), string(rec.Value))
		}
		if rec.Offset != expectedOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, expectedOffset, rec.Offset)
		}
	}
}

func TestCreateL0ObjectWithHeaders(t *testing.T) {
	// Create records with headers
	records := []*kgo.Record{
		{
			Key:    []byte("key1"),
			Value:  []byte("value1"),
			Offset: 0,
			Headers: []kgo.RecordHeader{
				{Key: "header1", Value: []byte("hvalue1")},
				{Key: "header2", Value: []byte("hvalue2")},
			},
			Timestamp: time.Unix(1000, 0),
		},
	}

	objectBytes, meta, err := CreateL0Object(records)
	if err != nil {
		t.Fatalf("CreateL0Object failed: %v", err)
	}

	// Deserialize and verify
	placeholder := &PlaceholderSerde{
		Offset:    meta.FirstByteOffset,
		SizeBytes: meta.ByteRangeSize,
	}

	decodedRecords, err := DeserializeL0Object(objectBytes, placeholder)
	if err != nil {
		t.Fatalf("DeserializeL0Object failed: %v", err)
	}

	if len(decodedRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decodedRecords))
	}

	rec := decodedRecords[0]
	if len(rec.Headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(rec.Headers))
	}

	if rec.Headers[0].Key != "header1" || string(rec.Headers[0].Value) != "hvalue1" {
		t.Errorf("header 0 mismatch: got %v", rec.Headers[0])
	}
	if rec.Headers[1].Key != "header2" || string(rec.Headers[1].Value) != "hvalue2" {
		t.Errorf("header 1 mismatch: got %v", rec.Headers[1])
	}
}

func TestCreateL0ObjectNilKeyValue(t *testing.T) {
	// Test with nil key and value (tombstone record)
	records := []*kgo.Record{
		{
			Key:       nil,
			Value:     nil,
			Offset:    0,
			Timestamp: time.Unix(1000, 0),
		},
	}

	objectBytes, meta, err := CreateL0Object(records)
	if err != nil {
		t.Fatalf("CreateL0Object failed: %v", err)
	}

	// Deserialize and verify
	placeholder := &PlaceholderSerde{
		Offset:    meta.FirstByteOffset,
		SizeBytes: meta.ByteRangeSize,
	}

	decodedRecords, err := DeserializeL0Object(objectBytes, placeholder)
	if err != nil {
		t.Fatalf("DeserializeL0Object failed: %v", err)
	}

	if len(decodedRecords) != 1 {
		t.Fatalf("expected 1 record, got %d", len(decodedRecords))
	}

	rec := decodedRecords[0]
	if rec.Key != nil {
		t.Errorf("expected nil key, got %v", rec.Key)
	}
	if rec.Value != nil {
		t.Errorf("expected nil value, got %v", rec.Value)
	}
}

func TestDeserializeL0ObjectOutOfRange(t *testing.T) {
	// Create a valid object
	records := []*kgo.Record{
		{
			Key:       []byte("key"),
			Value:     []byte("value"),
			Offset:    0,
			Timestamp: time.Unix(1000, 0),
		},
	}

	objectBytes, _, err := CreateL0Object(records)
	if err != nil {
		t.Fatalf("CreateL0Object failed: %v", err)
	}

	// Try to deserialize with out-of-range placeholder
	placeholder := &PlaceholderSerde{
		Offset:    0,
		SizeBytes: uint64(len(objectBytes) + 1000), // Out of range
	}

	_, err = DeserializeL0Object(objectBytes, placeholder)
	if err == nil {
		t.Error("expected error for out-of-range placeholder, got nil")
	}
}

func TestDeserializeL0ObjectInvalidBatch(t *testing.T) {
	// Try to deserialize invalid data
	invalidData := []byte{0, 1, 2, 3, 4, 5}
	placeholder := &PlaceholderSerde{
		Offset:    0,
		SizeBytes: uint64(len(invalidData)),
	}

	_, err := DeserializeL0Object(invalidData, placeholder)
	if err == nil {
		t.Error("expected error for invalid batch data, got nil")
	}
}
