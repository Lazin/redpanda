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
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMetadataResponseSerialization(t *testing.T) {
	// Test response for v12 (flexible) with 1 broker and 0 topics
	// NOTE: AuthorizedOperations is only for v8-v10, NOT present in v11+
	resp := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
				// Rack is not set (nil)
			},
		},
		ClusterID:    nil,
		ControllerID: 0,
		Topics:       []kmsg.MetadataResponseTopic{},
	}

	respBytes := resp.AppendTo(nil)

	t.Logf("MetadataResponse v12 serialized length: %d", len(respBytes))
	t.Logf("MetadataResponse v12 serialized bytes (hex): %s", hex.EncodeToString(respBytes))

	// Parse it back to verify - MUST set Version first so franz-go knows the format!
	resp2 := &kmsg.MetadataResponse{Version: 12}
	err := resp2.ReadFrom(respBytes)
	if err != nil {
		t.Fatalf("Failed to parse back: %v", err)
	}

	t.Logf("Parsed back: ThrottleMillis=%d, Brokers=%d, Topics=%d, ControllerID=%d",
		resp2.ThrottleMillis, len(resp2.Brokers), len(resp2.Topics), resp2.ControllerID)
}

func TestMetadataResponseV12WithNilTopics(t *testing.T) {
	// Test with Topics = nil (not explicitly set)
	resp := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
		ClusterID:    nil,
		ControllerID: 0,
		// Topics not set, defaults to nil
	}

	respBytes := resp.AppendTo(nil)

	t.Logf("MetadataResponse v12 (nil topics) serialized length: %d", len(respBytes))
	t.Logf("MetadataResponse v12 (nil topics) serialized bytes (hex): %s", hex.EncodeToString(respBytes))
}

func TestMetadataResponseV12EmptyVsNil(t *testing.T) {
	// Compare empty slice vs nil slice for Topics
	respEmpty := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
		ClusterID:    nil,
		ControllerID: 0,
		Topics:       []kmsg.MetadataResponseTopic{}, // explicitly empty
	}

	respNil := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
		ClusterID:    nil,
		ControllerID: 0,
		// Topics not set, defaults to nil
	}

	emptyBytes := respEmpty.AppendTo(nil)
	nilBytes := respNil.AppendTo(nil)

	t.Logf("Empty topics length: %d, hex: %s", len(emptyBytes), hex.EncodeToString(emptyBytes))
	t.Logf("Nil topics length: %d, hex: %s", len(nilBytes), hex.EncodeToString(nilBytes))

	if len(emptyBytes) != len(nilBytes) {
		t.Logf("DIFFERENCE in serialization! Empty: %d, Nil: %d", len(emptyBytes), len(nilBytes))
	}
}

func TestMetadataResponseErrorResponse(t *testing.T) {
	// This test matches exactly what errorResponse() creates
	// ControllerID = -1 (no controller)
	resp := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
		ClusterID:    nil,
		ControllerID: -1, // -1 indicates no controller
		Topics:       []kmsg.MetadataResponseTopic{},
	}

	respBytes := resp.AppendTo(nil)

	t.Logf("errorResponse v12 serialized length: %d", len(respBytes))
	t.Logf("errorResponse v12 serialized bytes (hex): %s", hex.EncodeToString(respBytes))

	// The full message sent over wire for flexible response:
	// Size (4) + Correlation ID (4) + TAG_BUFFER (1) + Body
	// Size value = 4 + 1 + len(respBytes) = 5 + len(respBytes)
	fullMessageSize := 4 + 1 + len(respBytes) // correlation + tag + body
	t.Logf("Full message size (after size field): %d", fullMessageSize)
	t.Logf("Total bytes written to socket: %d", 4+fullMessageSize)

	// Verify it can be parsed back
	resp2 := &kmsg.MetadataResponse{Version: 12}
	err := resp2.ReadFrom(respBytes)
	if err != nil {
		t.Fatalf("Failed to parse back: %v", err)
	}

	t.Logf("Parsed back: ThrottleMillis=%d, Brokers=%d, Topics=%d, ControllerID=%d",
		resp2.ThrottleMillis, len(resp2.Brokers), len(resp2.Topics), resp2.ControllerID)
}

func TestMetadataRequestParsing(t *testing.T) {
	// Test parsing a Metadata v12 request like the one rpk sends
	// Create request
	req := &kmsg.MetadataRequest{
		Version:                            12,
		Topics:                             nil, // null = list all topics
		AllowAutoTopicCreation:             false,
		IncludeClusterAuthorizedOperations: false,
		IncludeTopicAuthorizedOperations:   false,
	}

	reqBytes := req.AppendTo(nil)
	t.Logf("MetadataRequest v12 body length: %d", len(reqBytes))
	t.Logf("MetadataRequest v12 body (hex): %s", hex.EncodeToString(reqBytes))

	// Now simulate parsing back (like the server would)
	req2 := &kmsg.MetadataRequest{Version: 12}
	err := req2.ReadFrom(reqBytes)
	if err != nil {
		t.Fatalf("Failed to parse back: %v", err)
	}

	t.Logf("Parsed back: Topics=%v, AllowAutoTopicCreation=%v",
		req2.Topics, req2.AllowAutoTopicCreation)
}

func TestFullMetadataV12RoundTrip(t *testing.T) {
	// Simulate the full request/response cycle for Metadata v12

	// 1. Create request like rpk does
	req := &kmsg.MetadataRequest{
		Version:                            12,
		Topics:                             nil, // null = list all topics
		AllowAutoTopicCreation:             false,
		IncludeClusterAuthorizedOperations: false,
		IncludeTopicAuthorizedOperations:   false,
	}
	reqBody := req.AppendTo(nil)

	// 2. Build full request with header
	// Request header: API Key (2) + API Version (2) + Correlation ID (4) + Client ID (nullable string) + TAG_BUFFER
	clientID := "rpk"
	clientIDBytes := []byte(clientID)

	// Calculate request size (flexible header for v9+)
	headerSize := 2 + 2 + 4 + 2 + len(clientIDBytes) + 1 // apiKey + apiVersion + correlationID + clientID (non-flexible) + TAG_BUFFER
	requestSize := int32(headerSize + len(reqBody))

	fullRequest := make([]byte, 4+headerSize+len(reqBody))
	// Size field
	binary.BigEndian.PutUint32(fullRequest[0:4], uint32(requestSize))
	// API Key = 3 (Metadata)
	binary.BigEndian.PutUint16(fullRequest[4:6], 3)
	// API Version = 12
	binary.BigEndian.PutUint16(fullRequest[6:8], 12)
	// Correlation ID = 1
	binary.BigEndian.PutUint32(fullRequest[8:12], 1)
	// Client ID (non-flexible string: 2-byte length + bytes)
	binary.BigEndian.PutUint16(fullRequest[12:14], uint16(len(clientID)))
	copy(fullRequest[14:14+len(clientID)], clientIDBytes)
	// TAG_BUFFER = 0
	fullRequest[14+len(clientID)] = 0
	// Body
	copy(fullRequest[15+len(clientID):], reqBody)

	t.Logf("Full request size: %d", len(fullRequest))
	t.Logf("Request body offset: %d", 15+len(clientID))
	t.Logf("Request hex: %s", hex.EncodeToString(fullRequest))

	// 3. Create response like the server does
	resp := &kmsg.MetadataResponse{
		Version:        12,
		ThrottleMillis: 0,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
		ClusterID:    nil,
		ControllerID: -1,
		Topics:       []kmsg.MetadataResponseTopic{},
	}
	respBody := resp.AppendTo(nil)

	// 4. Build full response with header (flexible for v12)
	responseSize := int32(4 + 1 + len(respBody)) // correlation ID + TAG_BUFFER + body
	fullResponse := make([]byte, 4+4+1+len(respBody))
	binary.BigEndian.PutUint32(fullResponse[0:4], uint32(responseSize))
	binary.BigEndian.PutUint32(fullResponse[4:8], 1) // correlation ID
	fullResponse[8] = 0 // TAG_BUFFER
	copy(fullResponse[9:], respBody)

	t.Logf("Full response length: %d", len(fullResponse))
	t.Logf("Response size field: %d", responseSize)
	t.Logf("Response body length: %d", len(respBody))
	t.Logf("Response hex: %s", hex.EncodeToString(fullResponse))

	// 5. Parse response like client would
	// Read size
	sizeBytes := fullResponse[0:4]
	readSize := int32(binary.BigEndian.Uint32(sizeBytes))
	t.Logf("Client reads size: %d", readSize)

	// Read message
	message := fullResponse[4 : 4+readSize]
	t.Logf("Client reads message length: %d", len(message))

	// Parse: correlation ID + TAG_BUFFER + body
	if len(message) < 5 {
		t.Fatalf("Message too short")
	}
	corrID := int32(binary.BigEndian.Uint32(message[0:4]))
	tagBuffer := message[4]
	respBodyParsed := message[5:]

	t.Logf("Correlation ID: %d", corrID)
	t.Logf("TAG_BUFFER: %d", tagBuffer)
	t.Logf("Response body from message: %d bytes", len(respBodyParsed))

	// Parse the response body
	resp2 := &kmsg.MetadataResponse{Version: 12}
	err := resp2.ReadFrom(respBodyParsed)
	if err != nil {
		t.Fatalf("Failed to parse response body: %v (body hex: %s)", err, hex.EncodeToString(respBodyParsed))
	}

	t.Logf("Successfully parsed response: %d brokers, %d topics", len(resp2.Brokers), len(resp2.Topics))
}
