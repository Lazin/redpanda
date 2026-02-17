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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/admin"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// kafkaRequestHeader contains the parsed Kafka request header fields.
type kafkaRequestHeader struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	clientID      string
	bodyOffset    int // offset where the request body starts
}

// parseKafkaRequestHeader parses the Kafka request header and returns the header info
// and the offset where the actual request body starts.
// The request format is:
// - API Key (2 bytes)
// - API Version (2 bytes)
// - Correlation ID (4 bytes)
// - Client ID (nullable string: 2-byte length + bytes for non-flexible, varint + bytes for flexible)
// - [Tagged fields for flexible versions]
func parseKafkaRequestHeader(data []byte) (*kafkaRequestHeader, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short: need at least 8 bytes, got %d", len(data))
	}

	header := &kafkaRequestHeader{
		apiKey:        int16(binary.BigEndian.Uint16(data[0:2])),
		apiVersion:    int16(binary.BigEndian.Uint16(data[2:4])),
		correlationID: int32(binary.BigEndian.Uint32(data[4:8])),
	}

	offset := 8

	// Determine if this is a flexible version request
	// Flexible requests have a tagged fields section after the client ID (KIP-482)
	isFlexible := isFlexibleRequest(header.apiKey, header.apiVersion)

	// Parse client ID - ALWAYS uses non-flexible encoding (2-byte nullable string)
	// even for flexible API versions. The flexible encoding only affects the body
	// and adds a tagged fields section to the header.
	if offset+2 > len(data) {
		return nil, fmt.Errorf("request too short to read client ID length")
	}
	clientIDLen := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if clientIDLen >= 0 {
		if offset+int(clientIDLen) > len(data) {
			return nil, fmt.Errorf("request too short to read client ID string")
		}
		header.clientID = string(data[offset : offset+int(clientIDLen)])
		offset += int(clientIDLen)
	}

	// For flexible requests, skip the tagged fields section in the header
	if isFlexible {
		if offset >= len(data) {
			return nil, fmt.Errorf("request too short to read tagged fields")
		}
		numTags, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("failed to read tagged fields count")
		}
		offset += n
		// Sanity check - shouldn't have too many tags
		if numTags > 100 {
			return nil, fmt.Errorf("too many tagged fields: %d", numTags)
		}
		// Skip each tagged field
		for i := uint64(0); i < numTags; i++ {
			if offset >= len(data) {
				return nil, fmt.Errorf("request too short for tag %d key", i)
			}
			// Tag key (varint)
			_, n := binary.Uvarint(data[offset:])
			if n <= 0 {
				return nil, fmt.Errorf("failed to read tag key")
			}
			offset += n
			if offset >= len(data) {
				return nil, fmt.Errorf("request too short for tag %d length", i)
			}
			// Tag length (varint)
			tagLen, n := binary.Uvarint(data[offset:])
			if n <= 0 {
				return nil, fmt.Errorf("failed to read tag length")
			}
			offset += n
			// Sanity check tag length
			if offset+int(tagLen) > len(data) {
				return nil, fmt.Errorf("tag %d data extends beyond request (offset=%d, tagLen=%d, dataLen=%d)", i, offset, tagLen, len(data))
			}
			// Skip tag data
			offset += int(tagLen)
		}
	}

	header.bodyOffset = offset
	return header, nil
}

// isFlexibleRequest returns true if the given API key and version uses flexible encoding
// for the REQUEST HEADER.
// Flexible encoding was introduced in KIP-482 for most APIs starting at specific versions.
// IMPORTANT: ApiVersions is special - it ALWAYS uses non-flexible request headers
// because the client doesn't know broker capabilities until after ApiVersions completes.
func isFlexibleRequest(apiKey, apiVersion int16) bool {
	// ApiVersions NEVER uses flexible request headers for backward compatibility
	if apiKey == 18 { // ApiVersions
		return false
	}

	// Based on Kafka protocol specification
	switch apiKey {
	case 0: // Produce
		return apiVersion >= 9
	case 1: // Fetch
		return apiVersion >= 12
	case 2: // ListOffsets
		return apiVersion >= 6
	case 3: // Metadata
		return apiVersion >= 9
	default:
		// For APIs we don't explicitly handle, assume non-flexible for safety
		return false
	}
}

// Server is the main ct-proxy server that implements the Kafka protocol.
type Server struct {
	cfg             *config.Config
	logger          *zap.Logger
	listener        net.Listener
	adminClient     *admin.Client
	s3Client        *storage.S3Client
	producerHandler *ProducerHandler
	consumerHandler *ConsumerHandler
	metadataHandler *MetadataHandler
	// Simple in-memory message store for HTTP API (temporary for testing)
	messageStore map[string][]string
}

// NewServer creates a new ct-proxy server.
func NewServer(cfg *config.Config, logger *zap.Logger) (*Server, error) {
	// Create admin client
	adminClient, err := admin.NewClient(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	// Create S3 client
	s3Client, err := storage.NewS3Client(&cfg.CloudStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create handlers
	producerHandler := NewProducerHandler(adminClient, s3Client, cfg, logger)
	consumerHandler := NewConsumerHandler(adminClient, s3Client, cfg, logger)
	metadataHandler := NewMetadataHandler(adminClient, cfg, logger)

	return &Server{
		cfg:             cfg,
		logger:          logger,
		adminClient:     adminClient,
		s3Client:        s3Client,
		producerHandler: producerHandler,
		consumerHandler: consumerHandler,
		metadataHandler: metadataHandler,
		messageStore:    make(map[string][]string),
	}, nil
}

// Start starts the ct-proxy server.
func (s *Server) Start(ctx context.Context) error {
	// Create TCP listener for Kafka protocol
	listener, err := net.Listen("tcp", s.cfg.Server.KafkaListenAddress)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	s.listener = listener

	s.logger.Info("kafka server listening", zap.String("address", s.cfg.Server.KafkaListenAddress))

	// Start HTTP REST API server
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/api/health", s.handleHealth)
	httpMux.HandleFunc("/api/topics", s.handleListTopics)
	httpMux.HandleFunc("/api/produce", s.handleProduceHTTP)
	httpMux.HandleFunc("/api/consume", s.handleConsumeHTTP)
	httpMux.HandleFunc("/api/epoch", s.handleGetEpoch)

	httpServer := &http.Server{
		Addr:    s.cfg.Server.AdminListenAddress,
		Handler: httpMux,
	}

	// Start HTTP server in background
	go func() {
		s.logger.Info("http api server listening", zap.String("address", s.cfg.Server.AdminListenAddress))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", zap.Error(err))
		}
	}()

	// Accept Kafka connections
	for {
		select {
		case <-ctx.Done():
			httpServer.Shutdown(context.Background())
			return nil
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			s.logger.Error("failed to accept connection", zap.Error(err))
			continue
		}

		// Handle connection in goroutine
		go s.handleConnection(ctx, conn)
	}
}

// handleConnection handles a Kafka protocol connection.
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	s.logger.Debug("new connection", zap.String("remote", conn.RemoteAddr().String()))

	// Kafka protocol handling loop
	requestCount := 0
	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("connection context done", zap.String("remote", conn.RemoteAddr().String()))
			return
		default:
		}

		requestCount++
		s.logger.Debug("waiting for request",
			zap.String("remote", conn.RemoteAddr().String()),
			zap.Int("request_count", requestCount))

		// Read request size (4 bytes)
		sizeBuf := make([]byte, 4)
		if _, err := conn.Read(sizeBuf); err != nil {
			if err != io.EOF {
				s.logger.Error("failed to read request size", zap.Error(err))
			} else {
				s.logger.Debug("connection closed by client (EOF)",
					zap.String("remote", conn.RemoteAddr().String()),
					zap.Int("requests_handled", requestCount-1))
			}
			return
		}

		// Parse request size (first 4 bytes, big endian)
		requestSize := int32(binary.BigEndian.Uint32(sizeBuf))

		// Read the rest of the request
		requestBody := make([]byte, requestSize)
		if _, err := io.ReadFull(conn, requestBody); err != nil {
			s.logger.Error("failed to read request body", zap.Error(err))
			return
		}

		// Parse Kafka request header
		header, err := parseKafkaRequestHeader(requestBody)
		if err != nil {
			s.logger.Error("failed to parse request header",
				zap.Error(err),
				zap.Int("request_size", len(requestBody)),
				zap.Binary("first_bytes", requestBody[:min(len(requestBody), 32)]))
			return
		}

		s.logger.Debug("received request",
			zap.Int16("api_key", header.apiKey),
			zap.Int16("api_version", header.apiVersion),
			zap.Int32("correlation_id", header.correlationID),
			zap.String("client_id", header.clientID),
			zap.Int32("size", requestSize))

		// Dispatch to appropriate handler
		switch header.apiKey {
		case 18: // ApiVersions
			s.handleApiVersions(ctx, conn, requestBody, header)
		case 3: // Metadata
			s.handleMetadataRequest(ctx, conn, requestBody, header)
		case 0: // Produce
			s.handleProduceRequest(ctx, conn, requestBody, header)
		case 1: // Fetch
			s.handleFetchRequest(ctx, conn, requestBody, header)
		case 2: // ListOffsets
			s.handleListOffsetsRequest(ctx, conn, requestBody, header)
		default:
			s.logger.Warn("unsupported API key", zap.Int16("api_key", header.apiKey))
			// Send error response
		}
	}
}

// writeKafkaResponse writes a Kafka protocol response with the correlation ID.
// For flexible API versions (v9+ for most APIs), the response header includes
// a TAG_BUFFER after the correlation ID.
func (s *Server) writeKafkaResponse(conn net.Conn, header *kafkaRequestHeader, respBodyBytes []byte) error {
	// Check if this is a flexible response (needs TAG_BUFFER in header)
	isFlexible := isFlexibleRequest(header.apiKey, header.apiVersion)

	var respBytes []byte
	if isFlexible {
		// Flexible response format: Size (4) + Correlation ID (4) + TAG_BUFFER (1+) + Response Body
		// TAG_BUFFER = 0 for no tags
		responseSize := int32(4 + 1 + len(respBodyBytes)) // correlation ID + tag buffer + body

		respBytes = make([]byte, 4+4+1+len(respBodyBytes))
		// Size (4 bytes, big endian)
		binary.BigEndian.PutUint32(respBytes[0:4], uint32(responseSize))
		// Correlation ID (4 bytes)
		binary.BigEndian.PutUint32(respBytes[4:8], uint32(header.correlationID))
		// TAG_BUFFER = 0 (no tags)
		respBytes[8] = 0
		// Response body
		copy(respBytes[9:], respBodyBytes)

		s.logger.Debug("writing flexible response",
			zap.Int16("api_key", header.apiKey),
			zap.Int16("api_version", header.apiVersion),
			zap.Int32("correlation_id", header.correlationID),
			zap.Int("body_len", len(respBodyBytes)),
			zap.Int32("size_field", responseSize),
			zap.Int("total_bytes", len(respBytes)))
	} else {
		// Non-flexible response format: Size (4) + Correlation ID (4) + Response Body
		responseSize := int32(4 + len(respBodyBytes)) // correlation ID + body

		respBytes = make([]byte, 4+4+len(respBodyBytes))
		// Size (4 bytes, big endian)
		binary.BigEndian.PutUint32(respBytes[0:4], uint32(responseSize))
		// Correlation ID (4 bytes)
		binary.BigEndian.PutUint32(respBytes[4:8], uint32(header.correlationID))
		// Response body
		copy(respBytes[8:], respBodyBytes)

		s.logger.Debug("writing non-flexible response",
			zap.Int16("api_key", header.apiKey),
			zap.Int16("api_version", header.apiVersion),
			zap.Int32("correlation_id", header.correlationID),
			zap.Int("body_len", len(respBodyBytes)),
			zap.Int32("size_field", responseSize),
			zap.Int("total_bytes", len(respBytes)))
	}

	// Write response
	_, err := conn.Write(respBytes)
	return err
}

// handleApiVersions handles ApiVersions requests.
func (s *Server) handleApiVersions(ctx context.Context, conn net.Conn, requestBody []byte, header *kafkaRequestHeader) {
	s.logger.Debug("handling ApiVersions request",
		zap.Int16("request_version", header.apiVersion),
		zap.String("client_id", header.clientID))

	// Parse request - set version first, then parse body only
	req := &kmsg.ApiVersionsRequest{
		Version: header.apiVersion,
	}
	// ApiVersions request body is typically empty or contains ClientSoftwareName/Version for v3+
	bodySlice := requestBody[header.bodyOffset:]
	s.logger.Debug("ApiVersions request body",
		zap.Int("body_offset", header.bodyOffset),
		zap.Int("body_len", len(bodySlice)),
		zap.Binary("body_bytes", bodySlice))

	if len(bodySlice) > 0 {
		if err := req.ReadFrom(bodySlice); err != nil {
			s.logger.Error("failed to parse ApiVersions request",
				zap.Error(err),
				zap.Int16("version", header.apiVersion),
				zap.Int("body_len", len(bodySlice)))
			// Don't return - ApiVersions can work without parsing the body
		}
	}

	// Create response with supported API versions
	// IMPORTANT: Set Version to match the request version for correct serialization
	resp := &kmsg.ApiVersionsResponse{
		Version: header.apiVersion,
	}
	resp.ErrorCode = 0 // No error

	// Add minimal set of supported APIs
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{
			ApiKey:     0, // Produce
			MinVersion: 0,
			MaxVersion: 9,
		},
		{
			ApiKey:     1, // Fetch
			MinVersion: 0,
			MaxVersion: 12,
		},
		{
			ApiKey:     2, // ListOffsets
			MinVersion: 0,
			MaxVersion: 7,
		},
		{
			ApiKey:     3, // Metadata
			MinVersion: 0,
			MaxVersion: 12,
		},
		{
			ApiKey:     18, // ApiVersions
			MinVersion: 0,
			MaxVersion: 3,
		},
	}

	// Encode response and write with correlation ID
	respBodyBytes := resp.AppendTo(nil)

	s.logger.Debug("ApiVersions response encoded",
		zap.Int16("version", header.apiVersion),
		zap.Int("api_keys_count", len(resp.ApiKeys)),
		zap.Int("response_body_size", len(respBodyBytes)))

	if err := s.writeKafkaResponse(conn, header, respBodyBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent ApiVersions response")
}

// handleMetadataRequest handles Metadata requests.
func (s *Server) handleMetadataRequest(ctx context.Context, conn net.Conn, requestBody []byte, header *kafkaRequestHeader) {
	s.logger.Debug("handling Metadata request",
		zap.Int16("request_version", header.apiVersion),
		zap.String("client_id", header.clientID),
		zap.Int("body_offset", header.bodyOffset),
		zap.Int("total_len", len(requestBody)))

	// Parse request - set version first, then parse body only
	req := &kmsg.MetadataRequest{
		Version: header.apiVersion,
	}
	if header.bodyOffset < len(requestBody) {
		if err := req.ReadFrom(requestBody[header.bodyOffset:]); err != nil {
			s.logger.Error("failed to parse Metadata request",
				zap.Error(err),
				zap.Int("body_offset", header.bodyOffset),
				zap.Int("body_len", len(requestBody)-header.bodyOffset))
			return
		}
	}

	// Handle via metadata handler
	resp, err := s.metadataHandler.HandleMetadata(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle metadata request", zap.Error(err))
		return
	}

	// Encode response and write with correlation ID
	respBodyBytes := resp.AppendTo(nil)
	s.logger.Debug("Metadata response encoded",
		zap.Int16("version", resp.Version),
		zap.Int("num_brokers", len(resp.Brokers)),
		zap.Int("num_topics", len(resp.Topics)),
		zap.Int32("controller_id", resp.ControllerID),
		zap.Int("response_body_size", len(respBodyBytes)),
		zap.String("body_hex", hex.EncodeToString(respBodyBytes)))
	if err := s.writeKafkaResponse(conn, header, respBodyBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent Metadata response")
}

// handleProduceRequest handles Produce requests.
func (s *Server) handleProduceRequest(ctx context.Context, conn net.Conn, requestBody []byte, header *kafkaRequestHeader) {
	s.logger.Info("handling Produce request",
		zap.Int16("request_version", header.apiVersion),
		zap.Int32("correlation_id", header.correlationID),
		zap.Int("body_size", len(requestBody)),
		zap.Int("body_offset", header.bodyOffset))

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in produce handler", zap.Any("panic", r))
			// Send error response even on panic
			errorResp := &kmsg.ProduceResponse{Version: header.apiVersion}
			respBodyBytes := errorResp.AppendTo(nil)
			s.writeKafkaResponse(conn, header, respBodyBytes)
		}
	}()

	// Parse request - set version first, then parse body only
	req := &kmsg.ProduceRequest{
		Version: header.apiVersion,
	}
	if header.bodyOffset < len(requestBody) {
		bodyBytes := requestBody[header.bodyOffset:]
		s.logger.Info("parsing Produce request body",
			zap.Int("body_offset", header.bodyOffset),
			zap.Int("body_len", len(bodyBytes)),
			zap.Binary("first_bytes", bodyBytes[:min(len(bodyBytes), 64)]))
		if err := req.ReadFrom(bodyBytes); err != nil {
			s.logger.Error("failed to parse Produce request",
				zap.Error(err),
				zap.Int("body_len", len(bodyBytes)))
			// Send error response - CORRUPT_MESSAGE
			errorResp := &kmsg.ProduceResponse{Version: header.apiVersion}
			respBodyBytes := errorResp.AppendTo(nil)
			s.writeKafkaResponse(conn, header, respBodyBytes)
			return
		}
	}

	s.logger.Info("parsed Produce request",
		zap.Int("num_topics", len(req.Topics)))

	// Log the topics and partitions in the request
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			s.logger.Info("produce request partition",
				zap.String("topic", t.Topic),
				zap.Int32("partition", p.Partition),
				zap.Int("records_len", len(p.Records)))
		}
	}

	// Handle via producer handler
	s.logger.Info("calling producer handler")
	resp, err := s.producerHandler.HandleProduce(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle produce request", zap.Error(err))
		// Send error response
		errorResp := &kmsg.ProduceResponse{Version: header.apiVersion}
		respBodyBytes := errorResp.AppendTo(nil)
		s.writeKafkaResponse(conn, header, respBodyBytes)
		return
	}

	s.logger.Info("produce handler completed", zap.Int("num_topics", len(resp.Topics)))

	// Encode response and write with correlation ID
	respBodyBytes := resp.AppendTo(nil)
	s.logger.Info("Produce response encoded",
		zap.Int("response_body_size", len(respBodyBytes)))
	if err := s.writeKafkaResponse(conn, header, respBodyBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Info("sent Produce response successfully")
}

// handleFetchRequest handles Fetch requests.
func (s *Server) handleFetchRequest(ctx context.Context, conn net.Conn, requestBody []byte, header *kafkaRequestHeader) {
	s.logger.Debug("handling Fetch request",
		zap.Int16("request_version", header.apiVersion))

	// Parse request - set version first, then parse body only
	req := &kmsg.FetchRequest{
		Version: header.apiVersion,
	}
	if header.bodyOffset < len(requestBody) {
		if err := req.ReadFrom(requestBody[header.bodyOffset:]); err != nil {
			s.logger.Error("failed to parse Fetch request", zap.Error(err))
			return
		}
	}

	// Handle via consumer handler
	resp, err := s.consumerHandler.HandleFetch(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle fetch request", zap.Error(err))
		return
	}

	// Encode response and write with correlation ID
	respBodyBytes := resp.AppendTo(nil)
	if err := s.writeKafkaResponse(conn, header, respBodyBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent Fetch response")
}

// handleListOffsetsRequest handles ListOffsets requests.
func (s *Server) handleListOffsetsRequest(ctx context.Context, conn net.Conn, requestBody []byte, header *kafkaRequestHeader) {
	s.logger.Debug("handling ListOffsets request",
		zap.Int16("request_version", header.apiVersion))

	// Parse request - set version first, then parse body only
	req := &kmsg.ListOffsetsRequest{
		Version: header.apiVersion,
	}
	if header.bodyOffset < len(requestBody) {
		if err := req.ReadFrom(requestBody[header.bodyOffset:]); err != nil {
			s.logger.Error("failed to parse ListOffsets request", zap.Error(err))
			return
		}
	}

	// Build response
	resp := &kmsg.ListOffsetsResponse{
		Version: header.apiVersion,
	}

	// Process each topic in the request
	for _, topicReq := range req.Topics {
		topicResp := kmsg.ListOffsetsResponseTopic{
			Topic: topicReq.Topic,
		}

		// Process each partition
		for _, partReq := range topicReq.Partitions {
			var offset int64
			var timestamp int64 = -1

			// Determine the offset based on timestamp
			// -1 = latest, -2 = earliest
			switch partReq.Timestamp {
			case -2: // Earliest offset
				offset = 0
			case -1: // Latest offset
				// For now, return 0 as we don't have offset tracking yet
				// In a real implementation, this should query the actual end offset
				offset = 0
			default:
				// Offset for specific timestamp - not supported yet
				offset = 0
			}

			partResp := kmsg.ListOffsetsResponseTopicPartition{
				Partition:   partReq.Partition,
				ErrorCode:   0,
				Timestamp:   timestamp,
				Offset:      offset,
				LeaderEpoch: 0,
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)

			s.logger.Debug("ListOffsets partition response",
				zap.String("topic", topicReq.Topic),
				zap.Int32("partition", partReq.Partition),
				zap.Int64("requested_timestamp", partReq.Timestamp),
				zap.Int64("returned_offset", offset))
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	// Encode response and write with correlation ID
	respBodyBytes := resp.AppendTo(nil)
	if err := s.writeKafkaResponse(conn, header, respBodyBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent ListOffsets response")
}

// Close closes the server and cleans up resources.
func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.adminClient != nil {
		s.adminClient.Close()
	}
	return nil
}

// HTTP REST API handlers

type ProduceRequest struct {
	Topic    string   `json:"topic"`
	Messages []string `json:"messages"`
}

type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Count     int    `json:"count"`
}

type ConsumeRequest struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int32  `json:"max_bytes,omitempty"`
}

type ConsumeResponse struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Messages  []string `json:"messages"`
}

type TopicsResponse struct {
	Topics []string `json:"topics"`
}

// HealthResponse is the response for the GET /api/health endpoint.
type HealthResponse struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := HealthResponse{
		Status: "ok",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Use metadata handler to list topics
	req := &kmsg.MetadataRequest{}
	resp, err := s.metadataHandler.HandleMetadata(r.Context(), req)
	if err != nil {
		s.logger.Error("failed to list topics", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to list topics: %v", err), http.StatusInternalServerError)
		return
	}

	// Extract topic names
	topics := make([]string, 0, len(resp.Topics))
	for _, topic := range resp.Topics {
		if topic.Topic != nil {
			topics = append(topics, *topic.Topic)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(TopicsResponse{Topics: topics})
}

func (s *Server) handleProduceHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "Topic is required", http.StatusBadRequest)
		return
	}

	s.logger.Info("producing messages via HTTP",
		zap.String("topic", req.Topic),
		zap.Int("count", len(req.Messages)))

	// Store messages in memory for now
	if s.messageStore[req.Topic] == nil {
		s.messageStore[req.Topic] = make([]string, 0)
	}
	offset := int64(len(s.messageStore[req.Topic]))
	s.messageStore[req.Topic] = append(s.messageStore[req.Topic], req.Messages...)

	resp := ProduceResponse{
		Topic:     req.Topic,
		Partition: 0,
		Offset:    offset,
		Count:     len(req.Messages),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleConsumeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ConsumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "Topic is required", http.StatusBadRequest)
		return
	}

	s.logger.Info("consuming messages via HTTP",
		zap.String("topic", req.Topic),
		zap.Int64("offset", req.Offset))

	// Retrieve messages from memory
	messages := make([]string, 0)
	if stored, ok := s.messageStore[req.Topic]; ok {
		if req.Offset < int64(len(stored)) {
			messages = stored[req.Offset:]
		}
	}

	resp := ConsumeResponse{
		Topic:     req.Topic,
		Partition: req.Partition,
		Messages:  messages,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// EpochResponse is the response for the GET /api/epoch endpoint.
type EpochResponse struct {
	ClusterEpoch int64 `json:"cluster_epoch"`
}

func (s *Server) handleGetEpoch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Topic and partition parameters are ignored - cluster epoch is global.
	// Parameters kept for backward compatibility with existing clients.
	s.logger.Info("getting cluster epoch via HTTP")

	epoch, err := s.adminClient.GetClusterEpoch(r.Context())
	if err != nil {
		s.logger.Error("failed to get cluster epoch", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to get cluster epoch: %v", err), http.StatusInternalServerError)
		return
	}

	resp := EpochResponse{
		ClusterEpoch: epoch,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// parsePartition parses a partition string to int32.
func parsePartition(s string) (int32, error) {
	var partition int64
	_, err := fmt.Sscanf(s, "%d", &partition)
	if err != nil {
		return 0, err
	}
	if partition < 0 || partition > 2147483647 {
		return 0, fmt.Errorf("partition out of range: %d", partition)
	}
	return int32(partition), nil
}
// rebuild trigger
