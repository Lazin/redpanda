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
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read request header (request size + API key + API version + correlation ID + client ID)
		header := make([]byte, 4)
		if _, err := conn.Read(header); err != nil {
			if err != io.EOF {
				s.logger.Error("failed to read request header", zap.Error(err))
			}
			return
		}

		// Parse request size (first 4 bytes, big endian)
		requestSize := int32(header[0])<<24 | int32(header[1])<<16 | int32(header[2])<<8 | int32(header[3])

		// Read the rest of the request
		requestBody := make([]byte, requestSize)
		if _, err := io.ReadFull(conn, requestBody); err != nil {
			s.logger.Error("failed to read request body", zap.Error(err))
			return
		}

		// Parse API key (next 2 bytes)
		apiKey := int16(requestBody[0])<<8 | int16(requestBody[1])

		s.logger.Debug("received request",
			zap.Int16("api_key", apiKey),
			zap.Int32("size", requestSize))

		// Dispatch to appropriate handler
		// Note: This is a simplified implementation
		// Full implementation would use franz-go's kmsg package to parse requests
		switch apiKey {
		case 18: // ApiVersions
			s.handleApiVersions(ctx, conn, requestBody)
		case 3: // Metadata
			s.handleMetadataRequest(ctx, conn, requestBody)
		case 0: // Produce
			s.handleProduceRequest(ctx, conn, requestBody)
		case 1: // Fetch
			s.handleFetchRequest(ctx, conn, requestBody)
		default:
			s.logger.Warn("unsupported API key", zap.Int16("api_key", apiKey))
			// Send error response
		}
	}
}

// handleApiVersions handles ApiVersions requests.
func (s *Server) handleApiVersions(ctx context.Context, conn net.Conn, requestBody []byte) {
	s.logger.Debug("handling ApiVersions request")

	// Parse request
	req := &kmsg.ApiVersionsRequest{}
	if err := req.ReadFrom(requestBody); err != nil {
		s.logger.Error("failed to parse ApiVersions request", zap.Error(err))
		return
	}

	// Create response with supported API versions
	resp := &kmsg.ApiVersionsResponse{}
	resp.ErrorCode = 0 // No error

	// Add minimal set of supported APIs
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{
			ApiKey:     0,  // Produce
			MinVersion: 0,
			MaxVersion: 9,
		},
		{
			ApiKey:     1,  // Fetch
			MinVersion: 0,
			MaxVersion: 12,
		},
		{
			ApiKey:     3,  // Metadata
			MinVersion: 0,
			MaxVersion: 12,
		},
		{
			ApiKey:     18, // ApiVersions
			MinVersion: 0,
			MaxVersion: 3,
		},
	}

	// Encode response
	respBytes := resp.AppendTo(nil)

	// Send response size (4 bytes, big endian)
	sizeBytes := make([]byte, 4)
	responseSize := int32(len(respBytes))
	sizeBytes[0] = byte(responseSize >> 24)
	sizeBytes[1] = byte(responseSize >> 16)
	sizeBytes[2] = byte(responseSize >> 8)
	sizeBytes[3] = byte(responseSize)

	// Write size + response
	if _, err := conn.Write(sizeBytes); err != nil {
		s.logger.Error("failed to write response size", zap.Error(err))
		return
	}
	if _, err := conn.Write(respBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent ApiVersions response")
}

// handleMetadataRequest handles Metadata requests.
func (s *Server) handleMetadataRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request
	req := &kmsg.MetadataRequest{}
	if err := req.ReadFrom(requestBody); err != nil {
		s.logger.Error("failed to parse Metadata request", zap.Error(err))
		return
	}

	// Handle via metadata handler
	resp, err := s.metadataHandler.HandleMetadata(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle metadata request", zap.Error(err))
		return
	}

	// Encode response
	respBytes := resp.AppendTo(nil)

	// Send response size (4 bytes, big endian)
	sizeBytes := make([]byte, 4)
	responseSize := int32(len(respBytes))
	sizeBytes[0] = byte(responseSize >> 24)
	sizeBytes[1] = byte(responseSize >> 16)
	sizeBytes[2] = byte(responseSize >> 8)
	sizeBytes[3] = byte(responseSize)

	// Write size + response
	if _, err := conn.Write(sizeBytes); err != nil {
		s.logger.Error("failed to write response size", zap.Error(err))
		return
	}
	if _, err := conn.Write(respBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent Metadata response")
}

// handleProduceRequest handles Produce requests.
func (s *Server) handleProduceRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request
	req := &kmsg.ProduceRequest{}
	if err := req.ReadFrom(requestBody); err != nil {
		s.logger.Error("failed to parse Produce request", zap.Error(err))
		return
	}

	// Handle via producer handler
	resp, err := s.producerHandler.HandleProduce(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle produce request", zap.Error(err))
		return
	}

	// Encode response
	respBytes := resp.AppendTo(nil)

	// Send response size (4 bytes, big endian)
	sizeBytes := make([]byte, 4)
	responseSize := int32(len(respBytes))
	sizeBytes[0] = byte(responseSize >> 24)
	sizeBytes[1] = byte(responseSize >> 16)
	sizeBytes[2] = byte(responseSize >> 8)
	sizeBytes[3] = byte(responseSize)

	// Write size + response
	if _, err := conn.Write(sizeBytes); err != nil {
		s.logger.Error("failed to write response size", zap.Error(err))
		return
	}
	if _, err := conn.Write(respBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent Produce response")
}

// handleFetchRequest handles Fetch requests.
func (s *Server) handleFetchRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request
	req := &kmsg.FetchRequest{}
	if err := req.ReadFrom(requestBody); err != nil {
		s.logger.Error("failed to parse Fetch request", zap.Error(err))
		return
	}

	// Handle via consumer handler
	resp, err := s.consumerHandler.HandleFetch(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle fetch request", zap.Error(err))
		return
	}

	// Encode response
	respBytes := resp.AppendTo(nil)

	// Send response size (4 bytes, big endian)
	sizeBytes := make([]byte, 4)
	responseSize := int32(len(respBytes))
	sizeBytes[0] = byte(responseSize >> 24)
	sizeBytes[1] = byte(responseSize >> 16)
	sizeBytes[2] = byte(responseSize >> 8)
	sizeBytes[3] = byte(responseSize)

	// Write size + response
	if _, err := conn.Write(sizeBytes); err != nil {
		s.logger.Error("failed to write response size", zap.Error(err))
		return
	}
	if _, err := conn.Write(respBytes); err != nil {
		s.logger.Error("failed to write response", zap.Error(err))
		return
	}

	s.logger.Debug("sent Fetch response")
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
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	ClusterEpoch int64  `json:"cluster_epoch"`
}

func (s *Server) handleGetEpoch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic query parameter is required", http.StatusBadRequest)
		return
	}

	partitionStr := r.URL.Query().Get("partition")
	var partition int32 = 0
	if partitionStr != "" {
		p, err := parsePartition(partitionStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid partition: %v", err), http.StatusBadRequest)
			return
		}
		partition = p
	}

	s.logger.Info("getting cluster epoch via HTTP",
		zap.String("topic", topic),
		zap.Int32("partition", partition))

	epoch, err := s.adminClient.GetClusterEpoch(r.Context(), topic, partition)
	if err != nil {
		s.logger.Error("failed to get cluster epoch", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to get cluster epoch: %v", err), http.StatusInternalServerError)
		return
	}

	resp := EpochResponse{
		Topic:        topic,
		Partition:    partition,
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
