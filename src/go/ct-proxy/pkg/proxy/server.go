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
	"fmt"
	"io"
	"net"

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
	}, nil
}

// Start starts the ct-proxy server.
func (s *Server) Start(ctx context.Context) error {
	// Create TCP listener
	listener, err := net.Listen("tcp", s.cfg.Server.KafkaListenAddress)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	s.listener = listener

	s.logger.Info("server listening", zap.String("address", s.cfg.Server.KafkaListenAddress))

	// Accept connections
	for {
		select {
		case <-ctx.Done():
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
	// Return supported API versions
	// This is a minimal implementation
	s.logger.Debug("handling ApiVersions request")
	// TODO: Implement proper response
}

// handleMetadataRequest handles Metadata requests.
func (s *Server) handleMetadataRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request using franz-go
	// TODO: Parse request properly
	req := &kmsg.MetadataRequest{}

	// Handle via metadata handler
	resp, err := s.metadataHandler.HandleMetadata(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle metadata request", zap.Error(err))
		return
	}

	// Encode and send response
	// TODO: Encode response properly
	_ = resp
}

// handleProduceRequest handles Produce requests.
func (s *Server) handleProduceRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request using franz-go
	// TODO: Parse request properly
	req := &kmsg.ProduceRequest{}

	// Handle via producer handler
	resp, err := s.producerHandler.HandleProduce(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle produce request", zap.Error(err))
		return
	}

	// Encode and send response
	// TODO: Encode response properly
	_ = resp
}

// handleFetchRequest handles Fetch requests.
func (s *Server) handleFetchRequest(ctx context.Context, conn net.Conn, requestBody []byte) {
	// Parse request using franz-go
	// TODO: Parse request properly
	req := &kmsg.FetchRequest{}

	// Handle via consumer handler
	resp, err := s.consumerHandler.HandleFetch(ctx, req)
	if err != nil {
		s.logger.Error("failed to handle fetch request", zap.Error(err))
		return
	}

	// Encode and send response
	// TODO: Encode response properly
	_ = resp
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
