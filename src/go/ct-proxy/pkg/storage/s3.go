// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
)

// S3Client handles uploads and downloads of L0 objects to/from S3.
type S3Client struct {
	cfg      *config.CloudStorageConfig
	s3Client *s3.Client
	bucket   string
}

// NewS3Client creates a new S3 client.
func NewS3Client(cfg *config.CloudStorageConfig) (*S3Client, error) {
	// Load AWS configuration from environment
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCfg)

	return &S3Client{
		cfg:      cfg,
		s3Client: s3Client,
		bucket:   cfg.Bucket,
	}, nil
}

// Upload uploads an L0 object to S3.
func (s *S3Client) Upload(ctx context.Context, objectPath string, data []byte) error {
	// Create a reader from the data
	reader := bytes.NewReader(data)

	// Upload to S3
	_, err := s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(objectPath),
		Body:          reader,
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// Download downloads an L0 object from S3.
func (s *S3Client) Download(ctx context.Context, objectPath string) ([]byte, error) {
	// Get object from S3
	result, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectPath),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download from S3: %w", err)
	}
	defer result.Body.Close()

	// Read the object body
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 object: %w", err)
	}

	return data, nil
}
