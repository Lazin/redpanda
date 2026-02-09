// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				Server: ServerConfig{
					KafkaListenAddress: "0.0.0.0:9092",
				},
				Redpanda: RedpandaConfig{
					AdminAPI: AdminAPIConfig{
						Addresses: []string{"localhost:9644"},
					},
				},
				CloudStorage: CloudStorageConfig{
					Provider: "aws",
					Bucket:   "test-bucket",
				},
				CloudTopics: CloudTopicsConfig{
					AllowedTopics: []string{"topic1"},
				},
			},
			wantError: false,
		},
		{
			name: "missing kafka listen address",
			cfg: &Config{
				Redpanda: RedpandaConfig{
					AdminAPI: AdminAPIConfig{
						Addresses: []string{"localhost:9644"},
					},
				},
				CloudStorage: CloudStorageConfig{
					Provider: "aws",
					Bucket:   "test-bucket",
				},
				CloudTopics: CloudTopicsConfig{
					AllowedTopics: []string{"topic1"},
				},
			},
			wantError: true,
		},
		{
			name: "missing admin API addresses",
			cfg: &Config{
				Server: ServerConfig{
					KafkaListenAddress: "0.0.0.0:9092",
				},
				CloudStorage: CloudStorageConfig{
					Provider: "aws",
					Bucket:   "test-bucket",
				},
				CloudTopics: CloudTopicsConfig{
					AllowedTopics: []string{"topic1"},
				},
			},
			wantError: true,
		},
		{
			name: "missing cloud storage bucket",
			cfg: &Config{
				Server: ServerConfig{
					KafkaListenAddress: "0.0.0.0:9092",
				},
				Redpanda: RedpandaConfig{
					AdminAPI: AdminAPIConfig{
						Addresses: []string{"localhost:9644"},
					},
				},
				CloudStorage: CloudStorageConfig{
					Provider: "aws",
				},
				CloudTopics: CloudTopicsConfig{
					AllowedTopics: []string{"topic1"},
				},
			},
			wantError: true,
		},
		{
			name: "empty allowed topics",
			cfg: &Config{
				Server: ServerConfig{
					KafkaListenAddress: "0.0.0.0:9092",
				},
				Redpanda: RedpandaConfig{
					AdminAPI: AdminAPIConfig{
						Addresses: []string{"localhost:9644"},
					},
				},
				CloudStorage: CloudStorageConfig{
					Provider: "aws",
					Bucket:   "test-bucket",
				},
				CloudTopics: CloudTopicsConfig{
					AllowedTopics: []string{},
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.yaml")

	configContent := `
server:
  kafka_listen_address: "0.0.0.0:9092"

redpanda:
  admin_api:
    addresses:
      - "localhost:9644"

cloud_storage:
  provider: "aws"
  region: "us-west-2"
  bucket: "test-bucket"

cloud_topics:
  allowed_topics:
    - "test-topic"

logging:
  level: "info"
  format: "json"
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Load the config
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Validate loaded values
	if cfg.Server.KafkaListenAddress != "0.0.0.0:9092" {
		t.Errorf("unexpected kafka_listen_address: %s", cfg.Server.KafkaListenAddress)
	}

	if len(cfg.Redpanda.AdminAPI.Addresses) != 1 || cfg.Redpanda.AdminAPI.Addresses[0] != "localhost:9644" {
		t.Errorf("unexpected admin API addresses: %v", cfg.Redpanda.AdminAPI.Addresses)
	}

	if cfg.CloudStorage.Bucket != "test-bucket" {
		t.Errorf("unexpected bucket: %s", cfg.CloudStorage.Bucket)
	}

	if len(cfg.CloudTopics.AllowedTopics) != 1 || cfg.CloudTopics.AllowedTopics[0] != "test-topic" {
		t.Errorf("unexpected allowed topics: %v", cfg.CloudTopics.AllowedTopics)
	}
}
