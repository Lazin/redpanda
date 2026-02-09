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
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure for ct-proxy.
type Config struct {
	Server       ServerConfig       `yaml:"server"`
	Redpanda     RedpandaConfig     `yaml:"redpanda"`
	CloudStorage CloudStorageConfig `yaml:"cloud_storage"`
	CloudTopics  CloudTopicsConfig  `yaml:"cloud_topics"`
	Logging      LoggingConfig      `yaml:"logging"`
}

// ServerConfig configures the ct-proxy server listeners.
type ServerConfig struct {
	KafkaListenAddress string `yaml:"kafka_listen_address"`
	AdminListenAddress string `yaml:"admin_listen_address"`
}

// RedpandaConfig configures connection to Redpanda admin API.
type RedpandaConfig struct {
	AdminAPI AdminAPIConfig `yaml:"admin_api"`
}

// AdminAPIConfig configures the Redpanda admin API client.
type AdminAPIConfig struct {
	Addresses []string  `yaml:"addresses"`
	TLS       TLSConfig `yaml:"tls"`
}

// TLSConfig configures TLS for connections.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// CloudStorageConfig configures cloud storage access.
type CloudStorageConfig struct {
	Provider string `yaml:"provider"` // aws, gcp, azure
	Region   string `yaml:"region"`
	Bucket   string `yaml:"bucket"`
}

// CloudTopicsConfig configures cloud topics behavior.
type CloudTopicsConfig struct {
	AllowedTopics []string `yaml:"allowed_topics"`
}

// LoggingConfig configures logging behavior.
type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, text
}

// Load loads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Server.KafkaListenAddress == "" {
		return fmt.Errorf("server.kafka_listen_address is required")
	}

	if len(c.Redpanda.AdminAPI.Addresses) == 0 {
		return fmt.Errorf("redpanda.admin_api.addresses must have at least one address")
	}

	if c.CloudStorage.Provider == "" {
		return fmt.Errorf("cloud_storage.provider is required")
	}

	if c.CloudStorage.Bucket == "" {
		return fmt.Errorf("cloud_storage.bucket is required")
	}

	if len(c.CloudTopics.AllowedTopics) == 0 {
		return fmt.Errorf("cloud_topics.allowed_topics must have at least one topic")
	}

	return nil
}
