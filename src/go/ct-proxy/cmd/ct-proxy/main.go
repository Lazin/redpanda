// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ctproxy

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/ct-proxy/pkg/proxy"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "ct-proxy",
	Short: "Cloud Topics Kafka proxy for Redpanda",
	Long: `ct-proxy is a Kafka protocol proxy that allows external applications
to interact with Redpanda cloud topics by handling the data plane
(uploading/downloading L0 objects to/from cloud storage) while delegating
the metadata plane to Redpanda's admin API.`,
	RunE: run,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "config.yaml", "config file path")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize logger
	logger, err := initLogger(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("ct-proxy starting",
		zap.String("kafka_listen", cfg.Server.KafkaListenAddress),
		zap.Strings("redpanda_admin", cfg.Redpanda.AdminAPI.Addresses),
		zap.String("cloud_provider", cfg.CloudStorage.Provider),
		zap.String("cloud_bucket", cfg.CloudStorage.Bucket),
		zap.Strings("allowed_topics", cfg.CloudTopics.AllowedTopics))

	// Create proxy server
	server, err := proxy.NewServer(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		logger.Info("starting proxy server")
		if err := server.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		logger.Info("received shutdown signal")
		cancel()
		return nil
	case err := <-errChan:
		logger.Error("server error", zap.Error(err))
		return err
	}
}

func initLogger(cfg config.LoggingConfig) (*zap.Logger, error) {
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
	}

	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(level)

	if cfg.Format == "text" {
		zapCfg.Encoding = "console"
	} else {
		zapCfg.Encoding = "json"
	}

	return zapCfg.Build()
}
