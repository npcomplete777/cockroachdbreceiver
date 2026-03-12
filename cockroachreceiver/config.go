// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver // import "github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
)

// Config defines the configuration for the CockroachDB receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	// ConnectionString is the PostgreSQL-compatible connection string for CockroachDB.
	ConnectionString string `mapstructure:"connection_string"`

	// QueryTimeout is the maximum duration for a single query.
	QueryTimeout time.Duration `mapstructure:"query_timeout"`

	// QueryLimit is the maximum number of rows returned per query.
	QueryLimit int `mapstructure:"query_limit"`

	// MaxQueryLength is the maximum length of query text stored in metrics.
	// Set to 0 for unlimited. Minimum value is 50.
	MaxQueryLength int `mapstructure:"max_query_length"`

	// MetricsBuilderConfig controls which metrics are enabled.
	metadata.MetricsBuilderConfig `mapstructure:",squash"`
}

func (cfg *Config) Validate() error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("connection_string is required")
	}
	if cfg.ControllerConfig.CollectionInterval <= 0 {
		return fmt.Errorf("collection_interval must be positive")
	}
	if cfg.QueryTimeout < 0 {
		return fmt.Errorf("query_timeout cannot be negative")
	}
	if cfg.QueryLimit <= 0 {
		return fmt.Errorf("query_limit must be positive")
	}
	if cfg.MaxQueryLength != 0 && cfg.MaxQueryLength < 50 {
		return fmt.Errorf("max_query_length must be at least 50 characters or 0 for unlimited (got %d)", cfg.MaxQueryLength)
	}
	return nil
}
