package cockroachreceiver

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	ConnectionString               string        `mapstructure:"connection_string"`
	QueryTimeout                   time.Duration `mapstructure:"query_timeout"`
	QueryLimit                     int           `mapstructure:"query_limit"`
	MaxOpenConnections             int           `mapstructure:"max_open_connections"`
	MaxIdleConnections             int           `mapstructure:"max_idle_connections"`
	ConnectionMaxLifetime          time.Duration `mapstructure:"connection_max_lifetime"`
	ConnectionMaxIdleTime          time.Duration `mapstructure:"connection_max_idle_time"`
	Metrics                        MetricsConfig `mapstructure:"metrics"`
}

type MetricsConfig struct {
	// Production Safe - Low Overhead
	StatementStatistics    bool `mapstructure:"statement_statistics"`
	TransactionStatistics  bool `mapstructure:"transaction_statistics"`
	IndexUsageStatistics   bool `mapstructure:"index_usage_statistics"`
	ClusterQueries         bool `mapstructure:"cluster_queries"`
	ClusterSessions        bool `mapstructure:"cluster_sessions"`
	ClusterTransactions    bool `mapstructure:"cluster_transactions"`

	// Production Safe - Moderate Overhead
	ClusterContendedIndexes       bool `mapstructure:"cluster_contended_indexes"`
	ClusterContendedTables        bool `mapstructure:"cluster_contended_tables"`
	ClusterContendedKeys          bool `mapstructure:"cluster_contended_keys"`
	ClusterContentionEvents       bool `mapstructure:"cluster_contention_events"`
	ClusterLocks                  bool `mapstructure:"cluster_locks"`
	TransactionContentionEvents   bool `mapstructure:"transaction_contention_events"`

	// Not Production Safe - Expensive
	RangesNoLeases bool `mapstructure:"ranges_no_leases"`
	GossipLiveness bool `mapstructure:"gossip_liveness"`
	Jobs           bool `mapstructure:"jobs"`
	SchemaChanges  bool `mapstructure:"schema_changes"`
	NodeMetrics    bool `mapstructure:"node_metrics"`
	KVNodeStatus   bool `mapstructure:"kv_node_status"`
}

func (cfg *Config) Validate() error {
	if cfg.ConnectionString == "" {
		return errors.New("connection_string is required")
	}
	if cfg.ControllerConfig.CollectionInterval <= 0 {
		return errors.New("collection_interval must be positive")
	}
	if cfg.QueryTimeout < 0 {
		return errors.New("query_timeout must be non-negative")
	}
	if cfg.QueryLimit <= 0 {
		return errors.New("query_limit must be positive")
	}
	if cfg.MaxOpenConnections <= 0 {
		return errors.New("max_open_connections must be positive")
	}
	if cfg.MaxIdleConnections < 0 {
		return errors.New("max_idle_connections must be non-negative")
	}
	return nil
}

func createDefaultConfig() *Config {
	return &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 60 * time.Second,
		},
		QueryTimeout:          30 * time.Second,
		QueryLimit:            20,
		MaxOpenConnections:    10,
		MaxIdleConnections:    5,
		ConnectionMaxLifetime: time.Hour,
		ConnectionMaxIdleTime: 10 * time.Minute,
		Metrics: MetricsConfig{
			StatementStatistics:           true,
			TransactionStatistics:         true,
			IndexUsageStatistics:          true,
			ClusterQueries:                true,
			ClusterSessions:               true,
			ClusterTransactions:           true,
			ClusterContendedIndexes:       true,
			ClusterContendedTables:        true,
			ClusterContentionEvents:       true,
			ClusterContendedKeys:          false,
			ClusterLocks:                  false,
			TransactionContentionEvents:   false,
			RangesNoLeases:                false,
			GossipLiveness:                false,
			Jobs:                          false,
			SchemaChanges:                 false,
			NodeMetrics:                   false,
			KVNodeStatus:                  false,
		},
	}
}
