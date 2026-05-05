package cockroachreceiver

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	ConnectionString string `mapstructure:"connection_string"`

	QueryTimeout   time.Duration `mapstructure:"query_timeout"`
	QueryLimit     int           `mapstructure:"query_limit"`
	MaxQueryLength int           `mapstructure:"max_query_length"`

	MaxOpenConnections    int           `mapstructure:"max_open_connections"`
	MaxIdleConnections    int           `mapstructure:"max_idle_connections"`
	ConnectionMaxLifetime time.Duration `mapstructure:"connection_max_lifetime"`
	ConnectionMaxIdleTime time.Duration `mapstructure:"connection_max_idle_time"`

	Metrics MetricsConfig `mapstructure:"metrics"`
}

type MetricsConfig struct {
	StatementStatistics   bool `mapstructure:"statement_statistics"`
	TransactionStatistics bool `mapstructure:"transaction_statistics"`
	IndexUsageStatistics  bool `mapstructure:"index_usage_statistics"`
	ClusterQueries        bool `mapstructure:"cluster_queries"`
	ClusterSessions       bool `mapstructure:"cluster_sessions"`
	ClusterTransactions   bool `mapstructure:"cluster_transactions"`

	ClusterContendedIndexes     bool `mapstructure:"cluster_contended_indexes"`
	ClusterContendedTables      bool `mapstructure:"cluster_contended_tables"`
	ClusterContentionEvents     bool `mapstructure:"cluster_contention_events"`
	ClusterContendedKeys        bool `mapstructure:"cluster_contended_keys"`
	ClusterLocks                bool `mapstructure:"cluster_locks"`
	TransactionContentionEvents bool `mapstructure:"transaction_contention_events"`

	Jobs           bool `mapstructure:"jobs"`
	SchemaChanges  bool `mapstructure:"schema_changes"`
	RangesNoLeases bool `mapstructure:"ranges_no_leases"`
	GossipLiveness bool `mapstructure:"gossip_liveness"`
	NodeMetrics    bool `mapstructure:"node_metrics"`
	KVNodeStatus   bool `mapstructure:"kv_node_status"`
}

func (cfg *Config) Validate() error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("connection_string is required")
	}
	if cfg.CollectionInterval <= 0 {
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
	if cfg.MaxOpenConnections < 0 {
		return fmt.Errorf("max_open_connections cannot be negative")
	}
	if cfg.MaxIdleConnections < 0 {
		return fmt.Errorf("max_idle_connections cannot be negative")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 60 * time.Second,
		},
		QueryTimeout:   30 * time.Second,
		QueryLimit:     20,
		MaxQueryLength: 200,

		MaxOpenConnections:    10,
		MaxIdleConnections:    5,
		ConnectionMaxLifetime: time.Hour,
		ConnectionMaxIdleTime: 10 * time.Minute,

		Metrics: MetricsConfig{
			// Production-safe core metrics
			StatementStatistics:   true,
			TransactionStatistics: true,
			IndexUsageStatistics:  true,
			ClusterQueries:        true,
			ClusterSessions:       true,
			ClusterTransactions:   true,

			// Production-safe contention summaries
			ClusterContendedIndexes: true,
			ClusterContendedTables:  true,
			ClusterContentionEvents: true,

			// Off by default: expensive, troubleshooting-only, or unsupported on Serverless
			ClusterContendedKeys:        false,
			ClusterLocks:                false,
			TransactionContentionEvents: false,
			Jobs:                        false,
			SchemaChanges:               false,
			RangesNoLeases:              false,
			GossipLiveness:              false,
			NodeMetrics:                 false,
			KVNodeStatus:                false,
		},
	}
}
