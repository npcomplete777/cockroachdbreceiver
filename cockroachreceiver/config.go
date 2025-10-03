package cockroachreceiver

import (
	"fmt"
	"time"
)

type Config struct {
	ConnectionString string        `mapstructure:"connection_string"`
	CollectionInterval string      `mapstructure:"collection_interval"`
	
	// Connection pool settings
	MaxOpenConnections int           `mapstructure:"max_open_connections"`
	MaxIdleConnections int           `mapstructure:"max_idle_connections"`
	ConnectionMaxLifetime string    `mapstructure:"connection_max_lifetime"`
	ConnectionMaxIdleTime string    `mapstructure:"connection_max_idle_time"`
	QueryTimeout string              `mapstructure:"query_timeout"`
	
	// Selective metric collection
	Metrics MetricsConfig `mapstructure:"metrics"`
}

type MetricsConfig struct {
	// Production-safe metrics (low overhead, recommended for all environments)
	StatementStatistics    bool `mapstructure:"statement_statistics"`     // Query performance
	TransactionStatistics  bool `mapstructure:"transaction_statistics"`   // Transaction performance
	IndexUsageStatistics   bool `mapstructure:"index_usage_statistics"`   // Index usage patterns
	ClusterQueries         bool `mapstructure:"cluster_queries"`          // Active queries
	ClusterSessions        bool `mapstructure:"cluster_sessions"`         // Active sessions
	ClusterTransactions    bool `mapstructure:"cluster_transactions"`     // Active transactions
	
	// Production-safe contention metrics (moderate overhead)
	ClusterLocks              bool `mapstructure:"cluster_locks"`               // Lock states
	ClusterContendedIndexes   bool `mapstructure:"cluster_contended_indexes"`   // Contended indexes
	ClusterContendedKeys      bool `mapstructure:"cluster_contended_keys"`      // Contended keys
	ClusterContendedTables    bool `mapstructure:"cluster_contended_tables"`    // Contended tables
	ClusterContentionEvents   bool `mapstructure:"cluster_contention_events"`   // Contention history
	TransactionContentionEvents bool `mapstructure:"transaction_contention_events"` // Detailed contention
	
	// NOT production-safe (expensive RPC fan-out, use only for troubleshooting)
	RangesNoLeases    bool `mapstructure:"ranges_no_leases"`     // Range distribution (EXPENSIVE)
	GossipLiveness    bool `mapstructure:"gossip_liveness"`      // Node liveness (EXPENSIVE)
	Jobs              bool `mapstructure:"jobs"`                 // Background jobs (EXPENSIVE)
	SchemaChanges     bool `mapstructure:"schema_changes"`       // Schema operations (EXPENSIVE)
	NodeMetrics       bool `mapstructure:"node_metrics"`         // Node-level metrics (EXPENSIVE)
	KVNodeStatus      bool `mapstructure:"kv_node_status"`       // KV layer status (EXPENSIVE)
}

func (c *Config) Validate() error {
	if c.ConnectionString == "" {
		return fmt.Errorf("connection_string is required")
	}
	
	if c.CollectionInterval == "" {
		c.CollectionInterval = "60s"
	}
	
	if _, err := time.ParseDuration(c.CollectionInterval); err != nil {
		return fmt.Errorf("invalid collection_interval: %w", err)
	}
	
	// Set defaults
	if c.MaxOpenConnections == 0 {
		c.MaxOpenConnections = 10
	}
	if c.MaxIdleConnections == 0 {
		c.MaxIdleConnections = 5
	}
	if c.ConnectionMaxLifetime == "" {
		c.ConnectionMaxLifetime = "1h"
	}
	if c.ConnectionMaxIdleTime == "" {
		c.ConnectionMaxIdleTime = "10m"
	}
	if c.QueryTimeout == "" {
		c.QueryTimeout = "30s"
	}
	
	return nil
}
