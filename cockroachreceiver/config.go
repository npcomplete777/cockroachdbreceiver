package cockroachreceiver

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	ConnectionString                string        `mapstructure:"connection_string"`
	QueryTimeout                    time.Duration `mapstructure:"query_timeout"`
	QueryLimit                      int           `mapstructure:"query_limit"`
	MaxQueryLength                  int           `mapstructure:"max_query_length"`
	
	// Production-safe metrics (always available)
	CollectStatementStats       bool `mapstructure:"collect_statement_stats"`
	CollectTransactionStats     bool `mapstructure:"collect_transaction_stats"`
	CollectIndexUsage          bool `mapstructure:"collect_index_usage"`
	CollectClusterQueries      bool `mapstructure:"collect_cluster_queries"`
	CollectClusterSessions     bool `mapstructure:"collect_cluster_sessions"`
	CollectClusterTransactions bool `mapstructure:"collect_cluster_transactions"`
	CollectContentionMetrics   bool `mapstructure:"collect_contention_metrics"`
	
	// May be empty or limited on Serverless
	CollectJobs          bool `mapstructure:"collect_jobs"`
	CollectSchemaChanges bool `mapstructure:"collect_schema_changes"`
	
	// Not available on Serverless
	CollectNodeMetrics  bool `mapstructure:"collect_node_metrics"`
	CollectRangeMetrics bool `mapstructure:"collect_range_metrics"`
	
	// Legacy flags for backward compatibility
	CollectQueryStats    bool `mapstructure:"collect_query_stats"`    // Maps to CollectStatementStats
	CollectJobStatus     bool `mapstructure:"collect_job_status"`     // Maps to CollectJobs
	CollectRangeHealth   bool `mapstructure:"collect_range_health"`   // Maps to CollectRangeMetrics
	CollectNodeLiveness  bool `mapstructure:"collect_node_liveness"`  // Maps to CollectNodeMetrics
}

func (cfg *Config) Validate() error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("connection_string is required")
	}
	
	// Map legacy flags
	if cfg.CollectQueryStats {
		cfg.CollectStatementStats = true
	}
	if cfg.CollectJobStatus {
		cfg.CollectJobs = true
	}
	if cfg.CollectRangeHealth {
		cfg.CollectRangeMetrics = true
	}
	if cfg.CollectNodeLiveness {
		cfg.CollectNodeMetrics = true
	}
	
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 30 * time.Second,
		},
		QueryTimeout:   30 * time.Second,
		QueryLimit:     100,
		MaxQueryLength: 200,
		
		// Production-safe defaults
		CollectStatementStats:       true,
		CollectTransactionStats:     true,
		CollectIndexUsage:          true,
		CollectClusterQueries:      true,
		CollectClusterSessions:     true,
		CollectClusterTransactions: true,
		CollectContentionMetrics:   true,
		
		// May work on Serverless
		CollectJobs:          true,
		CollectSchemaChanges: true,
		
		// Disabled by default (not on Serverless)
		CollectNodeMetrics:  false,
		CollectRangeMetrics: false,
	}
}
