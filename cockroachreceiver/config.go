package cockroachreceiver

import (
    "errors"
    "strings"
    "time"
)

// Metric group names for selective collection
const (
    MetricGroupQuery       = "query"
    MetricGroupTransaction = "transaction"
    MetricGroupSession     = "session"
    MetricGroupIndex       = "index"
    MetricGroupTable       = "table"
    MetricGroupContention  = "contention"
    MetricGroupRange       = "range"
    MetricGroupNode        = "node"
    MetricGroupJob         = "job"
    MetricGroupChangefeed  = "changefeed"
    MetricGroupSchema      = "schema"
    MetricGroupError       = "error"
)

var defaultEnabledMetrics = []string{
    MetricGroupQuery,
    MetricGroupTransaction,
    MetricGroupSession,
    MetricGroupIndex,
    MetricGroupTable,
    MetricGroupContention,
    MetricGroupRange,
    MetricGroupNode,
    MetricGroupJob,
    MetricGroupChangefeed,
    MetricGroupSchema,
    MetricGroupError,
}

var validMetricGroups = map[string]bool{
    MetricGroupQuery:       true,
    MetricGroupTransaction: true,
    MetricGroupSession:     true,
    MetricGroupIndex:       true,
    MetricGroupTable:       true,
    MetricGroupContention:  true,
    MetricGroupRange:       true,
    MetricGroupNode:        true,
    MetricGroupJob:         true,
    MetricGroupChangefeed:  true,
    MetricGroupSchema:      true,
    MetricGroupError:       true,
}

type Config struct {
    ConnectionString   string        `mapstructure:"connection_string"`
    CollectionInterval string        `mapstructure:"collection_interval"`
    
    // Query configuration
    QueryTimeout     time.Duration `mapstructure:"query_timeout"`
    QueryLimit       int           `mapstructure:"query_limit"`
    
    // Connection pool configuration
    MaxOpenConns     int           `mapstructure:"max_open_connections"`
    MaxIdleConns     int           `mapstructure:"max_idle_connections"`
    ConnMaxLifetime  time.Duration `mapstructure:"connection_max_lifetime"`
    ConnMaxIdleTime  time.Duration `mapstructure:"connection_max_idle_time"`
    
    // Selective metric collection
    // If empty, all metrics are collected. Otherwise, only specified groups are collected.
    EnabledMetrics   []string      `mapstructure:"enabled_metrics"`
}

func (cfg *Config) Validate() error {
    if cfg.ConnectionString == "" {
        return errors.New("connection_string is required")
    }
    
    if cfg.CollectionInterval == "" {
        return errors.New("collection_interval is required")
    }
    
    if _, err := time.ParseDuration(cfg.CollectionInterval); err != nil {
        return errors.New("collection_interval must be a valid duration (e.g., '1m', '30s')")
    }
    
    if cfg.QueryTimeout <= 0 {
        return errors.New("query_timeout must be positive")
    }
    
    if cfg.QueryLimit <= 0 {
        return errors.New("query_limit must be positive")
    }
    
    if cfg.MaxOpenConns <= 0 {
        return errors.New("max_open_connections must be positive")
    }
    
    if cfg.MaxIdleConns < 0 {
        return errors.New("max_idle_connections must be non-negative")
    }
    
    if cfg.MaxIdleConns > cfg.MaxOpenConns {
        return errors.New("max_idle_connections cannot exceed max_open_connections")
    }
    
    // Validate enabled_metrics if specified
    if len(cfg.EnabledMetrics) > 0 {
        for _, metric := range cfg.EnabledMetrics {
            metricLower := strings.ToLower(strings.TrimSpace(metric))
            if !validMetricGroups[metricLower] {
                return errors.New("invalid metric group: " + metric + ". Valid groups: query, transaction, session, index, table, contention, range, node, job, changefeed, schema, error")
            }
        }
    }
    
    return nil
}

// IsMetricEnabled checks if a metric group is enabled for collection
func (cfg *Config) IsMetricEnabled(metricGroup string) bool {
    // If no metrics specified, all are enabled
    if len(cfg.EnabledMetrics) == 0 {
        return true
    }
    
    metricLower := strings.ToLower(strings.TrimSpace(metricGroup))
    for _, enabled := range cfg.EnabledMetrics {
        if strings.ToLower(strings.TrimSpace(enabled)) == metricLower {
            return true
        }
    }
    return false
}
