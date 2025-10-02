package cockroachreceiver

import (
    "errors"
    "time"
)

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
    
    return nil
}
