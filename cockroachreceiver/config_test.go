package cockroachreceiver

import (
    "testing"
    "time"
)

func TestConfigValidate(t *testing.T) {
    tests := []struct {
        name    string
        config  Config
        wantErr bool
        errMsg  string
    }{
        {
            name: "valid config",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
                ConnMaxLifetime:    time.Hour,
                ConnMaxIdleTime:    10 * time.Minute,
            },
            wantErr: false,
        },
        {
            name: "missing connection string",
            config: Config{
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "connection_string is required",
        },
        {
            name: "missing collection interval",
            config: Config{
                ConnectionString: "postgresql://user:pass@localhost:26257/db",
                QueryTimeout:     30 * time.Second,
                QueryLimit:       20,
                MaxOpenConns:     10,
                MaxIdleConns:     5,
            },
            wantErr: true,
            errMsg:  "collection_interval is required",
        },
        {
            name: "invalid collection interval",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "invalid",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "collection_interval must be a valid duration (e.g., '1m', '30s')",
        },
        {
            name: "negative query timeout",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       -1 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "query_timeout must be positive",
        },
        {
            name: "zero query limit",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         0,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "query_limit must be positive",
        },
        {
            name: "negative query limit",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         -5,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "query_limit must be positive",
        },
        {
            name: "zero max open connections",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       0,
                MaxIdleConns:       5,
            },
            wantErr: true,
            errMsg:  "max_open_connections must be positive",
        },
        {
            name: "negative max idle connections",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       -1,
            },
            wantErr: true,
            errMsg:  "max_idle_connections must be non-negative",
        },
        {
            name: "max idle connections exceeds max open connections",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       15,
            },
            wantErr: true,
            errMsg:  "max_idle_connections cannot exceed max_open_connections",
        },
        {
            name: "valid enabled metrics",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
                EnabledMetrics:     []string{"query", "session", "index"},
            },
            wantErr: false,
        },
        {
            name: "invalid metric group",
            config: Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: "1m",
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
                EnabledMetrics:     []string{"query", "invalid_group"},
            },
            wantErr: true,
            errMsg:  "invalid metric group: invalid_group. Valid groups: query, transaction, session, index, table, contention, range, node, job, changefeed, schema, error",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.config.Validate()
            if tt.wantErr {
                if err == nil {
                    t.Errorf("Validate() expected error containing %q, got nil", tt.errMsg)
                    return
                }
                if err.Error() != tt.errMsg {
                    t.Errorf("Validate() error = %v, want error containing %v", err, tt.errMsg)
                }
            } else {
                if err != nil {
                    t.Errorf("Validate() unexpected error = %v", err)
                }
            }
        })
    }
}

func TestConfigValidate_EdgeCases(t *testing.T) {
    t.Run("max idle equals max open is valid", func(t *testing.T) {
        cfg := Config{
            ConnectionString:   "postgresql://user:pass@localhost:26257/db",
            CollectionInterval: "1m",
            QueryTimeout:       30 * time.Second,
            QueryLimit:         20,
            MaxOpenConns:       10,
            MaxIdleConns:       10,
        }
        if err := cfg.Validate(); err != nil {
            t.Errorf("Validate() unexpected error when max_idle equals max_open: %v", err)
        }
    })

    t.Run("various valid duration formats", func(t *testing.T) {
        validDurations := []string{"1s", "30s", "1m", "5m", "1h", "1m30s"}
        for _, duration := range validDurations {
            cfg := Config{
                ConnectionString:   "postgresql://user:pass@localhost:26257/db",
                CollectionInterval: duration,
                QueryTimeout:       30 * time.Second,
                QueryLimit:         20,
                MaxOpenConns:       10,
                MaxIdleConns:       5,
            }
            if err := cfg.Validate(); err != nil {
                t.Errorf("Validate() unexpected error for duration %q: %v", duration, err)
            }
        }
    })
}

func TestIsMetricEnabled(t *testing.T) {
    tests := []struct {
        name           string
        enabledMetrics []string
        checkMetric    string
        want           bool
    }{
        {
            name:           "empty enabled metrics - all enabled",
            enabledMetrics: []string{},
            checkMetric:    "query",
            want:           true,
        },
        {
            name:           "nil enabled metrics - all enabled",
            enabledMetrics: nil,
            checkMetric:    "session",
            want:           true,
        },
        {
            name:           "metric is enabled",
            enabledMetrics: []string{"query", "session", "index"},
            checkMetric:    "query",
            want:           true,
        },
        {
            name:           "metric is not enabled",
            enabledMetrics: []string{"query", "session"},
            checkMetric:    "index",
            want:           false,
        },
        {
            name:           "case insensitive match",
            enabledMetrics: []string{"QUERY", "Session"},
            checkMetric:    "query",
            want:           true,
        },
        {
            name:           "whitespace trimming",
            enabledMetrics: []string{" query ", "session  "},
            checkMetric:    "query",
            want:           true,
        },
        {
            name:           "all metric groups enabled explicitly",
            enabledMetrics: []string{"query", "transaction", "session", "index", "table", "contention", "range", "node", "job", "changefeed", "schema", "error"},
            checkMetric:    "changefeed",
            want:           true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            cfg := Config{
                EnabledMetrics: tt.enabledMetrics,
            }
            got := cfg.IsMetricEnabled(tt.checkMetric)
            if got != tt.want {
                t.Errorf("IsMetricEnabled(%q) = %v, want %v", tt.checkMetric, got, tt.want)
            }
        })
    }
}
