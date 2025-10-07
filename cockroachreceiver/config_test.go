package cockroachreceiver

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/scraper/scraperhelper"
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
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 248,
			},
			wantErr: false,
		},
		{
			name: "missing connection string",
			config: Config{
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 248,
			},
			wantErr: true,
			errMsg:  "connection_string is required",
		},
		{
			name: "zero collection interval",
			config: Config{
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: 0,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 248,
			},
			wantErr: true,
			errMsg:  "collection_interval must be positive",
		},
		{
			name: "negative query timeout",
			config: Config{
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   -1 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 248,
			},
			wantErr: true,
			errMsg:  "query_timeout cannot be negative",
		},
		{
			name: "zero query limit",
			config: Config{
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     0,
				MaxQueryLength: 248,
			},
			wantErr: true,
			errMsg:  "query_limit must be positive",
		},
		{
			name: "max_query_length too small",
			config: Config{
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 25,
			},
			wantErr: true,
			errMsg:  "max_query_length must be at least 50 characters or 0 for unlimited (got 25)",
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
					t.Errorf("Validate() error = %v, want %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Validate() unexpected error = %v", err)
				}
			}
		})
	}
}
