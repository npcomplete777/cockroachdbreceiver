// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
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
		{
			name: "max_query_length zero is valid (unlimited)",
			config: Config{
				ConnectionString: "postgresql://user:pass@localhost:26257/db",
				ControllerConfig: scraperhelper.ControllerConfig{
					CollectionInterval: time.Minute,
				},
				QueryTimeout:   30 * time.Second,
				QueryLimit:     20,
				MaxQueryLength: 0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	expected := factory.CreateDefaultConfig().(*Config)
	expected.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	expected.ControllerConfig.CollectionInterval = 30 * time.Second
	expected.QueryTimeout = 30 * time.Second
	expected.QueryLimit = 100
	expected.MaxQueryLength = 200
	assert.Equal(t, expected, cfg)
}

func TestLoadConfigAll(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "all").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	cockroachCfg := cfg.(*Config)
	assert.Equal(t, "postgresql://root@localhost:26257/defaultdb?sslmode=disable", cockroachCfg.ConnectionString)
	assert.Equal(t, 60*time.Second, cockroachCfg.ControllerConfig.CollectionInterval)
	assert.Equal(t, 45*time.Second, cockroachCfg.QueryTimeout)
	assert.Equal(t, 50, cockroachCfg.QueryLimit)
	assert.Equal(t, 500, cockroachCfg.MaxQueryLength)
	assert.True(t, cockroachCfg.Metrics.CockroachdbRangesTotal.Enabled)
	assert.True(t, cockroachCfg.Metrics.CockroachdbNodesLive.Enabled)
}
