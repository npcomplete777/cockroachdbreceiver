// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
)

func TestNewCockroachScraper(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)
	require.NotNil(t, scraper)
	assert.NotNil(t, scraper.mb)
	assert.NotNil(t, scraper.logger)
}

func TestScrapeWithoutConnection(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)

	// Scraping without a connection should return an error.
	_, err := scraper.ScrapeMetrics(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database connection not initialized")
}

func TestShutdownWithoutConnection(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)

	// Shutdown without a connection should succeed.
	err := scraper.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestTruncateQuery(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	cfg.MaxQueryLength = 50

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "short query unchanged",
			input:    "SELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "long query truncated",
			input:    "SELECT very_long_column_name FROM very_long_table_name WHERE some_condition = true",
			expected: "SELECT very_long_column_name FROM...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scraper.truncateQuery(tt.input)
			if len(tt.input) <= 50 {
				assert.Equal(t, tt.input, result)
			} else {
				assert.True(t, len(result) <= 53) // 50 + "..."
				assert.Contains(t, result, "...")
			}
		})
	}
}

func TestTruncateQueryUnlimited(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	cfg.MaxQueryLength = 0

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)

	longQuery := "SELECT " + string(make([]byte, 1000))
	result := scraper.truncateQuery(longQuery)
	assert.Equal(t, longQuery, result)
}

func TestNullStr(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	params := receivertest.NewNopSettings(metadata.Type)
	scraper := newCockroachScraper(params, cfg)

	assert.Equal(t, "", scraper.nullStr(sql.NullString{Valid: false}))
	assert.Equal(t, "", scraper.nullStr(sql.NullString{Valid: true, String: ""}))
	assert.Equal(t, "test", scraper.nullStr(sql.NullString{Valid: true, String: "test"}))
}
