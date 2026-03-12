// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)
	assert.Equal(t, metadata.Type, factory.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)
	// Default config has no connection string, so Validate returns an error.
	// This is expected — the user must provide a connection string.
	require.Error(t, cfg.(*Config).Validate())
}

func TestCreateMetricsReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.ConnectionString = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

	params := receivertest.NewNopSettings(metadata.Type)
	receiver, err := factory.CreateMetrics(context.Background(), params, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NotNil(t, receiver)
}
