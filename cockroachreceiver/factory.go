// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver // import "github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
)

// NewFactory creates a new receiver factory for CockroachDB.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	cfg := scraperhelper.NewDefaultControllerConfig()
	cfg.CollectionInterval = 30 * time.Second

	return &Config{
		ControllerConfig:     cfg,
		QueryTimeout:         30 * time.Second,
		QueryLimit:           100,
		MaxQueryLength:       200,
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
}

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	rConf component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cfg := rConf.(*Config)

	ns := newCockroachScraper(params, cfg)

	return scraperhelper.NewMetricsController(
		&cfg.ControllerConfig,
		params,
		consumer,
		scraperhelper.AddScraper(metadata.Type, ns),
	)
}
