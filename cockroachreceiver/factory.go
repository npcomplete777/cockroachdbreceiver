package cockroachreceiver

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

var typeStr = component.MustNewType("cockroachdb")

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ConnectionString:      "",
		CollectionInterval:    "1m",
		QueryTimeout:          "30s",
		MaxOpenConnections:    10,
		MaxIdleConnections:    5,
		ConnectionMaxLifetime: "1h",
		ConnectionMaxIdleTime: "10m",
		Metrics: MetricsConfig{
			StatementStatistics:         true,
			TransactionStatistics:       true,
			IndexUsageStatistics:        true,
			ClusterQueries:              true,
			ClusterSessions:             true,
			ClusterTransactions:         true,
			ClusterContendedIndexes:     true,
			ClusterContendedTables:      true,
			ClusterContentionEvents:     true,
			ClusterLocks:                false,
			ClusterContendedKeys:        false,
			TransactionContentionEvents: false,
			RangesNoLeases:              false,
			GossipLiveness:              false,
			Jobs:                        false,
			SchemaChanges:               false,
			NodeMetrics:                 false,
			KVNodeStatus:                false,
		},
	}
}

func createMetricsReceiver(
	ctx context.Context,
	settings receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cockroachCfg := cfg.(*Config)

	interval, err := time.ParseDuration(cockroachCfg.CollectionInterval)
	if err != nil {
		return nil, err
	}

	s := newScraper(cockroachCfg, settings.TelemetrySettings)
	if s == nil {
		return nil, errors.New("failed to create scraper")
	}

	scraperCfg := &scraperhelper.ControllerConfig{
		CollectionInterval: interval,
		InitialDelay:       time.Second,
	}

	sc, err := scraper.NewMetrics(s.scrape, scraper.WithShutdown(s.Shutdown))
	if err != nil {
		return nil, err
	}

	return scraperhelper.NewMetricsController(
		scraperCfg,
		settings,
		consumer,
		scraperhelper.AddScraper(typeStr, sc),
	)
}
