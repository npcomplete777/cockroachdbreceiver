package cockroachreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

const typeStr = "cockroachdb"

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		func() component.Config {
			return createDefaultConfig()
		},
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cockroachCfg := cfg.(*Config)

	sc := newCockroachScraper(cockroachCfg, settings)

	return scraperhelper.NewMetricsController(
		&cockroachCfg.ControllerConfig,
		settings,
		consumer,
		scraperhelper.AddScraper(component.MustNewType(typeStr), sc),
	)
}
