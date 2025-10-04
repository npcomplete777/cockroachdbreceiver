package cockroachreceiver

import (
	"context"
	"errors"

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
		func() component.Config { return createDefaultConfig() },
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}

func createMetricsReceiver(
	ctx context.Context,
	settings receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cockroachCfg := cfg.(*Config)

	s := newScraper(cockroachCfg, settings.TelemetrySettings)
	if s == nil {
		return nil, errors.New("failed to create scraper")
	}

	sc, err := scraper.NewMetrics(s.scrape, scraper.WithShutdown(s.Shutdown))
	if err != nil {
		return nil, err
	}

	return scraperhelper.NewMetricsController(
		&cockroachCfg.ControllerConfig,
		settings,
		consumer,
		scraperhelper.AddScraper(typeStr, sc),
	)
}
