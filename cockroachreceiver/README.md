# CockroachDB Receiver Configuration Examples

This directory contains example configurations for different CockroachDB deployment scenarios:

- `production.yaml` - Production configuration with safe metrics only
- `development.yaml` - Development configuration with all metrics enabled
- `serverless.yaml` - Configuration for CockroachDB Serverless
- `docker-compose.yaml` - Full stack example with CockroachDB and OTel Collector

## Quick Start

1. Choose the appropriate configuration for your environment
2. Update the connection string with your CockroachDB credentials
3. Adjust the metrics selection based on your monitoring needs
4. Run the collector:
```bash
   otelcol --config production.yaml
