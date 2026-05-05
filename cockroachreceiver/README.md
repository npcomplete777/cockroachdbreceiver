# cockroachreceiver

OpenTelemetry receiver for CockroachDB. See the [top-level README](../README.md)
for build and configuration documentation.

The `examples/` directory has ready-to-edit configs:

- `production.yaml` — Dedicated/self-hosted with the safe metric set.
- `serverless.yaml` — Serverless / virtual-cluster tenants, with notes on
  which `crdb_internal` tables are gated.
- `development.yaml` — every metric enabled, for incident response or
  staging.

Update the `connection_string` in any of them, point the collector at the
file, and you have a working pipeline.
