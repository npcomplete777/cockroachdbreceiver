# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-05-05

Live-tested against CockroachDB Cloud Serverless v25.4.

### Reconciled config schema with documentation

Prior versions documented a config shape (`max_open_connections`,
`max_idle_connections`, `connection_max_*`, nested `metrics:` block with
per-table toggles) that the code never implemented. The receiver only
accepted a flat `collect_*` boolean schema that was not documented. Every
customer copying from the README hit `'cockroachdb' has invalid keys:
max_open_connections, metrics`.

- `Config` now exposes `max_open_connections`, `max_idle_connections`,
  `connection_max_lifetime`, `connection_max_idle_time`, and a nested
  `metrics:` block with one boolean per metric group.
- The pool fields are wired into `database/sql` in `Start()`; previously the
  pool was hardcoded to 10/5/5min regardless of config.
- Removed the undocumented flat `collect_*` schema.

### Bundled distribution

`builder-config-cockroach.yaml` now includes:

- `resourceprocessor` (contrib)
- `debugexporter`
- `opampextension` (contrib)

so customer configs that reference any of these no longer error with
`unknown type`. The previous distribution shipped only `batchprocessor` and
`otlphttpexporter`.

### Fixed: one failing scrape no longer drops the whole batch

`ScrapeMetrics` previously returned `(metrics, error)` whenever any single
metric group failed. The collector's scraper helper drops metrics on a
non-nil error, so enabling, e.g., `node_metrics` on Serverless silently
disabled all observability. Each group now logs and counts errors via
`cockroachdb.receiver.scrape_errors` while partial data continues to flow.

### Fixed: broken queries

- `crdb_internal.node_metrics` query referenced `node_id`, which does not
  exist on virtual clusters (Serverless). Switched to scanning
  `(store_id, name, value)`; `node_id` was always nullable and is now
  surfaced as a `store_id` attribute when present.
- `crdb_internal.gossip_liveness` query referenced an `is_live` column that
  does not exist. Liveness is now derived from the lease `expiration` plus
  `draining`/`membership` columns.
- `crdb_internal.kv_node_status` query referenced `network_latency_p50/p99`
  columns that do not exist in any CRDB version. The query now exposes node
  uptime and last-update age (`cockroachdb.node.uptime`,
  `cockroachdb.node.last_update.age`).

### Implemented previously dead metric groups

The following `crdb_internal` queries existed as Go constants in
`queries.go` but were never invoked. They are now wired up behind the
matching `metrics:` flag:

- `cluster_locks` → `cockroachdb.cluster.locks.active`,
  `cockroachdb.cluster.locks.duration.max`
- `cluster_contended_keys` → `cockroachdb.contention.key.events`
- `transaction_contention_events` →
  `cockroachdb.contention.transaction.duration`
- `gossip_liveness` → `cockroachdb.node.live` (Dedicated/self-hosted only)
- `kv_node_status` → `cockroachdb.node.uptime`,
  `cockroachdb.node.last_update.age` (Dedicated/self-hosted only)

### `Validate()` matches its tests

`Config.Validate` previously only checked `connection_string`; the unit
tests asserted error messages for `collection_interval`, `query_timeout`,
`query_limit`, and `max_query_length` that the function never produced.
The function is now in line with the tests and adds checks on
`max_open_connections` and `max_idle_connections`.

## [1.0.0] - 2025-10-02

### Added
- Initial release of CockroachDB OpenTelemetry Receiver
- Query performance monitoring (execution count, latency, percentiles)
- Index usage statistics tracking
- Lock contention metrics
- Range health monitoring (total, under-replicated, unavailable)
- Node liveness tracking (self-hosted only)
- Job monitoring (backups, schema changes, imports)
- Changefeed lag tracking
- Schema change progress monitoring
- Statement error analysis with error codes
- Configurable connection pooling
- Configurable query timeouts and limits
- Graceful shutdown with proper connection cleanup
- Comprehensive unit test suite (35 tests)
- Full metrics documentation
- Production-ready with example configurations

### Configuration Options
- `connection_string`: PostgreSQL connection string (required)
- `collection_interval`: Metric collection frequency (required)
- `query_timeout`: Individual query timeout (default: 30s)
- `query_limit`: Max results per query (default: 20)
- `max_open_connections`: Connection pool size (default: 10)
- `max_idle_connections`: Idle connections (default: 5)
- `connection_max_lifetime`: Connection lifetime (default: 1h)
- `connection_max_idle_time`: Connection idle time (default: 10m)

### Metrics Collected
- 24 distinct metric types covering:
  - Query performance and latency percentiles (P50, P95, P99)
  - Transaction statistics
  - Active sessions and connections
  - Index usage analytics
  - Lock contention tracking
  - Range health and replication status
  - Node liveness (self-hosted clusters)
  - Job monitoring and changefeed lag
  - Schema change progress
  - Statement errors by type

### Compatibility
- CockroachDB v23.x and v24.x
- CockroachDB Serverless and self-hosted
- OpenTelemetry Collector v0.135.0+
- Go 1.21+

### Documentation
- Complete README with installation instructions
- Full metrics reference documentation
- Example configurations
- Recommended production alerts
- Contributing guidelines
