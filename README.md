# CockroachDB Receiver

OpenTelemetry receiver that connects to a CockroachDB cluster over the
PostgreSQL wire protocol and emits metrics scraped from `crdb_internal.*`
tables: query/transaction performance, contention, jobs, schema changes,
ranges, and node-level health.

| Stability | Code owner |
|-----------|------------|
| Alpha     | @npcomplete777 |

## Prerequisites

- CockroachDB v22.1 or later (Dedicated, self-hosted, or Serverless).
- A SQL user with `SELECT` on the `crdb_internal` tables you intend to scrape.
- Network access to the SQL port (default `26257`).
- For `sslmode=verify-full`, the cluster CA cert at `~/.postgresql/root.crt`.
  Use `sslmode=require` if you have not placed it.

## Build

This repo ships a builder manifest that bundles the receiver with a
production-ready set of components (`batch` and `resource` processors, `opamp`
extension, `otlphttp` and `debug` exporters):

```bash
# Install the OpenTelemetry Collector Builder once.
go install go.opentelemetry.io/collector/cmd/builder@v0.136.0

# Build the distribution.
builder --config builder-config-cockroach.yaml

# Run.
./otelcol-cockroachdb/otelcol-cockroachdb --config config.yaml
```

The bundled `builder-config-cockroach.yaml` is the source of truth — it lists
the exact components and versions in the shipped binary.

## Configuration

### Minimal

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://user:pass@host:26257/defaultdb?sslmode=require"

processors:
  batch:

exporters:
  otlphttp:
    endpoint: "https://your-backend.example.com"

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [otlphttp]
```

### Full schema

Every key the receiver accepts. Defaults match `createDefaultConfig` in
`cockroachreceiver/config.go`.

```yaml
receivers:
  cockroachdb:
    # Required.
    connection_string: "postgresql://user:pass@host:26257/defaultdb?sslmode=require"

    # Scraping cadence (squashed from scraperhelper.ControllerConfig).
    collection_interval: 60s
    initial_delay: 1s
    timeout: 0s

    # Per-scrape query limits.
    query_timeout: 30s          # cap on each crdb_internal query
    query_limit: 20             # top-N rows per scrape, ordered by execution count
    max_query_length: 200       # truncate query_text attribute (0 = unlimited; min 50)

    # Connection pool (passed to database/sql).
    max_open_connections: 10
    max_idle_connections: 5
    connection_max_lifetime: 1h
    connection_max_idle_time: 10m

    # Per-metric-group toggles. All keys default as shown.
    metrics:
      # Production-safe core (all true by default)
      statement_statistics: true
      transaction_statistics: true
      index_usage_statistics: true
      cluster_queries: true
      cluster_sessions: true
      cluster_transactions: true

      # Production-safe contention summaries (all true by default)
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contention_events: true

      # Off by default — expensive, troubleshooting-only,
      # or not available on Serverless.
      cluster_contended_keys: false
      cluster_locks: false
      transaction_contention_events: false
      jobs: false
      schema_changes: false
      ranges_no_leases: false
      gossip_liveness: false       # Dedicated/self-hosted only
      node_metrics: false
      kv_node_status: false        # Dedicated/self-hosted only
```

See `cockroachreceiver/examples/` for ready-to-edit production, serverless,
and development configs.

## Available metrics

| Group flag | Emitted metrics |
|------------|-----------------|
| `statement_statistics` | `cockroachdb.statement.execution.count`, `cockroachdb.statement.latency.{service,parse,plan,run}.mean`, `cockroachdb.statement.rows.{read,written}.mean`, `cockroachdb.statement.bytes.read.mean`, `cockroachdb.statement.retries.max`, `cockroachdb.statement.error.count` |
| `transaction_statistics` | `cockroachdb.transaction.execution.count`, `cockroachdb.transaction.latency.{service,commit,retry}.mean`, `cockroachdb.transaction.rows.{read,written}.mean`, `cockroachdb.transaction.bytes.read.mean`, `cockroachdb.transaction.retries.max` |
| `index_usage_statistics` | `cockroachdb.index.reads.total`, `cockroachdb.index.seconds_since_last_read` |
| `cluster_queries` | `cockroachdb.cluster.queries.{active,duration}` |
| `cluster_sessions` | `cockroachdb.cluster.sessions.{active,age,memory_allocated}` |
| `cluster_transactions` | `cockroachdb.cluster.transactions.{active,duration}` |
| `cluster_contended_indexes` | `cockroachdb.contention.index.events` |
| `cluster_contended_tables` | `cockroachdb.contention.table.events` |
| `cluster_contention_events` | `cockroachdb.contention.time.cumulative` |
| `cluster_contended_keys` | `cockroachdb.contention.key.events` |
| `cluster_locks` | `cockroachdb.cluster.locks.active`, `cockroachdb.cluster.locks.duration.max` |
| `transaction_contention_events` | `cockroachdb.contention.transaction.duration` |
| `jobs` | `cockroachdb.jobs.active`, `cockroachdb.jobs.progress` |
| `schema_changes` | `cockroachdb.schema_changes.active` |
| `ranges_no_leases` | `cockroachdb.ranges.{total,under_replicated,unavailable}` |
| `gossip_liveness` | `cockroachdb.node.live` |
| `node_metrics` | `cockroachdb.node.cpu.percent`, `cockroachdb.node.memory.rss` |
| `kv_node_status` | `cockroachdb.node.uptime`, `cockroachdb.node.last_update.age` |

The receiver also emits its own health gauges every scrape:

- `cockroachdb.receiver.scrape_success` — `1` when no metric group failed,
  `0` otherwise.
- `cockroachdb.receiver.scrape_errors` — count of metric groups that failed
  in the most recent scrape.

## Serverless vs Dedicated

Tested live against CockroachDB Cloud Serverless v25.4. Items marked
"unsupported" return `unimplemented: operation is unsupported within a virtual
cluster` and will increment `cockroachdb.receiver.scrape_errors`; partial
results from the rest of the scrape still emit normally.

| Group | Serverless | Dedicated / self-hosted |
|-------|------------|-------------------------|
| `statement_statistics`, `transaction_statistics`, `index_usage_statistics` | ✅ | ✅ |
| `cluster_queries`, `cluster_sessions`, `cluster_transactions` | ✅ | ✅ |
| `cluster_contended_*`, `cluster_contention_events`, `cluster_locks`, `transaction_contention_events` | ✅ (often empty) | ✅ |
| `jobs`, `schema_changes` | ✅ | ✅ |
| `ranges_no_leases` | ✅ | ✅ |
| `node_metrics` | ✅ (no `node_id`, tagged with `store_id`) | ✅ |
| `gossip_liveness` | ❌ unsupported | ✅ |
| `kv_node_status` | ❌ unsupported | ✅ |

## Error handling

Each metric-group scrape runs independently. A failure in one group is
logged with a structured `metric_group` field, increments
`cockroachdb.receiver.scrape_errors`, and **does not** drop the rest of the
scrape. Look for these log lines:

```
Scrape failed  metric_group=<group>  error=<details>
```

## Troubleshooting

### `'cockroachdb' has invalid keys: ...`

You are running a build of the receiver older than the schema in your config.
Either update the receiver, or remove keys not in the [Full schema](#full-schema)
section. Some valid keys (`max_open_connections`, `metrics:`, the per-metric
toggles) were not implemented prior to this version.

### `processors unknown type: "resource"` / `exporters unknown type: "debug"` / `extensions unknown type: "opamp"`

Your collector binary was built without those components. The shipped
`builder-config-cockroach.yaml` includes all three. If you maintain your own
build manifest, add:

```yaml
extensions:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampextension v0.136.0

processors:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.136.0

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.136.0
```

### `failed to ping CockroachDB: ... root certificate file ... does not exist`

You used `sslmode=verify-full` but did not place the cluster CA at
`~/.postgresql/root.crt`. Either place the cert (download from the cluster's
"Connect" page) or fall back to `sslmode=require`.

### `pq: column "node_id" does not exist`

Fixed in this version. The receiver no longer references `node_id` from
`crdb_internal.node_metrics` (that column does not exist on virtual
clusters). Rebuild from this revision.

### `pq: unimplemented: operation is unsupported within a virtual cluster`

Some `crdb_internal` tables are gated to non-virtual clusters. On Serverless,
disable `gossip_liveness` and `kv_node_status` (or accept the per-scrape
warning — the rest of the metrics still emit).

## License

See [LICENSE](LICENSE).
