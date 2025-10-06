# CockroachDB Receiver

## Overview

The CockroachDB receiver connects to a CockroachDB cluster and collects comprehensive metrics about database performance, health, and operations by querying internal CockroachDB system tables (`crdb_internal.*`).

**Key Features:**
- **Query Performance Monitoring**: Captures actual SQL query text with execution statistics, latencies, and resource usage
- **Granular Metric Control**: Enable/disable individual metric groups based on environment needs
- **Production Safety Warnings**: Clear distinction between safe and expensive metrics
- **Query Limit Control**: Limits top N queries to prevent metric explosion (default: 20)
- **Direct Internal Table Access**: Queries CockroachDB's native statistics tables
- **Flexible Configuration**: Comprehensive connection pooling and query tuning

## Status

| Stability | Distributions | Issues | Code Owners |
|-----------|---------------|--------|-------------|
| Alpha     | N/A           | [Open Issues](https://github.com/npcomplete777/cockroachdbreceiver/issues) | @npcomplete777 |

## Prerequisites

**CockroachDB Requirements:**
- CockroachDB v22.1 or later (Dedicated or Serverless)
- Database user with `SELECT` permissions on required `crdb_internal` tables

**Network Requirements:**
- Network connectivity to CockroachDB on port 26257 (default)
- SSL/TLS configuration if required by your cluster

## Installation

### Building the Collector

1. **Install OpenTelemetry Collector Builder (OCB)**
```bash
wget https://github.com/open-telemetry/opentelemetry-collector/releases/download/cmd%2Fbuilder%2Fv0.136.0/ocb_0.136.0_linux_amd64
chmod +x ocb_0.136.0_linux_amd64
sudo mv ocb_0.136.0_linux_amd64 /usr/local/bin/ocb
```

2. **Create Builder Configuration** (`builder-config-cockroach.yaml`)
```yaml
dist:
  name: otelcol-cockroachdb
  description: OpenTelemetry Collector with CockroachDB receiver
  output_path: ./otelcol-cockroachdb

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/otlphttpexporter v0.136.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.136.0

receivers:
  - gomod: github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver v0.0.1
    path: ./cockroachreceiver
```

3. **Build the Collector**
```bash
ocb --config builder-config-cockroach.yaml
cd otelcol-cockroachdb
./otelcol-cockroachdb --config ../config.yaml
```

## Configuration

### Basic Configuration

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=require"
    collection_interval: "60s"
    query_timeout: "30s"
    query_limit: 20
```

### Complete Configuration

```yaml
receivers:
  cockroachdb:
    # Connection string (required)
    # Format: postgresql://[user[:password]@][host][:port][/database][?options]
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=require"
    
    # Collection interval (required)
    collection_interval: "60s"
    
    # Query timeout (optional, default: 30s)
    query_timeout: "30s"
    
    # Query limit - limits top N queries by execution count (optional, default: 20)
    # Controls cardinality by only collecting the most frequently executed queries
    query_limit: 20
    
    # Connection pool settings (optional)
    max_open_connections: 10           # Default: 10
    max_idle_connections: 5            # Default: 5
    connection_max_lifetime: "1h"      # Default: 1h
    connection_max_idle_time: "10m"    # Default: 10m
    
    # Granular metric collection control
    metrics:
      # ===== PRODUCTION SAFE (Low Overhead) =====
      # These metrics query aggregated statistics tables designed for continuous monitoring
      statement_statistics: true          # Query performance from crdb_internal.statement_statistics
      transaction_statistics: true        # Transaction performance from crdb_internal.transaction_statistics
      index_usage_statistics: true        # Index usage from crdb_internal.index_usage_statistics
      cluster_queries: true               # Active queries from crdb_internal.cluster_queries
      cluster_sessions: true              # Active sessions from crdb_internal.cluster_sessions
      cluster_transactions: true          # Active transactions from crdb_internal.cluster_transactions
      
      # ===== PRODUCTION SAFE (Moderate Overhead) =====
      # Contention metrics - safe but can add overhead under high contention
      cluster_contended_indexes: true     # Contended indexes summary
      cluster_contended_tables: true      # Contended tables summary
      cluster_contention_events: true     # Historical contention events
      cluster_contended_keys: false       # Detailed key-level contention (enable for debugging)
      cluster_locks: false                # Active lock states (enable for debugging deadlocks)
      transaction_contention_events: false # Detailed transaction contention (troubleshooting only)
      
      # ===== NOT PRODUCTION SAFE (Expensive Cluster-Wide RPCs) =====
      # WARNING: These trigger expensive cluster-wide operations
      # Only enable for troubleshooting in dev/staging environments
      ranges_no_leases: false      # EXPENSIVE: Range distribution (triggers RPC fan-out)
      gossip_liveness: false       # UNSTABLE: Node liveness (schema may change)
      jobs: false                  # EXPENSIVE: Background jobs
      schema_changes: false        # Schema change operations
      node_metrics: false          # EXPENSIVE: Node-level system metrics (RPC fan-out)
      kv_node_status: false        # EXPENSIVE: KV layer status (RPC fan-out)
```

## Production vs Non-Production Configurations

### Production Configuration (`config-production.yaml`)

**Recommended for:** Production CockroachDB clusters
**Characteristics:** Low overhead, production-safe metrics only

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://monitoring_user:password@prod.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    collection_interval: "60s"
    query_timeout: "30s"
    query_limit: 20
    
    metrics:
      # Core performance metrics (always safe)
      statement_statistics: true
      transaction_statistics: true
      index_usage_statistics: true
      cluster_queries: true
      cluster_sessions: true
      cluster_transactions: true
      
      # Contention summaries (moderate overhead)
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contention_events: true
      
      # Disable expensive metrics
      cluster_contended_keys: false
      cluster_locks: false
      transaction_contention_events: false
      ranges_no_leases: false
      gossip_liveness: false
      jobs: false
      schema_changes: false
      node_metrics: false
      kv_node_status: false

processors:
  batch:
    timeout: 10s

exporters:
  otlphttp:
    endpoint: "https://your-metrics-backend.com/api/v2/otlp"
    headers:
      Authorization: "Api-Token YOUR_API_TOKEN"

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [otlphttp]
```

### Non-Production Configuration (`config-nonproduction.yaml`)

**Recommended for:** Dev, staging, or incident response
**Characteristics:** Full observability, includes expensive metrics

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://admin:password@dev.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
    collection_interval: "30s"
    query_timeout: "60s"
    query_limit: 50
    max_open_connections: 15
    
    metrics:
      # All production-safe metrics
      statement_statistics: true
      transaction_statistics: true
      index_usage_statistics: true
      cluster_queries: true
      cluster_sessions: true
      cluster_transactions: true
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contention_events: true
      
      # Detailed contention metrics
      cluster_locks: true
      cluster_contended_keys: true
      transaction_contention_events: true
      
      # Expensive metrics enabled for troubleshooting
      ranges_no_leases: true
      gossip_liveness: true
      jobs: true
      schema_changes: true
      node_metrics: true
      kv_node_status: true

processors:
  batch:
    timeout: 10s

exporters:
  logging:
    verbosity: detailed
  otlphttp:
    endpoint: "https://your-metrics-backend.com/api/v2/otlp"
    headers:
      Authorization: "Api-Token YOUR_API_TOKEN"

service:
  telemetry:
    logs:
      level: debug
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [logging, otlphttp]
```

## Metrics Reference

### Production Safe Metrics (Low Overhead)

These metrics query aggregated statistics tables designed for continuous monitoring:

#### Statement Statistics (`statement_statistics: true`)

Source: `crdb_internal.statement_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.statement.execution.count` | Gauge | `1` | Statement execution count | `fingerprint_id`, `app_name`, `database`, `statement_type`, `query` |
| `cockroachdb.statement.error.count` | Gauge | `1` | Error count | `fingerprint_id`, `app_name`, `query`, `last_error_code` |
| `cockroachdb.statement.retries.max` | Gauge | `1` | Maximum retries | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.latency.service.mean` | Gauge | `s` | Average service latency | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.latency.parse.mean` | Gauge | `s` | Parse latency | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.latency.plan.mean` | Gauge | `s` | Planning latency | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.latency.run.mean` | Gauge | `s` | Execution latency | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.rows.read.mean` | Gauge | `1` | Average rows read | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.rows.written.mean` | Gauge | `1` | Average rows written | `fingerprint_id`, `app_name`, `query` |
| `cockroachdb.statement.bytes.read.mean` | Gauge | `By` | Average bytes read | `fingerprint_id`, `app_name`, `query` |

**IMPORTANT:** The `query` attribute contains the actual SQL query text, making metrics human-readable instead of showing only hex fingerprint IDs.

#### Transaction Statistics (`transaction_statistics: true`)

Source: `crdb_internal.transaction_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.transaction.execution.count` | Gauge | `1` | Transaction execution count | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.service` | Gauge | `s` | Average service latency | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.commit` | Gauge | `s` | Commit latency | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.retry` | Gauge | `s` | Retry latency | `fingerprint_id`, `app_name` |

#### Index Usage Statistics (`index_usage_statistics: true`)

Source: `crdb_internal.index_usage_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.index.reads.total` | Gauge | `1` | Total index reads | `table`, `index` |

#### Active Workload Metrics

- `cockroachdb.cluster.queries.active` - Currently executing queries
- `cockroachdb.cluster.sessions.active` - Active sessions
- `cockroachdb.cluster.transactions.active` - Active transactions
- `cockroachdb.session.memory.allocated` - Session memory usage

### Production Safe Metrics (Moderate Overhead)

#### Contention Metrics

| Metric | Type | Description | Attributes |
|--------|------|-------------|------------|
| `cockroachdb.index.contention.events` | Gauge | Contention events on index | `database`, `schema`, `table`, `index` |
| `cockroachdb.table.contention.events` | Gauge | Contention events on table | `database`, `schema`, `table` |
| `cockroachdb.key.contention.events` | Gauge | Contention events on key | `database`, `table`, `index` |
| `cockroachdb.cluster.locks.count` | Gauge | Lock count | `database`, `table`, `lock_strength`, `granted` |
| `cockroachdb.contention.events.total` | Gauge | Total contention events | |

### Not Production Safe (Expensive Operations)

**WARNING:** These metrics trigger expensive cluster-wide RPC operations. Only enable for troubleshooting in non-production environments.

| Metric | Source | Why Expensive | Impact |
|--------|--------|---------------|--------|
| `cockroachdb.ranges.*` | `ranges_no_leases` | Cluster-wide RPC fan-out | Very High |
| `cockroachdb.nodes.live` | `gossip_liveness` | Unstable schema, cluster-wide query | High |
| `cockroachdb.jobs.count` | `jobs` | Complex queries across all nodes | High |
| `cockroachdb.schema_changes.active` | `schema_changes` | Scans schema change metadata | Medium |
| `cockroachdb.node.*` | `node_metrics` | Cluster-wide RPC fan-out | Very High |
| `cockroachdb.nodes.total` | `kv_node_status` | KV layer diagnostics, RPC fan-out | Very High |

## Database User Setup

Create a dedicated monitoring user with minimal privileges:

```sql
-- Create monitoring user
CREATE USER otel_monitor WITH PASSWORD 'secure_password';

-- Grant necessary permissions for production-safe metrics
GRANT SELECT ON crdb_internal.statement_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.transaction_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.index_usage_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_queries TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_sessions TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_transactions TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_contended_indexes TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_contended_keys TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_contended_tables TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_contention_events TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_locks TO otel_monitor;
GRANT SELECT ON crdb_internal.transaction_contention_events TO otel_monitor;

-- Optional: Grant for expensive metrics (dev/staging only)
-- GRANT SELECT ON crdb_internal.ranges_no_leases TO otel_monitor;
-- GRANT SELECT ON crdb_internal.gossip_liveness TO otel_monitor;
-- GRANT SELECT ON crdb_internal.jobs TO otel_monitor;
-- GRANT SELECT ON crdb_internal.schema_changes TO otel_monitor;
-- GRANT SELECT ON crdb_internal.node_metrics TO otel_monitor;
-- GRANT SELECT ON crdb_internal.kv_node_status TO otel_monitor;

-- Verify permissions
SHOW GRANTS FOR otel_monitor;
```

## Performance Tuning

### Collection Interval Guidelines

| Cluster Size | Recommended Interval | Query Limit | Rationale |
|--------------|---------------------|-------------|-----------|
| Small (< 10 nodes, < 100 GB) | `60s` | `20` | Minimize overhead on small clusters |
| Medium (10-50 nodes, 100 GB - 1 TB) | `60s` | `30` | Balance between granularity and overhead |
| Large (> 50 nodes, > 1 TB) | `120s` | `20` | Reduce frequency to minimize impact |

### Query Limit

The `query_limit` parameter controls cardinality by limiting the number of queries collected:

```yaml
query_limit: 20  # Collects top 20 queries by execution count
```

**Benefits:**
- Prevents metric explosion in Dynatrace/observability backend
- Focuses on most frequently executed queries
- Reduces query time and memory usage

**Recommendations:**
- Production: `20` (default)
- Staging/Dev: `50` for more visibility
- Troubleshooting: `100` for comprehensive analysis

## Troubleshooting

## Receiver Self-Monitoring

The receiver emits its own telemetry metrics for monitoring health:

| Metric | Description |
|--------|-------------|
| `otelcol.cockroachdb.scrape.duration` | Duration of each scrape operation (seconds) |
| `otelcol.cockroachdb.scrape.errors` | Number of errors in the current scrape |
| `otelcol.cockroachdb.scrape.total` | Total number of scrapes performed |
| `otelcol.cockroachdb.scrape.errors.total` | Total number of failed scrapes |

Monitor these metrics to ensure the receiver is functioning properly.

## Important Note on Cardinality

This receiver preserves **full query text** in metric attributes for maximum observability. This design decision means:

- You can see actual SQL queries in your metrics backend
- Cardinality is controlled by the `query_limit` parameter (default: 20)
- Suitable for environments where query-level observability is critical
- May not be appropriate for extremely high-cardinality use cases

If cardinality becomes an issue, reduce `query_limit` or use fingerprint IDs only in your metric backend's processing rules.

### High Memory Usage

**Symptoms:** Collector using excessive memory

**Solutions:**
1. Reduce collection frequency:
   ```yaml
   collection_interval: "5m"
   ```

2. Disable expensive metrics:
   ```yaml
   metrics:
     ranges_no_leases: false
     node_metrics: false
     kv_node_status: false
   ```

3. Lower query limit:
   ```yaml
   query_limit: 10
   ```

### Query Timeouts

**Symptoms:** `context deadline exceeded` errors

**Solutions:**
```yaml
query_timeout: "90s"  # Increase timeout
```

For CockroachDB Serverless or high-latency networks, increase further.

### Missing Metrics

**Problem:** Expected metrics not appearing

**Solutions:**
1. Verify metric is enabled in config
2. Check user permissions:
   ```sql
   SHOW GRANTS FOR otel_monitor;
   ```
3. Enable debug logging:
   ```yaml
   service:
     telemetry:
       logs:
         level: debug
   ```

### Empty Dimension Warnings

**Symptoms:** Dynatrace warnings about empty dimensions (e.g., `database`)

**Explanation:** Some system queries don't have database names. This is normal. The receiver sets `app_name: "cockroachdb-internal"` for these queries, and Dynatrace drops empty attributes while keeping the metrics.

**Solution:** These warnings are harmless and can be ignored.

### Serverless Limitations

CockroachDB Serverless does not support:
- `gossip_liveness`
- `node_metrics`
- `kv_node_status`
- `ranges_no_leases` (limited visibility)

Disable these metrics when connecting to Serverless clusters.

## Security Considerations

1. **Credential Management**
   ```yaml
   connection_string: "${COCKROACHDB_CONNECTION}"  # Use environment variables
   ```

2. **SSL/TLS in Production**
   ```yaml
   connection_string: "postgresql://user:pass@host:26257/db?sslmode=verify-full&sslrootcert=/etc/ssl/ca.crt"
   ```

3. **Least Privilege Access**
   - Create dedicated monitoring user
   - Grant only necessary SELECT permissions
   - Avoid granting admin or VIEWACTIVITY roles

4. **Network Security**
   - Restrict collector network access
   - Use firewall rules
   - Consider mutual TLS

## Support

- **Issues**: [GitHub Issues](https://github.com/npcomplete777/cockroachdbreceiver/issues)
- **Documentation**: [OpenTelemetry Collector Docs](https://opentelemetry.io/docs/collector/)
- **CockroachDB Docs**: [crdb_internal Documentation](https://www.cockroachlabs.com/docs/stable/crdb-internal)

## License

Apache License 2.0
