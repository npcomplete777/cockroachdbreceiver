# CockroachDB Receiver

## Overview

The CockroachDB receiver connects to a CockroachDB cluster and collects comprehensive metrics about database performance, health, and operations by querying internal CockroachDB system tables (`crdb_internal.*`).

**Key Features:**
- **Granular Metric Control**: Enable/disable individual metric types
- **Production Safety Warnings**: Clear distinction between safe and expensive metrics
- **Direct Internal Table Access**: Queries CockroachDB's native statistics tables
- **Flexible Configuration**: Comprehensive connection pooling and query tuning
- **Production Ready**: Separate configurations for production and troubleshooting

## Prerequisites

**CockroachDB Requirements:**
- CockroachDB v22.1 or later (dedicated or serverless)
- Database user with `SELECT` permissions on required `crdb_internal` tables (see Database User Setup)

**Network Requirements:**
- Network connectivity to CockroachDB on port 26257 (default)
- SSL/TLS configuration if required by your cluster

## Configuration

### Basic Configuration

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=disable"
    collection_interval: "1m"
```

### Complete Configuration

```yaml
receivers:
  cockroachdb:
    # Connection string (required)
    # Format: postgresql://[user[:password]@][host][:port][/database][?option=value]
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=disable"
    
    # Collection interval (required)
    collection_interval: "1m"
    
    # Query timeout (optional, default: 30s)
    query_timeout: "30s"
    
    # Connection pool settings (optional)
    max_open_connections: 10           # Default: 10
    max_idle_connections: 5            # Default: 5
    connection_max_lifetime: "1h"      # Default: 1h
    connection_max_idle_time: "10m"    # Default: 10m
    
    # Granular metric collection control
    # All metrics default to appropriate values for production safety
    metrics:
      # ===== PRODUCTION SAFE (Low Overhead) =====
      # These metrics are safe for continuous production monitoring
      statement_statistics: true          # Query performance from crdb_internal.statement_statistics
      transaction_statistics: true        # Transaction perf from crdb_internal.transaction_statistics
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
      # ⚠️ WARNING: These trigger expensive cluster-wide operations
      # Only enable for troubleshooting in dev/staging environments
      ranges_no_leases: false      # ⚠️ EXPENSIVE: Range distribution (triggers RPC fan-out)
      gossip_liveness: false        # ⚠️ UNSTABLE: Node liveness (schema may change)
      jobs: false                   # ⚠️ EXPENSIVE: Background jobs
      schema_changes: false         # Schema change operations
      node_metrics: false           # ⚠️ EXPENSIVE: Node-level system metrics (RPC fan-out)
      kv_node_status: false         # ⚠️ EXPENSIVE: KV layer status (RPC fan-out)
```

### Connection String Format

**Standard Format:**
```
postgresql://[user[:password]@][host][:port][/database][?options]
```

**Common SSL Options:**
- `sslmode=disable` - No SSL (development only)
- `sslmode=require` - Require SSL but don't verify certificate
- `sslmode=verify-full` - Require SSL and verify certificate
- `sslrootcert=path` - Path to CA certificate

**Examples:**

```yaml
# Local development
connection_string: "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# Production with SSL
connection_string: "postgresql://monitor:${DB_PASSWORD}@prod.example.com:26257/defaultdb?sslmode=verify-full&sslrootcert=/etc/ssl/ca.crt"

# CockroachDB Serverless
connection_string: "postgresql://user:pass@cluster.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"

# Using environment variable
connection_string: "${COCKROACHDB_CONNECTION_STRING}"
```

## Metric Groups and Data Sources

### Production Safe Metrics (Low Overhead)

These metrics query aggregated statistics tables that are designed for continuous monitoring:

| Metric Type | Source Table | Description | Overhead |
|-------------|--------------|-------------|----------|
| `statement_statistics` | `crdb_internal.statement_statistics` | Aggregated statement-level performance metrics | Very Low |
| `transaction_statistics` | `crdb_internal.transaction_statistics` | Aggregated transaction-level metrics | Very Low |
| `index_usage_statistics` | `crdb_internal.index_usage_statistics` | Index read counts per table/index | Very Low |
| `cluster_queries` | `crdb_internal.cluster_queries` | Currently executing queries | Low |
| `cluster_sessions` | `crdb_internal.cluster_sessions` | Active sessions and memory usage | Low |
| `cluster_transactions` | `crdb_internal.cluster_transactions` | Active transactions | Low |

### Production Safe Metrics (Moderate Overhead)

These metrics are generally safe but may add overhead under certain conditions:

| Metric Type | Source Table | Description | When to Use |
|-------------|--------------|-------------|-------------|
| `cluster_contended_indexes` | `crdb_internal.cluster_contended_indexes` | Summary of indexes with contention | Always safe |
| `cluster_contended_tables` | `crdb_internal.cluster_contended_tables` | Summary of tables with contention | Always safe |
| `cluster_contention_events` | `crdb_internal.cluster_contention_events` | Historical contention events | Always safe |
| `cluster_locks` | `crdb_internal.cluster_locks` | Active locks (triggers RPC) | Enable for deadlock debugging |
| `cluster_contended_keys` | `crdb_internal.cluster_contended_keys` | Specific contended keys | Enable for detailed contention analysis |
| `transaction_contention_events` | `crdb_internal.transaction_contention_events` | Transaction-level contention details (triggers RPC) | Troubleshooting only |

### Not Production Safe (Expensive Operations)

**⚠️ WARNING:** These metrics trigger expensive cluster-wide RPCs and should only be enabled for troubleshooting:

| Metric Type | Source Table | Why Expensive | Impact |
|-------------|--------------|---------------|--------|
| `ranges_no_leases` | `crdb_internal.ranges_no_leases` | Cluster-wide RPC fan-out | Very High |
| `gossip_liveness` | `crdb_internal.gossip_liveness` | Unstable schema, cluster-wide query | High |
| `jobs` | `crdb_internal.jobs` | Complex queries across all nodes | High |
| `schema_changes` | `crdb_internal.schema_changes` | Scans schema change metadata | Medium |
| `node_metrics` | `crdb_internal.node_metrics` | Cluster-wide RPC fan-out | Very High |
| `kv_node_status` | `crdb_internal.kv_node_status` | KV layer diagnostics, RPC fan-out | Very High |

## Metrics Reference

### Statement Statistics (`statement_statistics: true`)

Source: `crdb_internal.statement_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.statement.execution.count` | Gauge | `1` | Statement execution count | `fingerprint_id`, `app_name`, `database`, `statement_type` |
| `cockroachdb.statement.latency.service` | Gauge | `s` | Average service latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.parse` | Gauge | `s` | Parse latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.plan` | Gauge | `s` | Planning latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.run` | Gauge | `s` | Execution latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.p50` | Gauge | `s` | 50th percentile latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.p95` | Gauge | `s` | 95th percentile latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.latency.p99` | Gauge | `s` | 99th percentile latency | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.rows_read` | Gauge | `1` | Average rows read | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.rows_written` | Gauge | `1` | Average rows written | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.bytes_read` | Gauge | `By` | Average bytes read from disk | `fingerprint_id`, `app_name` |
| `cockroachdb.statement.errors` | Gauge | `1` | Error count | `fingerprint_id`, `error_code` |
| `cockroachdb.statement.retries` | Gauge | `1` | Maximum retries | `fingerprint_id`, `app_name` |

### Transaction Statistics (`transaction_statistics: true`)

Source: `crdb_internal.transaction_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.transaction.execution.count` | Gauge | `1` | Transaction execution count | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.service` | Gauge | `s` | Average service latency | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.commit` | Gauge | `s` | Commit latency | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.latency.retry` | Gauge | `s` | Retry latency | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.rows_read` | Gauge | `1` | Average rows read | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.rows_written` | Gauge | `1` | Average rows written | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.bytes_read` | Gauge | `By` | Average bytes read | `fingerprint_id`, `app_name` |
| `cockroachdb.transaction.retries` | Gauge | `1` | Maximum retries | `fingerprint_id`, `app_name` |

### Index Usage Statistics (`index_usage_statistics: true`)

Source: `crdb_internal.index_usage_statistics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.index.reads.total` | Gauge | `1` | Total index reads | `database`, `table`, `index` |
| `cockroachdb.index.last_read` | Gauge | `s` | Seconds since last read | `database`, `table`, `index` |

### Active Queries (`cluster_queries: true`)

Source: `crdb_internal.cluster_queries`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.queries.active` | Gauge | `1` | Currently executing queries | `node_id`, `app_name`, `user_name` |
| `cockroachdb.query.duration` | Gauge | `s` | Query execution duration | `query_id`, `node_id`, `app_name` |

### Active Sessions (`cluster_sessions: true`)

Source: `crdb_internal.cluster_sessions`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.sessions.active` | Gauge | `1` | Active sessions | `node_id`, `app_name`, `user_name` |
| `cockroachdb.session.memory_usage` | Gauge | `By` | Session memory allocated | `session_id`, `node_id`, `app_name` |
| `cockroachdb.session.age` | Gauge | `s` | Session age | `session_id`, `node_id`, `app_name` |

### Active Transactions (`cluster_transactions: true`)

Source: `crdb_internal.cluster_transactions`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.transactions.active` | Gauge | `1` | Active transactions | `node_id`, `app_name` |
| `cockroachdb.transaction.age` | Gauge | `s` | Transaction age | `txn_id`, `node_id`, `app_name` |

### Contended Indexes (`cluster_contended_indexes: true`)

Source: `crdb_internal.cluster_contended_indexes`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.contention.index.events` | Gauge | `1` | Contention event count | `database`, `schema`, `table`, `index` |

### Contended Tables (`cluster_contended_tables: true`)

Source: `crdb_internal.cluster_contended_tables`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.contention.table.events` | Gauge | `1` | Contention event count | `database`, `schema`, `table` |

### Contention Events (`cluster_contention_events: true`)

Source: `crdb_internal.cluster_contention_events`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.contention.time` | Gauge | `s` | Cumulative contention time | `table_id`, `index_id` |
| `cockroachdb.contention.events.total` | Gauge | `1` | Total contention events | `table_id`, `index_id` |

### Contended Keys (`cluster_contended_keys: false`)

⚠️ **Enable only for debugging** - provides key-level contention detail.

Source: `crdb_internal.cluster_contended_keys`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.contention.key.events` | Gauge | `1` | Contention events per key | `database`, `schema`, `table`, `index`, `key` |

### Cluster Locks (`cluster_locks: false`)

⚠️ **Triggers RPC fan-out** - enable only when debugging deadlocks.

Source: `crdb_internal.cluster_locks`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.locks.active` | Gauge | `1` | Active locks | `database`, `table`, `lock_strength` |
| `cockroachdb.locks.waiting` | Gauge | `1` | Waiting locks | `database`, `table`, `lock_strength` |
| `cockroachdb.lock.duration` | Gauge | `s` | Lock hold duration | `txn_id`, `database`, `table` |

### Transaction Contention Events (`transaction_contention_events: false`)

⚠️ **Expensive RPC operation** - use only for troubleshooting.

Source: `crdb_internal.transaction_contention_events`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.transaction.contention.duration` | Gauge | `s` | Contention wait time | `blocking_txn_fingerprint_id`, `waiting_txn_fingerprint_id` |
| `cockroachdb.transaction.contention.events` | Gauge | `1` | Contention event count | `database`, `table`, `contention_type` |

### Ranges (`ranges_no_leases: false`)

⚠️ **EXPENSIVE: Cluster-wide RPC fan-out** - avoid in production.

Source: `crdb_internal.ranges_no_leases`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.ranges.total` | Gauge | `1` | Total ranges | |
| `cockroachdb.ranges.under_replicated` | Gauge | `1` | Under-replicated ranges | |
| `cockroachdb.ranges.unavailable` | Gauge | `1` | Unavailable ranges | |

### Node Liveness (`gossip_liveness: false`)

⚠️ **UNSTABLE SCHEMA** - schema may change without notice.

Source: `crdb_internal.gossip_liveness`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.node.liveness` | Gauge | `1` | Node liveness (1=live, 0=dead) | `node_id`, `address` |

### Background Jobs (`jobs: false`)

⚠️ **EXPENSIVE** - queries across all nodes.

Source: `crdb_internal.jobs`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.jobs.active` | Gauge | `1` | Active background jobs | `job_id`, `job_type`, `status` |
| `cockroachdb.job.progress` | Gauge | `1` | Job progress (0-1) | `job_id`, `job_type` |

### Schema Changes (`schema_changes: false`)

Source: `crdb_internal.schema_changes`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.schema_changes.in_progress` | Gauge | `1` | In-progress schema changes | `table`, `type`, `state` |

### Node Metrics (`node_metrics: false`)

⚠️ **EXPENSIVE: Cluster-wide RPC fan-out** - avoid in production.

Source: `crdb_internal.node_metrics`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.node.cpu.usage` | Gauge | `1` | Node CPU usage | `node_id` |
| `cockroachdb.node.memory.usage` | Gauge | `By` | Node memory usage | `node_id` |

### KV Node Status (`kv_node_status: false`)

⚠️ **EXPENSIVE: RPC fan-out** - troubleshooting only.

Source: `crdb_internal.kv_node_status`

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.kv.node.status` | Gauge | `1` | KV node status | `node_id` |

## Resource Attributes

All metrics include these resource attributes:

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `cockroachdb.endpoint` | string | Sanitized connection endpoint (credentials removed) | `cockroachdb://prod.example.com:26257/defaultdb` |
| `cockroachdb.cluster` | string | Cluster identifier (if available) | `production-cluster` |

## Example Configurations

### Production: Core Metrics Only

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "1m"
    query_timeout: "30s"
    
    metrics:
      # Core performance metrics
      statement_statistics: true
      transaction_statistics: true
      index_usage_statistics: true
      
      # Active workload
      cluster_queries: true
      cluster_sessions: true
      cluster_transactions: true
      
      # Contention summaries
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contention_events: true
      
      # Disable everything expensive
      cluster_locks: false
      cluster_contended_keys: false
      transaction_contention_events: false
      ranges_no_leases: false
      gossip_liveness: false
      jobs: false
      schema_changes: false
      node_metrics: false
      kv_node_status: false

exporters:
  otlphttp:
    endpoint: "https://your-backend.com/v1/metrics"
    headers:
      Authorization: "Bearer ${API_TOKEN}"

processors:
  batch:
    timeout: 10s

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [otlphttp]
```

### Development/Staging: Full Observability

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "30s"
    query_timeout: "60s"
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
      
      # Detailed contention (expensive but useful for debugging)
      cluster_locks: true
      cluster_contended_keys: true
      transaction_contention_events: true
      
      # ⚠️ Expensive metrics enabled for full visibility
      ranges_no_leases: true
      gossip_liveness: true
      jobs: true
      schema_changes: true
      node_metrics: true
      kv_node_status: true

exporters:
  logging:
    verbosity: detailed
  otlphttp:
    endpoint: "https://your-backend.com/v1/metrics"

service:
  telemetry:
    logs:
      level: debug
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [logging, otlphttp]
```

### Troubleshooting: Contention Analysis

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "10s"  # More frequent for debugging
    query_timeout: "60s"
    
    metrics:
      # Disable non-contention metrics
      statement_statistics: false
      transaction_statistics: false
      index_usage_statistics: false
      cluster_queries: false
      cluster_sessions: false
      cluster_transactions: false
      
      # Enable all contention metrics
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contended_keys: true
      cluster_contention_events: true
      cluster_locks: true
      transaction_contention_events: true
      
      # Keep other expensive metrics off
      ranges_no_leases: false
      gossip_liveness: false
      jobs: false
      schema_changes: false
      node_metrics: false
      kv_node_status: false

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [debug]
```

### Prometheus Export

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://monitor:${DB_PASS}@cockroach.internal:26257/defaultdb?sslmode=require"
    collection_interval: "1m"
    
    metrics:
      statement_statistics: true
      transaction_statistics: true
      index_usage_statistics: true
      cluster_queries: true
      cluster_sessions: true
      cluster_transactions: true
      cluster_contended_indexes: true
      cluster_contended_tables: true
      cluster_contention_events: true

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"
    namespace: "cockroachdb"

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [prometheus]
```

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

## Performance Impact & Tuning

### Production Settings Recommendations

**Small Cluster (< 10 nodes, < 100 GB)**
```yaml
collection_interval: "1m"
query_timeout: "30s"
max_open_connections: 5
metrics:
  # Use only lightweight metrics
  statement_statistics: true
  transaction_statistics: true
  cluster_queries: true
  cluster_sessions: true
```

**Medium Cluster (10-50 nodes, 100 GB - 1 TB)**
```yaml
collection_interval: "1m"
query_timeout: "45s"
max_open_connections: 10
metrics:
  # Add contention summaries
  statement_statistics: true
  transaction_statistics: true
  index_usage_statistics: true
  cluster_queries: true
  cluster_sessions: true
  cluster_transactions: true
  cluster_contended_indexes: true
  cluster_contended_tables: true
  cluster_contention_events: true
```

**Large Cluster (> 50 nodes, > 1 TB)**
```yaml
collection_interval: "2m"  # Less frequent
query_timeout: "60s"
max_open_connections: 15
metrics:
  # Be selective to reduce overhead
  statement_statistics: true
  transaction_statistics: true
  cluster_contended_indexes: true
  cluster_contended_tables: true
  cluster_contention_events: true
```

### Metric Selection Strategy

**Minimal (Health Check)**
- `cluster_queries`, `cluster_sessions`, `cluster_transactions`

**Standard (Performance Monitoring)**
- Add: `statement_statistics`, `transaction_statistics`, `index_usage_statistics`

**Comprehensive (Production)**
- Add: `cluster_contended_indexes`, `cluster_contended_tables`, `cluster_contention_events`

**Troubleshooting Only**
- Add: `cluster_locks`, `cluster_contended_keys`, `transaction_contention_events`

**Never in Production**
- `ranges_no_leases`, `gossip_liveness`, `jobs`, `node_metrics`, `kv_node_status`

## Troubleshooting

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
     cluster_locks: false
     transaction_contention_events: false
     ranges_no_leases: false
     node_metrics: false
   ```

3. Lower connection pool:
   ```yaml
   max_open_connections: 5
   max_idle_connections: 2
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

### Serverless Limitations

CockroachDB Serverless does not support:
- `gossip_liveness`
- `node_metrics`
- `kv_node_status`
- `ranges_no_leases` (limited visibility)

Disable these metrics when connecting to Serverless clusters.

## Data Coverage Analysis

### Currently Implemented

The receiver currently covers **12 of 60+** available `crdb_internal` tables:

✅ **Covered:**
- `statement_statistics`
- `transaction_statistics`
- `index_usage_statistics`
- `cluster_queries`
- `cluster_sessions`
- `cluster_transactions`
- `cluster_locks`
- `cluster_contended_indexes`
- `cluster_contended_keys`
- `cluster_contended_tables`
- `cluster_contention_events`
- `transaction_contention_events`
- `ranges_no_leases` (partial)
- `gossip_liveness`
- `jobs`
- `schema_changes`
- `node_metrics`
- `kv_node_status`

### Not Yet Implemented

Tables that could be added for expanded coverage:

**Potentially Useful (Production Safe):**
- `table_row_statistics` - Row counts per table
- `tables` - Table metadata and sizes
- `table_indexes` - Index definitions

**Advanced Diagnostics (Expensive):**
- `ranges` - Detailed range information
- `leases` - Lease holder information
- `cluster_distsql_flows` - DistSQL execution flows
- `cluster_execution_insights` - Query execution insights

**Specialized Use Cases:**
- `cluster_settings` - Cluster configuration
- `feature_usage` - Feature usage tracking
- `invalid_objects` - Invalid object detection

## Security Considerations

1. **Credential Management**
   ```yaml
   connection_string: "${COCKROACHDB_CONNECTION}"  # Use env vars
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

5. **Credential Sanitization**
   - Receiver automatically removes credentials from `cockroachdb.endpoint` attribute

## Deployment

### Docker

```dockerfile
FROM otel/opentelemetry-collector-contrib:latest
COPY config.yaml /etc/otelcol-contrib/config.yaml
CMD ["--config=/etc/otelcol-contrib/config.yaml"]
```

```bash
docker run -d \
  --name otel-cockroachdb \
  -e COCKROACHDB_CONNECTION="postgresql://user:pass@host:26257/db" \
  -v $(pwd)/config.yaml:/etc/otelcol-contrib/config.yaml \
  otel/opentelemetry-collector-contrib:latest
```

### Kubernetes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-config
data:
  config.yaml: |
    receivers:
      cockroachdb:
        connection_string: "${COCKROACHDB_CONNECTION}"
        collection_interval: "1m"
        metrics:
          statement_statistics: true
          transaction_statistics: true
          cluster_queries: true
          cluster_sessions: true
    exporters:
      otlphttp:
        endpoint: "http://otel-backend:4318"
    service:
      pipelines:
        metrics:
          receivers: [cockroachdb]
          exporters: [otlphttp]
---
apiVersion: v1
kind: Secret
metadata:
  name: cockroachdb-credentials
stringData:
  connection: "postgresql://user:password@cockroachdb:26257/defaultdb?sslmode=require"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      containers:
      - name: otel-collector
        image: otel/opentelemetry-collector-contrib:latest
        env:
        - name: COCKROACHDB_CONNECTION
          valueFrom:
            secretKeyRef:
              name: cockroachdb-credentials
              key: connection
        volumeMounts:
        - name: config
          mountPath: /etc/otelcol-contrib
      volumes:
      - name: config
        configMap:
          name: otel-collector-config
```

## Version Compatibility

| Receiver Version | CockroachDB Version | Collector Version | Notes |
|------------------|---------------------|-------------------|-------|
| v2.0.0+ | v22.1+ | v0.136.0+ | Granular metric control |
| v1.0.0 | v22.1+ | v0.136.0+ | Legacy group-based config |

## Support

- **Issues**: [GitHub Issues](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues)
- **Documentation**: [OpenTelemetry Collector Docs](https://opentelemetry.io/docs/collector/)
- **CockroachDB Docs**: [crdb_internal Documentation](https://www.cockroachlabs.com/docs/stable/crdb-internal)

## License

Apache License 2.0
