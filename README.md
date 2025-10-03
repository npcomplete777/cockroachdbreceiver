# CockroachDB Receiver

## Overview

The CockroachDB receiver connects to a CockroachDB cluster and collects comprehensive metrics about database performance, health, and operations. It queries internal CockroachDB system tables to gather metrics across 12 distinct categories.

**Key Features:**
- **Selective Metric Collection**: Enable only the metric groups you need
- **Parallel Collection**: Concurrent metric gathering for optimal performance
- **Security**: Automatically sanitizes connection strings in resource attributes
- **Flexible Configuration**: Comprehensive connection pooling and query tuning options
- **Production Ready**: Tested with CockroachDB v22.x and v23.x (both dedicated and serverless)

## Prerequisites

**CockroachDB Requirements:**
- CockroachDB v22.1 or later (dedicated or serverless)
- Database user with `SELECT` permissions on system tables:
  - `crdb_internal.cluster_statement_statistics`
  - `crdb_internal.cluster_transaction_statistics`
  - `crdb_internal.cluster_queries`
  - `crdb_internal.cluster_sessions`
  - `crdb_internal.cluster_contention_events`
  - `crdb_internal.ranges_no_leases`
  - `crdb_internal.gossip_liveness`
  - `crdb_internal.jobs`
  - `crdb_internal.schema_changes`
  - `crdb_internal.index_usage_statistics`
  - `crdb_internal.tables`
  - `crdb_internal.table_indexes`
  - `information_schema.schemata`

**Network Requirements:**
- Network connectivity to CockroachDB on port 26257 (default)
- SSL/TLS configuration if required by your cluster

## Configuration

### Basic Configuration

```yaml
receivers:
  cockroachdb:
    # Required: PostgreSQL-compatible connection string
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=disable"
    
    # Required: How often to collect metrics
    collection_interval: "1m"
```

### Complete Configuration

```yaml
receivers:
  cockroachdb:
    # Connection string (required)
    # Format: postgresql://[user[:password]@][host][:port][/database][?option=value]
    # Examples:
    #   postgresql://root@localhost:26257/defaultdb?sslmode=disable
    #   postgresql://myuser:mypass@crdb.example.com:26257/mydb?sslmode=require
    #   postgresql://admin:${DB_PASSWORD}@cockroach-lb.internal:26257/defaultdb
    connection_string: "postgresql://user:password@localhost:26257/defaultdb?sslmode=disable"
    
    # Collection interval (required)
    # How frequently to scrape metrics
    # Valid values: "30s", "1m", "5m", etc.
    collection_interval: "1m"
    
    # Query timeout (optional, default: 30s)
    # Maximum time to wait for any single SQL query to complete
    # Increase if you have slow queries or high latency
    query_timeout: 30s
    
    # Query limit (optional, default: 20)
    # Maximum number of results to return per query
    # Higher values = more comprehensive data but slower collection
    query_limit: 20
    
    # Connection pool settings (optional)
    # Maximum open connections to the database (default: 10)
    max_open_connections: 10
    
    # Maximum idle connections in the pool (default: 5)
    max_idle_connections: 5
    
    # Maximum lifetime of a connection (default: 1h)
    connection_max_lifetime: 1h
    
    # Maximum time a connection can be idle (default: 10m)
    connection_max_idle_time: 10m
    
    # Selective metric collection (optional)
    # If omitted or empty, ALL metric groups are collected
    # Include only the groups you need to reduce overhead
    enabled_metrics:
      - query          # Query execution and latency metrics
      - transaction    # Transaction statistics
      - session        # Active sessions and connections
      - index          # Index usage statistics
      - table          # Table sizes and row counts
      - contention     # Lock contention events
      - range          # Range health metrics
      - node           # Node liveness status
      - job            # Background job tracking
      - changefeed     # Changefeed lag metrics
      - schema         # Schema change progress
      - error          # Statement error tracking
```

### Connection String Format

The receiver uses PostgreSQL's connection string format (CockroachDB is PostgreSQL-wire compatible):

**Standard Format:**
```
postgresql://[user[:password]@][host][:port][/database][?options]
```

**Common Options:**
- `sslmode=disable` - No SSL (development only)
- `sslmode=require` - Require SSL but don't verify certificate
- `sslmode=verify-full` - Require SSL and verify certificate
- `sslcert=path` - Path to client certificate
- `sslkey=path` - Path to client key
- `sslrootcert=path` - Path to CA certificate

**Examples:**

```yaml
# Local development (no SSL)
connection_string: "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# Production with SSL
connection_string: "postgresql://monitor_user:${DB_PASSWORD}@prod-cluster.example.com:26257/defaultdb?sslmode=verify-full&sslrootcert=/etc/ssl/ca.crt"

# CockroachDB Serverless
connection_string: "postgresql://myuser:mypass@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"

# Using environment variable
connection_string: "${COCKROACHDB_CONNECTION_STRING}"
```

## Metric Groups

The receiver organizes metrics into 12 groups. Use `enabled_metrics` to collect only what you need.

| Group | Description | Use Case |
|-------|-------------|----------|
| `query` | Query execution counts, mean latency, percentiles (p50/p95/p99) | Query performance monitoring, slow query identification |
| `transaction` | Transaction counts and statistics | Transaction throughput monitoring |
| `session` | Active queries, sessions, connection counts | Connection pool sizing, capacity planning |
| `index` | Index read counts per table/index | Index usage analysis, identifying unused indexes |
| `table` | Table row counts and disk usage | Capacity planning, table growth tracking |
| `contention` | Lock contention time and event counts | Identifying hot spots, transaction conflicts |
| `range` | Total, under-replicated, unavailable ranges | Cluster health, replication status |
| `node` | Node liveness status | Node health monitoring, detecting failures |
| `job` | Active background jobs (backup, restore, import, changefeed) | Job monitoring, identifying stuck jobs |
| `changefeed` | Changefeed replication lag | CDC pipeline health |
| `schema` | In-progress schema changes | Schema change tracking |
| `error` | Statement error counts by error code | Error rate monitoring, debugging |

## Metrics Reference

### Query Metrics (`enabled_metrics: [query]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.query.execution_count` | Gauge | `1` | Number of times query executed | `query` |
| `cockroachdb.query.latency` | Gauge | `s` | Mean query latency | `query` |
| `cockroachdb.query.latency.p50` | Gauge | `s` | 50th percentile latency | `query` |
| `cockroachdb.query.latency.p95` | Gauge | `s` | 95th percentile latency | `query` |
| `cockroachdb.query.latency.p99` | Gauge | `s` | 99th percentile latency | `query` |
| `cockroachdb.query.errors` | Gauge | `1` | Query error count | `query` |

### Transaction Metrics (`enabled_metrics: [transaction]`)

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `cockroachdb.transactions.total` | Gauge | `1` | Total transaction count |

### Session Metrics (`enabled_metrics: [session]`)

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `cockroachdb.queries.active` | Gauge | `1` | Currently executing queries |
| `cockroachdb.sessions.active` | Gauge | `1` | Currently active sessions |
| `cockroachdb.connections.total` | Gauge | `1` | Total unique connections |

### Index Metrics (`enabled_metrics: [index]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.index.reads` | Gauge | `1` | Total reads per index | `table`, `index` |

### Table Metrics (`enabled_metrics: [table]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.databases.total` | Gauge | `1` | Total user databases | |
| `cockroachdb.table.rows` | Gauge | `1` | Estimated row count | `database`, `table` |
| `cockroachdb.table.disk_bytes` | Gauge | `By` | Disk space used | `database`, `table` |

### Contention Metrics (`enabled_metrics: [contention]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.contention.time` | Gauge | `s` | Cumulative contention time | `table`, `index` |
| `cockroachdb.contention.events` | Gauge | `1` | Number of contention events | `table`, `index` |

### Range Metrics (`enabled_metrics: [range]`)

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `cockroachdb.ranges.total` | Gauge | `1` | Total ranges in cluster |
| `cockroachdb.ranges.under_replicated` | Gauge | `1` | Ranges with < 3 replicas (warning) |
| `cockroachdb.ranges.unavailable` | Gauge | `1` | Ranges with no replicas (critical) |

### Node Metrics (`enabled_metrics: [node]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.node.live` | Gauge | `1` | Node liveness (1=live, 0=dead) | `node_id`, `address` |

### Job Metrics (`enabled_metrics: [job]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.jobs.active` | Gauge | `1` | Active background jobs | `job_id`, `job_type`, `status`, `running_status` |

### Changefeed Metrics (`enabled_metrics: [changefeed]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.changefeed.lag_seconds` | Gauge | `s` | Replication lag | `job_id` |

### Schema Metrics (`enabled_metrics: [schema]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.schema_changes.in_progress` | Gauge | `1` | In-progress schema changes | `table`, `type`, `state` |

### Error Metrics (`enabled_metrics: [error]`)

| Metric | Type | Unit | Description | Attributes |
|--------|------|------|-------------|------------|
| `cockroachdb.statement.errors` | Gauge | `1` | Statement error count | `query`, `error_code` |

## Resource Attributes

All metrics include these resource attributes:

| Attribute | Type | Description | Example |
|-----------|------|-------------|---------|
| `cockroachdb.endpoint` | string | Sanitized connection endpoint (credentials removed) | `cockroachdb://prod-cluster.example.com:26257/defaultdb` |

**Security Note:** Usernames and passwords are automatically removed from the endpoint attribute to prevent credential leakage in metrics.

## Example Configurations

### Development: Debug Output

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
    collection_interval: "30s"

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [debug]
```

### Production: Dynatrace

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "1m"
    query_timeout: 30s
    enabled_metrics:
      - query
      - range
      - node
      - contention

exporters:
  otlphttp:
    endpoint: "https://{your-environment-id}.live.dynatrace.com/api/v2/otlp"
    headers:
      Authorization: "Api-Token ${DYNATRACE_API_TOKEN}"

processors:
  batch:
    timeout: 10s
    send_batch_size: 1024

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [otlphttp]
```

### Production: Prometheus

```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://monitor:${DB_PASS}@cockroach-lb.internal:26257/defaultdb?sslmode=require"
    collection_interval: "1m"
    query_limit: 50
    enabled_metrics:
      - query
      - transaction
      - session
      - range
      - node

exporters:
  prometheus:
    endpoint: "0.0.0.0:8889"

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [prometheus]
```

### Minimal: Focus on Health

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "30s"
    enabled_metrics:
      - range    # Under-replicated/unavailable ranges
      - node     # Node liveness
      - error    # Statement errors

exporters:
  otlphttp:
    endpoint: "https://your-backend.com/v1/metrics"

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [otlphttp]
```

### Complete: All Metrics

```yaml
receivers:
  cockroachdb:
    connection_string: "${COCKROACHDB_CONNECTION}"
    collection_interval: "1m"
    query_timeout: 30s
    query_limit: 20
    max_open_connections: 10
    max_idle_connections: 5
    # enabled_metrics not specified = collect all groups

exporters:
  otlphttp:
    endpoint: "${OTEL_EXPORTER_OTLP_ENDPOINT}"
    headers:
      Authorization: "Bearer ${API_TOKEN}"

processors:
  batch:

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      processors: [batch]
      exporters: [otlphttp]
```

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
        enabled_metrics: [query, range, node]
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
  connection: "postgresql://user:password@cockroachdb:26257/defaultdb"
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

## Troubleshooting

### Connection Issues

**Problem:** `Failed to connect to database`

```
Error: failed to ping database: dial tcp: lookup cockroachdb on 8.8.8.8:53: no such host
```

**Solution:**
1. Verify hostname is correct: `nslookup cockroachdb`
2. Check network connectivity: `telnet cockroachdb 26257`
3. Verify connection string format
4. Check SSL settings match cluster requirements

---

**Problem:** `Authentication failed`

```
Error: pq: password authentication failed for user "myuser"
```

**Solution:**
1. Verify credentials are correct
2. Check user has proper permissions:
```sql
GRANT SELECT ON crdb_internal.* TO myuser;
GRANT SELECT ON information_schema.* TO myuser;
```
3. Ensure user exists: `SHOW USERS;`

---

### SSL/TLS Issues

**Problem:** `SSL is required`

```
Error: pq: server requires SSL
```

**Solution:**
Change `sslmode=disable` to `sslmode=require` or higher:
```yaml
connection_string: "postgresql://user:pass@host:26257/db?sslmode=require"
```

---

**Problem:** `Certificate verify failed`

```
Error: x509: certificate signed by unknown authority
```

**Solution:**
Provide CA certificate:
```yaml
connection_string: "postgresql://user:pass@host:26257/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt"
```

---

### Performance Issues

**Problem:** Slow metric collection (> 30s per scrape)

**Solution:**
1. Reduce `query_limit`:
```yaml
query_limit: 10  # Default is 20
```

2. Enable selective metrics:
```yaml
enabled_metrics:
  - query
  - range
  - node
```

3. Increase `query_timeout`:
```yaml
query_timeout: 60s
```

4. Check CockroachDB performance with `SHOW QUERIES;`

---

**Problem:** High CPU usage on CockroachDB

**Solution:**
1. Increase `collection_interval`:
```yaml
collection_interval: "5m"  # Default is 1m
```

2. Reduce concurrent metric collection by lowering `query_limit`

3. Use selective metrics to avoid expensive queries

---

### Missing Metrics

**Problem:** No metrics appear in backend

**Solution:**
1. Check receiver logs for errors:
```bash
# In collector logs
grep "cockroachdb" /var/log/otel-collector.log
```

2. Verify connection with debug exporter:
```yaml
exporters:
  debug:
    verbosity: detailed
```

3. Check that `enabled_metrics` includes desired groups

4. Verify user has SELECT permissions on system tables

---

**Problem:** Node metrics missing (empty)

**This is expected for CockroachDB Serverless** - node-level metrics aren't available in serverless deployments. Disable the `node` metric group:

```yaml
enabled_metrics:
  - query
  - range
  # node metrics not available in serverless
```

---

### Query Timeout Issues

**Problem:** Queries timing out

```
Error: context deadline exceeded
```

**Solution:**
```yaml
query_timeout: 60s  # Increase from default 30s
```

For slow clusters or high latency networks, increase timeout further.

---

### Memory Issues

**Problem:** High memory usage

**Solution:**
1. Limit result sets:
```yaml
query_limit: 10
```

2. Use selective metrics

3. Reduce `max_open_connections`:
```yaml
max_open_connections: 5
```

## Database User Setup

Create a dedicated monitoring user with minimal privileges:

```sql
-- Create monitoring user
CREATE USER otel_monitor WITH PASSWORD 'secure_password';

-- Grant necessary permissions
GRANT SELECT ON crdb_internal.cluster_statement_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_transaction_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_queries TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_sessions TO otel_monitor;
GRANT SELECT ON crdb_internal.cluster_contention_events TO otel_monitor;
GRANT SELECT ON crdb_internal.ranges_no_leases TO otel_monitor;
GRANT SELECT ON crdb_internal.gossip_liveness TO otel_monitor;
GRANT SELECT ON crdb_internal.jobs TO otel_monitor;
GRANT SELECT ON crdb_internal.schema_changes TO otel_monitor;
GRANT SELECT ON crdb_internal.index_usage_statistics TO otel_monitor;
GRANT SELECT ON crdb_internal.tables TO otel_monitor;
GRANT SELECT ON crdb_internal.table_indexes TO otel_monitor;
GRANT SELECT ON information_schema.schemata TO otel_monitor;

-- Verify permissions
SHOW GRANTS FOR otel_monitor;
```

## Security Considerations

1. **Credential Management**: Never hardcode passwords. Use environment variables:
```yaml
connection_string: "${COCKROACHDB_CONNECTION}"
```

2. **SSL/TLS**: Always use SSL in production:
```yaml
connection_string: "postgresql://user:pass@host:26257/db?sslmode=verify-full"
```

3. **Sanitized Metrics**: Connection credentials are automatically removed from the `cockroachdb.endpoint` resource attribute

4. **Least Privilege**: Create a dedicated read-only user for monitoring (see Database User Setup)

5. **Network Security**: Use firewalls to restrict collector access to CockroachDB

## Performance Tuning

### Recommended Settings by Scale

**Small Cluster (< 10 nodes, < 100 GB)**
```yaml
collection_interval: "1m"
query_timeout: 30s
query_limit: 20
max_open_connections: 5
```

**Medium Cluster (10-50 nodes, 100 GB - 1 TB)**
```yaml
collection_interval: "1m"
query_timeout: 45s
query_limit: 50
max_open_connections: 10
```

**Large Cluster (> 50 nodes, > 1 TB)**
```yaml
collection_interval: "2m"
query_timeout: 60s
query_limit: 100
max_open_connections: 15
```

### Selective Metrics Strategy

**Minimal (Health Monitoring)**
```yaml
enabled_metrics: [range, node, error]
```

**Standard (Performance + Health)**
```yaml
enabled_metrics: [query, session, range, node, contention]
```

**Comprehensive (Full Observability)**
```yaml
enabled_metrics: [query, transaction, session, index, range, node, job, error]
```

**Application-Focused**
```yaml
enabled_metrics: [query, transaction, contention, error]
```

## Development

### Building from Source

```bash
git clone https://github.com/open-telemetry/opentelemetry-collector-contrib.git
cd opentelemetry-collector-contrib/receiver/cockroachreceiver
go build
```

### Running Tests

```bash
# Run all tests
go test -v

# Run specific test
go test -v -run TestConfigValidate

# Run with coverage
go test -v -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Local Testing

1. Start CockroachDB locally:
```bash
cockroach start-single-node --insecure --listen-addr=localhost:26257
```

2. Create test config:
```yaml
receivers:
  cockroachdb:
    connection_string: "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
    collection_interval: "10s"

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [cockroachdb]
      exporters: [debug]
```

3. Run collector:
```bash
otelcol-contrib --config config.yaml
```

## Support

- **Issues**: [GitHub Issues](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues)
- **Discussions**: [CNCF Slack #otel-collector](https://cloud-native.slack.com/)
- **Documentation**: [OpenTelemetry Collector Docs](https://opentelemetry.io/docs/collector/)
- **CockroachDB Docs**: [CockroachDB Documentation](https://www.cockroachlabs.com/docs/)

## Version Compatibility

| Receiver Version | CockroachDB Version | Collector Version |
|------------------|---------------------|-------------------|
| v1.0.0+ | v22.1+ | v0.136.0+ |

**Tested Configurations:**
- CockroachDB v22.2 (Dedicated)
- CockroachDB v23.1 (Dedicated)
- CockroachDB Serverless (latest)

## License

Apache License 2.0
