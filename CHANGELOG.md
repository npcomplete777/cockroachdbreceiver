# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
