# CockroachDB Metrics

## Statement Metrics (10)
- cockroachdb.statement.execution_count
- cockroachdb.statement.service_latency_mean
- cockroachdb.statement.parse_latency_mean
- cockroachdb.statement.plan_latency_mean
- cockroachdb.statement.run_latency_mean
- cockroachdb.statement.rows_read_mean
- cockroachdb.statement.rows_written_mean
- cockroachdb.statement.bytes_read_mean
- cockroachdb.statement.max_retries
- cockroachdb.statement.error_count

## Transaction Metrics (8)
- cockroachdb.transaction.execution_count
- cockroachdb.transaction.service_latency_mean
- cockroachdb.transaction.commit_latency_mean
- cockroachdb.transaction.retry_latency_mean
- cockroachdb.transaction.rows_read_mean
- cockroachdb.transaction.rows_written_mean
- cockroachdb.transaction.bytes_read_mean
- cockroachdb.transaction.max_retries

## Cluster Metrics (7)
- cockroachdb.cluster.queries.active
- cockroachdb.cluster.queries.duration
- cockroachdb.cluster.sessions.active
- cockroachdb.cluster.sessions.age
- cockroachdb.cluster.sessions.memory_allocated
- cockroachdb.cluster.transactions.active
- cockroachdb.cluster.transactions.duration

## Other Metrics
- cockroachdb.index.total_reads
- cockroachdb.index.seconds_since_last_read
- cockroachdb.jobs.active
- cockroachdb.jobs.progress
- cockroachdb.contention.* (various)
