# CockroachDB Metrics

## Statement Metrics (10)
- cockroachdb.statement.execution.count
- cockroachdb.statement.latency.service.mean
- cockroachdb.statement.latency.parse.mean
- cockroachdb.statement.latency.plan.mean
- cockroachdb.statement.latency.run.mean
- cockroachdb.statement.rows.read.mean
- cockroachdb.statement.rows.written.mean
- cockroachdb.statement.bytes.read.mean
- cockroachdb.statement.retries.max
- cockroachdb.statement.error.count

## Transaction Metrics (8)
- cockroachdb.transaction.execution.count
- cockroachdb.transaction.latency.service.mean
- cockroachdb.transaction.latency.commit.mean
- cockroachdb.transaction.latency.retry.mean
- cockroachdb.transaction.rows.read.mean
- cockroachdb.transaction.rows.written.mean
- cockroachdb.transaction.bytes.read.mean
- cockroachdb.transaction.retries.max

## Cluster Metrics (7)
- cockroachdb.cluster.queries.active
- cockroachdb.cluster.queries.duration
- cockroachdb.cluster.sessions.active
- cockroachdb.cluster.sessions.age
- cockroachdb.cluster.sessions.memory_allocated
- cockroachdb.cluster.transactions.active
- cockroachdb.cluster.transactions.duration

## Other Metrics
- cockroachdb.index.reads.total
- cockroachdb.index.seconds_since_last_read
- cockroachdb.jobs.active
- cockroachdb.jobs.progress
- cockroachdb.contention.index.events
- cockroachdb.contention.table.events
- cockroachdb.contention.time.cumulative
- cockroachdb.schema_changes.active
- cockroachdb.node.cpu.percent
- cockroachdb.node.memory.rss
- cockroachdb.ranges.total
- cockroachdb.ranges.under_replicated
- cockroachdb.ranges.unavailable
- cockroachdb.receiver.scrape_success
- cockroachdb.receiver.scrape_errors
