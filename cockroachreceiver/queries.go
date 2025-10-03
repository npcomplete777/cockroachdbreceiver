package cockroachreceiver

const (
	// PRODUCTION SAFE - Query Performance
	QueryStatementStatistics = `
SELECT 
    aggregated_ts,
    encode(fingerprint_id, 'hex') as fingerprint_id,
    encode(transaction_fingerprint_id, 'hex') as transaction_fingerprint_id,
    encode(plan_hash, 'hex') as plan_hash,
    app_name,
    metadata->>'query' as query_text,
    metadata->>'db' as database_name,
    metadata->>'querySummary' as query_summary,
    metadata->>'stmtTyp' as statement_type,
    (metadata->>'fullScan')::boolean as full_scan,
    (metadata->>'vec')::boolean as vectorized,
    (metadata->>'implicitTxn')::boolean as implicit_txn,
    (statistics->'statistics'->>'cnt')::bigint as execution_count,
    (statistics->'statistics'->'svcLat'->>'mean')::float as service_latency_mean,
    (statistics->'statistics'->'runLat'->>'mean')::float as run_latency_mean,
    (statistics->'statistics'->'parseLat'->>'mean')::float as parse_latency_mean,
    (statistics->'statistics'->'planLat'->>'mean')::float as plan_latency_mean,
    (statistics->'statistics'->'ovhLat'->>'mean')::float as overhead_latency_mean,
    (statistics->'statistics'->'numRows'->>'mean')::float as rows_mean,
    (statistics->'statistics'->'rowsRead'->>'mean')::float as rows_read_mean,
    (statistics->'statistics'->'bytesRead'->>'mean')::float as bytes_read_mean,
    (statistics->'statistics'->>'maxRetries')::bigint as max_retries,
    statistics->'statistics'->>'lastExecAt' as last_execution_at
FROM crdb_internal.statement_statistics
WHERE aggregated_ts > NOW() - INTERVAL '1 hour'
LIMIT 1000;`

	// PRODUCTION SAFE - Transaction Performance
	QueryTransactionStatistics = `
SELECT
    aggregated_ts,
    encode(fingerprint_id, 'hex') as fingerprint_id,
    app_name,
    (statistics->'execution_statistics'->>'cnt')::bigint as execution_count,
    (statistics->'statistics'->>'cnt')::bigint as total_count,
    (statistics->'statistics'->'svcLat'->>'mean')::float as service_latency_mean,
    (statistics->'statistics'->'commitLat'->>'mean')::float as commit_latency_mean,
    (statistics->'statistics'->'retryLat'->>'mean')::float as retry_latency_mean,
    (statistics->'statistics'->'numRows'->>'mean')::float as rows_mean,
    (statistics->'statistics'->'rowsRead'->>'mean')::float as rows_read_mean,
    (statistics->'statistics'->'rowsWritten'->>'mean')::float as rows_written_mean,
    (statistics->'statistics'->'bytesRead'->>'mean')::float as bytes_read_mean,
    (statistics->'statistics'->>'maxRetries')::bigint as max_retries,
    (statistics->'execution_statistics'->'contentionTime'->>'mean')::float as contention_time_mean
FROM crdb_internal.transaction_statistics
WHERE aggregated_ts > NOW() - INTERVAL '1 hour'
LIMIT 1000;`

	// PRODUCTION SAFE - Index Usage
	QueryIndexUsageStatistics = `
SELECT
    ti.descriptor_name as table_name,
    ti.index_name,
    us.total_reads,
    us.last_read
FROM crdb_internal.index_usage_statistics AS us
JOIN crdb_internal.table_indexes ti
    ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
ORDER BY total_reads DESC
LIMIT 500;`

	// PRODUCTION SAFE - Active Queries
	QueryClusterQueries = `
SELECT
    query_id,
    encode(txn_id::BYTES, 'hex') as txn_id,
    node_id,
    session_id,
    user_name,
    start,
    query,
    client_address,
    application_name,
    distributed,
    phase
FROM crdb_internal.cluster_queries
LIMIT 500;`

	// PRODUCTION SAFE - Active Sessions
	QueryClusterSessions = `
SELECT
    node_id,
    session_id,
    user_name,
    client_address,
    application_name,
    active_queries,
    last_active_query,
    session_start,
    active_query_start,
    kv_txn,
    alloc_bytes,
    max_alloc_bytes
FROM crdb_internal.cluster_sessions
LIMIT 500;`

	// PRODUCTION SAFE - Active Transactions
	QueryClusterTransactions = `
SELECT
    encode(id::BYTES, 'hex') as id,
    node_id,
    session_id,
    start,
    txn_string,
    application_name,
    num_stmts,
    num_retries,
    num_auto_retries
FROM crdb_internal.cluster_transactions
LIMIT 500;`

	// PRODUCTION SAFE - Lock Information (WARNING: can be expensive on large clusters)
	QueryClusterLocks = `
SELECT
    range_id,
    table_id,
    database_name,
    schema_name,
    table_name,
    index_name,
    lock_key_pretty,
    encode(txn_id::BYTES, 'hex') as txn_id,
    ts,
    lock_strength,
    durability,
    granted,
    contended,
    duration
FROM crdb_internal.cluster_locks
WHERE duration > INTERVAL '1 second'
LIMIT 1000;`

	// PRODUCTION SAFE - Contended Indexes
	QueryClusterContendedIndexes = `
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    num_contention_events
FROM crdb_internal.cluster_contended_indexes
ORDER BY num_contention_events DESC
LIMIT 100;`

	// PRODUCTION SAFE - Contended Keys
	QueryClusterContendedKeys = `
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    encode(key, 'hex') as key_hex,
    num_contention_events
FROM crdb_internal.cluster_contended_keys
ORDER BY num_contention_events DESC
LIMIT 100;`

	// PRODUCTION SAFE - Contended Tables
	QueryClusterContendedTables = `
SELECT
    database_name,
    schema_name,
    table_name,
    num_contention_events
FROM crdb_internal.cluster_contended_tables
ORDER BY num_contention_events DESC
LIMIT 100;`

	// PRODUCTION SAFE - Contention Events
	QueryClusterContentionEvents = `
SELECT
    table_id,
    index_id,
    num_contention_events,
    cumulative_contention_time,
    encode(key, 'hex') as key_hex,
    encode(txn_id::BYTES, 'hex') as txn_id,
    count
FROM crdb_internal.cluster_contention_events
ORDER BY cumulative_contention_time DESC
LIMIT 500;`

	// PRODUCTION SAFE - Transaction Contention Events
	QueryTransactionContentionEvents = `
SELECT
    collection_ts,
    encode(blocking_txn_id::BYTES, 'hex') as blocking_txn_id,
    encode(blocking_txn_fingerprint_id, 'hex') as blocking_txn_fingerprint_id,
    encode(waiting_txn_id::BYTES, 'hex') as waiting_txn_id,
    encode(waiting_txn_fingerprint_id, 'hex') as waiting_txn_fingerprint_id,
    waiting_stmt_id,
    encode(waiting_stmt_fingerprint_id, 'hex') as waiting_stmt_fingerprint_id,
    contention_duration,
    contending_pretty_key,
    database_name,
    schema_name,
    table_name,
    index_name,
    contention_type
FROM crdb_internal.transaction_contention_events
WHERE collection_ts > NOW() - INTERVAL '1 hour'
ORDER BY contention_duration DESC
LIMIT 500;`

	// NOT PRODUCTION SAFE - Range Health (expensive cluster-wide RPC)
	QueryRangesNoLeases = `
SELECT
    range_id,
    start_key,
    start_pretty,
    end_key,
    end_pretty,
    database_name,
    table_name,
    index_name,
    replicas,
    replica_localities,
    voting_replicas,
    non_voting_replicas,
    split_enforced_until
FROM crdb_internal.ranges_no_leases
LIMIT 1000;`

	// NOT PRODUCTION SAFE - Node Liveness (unstable schema)
	QueryGossipLiveness = `
SELECT
    node_id,
    epoch,
    expiration,
    draining,
    decommissioning,
    membership,
    updated_at
FROM crdb_internal.gossip_liveness
LIMIT 100;`

	// NOT PRODUCTION SAFE - Background Jobs
	QueryJobs = `
SELECT
    job_id,
    job_type,
    description,
    status,
    created,
    started,
    finished,
    modified,
    fraction_completed,
    error,
    coordinator_id
FROM crdb_internal.jobs
WHERE status IN ('running', 'pending', 'paused')
LIMIT 500;`

	// NOT PRODUCTION SAFE - Schema Changes
	QuerySchemaChanges = `
SELECT
    table_id,
    parent_id,
    name,
    type,
    target_id,
    target_name,
    state,
    direction
FROM crdb_internal.schema_changes
LIMIT 100;`

	// NOT PRODUCTION SAFE - Node Metrics
	QueryNodeMetrics = `
SELECT
    node_id,
    store_id,
    name,
    value
FROM crdb_internal.node_metrics
WHERE name IN (
    'sql.conns',
    'sql.txns.open',
    'sql.mem.current',
    'sys.cpu.user.percent',
    'sys.cpu.sys.percent',
    'sys.rss'
)
LIMIT 1000;`

	// NOT PRODUCTION SAFE - KV Node Status
	QueryKVNodeStatus = `
SELECT
    node_id,
    network,
    address,
    attrs,
    locality,
    server_version,
    go_version,
    tag,
    time,
    revision,
    cgo_compiler,
    platform,
    distribution,
    type,
    dependencies,
    started_at,
    updated_at
FROM crdb_internal.kv_node_status
LIMIT 100;`
)
