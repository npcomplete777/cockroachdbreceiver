package cockroachreceiver

const (
	// Statement statistics with query text - ORDER BY execution count, LIMIT by query_limit
	queryStatementStatistics = `
SELECT 
	encode(fingerprint_id, 'hex') as fingerprint_id,
	app_name,
	metadata->>'db' as database,
	metadata->>'query' as query,
	metadata->>'stmtTyp' as statement_type,
	(statistics->'statistics'->>'cnt')::bigint as execution_count,
	(statistics->'statistics'->'svcLat'->>'mean')::float as service_latency_mean,
	(statistics->'statistics'->'parseLat'->>'mean')::float as parse_latency_mean,
	(statistics->'statistics'->'planLat'->>'mean')::float as plan_latency_mean,
	(statistics->'statistics'->'runLat'->>'mean')::float as run_latency_mean,
	(statistics->'statistics'->'numRows'->>'mean')::float as rows_read_mean,
	(statistics->'statistics'->'rowsWritten'->>'mean')::float as rows_written_mean,
	(statistics->'statistics'->'bytesRead'->>'mean')::float as bytes_read_mean,
	(statistics->'statistics'->>'maxRetries')::bigint as max_retries,
	(statistics->'statistics'->>'failureCount')::bigint as error_count,
	statistics->'statistics'->>'lastErrorCode' as last_error_code
FROM crdb_internal.statement_statistics
ORDER BY (statistics->'statistics'->>'cnt')::bigint DESC
LIMIT $1`

	queryTransactionStatistics = `
SELECT 
	encode(fingerprint_id, 'hex') as fingerprint_id,
	app_name,
	(statistics->'statistics'->>'cnt')::bigint as execution_count,
	(statistics->'statistics'->'svcLat'->>'mean')::float as service_latency_mean,
	(statistics->'statistics'->'commitLat'->>'mean')::float as commit_latency_mean,
	(statistics->'statistics'->'retryLat'->>'mean')::float as retry_latency_mean,
	(statistics->'statistics'->'numRows'->>'mean')::float as rows_read_mean,
	(statistics->'statistics'->'rowsWritten'->>'mean')::float as rows_written_mean,
	(statistics->'statistics'->'bytesRead'->>'mean')::float as bytes_read_mean,
	(statistics->'statistics'->>'maxRetries')::bigint as max_retries
FROM crdb_internal.transaction_statistics
ORDER BY (statistics->'statistics'->>'cnt')::bigint DESC
LIMIT $1`

	queryIndexUsageStatistics = `
SELECT 
	ti.descriptor_name as table_name,
	ti.index_name,
	us.total_reads,
	EXTRACT(EPOCH FROM (NOW() - us.last_read)) as seconds_since_last_read
FROM crdb_internal.index_usage_statistics us
JOIN crdb_internal.table_indexes ti 
	ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
WHERE us.total_reads > 0
ORDER BY us.total_reads DESC
LIMIT $1`

	queryClusterQueries = `
SELECT 
	query_id,
	node_id,
	user_name,
	application_name,
	EXTRACT(EPOCH FROM (NOW() - start)) as duration_seconds,
	query
FROM crdb_internal.cluster_queries
LIMIT $1`

	queryClusterSessions = `
SELECT 
	session_id,
	node_id,
	user_name,
	application_name,
	alloc_bytes,
	EXTRACT(EPOCH FROM (NOW() - session_start)) as session_age_seconds
FROM crdb_internal.cluster_sessions
LIMIT $1`

	queryClusterTransactions = `
SELECT 
	encode(id::BYTES, 'hex') as txn_id,
	node_id,
	application_name,
	EXTRACT(EPOCH FROM (NOW() - start)) as duration_seconds
FROM crdb_internal.cluster_transactions
LIMIT $1`

	queryClusterContendedIndexes = `
SELECT 
	database_name,
	schema_name,
	table_name,
	index_name,
	num_contention_events
FROM crdb_internal.cluster_contended_indexes
ORDER BY num_contention_events DESC
LIMIT $1`

	queryClusterContendedTables = `
SELECT 
	database_name,
	schema_name,
	table_name,
	num_contention_events
FROM crdb_internal.cluster_contended_tables
ORDER BY num_contention_events DESC
LIMIT $1`

	queryClusterContendedKeys = `
SELECT 
	database_name,
	schema_name,
	table_name,
	index_name,
	num_contention_events
FROM crdb_internal.cluster_contended_keys
ORDER BY num_contention_events DESC
LIMIT $1`

	queryClusterContentionEvents = `
SELECT 
	table_id,
	index_id,
	num_contention_events,
	EXTRACT(EPOCH FROM cumulative_contention_time) as cumulative_contention_seconds
FROM crdb_internal.cluster_contention_events
ORDER BY num_contention_events DESC
LIMIT $1`

	queryClusterLocks = `
SELECT 
	database_name,
	table_name,
	lock_strength,
	granted,
	COUNT(*) as lock_count,
	MAX(EXTRACT(EPOCH FROM duration)) as max_duration_seconds
FROM crdb_internal.cluster_locks
GROUP BY database_name, table_name, lock_strength, granted
LIMIT $1`

	queryTransactionContentionEvents = `
SELECT 
	database_name,
	table_name,
	contention_type,
	EXTRACT(EPOCH FROM contention_duration) as contention_duration_seconds
FROM crdb_internal.transaction_contention_events
ORDER BY contention_duration DESC
LIMIT $1`

	// FIXED: Using actual columns from ranges_no_leases
	queryRangesNoLeases = `
SELECT 
	COUNT(*) as total_ranges,
	COUNT(CASE WHEN array_length(replicas, 1) < 3 THEN 1 END) as under_replicated_ranges,
	0 as unavailable_ranges
FROM crdb_internal.ranges_no_leases`

	// Derive liveness from the lease expiration: a node is "live" when its
	// liveness lease has not expired and it is not draining or decommissioned.
	// gossip_liveness is unavailable on virtual clusters (Serverless).
	queryGossipLiveness = `
SELECT
	node_id,
	CASE
		WHEN expiration::TIMESTAMPTZ > now()
		     AND COALESCE(draining, false) = false
		     AND COALESCE(membership, '') != 'decommissioned'
		THEN 1 ELSE 0
	END AS is_live,
	COALESCE(membership, 'unknown') AS membership
FROM crdb_internal.gossip_liveness`

	queryJobs = `
SELECT 
	job_id,
	job_type,
	status,
	running_status,
	fraction_completed
FROM crdb_internal.jobs
WHERE status IN ('running', 'pending')
LIMIT $1`

	// FIXED: Using 'name' column and aliasing to table_name
	querySchemaChanges = `
SELECT 
	name as table_name,
	type,
	state
FROM crdb_internal.schema_changes
WHERE state IN ('waiting', 'running')
LIMIT $1`

	// node_metrics on virtual clusters (Serverless) does not expose node_id;
	// store_id is also nullable. Only store_id, name, and value are guaranteed.
	queryNodeMetrics = `
SELECT
	store_id,
	name,
	value
FROM crdb_internal.node_metrics
WHERE name IN ('sys.cpu.combined.percent-normalized', 'sys.rss')
LIMIT $1`

	// kv_node_status exposes per-node uptime and last-update timing. The
	// network_latency_* columns referenced in earlier versions never existed in
	// this table; latency lives in the per-node metrics blob (not exposed
	// here). kv_node_status is unavailable on virtual clusters (Serverless).
	queryKVNodeStatus = `
SELECT
	node_id,
	EXTRACT(EPOCH FROM (now() - started_at::TIMESTAMPTZ)) AS uptime_seconds,
	EXTRACT(EPOCH FROM (now() - updated_at::TIMESTAMPTZ)) AS last_update_seconds_ago
FROM crdb_internal.kv_node_status
LIMIT $1`
)
