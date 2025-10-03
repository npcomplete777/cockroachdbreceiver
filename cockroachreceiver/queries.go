package cockroachreceiver

// SQL queries for CockroachDB metrics collection
// All queries are defined as constants for maintainability and reusability

const (
    // Query statistics
    queryStatsSQL = `
        SELECT 
            metadata->>'query' as query,
            (statistics->'statistics'->'cnt')::INT as execution_count,
            (statistics->'statistics'->'runLat'->'mean')::FLOAT as mean_latency
        FROM crdb_internal.cluster_statement_statistics
        WHERE metadata->>'query' IS NOT NULL
        AND (statistics->'statistics'->'cnt')::INT > 0
        ORDER BY (statistics->'statistics'->'cnt')::INT DESC
        LIMIT $1
    `

    // Transaction statistics
    transactionStatsSQL = `
        SELECT 
            (statistics->'statistics'->'cnt')::INT as tx_count,
            (statistics->'statistics'->'numRows'->'mean')::FLOAT as mean_rows
        FROM crdb_internal.cluster_transaction_statistics
        WHERE (statistics->'statistics'->'cnt')::INT > 0
        LIMIT 10
    `

    // Active queries count
    activeQueriesSQL = `SELECT count(*) FROM crdb_internal.cluster_queries`

    // Active sessions count
    activeSessionsSQL = `SELECT count(*) FROM crdb_internal.cluster_sessions`

    // Query latency percentiles
    queryLatencyPercentilesSQL = `
        SELECT 
            metadata->>'query' as query,
            COALESCE((statistics->'statistics'->'latencyInfo'->'p50')::FLOAT, 0) as p50,
            COALESCE((statistics->'statistics'->'latencyInfo'->'p95')::FLOAT, 0) as p95,
            COALESCE((statistics->'statistics'->'latencyInfo'->'p99')::FLOAT, 0) as p99,
            COALESCE((statistics->'statistics'->'execStats'->'numErrors')::INT, 0) as errors
        FROM crdb_internal.cluster_statement_statistics
        WHERE metadata->>'query' IS NOT NULL
        AND (statistics->'statistics'->'cnt')::INT > 0
        ORDER BY (statistics->'statistics'->'cnt')::INT DESC
        LIMIT $1
    `

    // Index usage statistics
    indexUsageSQL = `
        SELECT 
            t.name as table_name,
            ti.index_name,
            ius.total_reads
        FROM crdb_internal.index_usage_statistics ius
        JOIN crdb_internal.tables t ON ius.table_id = t.table_id
        JOIN crdb_internal.table_indexes ti ON ius.table_id = ti.descriptor_id AND ius.index_id = ti.index_id
        WHERE ius.total_reads > 0
        ORDER BY ius.total_reads DESC
        LIMIT $1
    `

    // Connection count
    connectionCountSQL = `
        SELECT count(DISTINCT session_id) 
        FROM crdb_internal.cluster_sessions
    `

    // Database count
    databaseCountSQL = `
        SELECT count(*) FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
    `

    // Table sizes
    tableSizesSQL = `
        SELECT 
            database_name,
            name as table_name,
            0 as disk_bytes,
            0 as row_count
        FROM crdb_internal.tables
        WHERE database_name NOT IN ('system')
        ORDER BY name
        LIMIT $1
    `

    // Contention statistics
    contentionStatsSQL = `
        SELECT 
            t.name as table_name,
            COALESCE(ti.index_name, 'primary') as index_name,
            ce.cumulative_contention_time::FLOAT / 1e9 as contention_seconds,
            ce.num_contention_events
        FROM crdb_internal.cluster_contention_events ce
        JOIN crdb_internal.tables t ON ce.table_id = t.table_id
        LEFT JOIN crdb_internal.table_indexes ti ON ce.table_id = ti.descriptor_id AND ce.index_id = ti.index_id
        WHERE ce.num_contention_events > 0
        ORDER BY ce.cumulative_contention_time DESC
        LIMIT $1
    `

    // Range health - total ranges
    rangeHealthTotalSQL = `SELECT count(*) FROM crdb_internal.ranges_no_leases`

    // Range health - under-replicated ranges
    // Note: Hardcoded threshold of 3 replicas - may need to be configurable for different replication factors
    rangeHealthUnderReplicatedSQL = `
        SELECT count(*) 
        FROM crdb_internal.ranges_no_leases 
        WHERE array_length(voting_replicas, 1) < 3
    `

    // Range health - unavailable ranges
    rangeHealthUnavailableSQL = `
        SELECT count(*) 
        FROM crdb_internal.ranges_no_leases 
        WHERE array_length(voting_replicas, 1) IS NULL OR array_length(voting_replicas, 1) = 0
    `

    // Node status from gossip liveness
    nodeStatusSQL = `
        SELECT 
            node_id,
            CASE WHEN expiration::TIMESTAMP > now() THEN true ELSE false END as is_live,
            'n' || node_id::TEXT as address
        FROM crdb_internal.gossip_liveness
        ORDER BY node_id
    `

    // Job statistics
    jobStatsSQL = `
        SELECT 
            job_id,
            job_type,
            status,
            COALESCE(NULLIF(running_status, ''), 'none') as running_status,
            created::TEXT as created
        FROM crdb_internal.jobs
        WHERE status IN ('running', 'paused', 'reverting', 'pending')
        ORDER BY created DESC
        LIMIT $1
    `

    // Changefeed lag
    changefeedLagSQL = `
        SELECT 
            job_id,
            CASE 
                WHEN high_water_timestamp IS NOT NULL 
                THEN extract(epoch from now()) - (high_water_timestamp::FLOAT / 1e9)
                ELSE 0 
            END as lag_seconds
        FROM crdb_internal.jobs
        WHERE job_type = 'CHANGEFEED' 
        AND status = 'running'
    `

    // Schema changes in progress
    schemaChangesSQL = `
        SELECT 
            name,
            type,
            state
        FROM crdb_internal.schema_changes
        WHERE state != 'done'
        ORDER BY name
    `

    // Statement errors
    statementErrorsSQL = `
        SELECT 
            metadata->>'query' as query,
            COALESCE(statistics->'statistics'->'execStats'->>'lastErrorCode', 'unknown') as error_code,
            (statistics->'statistics'->'execStats'->>'numErrors')::INT as error_count
        FROM crdb_internal.cluster_statement_statistics
        WHERE (statistics->'statistics'->'execStats'->>'numErrors')::INT > 0
        ORDER BY (statistics->'statistics'->'execStats'->>'numErrors')::INT DESC
        LIMIT $1
    `
)
