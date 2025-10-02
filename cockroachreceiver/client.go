package cockroachreceiver

import (
    "context"
    "database/sql"
    "time"
    
    _ "github.com/lib/pq"
)

type cockroachClient struct {
    db           *sql.DB
    connStr      string
    queryTimeout time.Duration
    queryLimit   int
}

func newCockroachClient(cfg *Config) (*cockroachClient, error) {
    db, err := sql.Open("postgres", cfg.ConnectionString)
    if err != nil {
        return nil, err
    }
    
    // Configure connection pool
    db.SetMaxOpenConns(cfg.MaxOpenConns)
    db.SetMaxIdleConns(cfg.MaxIdleConns)
    db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
    db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
    
    // Test connection with timeout
    ctx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
    defer cancel()
    
    if err := db.PingContext(ctx); err != nil {
        db.Close()
        return nil, err
    }
    
    return &cockroachClient{
        db:           db,
        connStr:      cfg.ConnectionString,
        queryTimeout: cfg.QueryTimeout,
        queryLimit:   cfg.QueryLimit,
    }, nil
}

func (c *cockroachClient) Close() error {
    if c.db != nil {
        return c.db.Close()
    }
    return nil
}

// createQueryContext creates a context with timeout for database queries
func (c *cockroachClient) createQueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
    return context.WithTimeout(ctx, c.queryTimeout)
}

type QueryStats struct {
    Query         string
    ExecutionCount int64
    MeanLatency   float64
}

type TransactionStats struct {
    TxCount  int64
    MeanRows float64
}

type QueryLatencyStats struct {
    QueryFingerprint string
    P50Latency      float64
    P95Latency      float64
    P99Latency      float64
    ErrorCount      int64
}

type IndexUsageStats struct {
    TableName  string
    IndexName  string
    TotalReads int64
}

type TableSizeStats struct {
    DatabaseName string
    TableName    string
    RowCount     int64
    DiskBytes    int64
}

type ContentionStats struct {
    TableName      string
    IndexName      string
    ContentionTime float64
    NumContention  int64
}

type RangeHealthStats struct {
    UnderReplicatedRanges int64
    UnavailableRanges     int64
    TotalRanges           int64
}

type NodeStatus struct {
    NodeID  int64
    IsLive  bool
    Address string
}

type JobStats struct {
    JobID         int64
    JobType       string
    Status        string
    RunningStatus string
    Created       string
}

type ChangefeedLag struct {
    JobID      int64
    LagSeconds float64
}

type SchemaChange struct {
    TableName string
    Type      string
    State     string
}

type StatementError struct {
    Query     string
    ErrorCode string
    ErrorCount int64
}

func (c *cockroachClient) GetQueryStats(ctx context.Context) ([]QueryStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []QueryStats
    for rows.Next() {
        var s QueryStats
        if err := rows.Scan(&s.Query, &s.ExecutionCount, &s.MeanLatency); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetTransactionStats(ctx context.Context) ([]TransactionStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, `
        SELECT 
            (statistics->'statistics'->'cnt')::INT as tx_count,
            (statistics->'statistics'->'numRows'->'mean')::FLOAT as mean_rows
        FROM crdb_internal.cluster_transaction_statistics
        WHERE (statistics->'statistics'->'cnt')::INT > 0
        LIMIT 10
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []TransactionStats
    for rows.Next() {
        var s TransactionStats
        if err := rows.Scan(&s.TxCount, &s.MeanRows); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetActiveQueries(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, "SELECT count(*) FROM crdb_internal.cluster_queries").Scan(&count)
    return count, err
}

func (c *cockroachClient) GetActiveSessions(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, "SELECT count(*) FROM crdb_internal.cluster_sessions").Scan(&count)
    return count, err
}

func (c *cockroachClient) GetQueryLatencyPercentiles(ctx context.Context) ([]QueryLatencyStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []QueryLatencyStats
    for rows.Next() {
        var s QueryLatencyStats
        if err := rows.Scan(&s.QueryFingerprint, &s.P50Latency, &s.P95Latency, &s.P99Latency, &s.ErrorCount); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetIndexUsage(ctx context.Context) ([]IndexUsageStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []IndexUsageStats
    for rows.Next() {
        var s IndexUsageStats
        if err := rows.Scan(&s.TableName, &s.IndexName, &s.TotalReads); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetConnectionCount(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, `
        SELECT count(DISTINCT session_id) 
        FROM crdb_internal.cluster_sessions
    `).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetDatabaseCount(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, `
        SELECT count(*) FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')
    `).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetTableSizes(ctx context.Context) ([]TableSizeStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []TableSizeStats
    for rows.Next() {
        var s TableSizeStats
        if err := rows.Scan(&s.DatabaseName, &s.TableName, &s.DiskBytes, &s.RowCount); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetContentionStats(ctx context.Context) ([]ContentionStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []ContentionStats
    for rows.Next() {
        var s ContentionStats
        if err := rows.Scan(&s.TableName, &s.IndexName, &s.ContentionTime, &s.NumContention); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetRangeHealth(ctx context.Context) (RangeHealthStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var stats RangeHealthStats
    
    // Total ranges
    err := c.db.QueryRowContext(queryCtx, `
        SELECT count(*) FROM crdb_internal.ranges_no_leases
    `).Scan(&stats.TotalRanges)
    if err != nil {
        return stats, err
    }
    
    // Under-replicated ranges (fewer than expected replicas based on array length)
    err = c.db.QueryRowContext(queryCtx, `
        SELECT count(*) 
        FROM crdb_internal.ranges_no_leases 
        WHERE array_length(voting_replicas, 1) < 3
    `).Scan(&stats.UnderReplicatedRanges)
    if err != nil {
        return stats, err
    }
    
    // Unavailable ranges (critical - no voting replicas)
    err = c.db.QueryRowContext(queryCtx, `
        SELECT count(*) 
        FROM crdb_internal.ranges_no_leases 
        WHERE array_length(voting_replicas, 1) IS NULL OR array_length(voting_replicas, 1) = 0
    `).Scan(&stats.UnavailableRanges)
    if err != nil {
        return stats, err
    }
    
    return stats, nil
}

func (c *cockroachClient) GetNodeStatus(ctx context.Context) ([]NodeStatus, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, `
        SELECT 
            node_id,
            CASE WHEN expiration::TIMESTAMP > now() THEN true ELSE false END as is_live,
            'n' || node_id::TEXT as address
        FROM crdb_internal.gossip_liveness
        ORDER BY node_id
    `)
    if err != nil {
        // Return empty slice for serverless clusters where this table isn't available
        // This is expected behavior - serverless abstracts away node management
        return []NodeStatus{}, nil
    }
    defer rows.Close()
    
    var stats []NodeStatus
    for rows.Next() {
        var s NodeStatus
        if err := rows.Scan(&s.NodeID, &s.IsLive, &s.Address); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetJobStats(ctx context.Context) ([]JobStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
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
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []JobStats
    for rows.Next() {
        var s JobStats
        if err := rows.Scan(&s.JobID, &s.JobType, &s.Status, &s.RunningStatus, &s.Created); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetChangefeedLag(ctx context.Context) ([]ChangefeedLag, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, `
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
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []ChangefeedLag
    for rows.Next() {
        var s ChangefeedLag
        if err := rows.Scan(&s.JobID, &s.LagSeconds); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetSchemaChanges(ctx context.Context) ([]SchemaChange, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, `
        SELECT 
            name,
            type,
            state
        FROM crdb_internal.schema_changes
        WHERE state != 'done'
        ORDER BY name
    `)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []SchemaChange
    for rows.Next() {
        var s SchemaChange
        if err := rows.Scan(&s.TableName, &s.Type, &s.State); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}

func (c *cockroachClient) GetStatementErrors(ctx context.Context) ([]StatementError, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    query := `
        SELECT 
            metadata->>'query' as query,
            COALESCE(statistics->'statistics'->'execStats'->>'lastErrorCode', 'unknown') as error_code,
            (statistics->'statistics'->'execStats'->>'numErrors')::INT as error_count
        FROM crdb_internal.cluster_statement_statistics
        WHERE (statistics->'statistics'->'execStats'->>'numErrors')::INT > 0
        ORDER BY (statistics->'statistics'->'execStats'->>'numErrors')::INT DESC
        LIMIT $1
    `
    
    rows, err := c.db.QueryContext(queryCtx, query, c.queryLimit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var stats []StatementError
    for rows.Next() {
        var s StatementError
        if err := rows.Scan(&s.Query, &s.ErrorCode, &s.ErrorCount); err != nil {
            continue
        }
        stats = append(stats, s)
    }
    
    return stats, rows.Err()
}
