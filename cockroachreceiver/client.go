package cockroachreceiver

import (
    "context"
    "database/sql"
    "time"
    
    _ "github.com/lib/pq"
)

// CockroachClient defines the interface for interacting with CockroachDB
type CockroachClient interface {
    Close() error
    GetQueryStats(ctx context.Context) ([]QueryStats, error)
    GetTransactionStats(ctx context.Context) ([]TransactionStats, error)
    GetActiveQueries(ctx context.Context) (int64, error)
    GetActiveSessions(ctx context.Context) (int64, error)
    GetQueryLatencyPercentiles(ctx context.Context) ([]QueryLatencyStats, error)
    GetIndexUsage(ctx context.Context) ([]IndexUsageStats, error)
    GetConnectionCount(ctx context.Context) (int64, error)
    GetDatabaseCount(ctx context.Context) (int64, error)
    GetTableSizes(ctx context.Context) ([]TableSizeStats, error)
    GetContentionStats(ctx context.Context) ([]ContentionStats, error)
    GetRangeHealth(ctx context.Context) (RangeHealthStats, error)
    GetNodeStatus(ctx context.Context) ([]NodeStatus, error)
    GetJobStats(ctx context.Context) ([]JobStats, error)
    GetChangefeedLag(ctx context.Context) ([]ChangefeedLag, error)
    GetSchemaChanges(ctx context.Context) ([]SchemaChange, error)
    GetStatementErrors(ctx context.Context) ([]StatementError, error)
}

type cockroachClient struct {
    db           *sql.DB
    connStr      string
    queryTimeout time.Duration
    queryLimit   int
}

func newCockroachClient(cfg *Config) (CockroachClient, error) {
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
    
    rows, err := c.db.QueryContext(queryCtx, queryStatsSQL, c.queryLimit)
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
    
    rows, err := c.db.QueryContext(queryCtx, transactionStatsSQL)
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
    err := c.db.QueryRowContext(queryCtx, activeQueriesSQL).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetActiveSessions(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, activeSessionsSQL).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetQueryLatencyPercentiles(ctx context.Context) ([]QueryLatencyStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, queryLatencyPercentilesSQL, c.queryLimit)
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
    
    rows, err := c.db.QueryContext(queryCtx, indexUsageSQL, c.queryLimit)
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
    err := c.db.QueryRowContext(queryCtx, connectionCountSQL).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetDatabaseCount(ctx context.Context) (int64, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    var count int64
    err := c.db.QueryRowContext(queryCtx, databaseCountSQL).Scan(&count)
    return count, err
}

func (c *cockroachClient) GetTableSizes(ctx context.Context) ([]TableSizeStats, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, tableSizesSQL, c.queryLimit)
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
    
    rows, err := c.db.QueryContext(queryCtx, contentionStatsSQL, c.queryLimit)
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
    err := c.db.QueryRowContext(queryCtx, rangeHealthTotalSQL).Scan(&stats.TotalRanges)
    if err != nil {
        return stats, err
    }
    
    // Under-replicated ranges
    err = c.db.QueryRowContext(queryCtx, rangeHealthUnderReplicatedSQL).Scan(&stats.UnderReplicatedRanges)
    if err != nil {
        return stats, err
    }
    
    // Unavailable ranges
    err = c.db.QueryRowContext(queryCtx, rangeHealthUnavailableSQL).Scan(&stats.UnavailableRanges)
    if err != nil {
        return stats, err
    }
    
    return stats, nil
}

func (c *cockroachClient) GetNodeStatus(ctx context.Context) ([]NodeStatus, error) {
    queryCtx, cancel := c.createQueryContext(ctx)
    defer cancel()
    
    rows, err := c.db.QueryContext(queryCtx, nodeStatusSQL)
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
    
    rows, err := c.db.QueryContext(queryCtx, jobStatsSQL, c.queryLimit)
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
    
    rows, err := c.db.QueryContext(queryCtx, changefeedLagSQL)
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
    
    rows, err := c.db.QueryContext(queryCtx, schemaChangesSQL)
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
    
    rows, err := c.db.QueryContext(queryCtx, statementErrorsSQL, c.queryLimit)
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
