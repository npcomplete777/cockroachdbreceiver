package cockroachreceiver

import (
    "context"
    "sync"
    "time"
    
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.uber.org/zap"
)

type cockroachScraper struct {
    client   CockroachClient
    connStr  string
    config   *Config
    logger   *zap.Logger
    settings component.TelemetrySettings
}

func newScraper(cfg *Config, settings component.TelemetrySettings) *cockroachScraper {
    client, err := newCockroachClient(cfg)
    if err != nil {
        settings.Logger.Error("Failed to create client", zap.Error(err))
        return nil
    }
    
    return &cockroachScraper{
        client:   client,
        connStr:  cfg.ConnectionString,
        config:   cfg,
        logger:   settings.Logger,
        settings: settings,
    }
}

// Shutdown gracefully closes the database connection
func (s *cockroachScraper) Shutdown(ctx context.Context) error {
    if s.client != nil {
        s.logger.Info("Shutting down CockroachDB receiver, closing database connection")
        if err := s.client.Close(); err != nil {
            s.logger.Error("Error closing database connection during shutdown", zap.Error(err))
            return err
        }
        s.logger.Info("Database connection closed successfully")
    }
    return nil
}

func (s *cockroachScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
    metrics := pmetric.NewMetrics()
    resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
    
    resource := resourceMetrics.Resource()
    // SECURITY FIX: Sanitize connection string to remove credentials
    sanitizedConn := sanitizeConnectionString(s.connStr)
    resource.Attributes().PutStr("cockroachdb.endpoint", sanitizedConn)
    
    scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
    scopeMetrics.Scope().SetName("cockroachreceiver")
    
    now := pcommon.NewTimestampFromTime(time.Now())
    
    // Use mutex to protect concurrent writes to scopeMetrics
    var mu sync.Mutex
    var wg sync.WaitGroup
    
    // Collect all enabled metrics concurrently
    collectors := []struct {
        group   string
        collect func(context.Context, pmetric.ScopeMetrics, pcommon.Timestamp)
    }{
        {MetricGroupQuery, s.collectQueryMetrics},
        {MetricGroupTransaction, s.collectTransactionMetrics},
        {MetricGroupSession, s.collectSessionMetrics},
        {MetricGroupIndex, s.collectIndexMetrics},
        {MetricGroupTable, s.collectTableMetrics},
        {MetricGroupContention, s.collectContentionMetrics},
        {MetricGroupRange, s.collectRangeMetrics},
        {MetricGroupNode, s.collectNodeMetrics},
        {MetricGroupJob, s.collectJobMetrics},
        {MetricGroupChangefeed, s.collectChangefeedMetrics},
        {MetricGroupSchema, s.collectSchemaMetrics},
        {MetricGroupError, s.collectErrorMetrics},
    }
    
    for _, collector := range collectors {
        if s.config.IsMetricEnabled(collector.group) {
            wg.Add(1)
            go func(c func(context.Context, pmetric.ScopeMetrics, pcommon.Timestamp)) {
                defer wg.Done()
                mu.Lock()
                c(ctx, scopeMetrics, now)
                mu.Unlock()
            }(collector.collect)
        }
    }
    
    wg.Wait()
    
    return metrics, nil
}

// Collector functions for each metric group

func (s *cockroachScraper) collectQueryMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    // Get query statistics
    queryStats, err := s.client.GetQueryStats(ctx)
    if err != nil {
        s.logger.Error("Failed to get query stats", zap.Error(err))
    } else {
        s.addQueryMetrics(scopeMetrics, queryStats, now)
    }
    
    // Get query latency percentiles
    latencyStats, err := s.client.GetQueryLatencyPercentiles(ctx)
    if err != nil {
        s.logger.Error("Failed to get latency percentiles", zap.Error(err))
    } else {
        s.addLatencyPercentileMetrics(scopeMetrics, latencyStats, now)
    }
}

func (s *cockroachScraper) collectTransactionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    txStats, err := s.client.GetTransactionStats(ctx)
    if err != nil {
        s.logger.Error("Failed to get transaction stats", zap.Error(err))
    } else {
        s.addTransactionMetrics(scopeMetrics, txStats, now)
    }
}

func (s *cockroachScraper) collectSessionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    // Get active queries count
    activeQueries, err := s.client.GetActiveQueries(ctx)
    if err != nil {
        s.logger.Error("Failed to get active queries", zap.Error(err))
    } else {
        s.addActiveQueriesMetric(scopeMetrics, activeQueries, now)
    }
    
    // Get active sessions count
    activeSessions, err := s.client.GetActiveSessions(ctx)
    if err != nil {
        s.logger.Error("Failed to get active sessions", zap.Error(err))
    } else {
        s.addActiveSessionsMetric(scopeMetrics, activeSessions, now)
    }
    
    // Get connection count
    connCount, err := s.client.GetConnectionCount(ctx)
    if err != nil {
        s.logger.Error("Failed to get connection count", zap.Error(err))
    } else {
        s.addConnectionCountMetric(scopeMetrics, connCount, now)
    }
}

func (s *cockroachScraper) collectIndexMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    indexStats, err := s.client.GetIndexUsage(ctx)
    if err != nil {
        s.logger.Error("Failed to get index usage", zap.Error(err))
    } else {
        s.addIndexUsageMetrics(scopeMetrics, indexStats, now)
    }
}

func (s *cockroachScraper) collectTableMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    // Get database count
    dbCount, err := s.client.GetDatabaseCount(ctx)
    if err != nil {
        s.logger.Error("Failed to get database count", zap.Error(err))
    } else {
        s.addDatabaseCountMetric(scopeMetrics, dbCount, now)
    }
    
    // Get table sizes
    tableSizes, err := s.client.GetTableSizes(ctx)
    if err != nil {
        s.logger.Error("Failed to get table sizes", zap.Error(err))
    } else {
        s.addTableSizeMetrics(scopeMetrics, tableSizes, now)
    }
}

func (s *cockroachScraper) collectContentionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    contentionStats, err := s.client.GetContentionStats(ctx)
    if err != nil {
        s.logger.Error("Failed to get contention stats", zap.Error(err))
    } else {
        s.addContentionMetrics(scopeMetrics, contentionStats, now)
    }
}

func (s *cockroachScraper) collectRangeMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    rangeHealth, err := s.client.GetRangeHealth(ctx)
    if err != nil {
        s.logger.Error("Failed to get range health", zap.Error(err))
    } else {
        s.addRangeHealthMetrics(scopeMetrics, rangeHealth, now)
    }
}

func (s *cockroachScraper) collectNodeMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    nodeStatus, err := s.client.GetNodeStatus(ctx)
    if err != nil {
        s.logger.Error("Failed to get node status", zap.Error(err))
    } else {
        s.addNodeStatusMetrics(scopeMetrics, nodeStatus, now)
    }
}

func (s *cockroachScraper) collectJobMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    jobStats, err := s.client.GetJobStats(ctx)
    if err != nil {
        s.logger.Error("Failed to get job stats", zap.Error(err))
    } else {
        s.addJobMetrics(scopeMetrics, jobStats, now)
    }
}

func (s *cockroachScraper) collectChangefeedMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    changefeedLag, err := s.client.GetChangefeedLag(ctx)
    if err != nil {
        s.logger.Error("Failed to get changefeed lag", zap.Error(err))
    } else {
        s.addChangefeedLagMetrics(scopeMetrics, changefeedLag, now)
    }
}

func (s *cockroachScraper) collectSchemaMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    schemaChanges, err := s.client.GetSchemaChanges(ctx)
    if err != nil {
        s.logger.Error("Failed to get schema changes", zap.Error(err))
    } else {
        s.addSchemaChangeMetrics(scopeMetrics, schemaChanges, now)
    }
}

func (s *cockroachScraper) collectErrorMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    stmtErrors, err := s.client.GetStatementErrors(ctx)
    if err != nil {
        s.logger.Error("Failed to get statement errors", zap.Error(err))
    } else {
        s.addStatementErrorMetrics(scopeMetrics, stmtErrors, now)
    }
}

// Metric adding functions (unchanged)

func (s *cockroachScraper) addQueryMetrics(scopeMetrics pmetric.ScopeMetrics, stats []QueryStats, now pcommon.Timestamp) {
    // Query execution count
    countMetric := scopeMetrics.Metrics().AppendEmpty()
    countMetric.SetName("cockroachdb.query.execution_count")
    countMetric.SetDescription("Number of times each query has been executed")
    countMetric.SetUnit("1")
    
    gauge := countMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.ExecutionCount)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.Query[:min(100, len(stat.Query))])
    }
    
    // Query latency
    latencyMetric := scopeMetrics.Metrics().AppendEmpty()
    latencyMetric.SetName("cockroachdb.query.latency")
    latencyMetric.SetDescription("Mean query latency in seconds")
    latencyMetric.SetUnit("s")
    
    latencyGauge := latencyMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := latencyGauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.MeanLatency)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.Query[:min(100, len(stat.Query))])
    }
}

func (s *cockroachScraper) addTransactionMetrics(scopeMetrics pmetric.ScopeMetrics, stats []TransactionStats, now pcommon.Timestamp) {
    totalTx := int64(0)
    for _, stat := range stats {
        totalTx += stat.TxCount
    }
    
    txMetric := scopeMetrics.Metrics().AppendEmpty()
    txMetric.SetName("cockroachdb.transactions.total")
    txMetric.SetDescription("Total number of transactions")
    txMetric.SetUnit("1")
    
    gauge := txMetric.SetEmptyGauge()
    dp := gauge.DataPoints().AppendEmpty()
    dp.SetIntValue(totalTx)
    dp.SetTimestamp(now)
}

func (s *cockroachScraper) addActiveQueriesMetric(scopeMetrics pmetric.ScopeMetrics, count int64, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.queries.active")
    metric.SetDescription("Number of currently active queries")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    dp := gauge.DataPoints().AppendEmpty()
    dp.SetIntValue(count)
    dp.SetTimestamp(now)
}

func (s *cockroachScraper) addActiveSessionsMetric(scopeMetrics pmetric.ScopeMetrics, count int64, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.sessions.active")
    metric.SetDescription("Number of currently active sessions")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    dp := gauge.DataPoints().AppendEmpty()
    dp.SetIntValue(count)
    dp.SetTimestamp(now)
}

func (s *cockroachScraper) addLatencyPercentileMetrics(scopeMetrics pmetric.ScopeMetrics, stats []QueryLatencyStats, now pcommon.Timestamp) {
    // P50 latency
    p50Metric := scopeMetrics.Metrics().AppendEmpty()
    p50Metric.SetName("cockroachdb.query.latency.p50")
    p50Metric.SetDescription("P50 query latency in seconds")
    p50Metric.SetUnit("s")
    
    p50Gauge := p50Metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := p50Gauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.P50Latency)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.QueryFingerprint[:min(100, len(stat.QueryFingerprint))])
    }
    
    // P95 latency
    p95Metric := scopeMetrics.Metrics().AppendEmpty()
    p95Metric.SetName("cockroachdb.query.latency.p95")
    p95Metric.SetDescription("P95 query latency in seconds")
    p95Metric.SetUnit("s")
    
    p95Gauge := p95Metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := p95Gauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.P95Latency)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.QueryFingerprint[:min(100, len(stat.QueryFingerprint))])
    }
    
    // P99 latency
    p99Metric := scopeMetrics.Metrics().AppendEmpty()
    p99Metric.SetName("cockroachdb.query.latency.p99")
    p99Metric.SetDescription("P99 query latency in seconds")
    p99Metric.SetUnit("s")
    
    p99Gauge := p99Metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := p99Gauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.P99Latency)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.QueryFingerprint[:min(100, len(stat.QueryFingerprint))])
    }
    
    // Query errors
    errorMetric := scopeMetrics.Metrics().AppendEmpty()
    errorMetric.SetName("cockroachdb.query.errors")
    errorMetric.SetDescription("Number of query errors")
    errorMetric.SetUnit("1")
    
    errorGauge := errorMetric.SetEmptyGauge()
    for _, stat := range stats {
        if stat.ErrorCount > 0 {
            dp := errorGauge.DataPoints().AppendEmpty()
            dp.SetIntValue(stat.ErrorCount)
            dp.SetTimestamp(now)
            dp.Attributes().PutStr("query", stat.QueryFingerprint[:min(100, len(stat.QueryFingerprint))])
        }
    }
}

func (s *cockroachScraper) addIndexUsageMetrics(scopeMetrics pmetric.ScopeMetrics, stats []IndexUsageStats, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.index.reads")
    metric.SetDescription("Total reads per index")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.TotalReads)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("table", stat.TableName)
        dp.Attributes().PutStr("index", stat.IndexName)
    }
}

func (s *cockroachScraper) addConnectionCountMetric(scopeMetrics pmetric.ScopeMetrics, count int64, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.connections.total")
    metric.SetDescription("Total number of unique connections")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    dp := gauge.DataPoints().AppendEmpty()
    dp.SetIntValue(count)
    dp.SetTimestamp(now)
}

func (s *cockroachScraper) addDatabaseCountMetric(scopeMetrics pmetric.ScopeMetrics, count int64, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.databases.total")
    metric.SetDescription("Total number of user databases")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    dp := gauge.DataPoints().AppendEmpty()
    dp.SetIntValue(count)
    dp.SetTimestamp(now)
}

func (s *cockroachScraper) addTableSizeMetrics(scopeMetrics pmetric.ScopeMetrics, stats []TableSizeStats, now pcommon.Timestamp) {
    // Table row counts
    rowMetric := scopeMetrics.Metrics().AppendEmpty()
    rowMetric.SetName("cockroachdb.table.rows")
    rowMetric.SetDescription("Number of rows per table")
    rowMetric.SetUnit("1")
    
    rowGauge := rowMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := rowGauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.RowCount)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("database", stat.DatabaseName)
        dp.Attributes().PutStr("table", stat.TableName)
    }
    
    // Table disk usage
    diskMetric := scopeMetrics.Metrics().AppendEmpty()
    diskMetric.SetName("cockroachdb.table.disk_bytes")
    diskMetric.SetDescription("Disk space used by table in bytes")
    diskMetric.SetUnit("By")
    
    diskGauge := diskMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := diskGauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.DiskBytes)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("database", stat.DatabaseName)
        dp.Attributes().PutStr("table", stat.TableName)
    }
}

func (s *cockroachScraper) addContentionMetrics(scopeMetrics pmetric.ScopeMetrics, stats []ContentionStats, now pcommon.Timestamp) {
    // Contention time
    timeMetric := scopeMetrics.Metrics().AppendEmpty()
    timeMetric.SetName("cockroachdb.contention.time")
    timeMetric.SetDescription("Total contention time in seconds")
    timeMetric.SetUnit("s")
    
    timeGauge := timeMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := timeGauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.ContentionTime)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("table", stat.TableName)
        dp.Attributes().PutStr("index", stat.IndexName)
    }
    
    // Contention event count
    countMetric := scopeMetrics.Metrics().AppendEmpty()
    countMetric.SetName("cockroachdb.contention.events")
    countMetric.SetDescription("Number of contention events")
    countMetric.SetUnit("1")
    
    countGauge := countMetric.SetEmptyGauge()
    for _, stat := range stats {
        dp := countGauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.NumContention)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("table", stat.TableName)
        dp.Attributes().PutStr("index", stat.IndexName)
    }
}

func (s *cockroachScraper) addRangeHealthMetrics(scopeMetrics pmetric.ScopeMetrics, stats RangeHealthStats, now pcommon.Timestamp) {
    // Total ranges
    totalMetric := scopeMetrics.Metrics().AppendEmpty()
    totalMetric.SetName("cockroachdb.ranges.total")
    totalMetric.SetDescription("Total number of ranges in the cluster")
    totalMetric.SetUnit("1")
    
    totalGauge := totalMetric.SetEmptyGauge()
    dp := totalGauge.DataPoints().AppendEmpty()
    dp.SetIntValue(stats.TotalRanges)
    dp.SetTimestamp(now)
    
    // Under-replicated ranges
    underRepMetric := scopeMetrics.Metrics().AppendEmpty()
    underRepMetric.SetName("cockroachdb.ranges.under_replicated")
    underRepMetric.SetDescription("Number of under-replicated ranges (warning)")
    underRepMetric.SetUnit("1")
    
    underRepGauge := underRepMetric.SetEmptyGauge()
    dp2 := underRepGauge.DataPoints().AppendEmpty()
    dp2.SetIntValue(stats.UnderReplicatedRanges)
    dp2.SetTimestamp(now)
    
    // Unavailable ranges
    unavailMetric := scopeMetrics.Metrics().AppendEmpty()
    unavailMetric.SetName("cockroachdb.ranges.unavailable")
    unavailMetric.SetDescription("Number of unavailable ranges (critical)")
    unavailMetric.SetUnit("1")
    
    unavailGauge := unavailMetric.SetEmptyGauge()
    dp3 := unavailGauge.DataPoints().AppendEmpty()
    dp3.SetIntValue(stats.UnavailableRanges)
    dp3.SetTimestamp(now)
}

func (s *cockroachScraper) addNodeStatusMetrics(scopeMetrics pmetric.ScopeMetrics, stats []NodeStatus, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.node.live")
    metric.SetDescription("Node liveness status (1 = live, 0 = dead)")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        if stat.IsLive {
            dp.SetIntValue(1)
        } else {
            dp.SetIntValue(0)
        }
        dp.SetTimestamp(now)
        dp.Attributes().PutInt("node_id", stat.NodeID)
        dp.Attributes().PutStr("address", stat.Address)
    }
}

func (s *cockroachScraper) addJobMetrics(scopeMetrics pmetric.ScopeMetrics, stats []JobStats, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.jobs.active")
    metric.SetDescription("Active jobs by type and status")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetIntValue(1)
        dp.SetTimestamp(now)
        dp.Attributes().PutInt("job_id", stat.JobID)
        dp.Attributes().PutStr("job_type", stat.JobType)
        dp.Attributes().PutStr("status", stat.Status)
        dp.Attributes().PutStr("running_status", stat.RunningStatus)
    }
}

func (s *cockroachScraper) addChangefeedLagMetrics(scopeMetrics pmetric.ScopeMetrics, stats []ChangefeedLag, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.changefeed.lag_seconds")
    metric.SetDescription("Changefeed lag in seconds behind current time")
    metric.SetUnit("s")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetDoubleValue(stat.LagSeconds)
        dp.SetTimestamp(now)
        dp.Attributes().PutInt("job_id", stat.JobID)
    }
}

func (s *cockroachScraper) addSchemaChangeMetrics(scopeMetrics pmetric.ScopeMetrics, stats []SchemaChange, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.schema_changes.in_progress")
    metric.SetDescription("Schema changes currently in progress")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetIntValue(1)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("table", stat.TableName)
        dp.Attributes().PutStr("type", stat.Type)
        dp.Attributes().PutStr("state", stat.State)
    }
}

func (s *cockroachScraper) addStatementErrorMetrics(scopeMetrics pmetric.ScopeMetrics, stats []StatementError, now pcommon.Timestamp) {
    metric := scopeMetrics.Metrics().AppendEmpty()
    metric.SetName("cockroachdb.statement.errors")
    metric.SetDescription("Statement errors by query and error code")
    metric.SetUnit("1")
    
    gauge := metric.SetEmptyGauge()
    for _, stat := range stats {
        dp := gauge.DataPoints().AppendEmpty()
        dp.SetIntValue(stat.ErrorCount)
        dp.SetTimestamp(now)
        dp.Attributes().PutStr("query", stat.Query[:min(100, len(stat.Query))])
        dp.Attributes().PutStr("error_code", stat.ErrorCode)
    }
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
