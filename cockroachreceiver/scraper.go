package cockroachreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type cockroachScraper struct {
	client *sql.DB
	config *Config
	logger *zap.Logger
}

func newScraper(cfg *Config, settings component.TelemetrySettings) *cockroachScraper {
	// Create client immediately
	db, err := sql.Open("postgres", cfg.ConnectionString)
	if err != nil {
		settings.Logger.Error("Failed to open database", zap.Error(err))
		return nil
	}

	// Configure connection pool
	maxLifetime, _ := time.ParseDuration(cfg.ConnectionMaxLifetime)
	maxIdleTime, _ := time.ParseDuration(cfg.ConnectionMaxIdleTime)

	db.SetMaxOpenConns(cfg.MaxOpenConnections)
	db.SetMaxIdleConns(cfg.MaxIdleConnections)
	db.SetConnMaxLifetime(maxLifetime)
	db.SetConnMaxIdleTime(maxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		settings.Logger.Error("Failed to ping database", zap.Error(err))
		return nil
	}

	settings.Logger.Info("Successfully connected to CockroachDB")

	return &cockroachScraper{
		client: db,
		config: cfg,
		logger: settings.Logger,
	}
}

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
	if s.client == nil {
		return pmetric.NewMetrics(), fmt.Errorf("database client not initialized")
	}

	queryTimeout, _ := time.ParseDuration(s.config.QueryTimeout)
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	// Add resource attributes
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "cockroachdb")
	attrs.PutStr("db.system", "cockroachdb")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/npcomplete777/cockroachdb-receiver")
	sm.Scope().SetVersion("1.0.0")

	// Production-safe metrics
	if s.config.Metrics.StatementStatistics {
		if err := s.scrapeStatementStatistics(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape statement statistics", zap.Error(err))
		}
	}

	if s.config.Metrics.TransactionStatistics {
		if err := s.scrapeTransactionStatistics(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape transaction statistics", zap.Error(err))
		}
	}

	if s.config.Metrics.IndexUsageStatistics {
		if err := s.scrapeIndexUsage(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape index usage", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterQueries {
		if err := s.scrapeClusterQueries(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape cluster queries", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterSessions {
		if err := s.scrapeClusterSessions(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape cluster sessions", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterTransactions {
		if err := s.scrapeClusterTransactions(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape cluster transactions", zap.Error(err))
		}
	}

	// Contention metrics
	if s.config.Metrics.ClusterLocks {
		if err := s.scrapeClusterLocks(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape cluster locks", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterContendedIndexes {
		if err := s.scrapeContendedIndexes(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape contended indexes", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterContendedKeys {
		if err := s.scrapeContendedKeys(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape contended keys", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterContendedTables {
		if err := s.scrapeContendedTables(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape contended tables", zap.Error(err))
		}
	}

	if s.config.Metrics.ClusterContentionEvents {
		if err := s.scrapeContentionEvents(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape contention events", zap.Error(err))
		}
	}

	if s.config.Metrics.TransactionContentionEvents {
		if err := s.scrapeTransactionContentionEvents(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape transaction contention events", zap.Error(err))
		}
	}

	// Non-production safe metrics with warnings
	if s.config.Metrics.RangesNoLeases {
		s.logger.Warn("⚠️  Collecting ranges_no_leases - triggers expensive cluster-wide RPC")
		if err := s.scrapeRanges(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape ranges", zap.Error(err))
		}
	}

	if s.config.Metrics.GossipLiveness {
		s.logger.Warn("⚠️  Collecting gossip_liveness - schema is unstable")
		if err := s.scrapeGossipLiveness(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape gossip liveness", zap.Error(err))
		}
	}

	if s.config.Metrics.Jobs {
		s.logger.Warn("⚠️  Collecting jobs - not recommended for production")
		if err := s.scrapeJobs(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape jobs", zap.Error(err))
		}
	}

	if s.config.Metrics.SchemaChanges {
		s.logger.Warn("⚠️  Collecting schema_changes - not recommended for production")
		if err := s.scrapeSchemaChanges(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape schema changes", zap.Error(err))
		}
	}

	if s.config.Metrics.NodeMetrics {
		s.logger.Warn("⚠️  Collecting node_metrics - triggers expensive cluster-wide RPC")
		if err := s.scrapeNodeMetrics(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape node metrics", zap.Error(err))
		}
	}

	if s.config.Metrics.KVNodeStatus {
		s.logger.Warn("⚠️  Collecting kv_node_status - triggers expensive cluster-wide RPC")
		if err := s.scrapeKVNodeStatus(ctx, sm); err != nil {
			s.logger.Error("Failed to scrape KV node status", zap.Error(err))
		}
	}

	return metrics, nil
}

// ============================================================================
// PRODUCTION SAFE METRICS
// ============================================================================

func (s *cockroachScraper) scrapeStatementStatistics(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryStatementStatistics)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			aggregatedTs      time.Time
			fingerprintID     string
			txnFingerprintID  string
			planHash          string
			appName           string
			queryText         sql.NullString
			databaseName      sql.NullString
			querySummary      sql.NullString
			stmtType          sql.NullString
			fullScan          sql.NullBool
			vectorized        sql.NullBool
			implicitTxn       sql.NullBool
			executionCount    sql.NullInt64
			serviceLatencyMean sql.NullFloat64
			runLatencyMean    sql.NullFloat64
			parseLatencyMean  sql.NullFloat64
			planLatencyMean   sql.NullFloat64
			overheadLatencyMean sql.NullFloat64
			rowsMean          sql.NullFloat64
			rowsReadMean      sql.NullFloat64
			bytesReadMean     sql.NullFloat64
			maxRetries        sql.NullInt64
			lastExecAt        sql.NullString
		)

		if err := rows.Scan(
			&aggregatedTs, &fingerprintID, &txnFingerprintID, &planHash, &appName,
			&queryText, &databaseName, &querySummary, &stmtType,
			&fullScan, &vectorized, &implicitTxn,
			&executionCount, &serviceLatencyMean, &runLatencyMean,
			&parseLatencyMean, &planLatencyMean, &overheadLatencyMean,
			&rowsMean, &rowsReadMean, &bytesReadMean, &maxRetries, &lastExecAt,
		); err != nil {
			s.logger.Warn("Failed to scan statement statistics row", zap.Error(err))
			continue
		}

		// Create gauge metrics for statement statistics
		timestamp := pcommon.NewTimestampFromTime(aggregatedTs)

		// Execution count
		if executionCount.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.statement.execution.count")
			m.SetDescription("Number of times this statement was executed")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(executionCount.Int64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
			if databaseName.Valid {
				dp.Attributes().PutStr("database", databaseName.String)
			}
			if stmtType.Valid {
				dp.Attributes().PutStr("statement_type", stmtType.String)
			}
		}

		// Service latency
		if serviceLatencyMean.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.statement.latency.service")
			m.SetDescription("Average service latency (parse + plan + run)")
			m.SetUnit("s")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(serviceLatencyMean.Float64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
		}

		// Run latency
		if runLatencyMean.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.statement.latency.run")
			m.SetDescription("Average execution latency")
			m.SetUnit("s")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(runLatencyMean.Float64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
		}

		// Rows read
		if rowsReadMean.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.statement.rows.read")
			m.SetDescription("Average number of rows read")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(rowsReadMean.Float64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
			if fullScan.Valid {
				dp.Attributes().PutBool("full_scan", fullScan.Bool)
			}
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeTransactionStatistics(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryTransactionStatistics)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			aggregatedTs     time.Time
			fingerprintID    string
			appName          string
			executionCount   sql.NullInt64
			totalCount       sql.NullInt64
			serviceLatencyMean sql.NullFloat64
			commitLatencyMean  sql.NullFloat64
			retryLatencyMean   sql.NullFloat64
			rowsMean         sql.NullFloat64
			rowsReadMean     sql.NullFloat64
			rowsWrittenMean  sql.NullFloat64
			bytesReadMean    sql.NullFloat64
			maxRetries       sql.NullInt64
			contentionTimeMean sql.NullFloat64
		)

		if err := rows.Scan(
			&aggregatedTs, &fingerprintID, &appName,
			&executionCount, &totalCount,
			&serviceLatencyMean, &commitLatencyMean, &retryLatencyMean,
			&rowsMean, &rowsReadMean, &rowsWrittenMean, &bytesReadMean,
			&maxRetries, &contentionTimeMean,
		); err != nil {
			s.logger.Warn("Failed to scan transaction statistics row", zap.Error(err))
			continue
		}

		timestamp := pcommon.NewTimestampFromTime(aggregatedTs)

		// Execution count
		if executionCount.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.transaction.execution.count")
			m.SetDescription("Number of times this transaction was executed")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(executionCount.Int64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
		}

		// Service latency
		if serviceLatencyMean.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.transaction.latency.service")
			m.SetDescription("Average transaction service latency")
			m.SetUnit("s")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(serviceLatencyMean.Float64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
		}

		// Contention time
		if contentionTimeMean.Valid && contentionTimeMean.Float64 > 0 {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.transaction.contention.time")
			m.SetDescription("Average time spent in contention")
			m.SetUnit("s")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(contentionTimeMean.Float64)
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			dp.Attributes().PutStr("app_name", appName)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeIndexUsage(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryIndexUsageStatistics)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			tableName  string
			indexName  string
			totalReads sql.NullInt64
			lastRead   sql.NullTime
		)

		if err := rows.Scan(&tableName, &indexName, &totalReads, &lastRead); err != nil {
			s.logger.Warn("Failed to scan index usage row", zap.Error(err))
			continue
		}

		if totalReads.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.index.reads.total")
			m.SetDescription("Total number of reads from this index")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(totalReads.Int64)
			dp.Attributes().PutStr("table", tableName)
			dp.Attributes().PutStr("index", indexName)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeClusterQueries(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterQueries)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			queryID     string
			txnID       sql.NullString
			nodeID      sql.NullInt64
			sessionID   string
			userName    string
			start       time.Time
			query       string
			clientAddr  string
			appName     string
			distributed bool
			phase       string
		)

		if err := rows.Scan(&queryID, &txnID, &nodeID, &sessionID, &userName,
			&start, &query, &clientAddr, &appName, &distributed, &phase); err != nil {
			s.logger.Warn("Failed to scan cluster queries row", zap.Error(err))
			continue
		}
		count++
	}

	// Emit count of active queries
	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.cluster.queries.active")
	m.SetDescription("Number of currently active queries")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}

func (s *cockroachScraper) scrapeClusterSessions(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterSessions)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			nodeID           sql.NullInt64
			sessionID        string
			userName         string
			clientAddr       string
			appName          string
			activeQueries    string
			lastActiveQuery  string
			sessionStart     time.Time
			activeQueryStart sql.NullTime
			kvTxn            sql.NullString
			allocBytes       sql.NullInt64
			maxAllocBytes    sql.NullInt64
		)

		if err := rows.Scan(&nodeID, &sessionID, &userName, &clientAddr, &appName,
			&activeQueries, &lastActiveQuery, &sessionStart, &activeQueryStart,
			&kvTxn, &allocBytes, &maxAllocBytes); err != nil {
			s.logger.Warn("Failed to scan cluster sessions row", zap.Error(err))
			continue
		}
		count++

		// Memory usage per session
		if allocBytes.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.session.memory.allocated")
			m.SetDescription("Memory allocated by session")
			m.SetUnit("By")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(allocBytes.Int64)
			dp.Attributes().PutStr("session_id", sessionID)
			dp.Attributes().PutStr("user", userName)
			dp.Attributes().PutStr("app_name", appName)
		}
	}

	// Active sessions count
	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.cluster.sessions.active")
	m.SetDescription("Number of active sessions")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(timestamp)
	dp.SetIntValue(int64(count))

	return rows.Err()
}

func (s *cockroachScraper) scrapeClusterTransactions(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterTransactions)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			id             string
			nodeID         sql.NullInt64
			sessionID      string
			start          time.Time
			txnString      string
			appName        string
			numStmts       sql.NullInt64
			numRetries     sql.NullInt64
			numAutoRetries sql.NullInt64
		)

		if err := rows.Scan(&id, &nodeID, &sessionID, &start, &txnString,
			&appName, &numStmts, &numRetries, &numAutoRetries); err != nil {
			s.logger.Warn("Failed to scan cluster transactions row", zap.Error(err))
			continue
		}
		count++
	}

	// Active transactions count
	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.cluster.transactions.active")
	m.SetDescription("Number of active transactions")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}

// ============================================================================
// CONTENTION METRICS (Production-safe but moderate overhead)
// ============================================================================

func (s *cockroachScraper) scrapeClusterLocks(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterLocks)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	totalLocks := 0
	grantedLocks := 0
	contentedLocks := 0

	for rows.Next() {
		var (
			rangeID       sql.NullInt64
			tableID       sql.NullInt64
			databaseName  sql.NullString
			schemaName    sql.NullString
			tableName     sql.NullString
			indexName     sql.NullString
			lockKeyPretty sql.NullString
			txnID         sql.NullString
			ts            sql.NullTime
			lockStrength  sql.NullString
			durability    sql.NullString
			granted       sql.NullBool
			contended     sql.NullBool
			duration      sql.NullString
		)

		if err := rows.Scan(&rangeID, &tableID, &databaseName, &schemaName, &tableName,
			&indexName, &lockKeyPretty, &txnID, &ts, &lockStrength, &durability,
			&granted, &contended, &duration); err != nil {
			s.logger.Warn("Failed to scan cluster locks row", zap.Error(err))
			continue
		}

		totalLocks++
		if granted.Valid && granted.Bool {
			grantedLocks++
		}
		if contended.Valid && contended.Bool {
			contentedLocks++
		}
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Total locks
	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.cluster.locks.total")
	m.SetDescription("Total number of locks")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(timestamp)
	dp.SetIntValue(int64(totalLocks))

	// Granted locks
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("cockroachdb.cluster.locks.granted")
	m2.SetDescription("Number of granted locks")
	m2.SetUnit("1")
	g2 := m2.SetEmptyGauge()
	dp2 := g2.DataPoints().AppendEmpty()
	dp2.SetTimestamp(timestamp)
	dp2.SetIntValue(int64(grantedLocks))

	// Contended locks
	m3 := sm.Metrics().AppendEmpty()
	m3.SetName("cockroachdb.cluster.locks.contended")
	m3.SetDescription("Number of contended locks")
	m3.SetUnit("1")
	g3 := m3.SetEmptyGauge()
	dp3 := g3.DataPoints().AppendEmpty()
	dp3.SetTimestamp(timestamp)
	dp3.SetIntValue(int64(contentedLocks))

	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedIndexes(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterContendedIndexes)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        string
			schemaName          string
			tableName           string
			indexName           string
			numContentionEvents sql.NullInt64
		)

		if err := rows.Scan(&databaseName, &schemaName, &tableName, &indexName, &numContentionEvents); err != nil {
			s.logger.Warn("Failed to scan contended indexes row", zap.Error(err))
			continue
		}

		if numContentionEvents.Valid && numContentionEvents.Int64 > 0 {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.index.contention.events")
			m.SetDescription("Number of contention events on index")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(numContentionEvents.Int64)
			dp.Attributes().PutStr("database", databaseName)
			dp.Attributes().PutStr("schema", schemaName)
			dp.Attributes().PutStr("table", tableName)
			dp.Attributes().PutStr("index", indexName)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedKeys(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterContendedKeys)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        string
			schemaName          string
			tableName           string
			indexName           string
			keyHex              string
			numContentionEvents sql.NullInt64
		)

		if err := rows.Scan(&databaseName, &schemaName, &tableName, &indexName, &keyHex, &numContentionEvents); err != nil {
			s.logger.Warn("Failed to scan contended keys row", zap.Error(err))
			continue
		}

		if numContentionEvents.Valid && numContentionEvents.Int64 > 0 {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.key.contention.events")
			m.SetDescription("Number of contention events on specific key")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(numContentionEvents.Int64)
			dp.Attributes().PutStr("database", databaseName)
			dp.Attributes().PutStr("table", tableName)
			dp.Attributes().PutStr("index", indexName)
			dp.Attributes().PutStr("key", keyHex[:16]) // Truncate for cardinality
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedTables(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterContendedTables)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        string
			schemaName          string
			tableName           string
			numContentionEvents sql.NullInt64
		)

		if err := rows.Scan(&databaseName, &schemaName, &tableName, &numContentionEvents); err != nil {
			s.logger.Warn("Failed to scan contended tables row", zap.Error(err))
			continue
		}

		if numContentionEvents.Valid && numContentionEvents.Int64 > 0 {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.table.contention.events")
			m.SetDescription("Number of contention events on table")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(numContentionEvents.Int64)
			dp.Attributes().PutStr("database", databaseName)
			dp.Attributes().PutStr("schema", schemaName)
			dp.Attributes().PutStr("table", tableName)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeContentionEvents(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryClusterContentionEvents)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	totalEvents := 0
	var totalContentionTime float64

	for rows.Next() {
		var (
			tableID                  sql.NullInt64
			indexID                  sql.NullInt64
			numContentionEvents      sql.NullInt64
			cumulativeContentionTime sql.NullString
			keyHex                   sql.NullString
			txnID                    sql.NullString
			count                    sql.NullInt64
		)

		if err := rows.Scan(&tableID, &indexID, &numContentionEvents, &cumulativeContentionTime,
			&keyHex, &txnID, &count); err != nil {
			s.logger.Warn("Failed to scan contention events row", zap.Error(err))
			continue
		}

		if numContentionEvents.Valid {
			totalEvents += int(numContentionEvents.Int64)
		}

		// Parse interval string to seconds (simplified)
		if cumulativeContentionTime.Valid {
			// Would need proper interval parsing here
			totalContentionTime += 0.001 // Placeholder
		}
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.contention.events.total")
	m.SetDescription("Total contention events")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(timestamp)
	dp.SetIntValue(int64(totalEvents))

	return rows.Err()
}

func (s *cockroachScraper) scrapeTransactionContentionEvents(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryTransactionContentionEvents)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			collectionTs             time.Time
			blockingTxnID            string
			blockingTxnFingerprintID string
			waitingTxnID             string
			waitingTxnFingerprintID  string
			waitingStmtID            string
			waitingStmtFingerprintID string
			contentionDuration       sql.NullString
			contentingPrettyKey      sql.NullString
			databaseName             sql.NullString
			schemaName               sql.NullString
			tableName                sql.NullString
			indexName                sql.NullString
			contentionType           sql.NullString
		)

		if err := rows.Scan(&collectionTs, &blockingTxnID, &blockingTxnFingerprintID,
			&waitingTxnID, &waitingTxnFingerprintID, &waitingStmtID, &waitingStmtFingerprintID,
			&contentionDuration, &contentingPrettyKey, &databaseName, &schemaName,
			&tableName, &indexName, &contentionType); err != nil {
			s.logger.Warn("Failed to scan transaction contention events row", zap.Error(err))
			continue
		}
		count++
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.transaction.contention.events.count")
	m.SetDescription("Recent transaction contention events")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}

// ============================================================================
// NON-PRODUCTION SAFE METRICS (Use with extreme caution)
// ============================================================================

func (s *cockroachScraper) scrapeRanges(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryRangesNoLeases)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			rangeID            sql.NullInt64
			startKey           sql.NullString
			startPretty        sql.NullString
			endKey             sql.NullString
			endPretty          sql.NullString
			databaseName       sql.NullString
			tableName          sql.NullString
			indexName          sql.NullString
			replicas           sql.NullString
			replicaLocalities  sql.NullString
			votingReplicas     sql.NullString
			nonVotingReplicas  sql.NullString
			splitEnforcedUntil sql.NullTime
		)

		if err := rows.Scan(&rangeID, &startKey, &startPretty, &endKey, &endPretty,
			&databaseName, &tableName, &indexName, &replicas, &replicaLocalities,
			&votingReplicas, &nonVotingReplicas, &splitEnforcedUntil); err != nil {
			s.logger.Warn("Failed to scan ranges row", zap.Error(err))
			continue
		}
		count++
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.ranges.count")
	m.SetDescription("Total number of ranges")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}

func (s *cockroachScraper) scrapeGossipLiveness(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryGossipLiveness)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	liveNodes := 0
	for rows.Next() {
		var (
			nodeID          sql.NullInt64
			epoch           sql.NullInt64
			expiration      sql.NullTime
			draining        sql.NullBool
			decommissioning sql.NullBool
			membership      sql.NullString
			updatedAt       sql.NullTime
		)

		if err := rows.Scan(&nodeID, &epoch, &expiration, &draining, &decommissioning, &membership, &updatedAt); err != nil {
			s.logger.Warn("Failed to scan gossip liveness row", zap.Error(err))
			continue
		}

		if !draining.Valid || !draining.Bool {
			liveNodes++
		}
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.nodes.live")
	m.SetDescription("Number of live nodes")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(liveNodes))

	return rows.Err()
}

func (s *cockroachScraper) scrapeJobs(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryJobs)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	jobsByStatus := make(map[string]int)

	for rows.Next() {
		var (
			jobID             sql.NullInt64
			jobType           sql.NullString
			description       sql.NullString
			status            sql.NullString
			created           sql.NullTime
			started           sql.NullTime
			finished          sql.NullTime
			modified          sql.NullTime
			fractionCompleted sql.NullFloat64
			errorMsg          sql.NullString
			coordinatorID     sql.NullInt64
		)

		if err := rows.Scan(&jobID, &jobType, &description, &status, &created, &started,
			&finished, &modified, &fractionCompleted, &errorMsg, &coordinatorID); err != nil {
			s.logger.Warn("Failed to scan jobs row", zap.Error(err))
			continue
		}

		if status.Valid {
			jobsByStatus[status.String]++
		}
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for status, count := range jobsByStatus {
		m := sm.Metrics().AppendEmpty()
		m.SetName("cockroachdb.jobs.count")
		m.SetDescription("Number of jobs by status")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(int64(count))
		dp.Attributes().PutStr("status", status)
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeSchemaChanges(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QuerySchemaChanges)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			tableID    sql.NullInt64
			parentID   sql.NullInt64
			name       sql.NullString
			changeType sql.NullString
			targetID   sql.NullInt64
			targetName sql.NullString
			state      sql.NullString
			direction  sql.NullString
		)

		if err := rows.Scan(&tableID, &parentID, &name, &changeType, &targetID, &targetName, &state, &direction); err != nil {
			s.logger.Warn("Failed to scan schema changes row", zap.Error(err))
			continue
		}
		count++
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.schema_changes.active")
	m.SetDescription("Number of active schema changes")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}

func (s *cockroachScraper) scrapeNodeMetrics(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryNodeMetrics)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			nodeID  sql.NullInt64
			storeID sql.NullInt64
			name    sql.NullString
			value   sql.NullFloat64
		)

		if err := rows.Scan(&nodeID, &storeID, &name, &value); err != nil {
			s.logger.Warn("Failed to scan node metrics row", zap.Error(err))
			continue
		}

		if name.Valid && value.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName(fmt.Sprintf("cockroachdb.node.%s", name.String))
			m.SetDescription(fmt.Sprintf("Node metric: %s", name.String))
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetDoubleValue(value.Float64)
			if nodeID.Valid {
				dp.Attributes().PutInt("node_id", nodeID.Int64)
			}
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeKVNodeStatus(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, QueryKVNodeStatus)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			nodeID        sql.NullInt64
			network       sql.NullString
			address       sql.NullString
			attrs         sql.NullString
			locality      sql.NullString
			serverVersion sql.NullString
			goVersion     sql.NullString
			tag           sql.NullString
			time          sql.NullTime
			revision      sql.NullString
			cgoCompiler   sql.NullString
			platform      sql.NullString
			distribution  sql.NullString
			nodeType      sql.NullString
			dependencies  sql.NullString
			startedAt     sql.NullTime
			updatedAt     sql.NullTime
		)

		if err := rows.Scan(&nodeID, &network, &address, &attrs, &locality, &serverVersion,
			&goVersion, &tag, &time, &revision, &cgoCompiler, &platform, &distribution,
			&nodeType, &dependencies, &startedAt, &updatedAt); err != nil {
			s.logger.Warn("Failed to scan KV node status row", zap.Error(err))
			continue
		}
		count++
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.nodes.total")
	m.SetDescription("Total number of nodes in cluster")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(count))

	return rows.Err()
}
