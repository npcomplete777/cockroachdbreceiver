package cockroachreceiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type cockroachScraper struct {
	config   *Config
	db       *sql.DB
	logger   *zap.Logger
	settings receiver.Settings
	mu       sync.Mutex
}

func newCockroachScraper(config *Config, settings receiver.Settings) *cockroachScraper {
	return &cockroachScraper{
		config:   config,
		settings: settings,
		logger:   settings.Logger,
	}
}

func (s *cockroachScraper) truncateQuery(query string) string {
	maxLen := s.config.MaxQueryLength
	if maxLen == 0 || len(query) <= maxLen {
		return query
	}
	truncated := query[:maxLen]
	lastSpace := strings.LastIndex(truncated, " ")
	if lastSpace > maxLen-20 {
		truncated = truncated[:lastSpace]
	}
	return truncated + "..."
}

func (s *cockroachScraper) sanitizeDatabase(db sql.NullString) string {
	if !db.Valid || db.String == "" {
		return "system"
	}
	return db.String
}

func (s *cockroachScraper) Start(ctx context.Context, host component.Host) error {
	s.logger.Info("Starting CockroachDB receiver")
	db, err := sql.Open("postgres", s.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to CockroachDB: %w", err)
	}
	
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping CockroachDB: %w", err)
	}
	
	s.mu.Lock()
	s.db = db
	s.mu.Unlock()
	
	s.logger.Info("Successfully connected to CockroachDB",
		zap.Int("max_open_conns", 10),
		zap.Int("max_idle_conns", 5),
		zap.Int("max_query_length", s.config.MaxQueryLength))
	return nil
}

func (s *cockroachScraper) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *cockroachScraper) ScrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()
	
	if db == nil {
		return pmetric.NewMetrics(), errors.New("database connection not initialized")
	}
	
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	
	var scrapeErrors []error
	now := pcommon.NewTimestampFromTime(time.Now())
	
	// Statement Statistics
	if s.config.CollectStatementStats {
		if err := s.scrapeStatementStats(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("statement stats: %w", err))
		}
	}
	
	// Transaction Statistics
	if s.config.CollectTransactionStats {
		if err := s.scrapeTransactionStats(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("transaction stats: %w", err))
		}
	}
	
	// Index Usage
	if s.config.CollectIndexUsage {
		if err := s.scrapeIndexUsage(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("index usage: %w", err))
		}
	}
	
	// Cluster Queries
	if s.config.CollectClusterQueries {
		if err := s.scrapeClusterQueries(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("cluster queries: %w", err))
		}
	}
	
	// Cluster Sessions
	if s.config.CollectClusterSessions {
		if err := s.scrapeClusterSessions(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("cluster sessions: %w", err))
		}
	}
	
	// Cluster Transactions
	if s.config.CollectClusterTransactions {
		if err := s.scrapeClusterTransactions(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("cluster transactions: %w", err))
		}
	}
	
	// Contention Metrics
	if s.config.CollectContentionMetrics {
		if err := s.scrapeContentionMetrics(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("contention metrics: %w", err))
		}
	}
	
	// Jobs (may be empty on Serverless)
	if s.config.CollectJobs {
		if err := s.scrapeJobs(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("jobs: %w", err))
		}
	}
	
	// Schema Changes (may be empty on Serverless)
	if s.config.CollectSchemaChanges {
		if err := s.scrapeSchemaChanges(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("schema changes: %w", err))
		}
	}
	
	// Node-level metrics (not available on Serverless)
	if s.config.CollectNodeMetrics {
		if err := s.scrapeNodeMetrics(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("node metrics: %w", err))
		}
	}
	
	// Range metrics (problematic on Serverless)
	if s.config.CollectRangeMetrics {
		if err := s.scrapeRangeMetrics(ctx, db, sm, now); err != nil {
			scrapeErrors = append(scrapeErrors, fmt.Errorf("range metrics: %w", err))
		}
	}
	
	s.recordReceiverMetrics(sm, now, len(scrapeErrors))
	
	if len(scrapeErrors) > 0 {
		return metrics, fmt.Errorf("scrape errors: %v", scrapeErrors)
	}
	return metrics, nil
}

// Statement Statistics - ALL metrics
func (s *cockroachScraper) scrapeStatementStats(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryStatementStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query statement statistics: %w", err)
	}
	defer rows.Close()
	
	// Create all statement metrics with OTel-compliant names
	execCountMetric := sm.Metrics().AppendEmpty()
	execCountMetric.SetName("cockroachdb.statement.execution.count")  // Changed from execution_count
	execCountMetric.SetUnit("{executions}")
	execCountMetric.SetEmptyGauge()
	
	svcLatMetric := sm.Metrics().AppendEmpty()
	svcLatMetric.SetName("cockroachdb.statement.latency.service.mean")  // Changed from service_latency_mean
	svcLatMetric.SetUnit("s")
	svcLatMetric.SetEmptyGauge()
	
	parseLatMetric := sm.Metrics().AppendEmpty()
	parseLatMetric.SetName("cockroachdb.statement.latency.parse.mean")  // Changed from parse_latency_mean
	parseLatMetric.SetUnit("s")
	parseLatMetric.SetEmptyGauge()
	
	planLatMetric := sm.Metrics().AppendEmpty()
	planLatMetric.SetName("cockroachdb.statement.latency.plan.mean")  // Changed from plan_latency_mean
	planLatMetric.SetUnit("s")
	planLatMetric.SetEmptyGauge()
	
	runLatMetric := sm.Metrics().AppendEmpty()
	runLatMetric.SetName("cockroachdb.statement.latency.run.mean")  // Changed from run_latency_mean
	runLatMetric.SetUnit("s")
	runLatMetric.SetEmptyGauge()
	
	rowsReadMetric := sm.Metrics().AppendEmpty()
	rowsReadMetric.SetName("cockroachdb.statement.rows.read.mean")  // Changed from rows_read_mean
	rowsReadMetric.SetUnit("{rows}")
	rowsReadMetric.SetEmptyGauge()
	
	rowsWrittenMetric := sm.Metrics().AppendEmpty()
	rowsWrittenMetric.SetName("cockroachdb.statement.rows.written.mean")  // Changed from rows_written_mean
	rowsWrittenMetric.SetUnit("{rows}")
	rowsWrittenMetric.SetEmptyGauge()
	
	bytesReadMetric := sm.Metrics().AppendEmpty()
	bytesReadMetric.SetName("cockroachdb.statement.bytes.read.mean")  // Changed from bytes_read_mean
	bytesReadMetric.SetUnit("By")
	bytesReadMetric.SetEmptyGauge()
	
	maxRetriesMetric := sm.Metrics().AppendEmpty()
	maxRetriesMetric.SetName("cockroachdb.statement.retries.max")  // Changed from max_retries
	maxRetriesMetric.SetUnit("{retries}")
	maxRetriesMetric.SetEmptyGauge()
	
	errorCountMetric := sm.Metrics().AppendEmpty()
	errorCountMetric.SetName("cockroachdb.statement.error.count")  // Changed from error_count
	errorCountMetric.SetUnit("{errors}")
	errorCountMetric.SetEmptyGauge()
	
	for rows.Next() {
		var fingerprintID, appName, database, query, stmtType, lastErrorCode sql.NullString
		var execCount, maxRetries, errorCount sql.NullInt64
		var svcLatMean, parseLatMean, planLatMean, runLatMean sql.NullFloat64
		var rowsReadMean, rowsWrittenMean, bytesReadMean sql.NullFloat64
		
		err := rows.Scan(&fingerprintID, &appName, &database, &query, &stmtType,
			&execCount, &svcLatMean, &parseLatMean, &planLatMean, &runLatMean,
			&rowsReadMean, &rowsWrittenMean, &bytesReadMean,
			&maxRetries, &errorCount, &lastErrorCode)
		if err != nil {
			s.logger.Error("Failed to scan statement statistics row", zap.Error(err))
			continue
		}
		
		queryText := ""
		if query.Valid {
			queryText = s.truncateQuery(query.String)
		}
		dbName := s.sanitizeDatabase(database)
		
		// Common attributes for all metrics from this row
		attrs := func(dp pmetric.NumberDataPoint) {
			dp.Attributes().PutStr("query_text", queryText)
			dp.Attributes().PutStr("database", dbName)
			if appName.Valid && appName.String != "" {
				dp.Attributes().PutStr("app_name", appName.String)
			}
			if stmtType.Valid && stmtType.String != "" {
				dp.Attributes().PutStr("statement_type", stmtType.String)
			}
			if fingerprintID.Valid && fingerprintID.String != "" {
				dp.Attributes().PutStr("fingerprint_id", fingerprintID.String)
			}
		}
		
		if execCount.Valid {
			dp := execCountMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(execCount.Int64)
			attrs(dp)
		}
		
		if svcLatMean.Valid {
			dp := svcLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(svcLatMean.Float64)
			attrs(dp)
		}
		
		if parseLatMean.Valid {
			dp := parseLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(parseLatMean.Float64)
			attrs(dp)
		}
		
		if planLatMean.Valid {
			dp := planLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(planLatMean.Float64)
			attrs(dp)
		}
		
		if runLatMean.Valid {
			dp := runLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(runLatMean.Float64)
			attrs(dp)
		}
		
		if rowsReadMean.Valid {
			dp := rowsReadMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(rowsReadMean.Float64)
			attrs(dp)
		}
		
		if rowsWrittenMean.Valid {
			dp := rowsWrittenMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(rowsWrittenMean.Float64)
			attrs(dp)
		}
		
		if bytesReadMean.Valid {
			dp := bytesReadMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(bytesReadMean.Float64)
			attrs(dp)
		}
		
		if maxRetries.Valid {
			dp := maxRetriesMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(maxRetries.Int64)
			attrs(dp)
		}
		
		if errorCount.Valid {
			dp := errorCountMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(errorCount.Int64)
			attrs(dp)
			if lastErrorCode.Valid && lastErrorCode.String != "" {
				dp.Attributes().PutStr("last_error_code", lastErrorCode.String)
			}
		}
	}
	
	return rows.Err()
}

// Transaction Statistics - ALL metrics
func (s *cockroachScraper) scrapeTransactionStats(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryTransactionStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query transaction statistics: %w", err)
	}
	defer rows.Close()
	
	// Create transaction metrics with OTel-compliant names
	txnExecCountMetric := sm.Metrics().AppendEmpty()
	txnExecCountMetric.SetName("cockroachdb.transaction.execution.count")  // Changed from execution_count
	txnExecCountMetric.SetUnit("{executions}")
	txnExecCountMetric.SetEmptyGauge()
	
	txnSvcLatMetric := sm.Metrics().AppendEmpty()
	txnSvcLatMetric.SetName("cockroachdb.transaction.latency.service.mean")  // Changed from service_latency_mean
	txnSvcLatMetric.SetUnit("s")
	txnSvcLatMetric.SetEmptyGauge()
	
	txnCommitLatMetric := sm.Metrics().AppendEmpty()
	txnCommitLatMetric.SetName("cockroachdb.transaction.latency.commit.mean")  // Changed from commit_latency_mean
	txnCommitLatMetric.SetUnit("s")
	txnCommitLatMetric.SetEmptyGauge()
	
	txnRetryLatMetric := sm.Metrics().AppendEmpty()
	txnRetryLatMetric.SetName("cockroachdb.transaction.latency.retry.mean")  // Changed from retry_latency_mean
	txnRetryLatMetric.SetUnit("s")
	txnRetryLatMetric.SetEmptyGauge()
	
	txnRowsReadMetric := sm.Metrics().AppendEmpty()
	txnRowsReadMetric.SetName("cockroachdb.transaction.rows.read.mean")  // Changed from rows_read_mean
	txnRowsReadMetric.SetUnit("{rows}")
	txnRowsReadMetric.SetEmptyGauge()
	
	txnRowsWrittenMetric := sm.Metrics().AppendEmpty()
	txnRowsWrittenMetric.SetName("cockroachdb.transaction.rows.written.mean")  // Changed from rows_written_mean
	txnRowsWrittenMetric.SetUnit("{rows}")
	txnRowsWrittenMetric.SetEmptyGauge()
	
	txnBytesReadMetric := sm.Metrics().AppendEmpty()
	txnBytesReadMetric.SetName("cockroachdb.transaction.bytes.read.mean")  // Changed from bytes_read_mean
	txnBytesReadMetric.SetUnit("By")
	txnBytesReadMetric.SetEmptyGauge()
	
	txnMaxRetriesMetric := sm.Metrics().AppendEmpty()
	txnMaxRetriesMetric.SetName("cockroachdb.transaction.retries.max")  // Changed from max_retries
	txnMaxRetriesMetric.SetUnit("{retries}")
	txnMaxRetriesMetric.SetEmptyGauge()
	
	for rows.Next() {
		var fingerprintID, appName sql.NullString
		var execCount, maxRetries sql.NullInt64
		var svcLatMean, commitLatMean, retryLatMean sql.NullFloat64
		var rowsReadMean, rowsWrittenMean, bytesReadMean sql.NullFloat64
		
		err := rows.Scan(&fingerprintID, &appName, &execCount,
			&svcLatMean, &commitLatMean, &retryLatMean,
			&rowsReadMean, &rowsWrittenMean, &bytesReadMean, &maxRetries)
		if err != nil {
			s.logger.Error("Failed to scan transaction statistics row", zap.Error(err))
			continue
		}
		
		attrs := func(dp pmetric.NumberDataPoint) {
			if appName.Valid && appName.String != "" {
				dp.Attributes().PutStr("app_name", appName.String)
			}
			if fingerprintID.Valid && fingerprintID.String != "" {
				dp.Attributes().PutStr("fingerprint_id", fingerprintID.String)
			}
		}
		
		if execCount.Valid {
			dp := txnExecCountMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(execCount.Int64)
			attrs(dp)
		}
		
		if svcLatMean.Valid {
			dp := txnSvcLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(svcLatMean.Float64)
			attrs(dp)
		}
		
		if commitLatMean.Valid {
			dp := txnCommitLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(commitLatMean.Float64)
			attrs(dp)
		}
		
		if retryLatMean.Valid {
			dp := txnRetryLatMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(retryLatMean.Float64)
			attrs(dp)
		}
		
		if rowsReadMean.Valid {
			dp := txnRowsReadMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(rowsReadMean.Float64)
			attrs(dp)
		}
		
		if rowsWrittenMean.Valid {
			dp := txnRowsWrittenMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(rowsWrittenMean.Float64)
			attrs(dp)
		}
		
		if bytesReadMean.Valid {
			dp := txnBytesReadMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(bytesReadMean.Float64)
			attrs(dp)
		}
		
		if maxRetries.Valid {
			dp := txnMaxRetriesMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(maxRetries.Int64)
			attrs(dp)
		}
	}
	
	return rows.Err()
}

// Index Usage
func (s *cockroachScraper) scrapeIndexUsage(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryIndexUsageStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query index usage: %w", err)
	}
	defer rows.Close()
	
	totalReadsMetric := sm.Metrics().AppendEmpty()
	totalReadsMetric.SetName("cockroachdb.index.reads.total")  // Changed from total_reads
	totalReadsMetric.SetUnit("{reads}")
	totalReadsMetric.SetEmptyGauge()
	
	lastReadMetric := sm.Metrics().AppendEmpty()
	lastReadMetric.SetName("cockroachdb.index.seconds_since_last_read")  // Keep as is - follows pattern
	lastReadMetric.SetUnit("s")
	lastReadMetric.SetEmptyGauge()
	
	for rows.Next() {
		var tableName, indexName string
		var totalReads int64
		var secondsSinceLastRead sql.NullFloat64
		
		err := rows.Scan(&tableName, &indexName, &totalReads, &secondsSinceLastRead)
		if err != nil {
			s.logger.Error("Failed to scan index usage row", zap.Error(err))
			continue
		}
		
		dp := totalReadsMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(totalReads)
		dp.Attributes().PutStr("table", tableName)
		dp.Attributes().PutStr("index", indexName)
		
		if secondsSinceLastRead.Valid {
			dp2 := lastReadMetric.Gauge().DataPoints().AppendEmpty()
			dp2.SetTimestamp(now)
			dp2.SetDoubleValue(secondsSinceLastRead.Float64)
			dp2.Attributes().PutStr("table", tableName)
			dp2.Attributes().PutStr("index", indexName)
		}
	}
	
	return rows.Err()
}

// Cluster Queries
func (s *cockroachScraper) scrapeClusterQueries(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterQueries, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query cluster queries: %w", err)
	}
	defer rows.Close()
	
	activeQueriesMetric := sm.Metrics().AppendEmpty()
	activeQueriesMetric.SetName("cockroachdb.cluster.queries.active")  // Already correct
	activeQueriesMetric.SetUnit("{queries}")
	activeQueriesMetric.SetEmptyGauge()
	
	queryDurationMetric := sm.Metrics().AppendEmpty()
	queryDurationMetric.SetName("cockroachdb.cluster.queries.duration")  // Already correct
	queryDurationMetric.SetUnit("s")
	queryDurationMetric.SetEmptyGauge()
	
	for rows.Next() {
		var queryID, userName, appName, query sql.NullString
		var nodeID sql.NullInt64
		var duration sql.NullFloat64
		
		err := rows.Scan(&queryID, &nodeID, &userName, &appName, &duration, &query)
		if err != nil {
			s.logger.Error("Failed to scan cluster queries row", zap.Error(err))
			continue
		}
		
		// Active query count
		dp := activeQueriesMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		if nodeID.Valid {
			dp.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
		}
		if userName.Valid && userName.String != "" {
			dp.Attributes().PutStr("user_name", userName.String)
		}
		if appName.Valid && appName.String != "" {
			dp.Attributes().PutStr("app_name", appName.String)
		}
		
		// Query duration
		if duration.Valid {
			dp2 := queryDurationMetric.Gauge().DataPoints().AppendEmpty()
			dp2.SetTimestamp(now)
			dp2.SetDoubleValue(duration.Float64)
			if nodeID.Valid {
				dp2.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
			}
			if userName.Valid && userName.String != "" {
				dp2.Attributes().PutStr("user_name", userName.String)
			}
			if appName.Valid && appName.String != "" {
				dp2.Attributes().PutStr("app_name", appName.String)
			}
		}
	}
	
	return rows.Err()
}

// Cluster Sessions
func (s *cockroachScraper) scrapeClusterSessions(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterSessions, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query cluster sessions: %w", err)
	}
	defer rows.Close()
	
	activeSessionsMetric := sm.Metrics().AppendEmpty()
	activeSessionsMetric.SetName("cockroachdb.cluster.sessions.active")  // Already correct
	activeSessionsMetric.SetUnit("{sessions}")
	activeSessionsMetric.SetEmptyGauge()
	
	sessionMemoryMetric := sm.Metrics().AppendEmpty()
	sessionMemoryMetric.SetName("cockroachdb.cluster.sessions.memory_allocated")  // Already correct
	sessionMemoryMetric.SetUnit("By")
	sessionMemoryMetric.SetEmptyGauge()
	
	sessionAgeMetric := sm.Metrics().AppendEmpty()
	sessionAgeMetric.SetName("cockroachdb.cluster.sessions.age")  // Already correct
	sessionAgeMetric.SetUnit("s")
	sessionAgeMetric.SetEmptyGauge()
	
	for rows.Next() {
		var sessionID, userName, appName sql.NullString
		var nodeID, allocBytes sql.NullInt64
		var sessionAge sql.NullFloat64
		
		err := rows.Scan(&sessionID, &nodeID, &userName, &appName, &allocBytes, &sessionAge)
		if err != nil {
			s.logger.Error("Failed to scan cluster sessions row", zap.Error(err))
			continue
		}
		
		// Active session count
		dp := activeSessionsMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		if nodeID.Valid {
			dp.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
		}
		if userName.Valid && userName.String != "" {
			dp.Attributes().PutStr("user_name", userName.String)
		}
		if appName.Valid && appName.String != "" {
			dp.Attributes().PutStr("app_name", appName.String)
		}
		
		// Session memory
		if allocBytes.Valid {
			dp2 := sessionMemoryMetric.Gauge().DataPoints().AppendEmpty()
			dp2.SetTimestamp(now)
			dp2.SetIntValue(allocBytes.Int64)
			if nodeID.Valid {
				dp2.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
			}
			if userName.Valid && userName.String != "" {
				dp2.Attributes().PutStr("user_name", userName.String)
			}
		}
		
		// Session age
		if sessionAge.Valid {
			dp3 := sessionAgeMetric.Gauge().DataPoints().AppendEmpty()
			dp3.SetTimestamp(now)
			dp3.SetDoubleValue(sessionAge.Float64)
			if nodeID.Valid {
				dp3.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
			}
			if userName.Valid && userName.String != "" {
				dp3.Attributes().PutStr("user_name", userName.String)
			}
		}
	}
	
	return rows.Err()
}

// Cluster Transactions
func (s *cockroachScraper) scrapeClusterTransactions(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterTransactions, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query cluster transactions: %w", err)
	}
	defer rows.Close()
	
	activeTxnMetric := sm.Metrics().AppendEmpty()
	activeTxnMetric.SetName("cockroachdb.cluster.transactions.active")  // Already correct
	activeTxnMetric.SetUnit("{transactions}")
	activeTxnMetric.SetEmptyGauge()
	
	txnDurationMetric := sm.Metrics().AppendEmpty()
	txnDurationMetric.SetName("cockroachdb.cluster.transactions.duration")  // Already correct
	txnDurationMetric.SetUnit("s")
	txnDurationMetric.SetEmptyGauge()
	
	for rows.Next() {
		var txnID, appName sql.NullString
		var nodeID sql.NullInt64
		var duration sql.NullFloat64
		
		err := rows.Scan(&txnID, &nodeID, &appName, &duration)
		if err != nil {
			s.logger.Error("Failed to scan cluster transactions row", zap.Error(err))
			continue
		}
		
		dp := activeTxnMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		if nodeID.Valid {
			dp.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
		}
		if appName.Valid && appName.String != "" {
			dp.Attributes().PutStr("app_name", appName.String)
		}
		
		if duration.Valid {
			dp2 := txnDurationMetric.Gauge().DataPoints().AppendEmpty()
			dp2.SetTimestamp(now)
			dp2.SetDoubleValue(duration.Float64)
			if nodeID.Valid {
				dp2.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
			}
			if appName.Valid && appName.String != "" {
				dp2.Attributes().PutStr("app_name", appName.String)
			}
		}
	}
	
	return rows.Err()
}

// Contention Metrics
func (s *cockroachScraper) scrapeContentionMetrics(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	// Multiple contention tables - handle each
	
	// Contended Indexes
	if err := s.scrapeContendedIndexes(ctx, db, sm, now); err != nil {
		s.logger.Error("Failed to scrape contended indexes", zap.Error(err))
	}
	
	// Contended Tables
	if err := s.scrapeContendedTables(ctx, db, sm, now); err != nil {
		s.logger.Error("Failed to scrape contended tables", zap.Error(err))
	}
	
	// Contention Events
	if err := s.scrapeContentionEvents(ctx, db, sm, now); err != nil {
		s.logger.Error("Failed to scrape contention events", zap.Error(err))
	}
	
	return nil
}

func (s *cockroachScraper) scrapeContendedIndexes(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterContendedIndexes, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	contentionMetric := sm.Metrics().AppendEmpty()
	contentionMetric.SetName("cockroachdb.contention.index.events")  // Already correct
	contentionMetric.SetUnit("{events}")
	contentionMetric.SetEmptyGauge()
	
	for rows.Next() {
		var dbName, schemaName, tableName, indexName sql.NullString
		var numEvents sql.NullInt64
		
		err := rows.Scan(&dbName, &schemaName, &tableName, &indexName, &numEvents)
		if err != nil {
			continue
		}
		
		if numEvents.Valid {
			dp := contentionMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(numEvents.Int64)
			if dbName.Valid {
				dp.Attributes().PutStr("database", dbName.String)
			}
			if tableName.Valid {
				dp.Attributes().PutStr("table", tableName.String)
			}
			if indexName.Valid {
				dp.Attributes().PutStr("index", indexName.String)
			}
		}
	}
	
	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedTables(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterContendedTables, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	contentionMetric := sm.Metrics().AppendEmpty()
	contentionMetric.SetName("cockroachdb.contention.table.events")  // Already correct
	contentionMetric.SetUnit("{events}")
	contentionMetric.SetEmptyGauge()
	
	for rows.Next() {
		var dbName, schemaName, tableName sql.NullString
		var numEvents sql.NullInt64
		
		err := rows.Scan(&dbName, &schemaName, &tableName, &numEvents)
		if err != nil {
			continue
		}
		
		if numEvents.Valid {
			dp := contentionMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetIntValue(numEvents.Int64)
			if dbName.Valid {
				dp.Attributes().PutStr("database", dbName.String)
			}
			if tableName.Valid {
				dp.Attributes().PutStr("table", tableName.String)
			}
		}
	}
	
	return rows.Err()
}

func (s *cockroachScraper) scrapeContentionEvents(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryClusterContentionEvents, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	contentionTimeMetric := sm.Metrics().AppendEmpty()
	contentionTimeMetric.SetName("cockroachdb.contention.time.cumulative")  // Changed from cumulative_time
	contentionTimeMetric.SetUnit("s")
	contentionTimeMetric.SetEmptyGauge()
	
	for rows.Next() {
		var tableID, indexID, numEvents sql.NullInt64
		var cumulativeTime sql.NullFloat64
		
		err := rows.Scan(&tableID, &indexID, &numEvents, &cumulativeTime)
		if err != nil {
			continue
		}
		
		if cumulativeTime.Valid {
			dp := contentionTimeMetric.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(cumulativeTime.Float64)
			if tableID.Valid {
				dp.Attributes().PutStr("table_id", fmt.Sprintf("%d", tableID.Int64))
			}
			if indexID.Valid {
				dp.Attributes().PutStr("index_id", fmt.Sprintf("%d", indexID.Int64))
			}
		}
	}
	
	return rows.Err()
}

// Jobs
func (s *cockroachScraper) scrapeJobs(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryJobs, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()
	
	jobsMetric := sm.Metrics().AppendEmpty()
	jobsMetric.SetName("cockroachdb.jobs.active")  // Already correct
	jobsMetric.SetUnit("{jobs}")
	jobsMetric.SetEmptyGauge()
	
	progressMetric := sm.Metrics().AppendEmpty()
	progressMetric.SetName("cockroachdb.jobs.progress")  // Already correct
	progressMetric.SetUnit("%")
	progressMetric.SetEmptyGauge()
	
	for rows.Next() {
		var jobID int64
		var jobType, status, runningStatus sql.NullString
		var fractionCompleted sql.NullFloat64
		
		err := rows.Scan(&jobID, &jobType, &status, &runningStatus, &fractionCompleted)
		if err != nil {
			s.logger.Error("Failed to scan job row", zap.Error(err))
			continue
		}
		
		// Active job count
		dp := jobsMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		dp.Attributes().PutStr("job_id", fmt.Sprintf("%d", jobID))
		if jobType.Valid && jobType.String != "" {
			dp.Attributes().PutStr("job_type", jobType.String)
		}
		if status.Valid {
			dp.Attributes().PutStr("status", status.String)
		}
		
		// Job progress
		if fractionCompleted.Valid {
			dp2 := progressMetric.Gauge().DataPoints().AppendEmpty()
			dp2.SetTimestamp(now)
			dp2.SetDoubleValue(fractionCompleted.Float64 * 100)
			dp2.Attributes().PutStr("job_id", fmt.Sprintf("%d", jobID))
			if jobType.Valid && jobType.String != "" {
				dp2.Attributes().PutStr("job_type", jobType.String)
			}
		}
	}
	
	return rows.Err()
}

// Schema Changes
func (s *cockroachScraper) scrapeSchemaChanges(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, querySchemaChanges, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query schema changes: %w", err)
	}
	defer rows.Close()
	
	changeCountMetric := sm.Metrics().AppendEmpty()
	changeCountMetric.SetName("cockroachdb.schema_changes.active")  // Already correct
	changeCountMetric.SetUnit("{changes}")
	changeCountMetric.SetEmptyGauge()
	
	for rows.Next() {
		var tableName, changeType, state string
		err := rows.Scan(&tableName, &changeType, &state)
		if err != nil {
			s.logger.Error("Failed to scan schema change row", zap.Error(err))
			continue
		}
		
		dp := changeCountMetric.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(1)
		dp.Attributes().PutStr("table", tableName)
		dp.Attributes().PutStr("change_type", changeType)
		dp.Attributes().PutStr("state", state)
	}
	
	return rows.Err()
}

// Node Metrics (not available on Serverless)
func (s *cockroachScraper) scrapeNodeMetrics(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	rows, err := db.QueryContext(ctx, queryNodeMetrics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("failed to query node metrics: %w", err)
	}
	defer rows.Close()
	
	cpuMetric := sm.Metrics().AppendEmpty()
	cpuMetric.SetName("cockroachdb.node.cpu.percent")  // Already correct
	cpuMetric.SetUnit("%")
	cpuMetric.SetEmptyGauge()
	
	memoryMetric := sm.Metrics().AppendEmpty()
	memoryMetric.SetName("cockroachdb.node.memory.rss")  // Already correct
	memoryMetric.SetUnit("By")
	memoryMetric.SetEmptyGauge()
	
	for rows.Next() {
		var nodeID, storeID sql.NullInt64
		var name sql.NullString
		var value sql.NullFloat64
		
		err := rows.Scan(&nodeID, &storeID, &name, &value)
		if err != nil {
			continue
		}
		
		if name.Valid && value.Valid {
			switch name.String {
			case "sys.cpu.combined.percent-normalized":
				dp := cpuMetric.Gauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(now)
				dp.SetDoubleValue(value.Float64 * 100)
				if nodeID.Valid {
					dp.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
				}
			case "sys.rss":
				dp := memoryMetric.Gauge().DataPoints().AppendEmpty()
				dp.SetTimestamp(now)
				dp.SetDoubleValue(value.Float64)
				if nodeID.Valid {
					dp.Attributes().PutStr("node_id", fmt.Sprintf("%d", nodeID.Int64))
				}
			}
		}
	}
	
	return rows.Err()
}

// Range Metrics (problematic on Serverless)
func (s *cockroachScraper) scrapeRangeMetrics(ctx context.Context, db *sql.DB, sm pmetric.ScopeMetrics, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	
	row := db.QueryRowContext(ctx, queryRangesNoLeases)
	
	var totalRanges, underReplicatedRanges, unavailableRanges int64
	err := row.Scan(&totalRanges, &underReplicatedRanges, &unavailableRanges)
	if err != nil {
		return fmt.Errorf("failed to query range metrics: %w", err)
	}
	
	totalMetric := sm.Metrics().AppendEmpty()
	totalMetric.SetName("cockroachdb.ranges.total")  // Already correct
	totalMetric.SetUnit("{ranges}")
	totalMetric.SetEmptyGauge()
	dp := totalMetric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(now)
	dp.SetIntValue(totalRanges)
	
	underRepMetric := sm.Metrics().AppendEmpty()
	underRepMetric.SetName("cockroachdb.ranges.under_replicated")  // Already correct
	underRepMetric.SetUnit("{ranges}")
	underRepMetric.SetEmptyGauge()
	dp = underRepMetric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(now)
	dp.SetIntValue(underReplicatedRanges)
	
	unavailMetric := sm.Metrics().AppendEmpty()
	unavailMetric.SetName("cockroachdb.ranges.unavailable")  // Already correct
	unavailMetric.SetUnit("{ranges}")
	unavailMetric.SetEmptyGauge()
	dp = unavailMetric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(now)
	dp.SetIntValue(unavailableRanges)
	
	return nil
}

func (s *cockroachScraper) recordReceiverMetrics(sm pmetric.ScopeMetrics, now pcommon.Timestamp, errorCount int) {
	successMetric := sm.Metrics().AppendEmpty()
	successMetric.SetName("cockroachdb.receiver.scrape_success")  // Already correct
	successMetric.SetEmptyGauge()
	dp := successMetric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(now)
	if errorCount == 0 {
		dp.SetIntValue(1)
	} else {
		dp.SetIntValue(0)
	}
	
	errorMetric := sm.Metrics().AppendEmpty()
	errorMetric.SetName("cockroachdb.receiver.scrape_errors")  // Already correct
	errorMetric.SetEmptyGauge()
	dp = errorMetric.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(now)
	dp.SetIntValue(int64(errorCount))
}
