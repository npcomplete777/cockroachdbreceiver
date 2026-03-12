// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cockroachreceiver // import "github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver for CockroachDB
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/npcomplete777/cockroachdb-receiver/cockroachreceiver/internal/metadata"
)

type cockroachScraper struct {
	config *Config
	db     *sql.DB
	logger *zap.Logger
	mb     *metadata.MetricsBuilder
	mu     sync.Mutex
}

func newCockroachScraper(settings receiver.Settings, config *Config) *cockroachScraper {
	return &cockroachScraper{
		config: config,
		logger: settings.Logger,
		mb:     metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
	}
}

func (s *cockroachScraper) Start(ctx context.Context, _ component.Host) error {
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

	s.logger.Info("Successfully connected to CockroachDB")
	return nil
}

func (s *cockroachScraper) Shutdown(_ context.Context) error {
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
		return pmetric.NewMetrics(), fmt.Errorf("database connection not initialized")
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	var errs []error

	if err := s.scrapeStatementStats(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("statement stats: %w", err))
	}
	if err := s.scrapeTransactionStats(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("transaction stats: %w", err))
	}
	if err := s.scrapeIndexUsage(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("index usage: %w", err))
	}
	if err := s.scrapeClusterQueries(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("cluster queries: %w", err))
	}
	if err := s.scrapeClusterSessions(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("cluster sessions: %w", err))
	}
	if err := s.scrapeClusterTransactions(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("cluster transactions: %w", err))
	}
	if err := s.scrapeContentionMetrics(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("contention metrics: %w", err))
	}
	if err := s.scrapeJobs(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("jobs: %w", err))
	}
	if err := s.scrapeSchemaChanges(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("schema changes: %w", err))
	}
	if err := s.scrapeRangeMetrics(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("range metrics: %w", err))
	}
	if err := s.scrapeNodeMetrics(ctx, db, now); err != nil {
		errs = append(errs, fmt.Errorf("node metrics: %w", err))
	}

	rb := s.mb.NewResourceBuilder()
	rb.SetDbSystemNameCockroachdb()

	if len(errs) > 0 {
		return s.mb.Emit(metadata.WithResource(rb.Emit())), fmt.Errorf("scrape errors: %v", errs)
	}
	return s.mb.Emit(metadata.WithResource(rb.Emit())), nil
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

func (s *cockroachScraper) nullStr(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}

func (s *cockroachScraper) scrapeStatementStats(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryStatementStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query statement statistics: %w", err)
	}
	defer rows.Close()

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

		fp := s.nullStr(fingerprintID)
		app := s.nullStr(appName)
		db := s.nullStr(database)
		q := s.truncateQuery(s.nullStr(query))
		st := s.nullStr(stmtType)

		if execCount.Valid {
			s.mb.RecordCockroachdbStatementExecutionCountDataPoint(now, execCount.Int64, fp, app, db, q, st)
		}
		if svcLatMean.Valid {
			s.mb.RecordCockroachdbStatementLatencyServiceMeanDataPoint(now, svcLatMean.Float64, fp, app, q)
		}
		if parseLatMean.Valid {
			s.mb.RecordCockroachdbStatementLatencyParseMeanDataPoint(now, parseLatMean.Float64, fp, app, q)
		}
		if planLatMean.Valid {
			s.mb.RecordCockroachdbStatementLatencyPlanMeanDataPoint(now, planLatMean.Float64, fp, app, q)
		}
		if runLatMean.Valid {
			s.mb.RecordCockroachdbStatementLatencyRunMeanDataPoint(now, runLatMean.Float64, fp, app, q)
		}
		if rowsReadMean.Valid {
			s.mb.RecordCockroachdbStatementRowsReadMeanDataPoint(now, rowsReadMean.Float64, fp, app, q)
		}
		if rowsWrittenMean.Valid {
			s.mb.RecordCockroachdbStatementRowsWrittenMeanDataPoint(now, rowsWrittenMean.Float64, fp, app, q)
		}
		if bytesReadMean.Valid {
			s.mb.RecordCockroachdbStatementBytesReadMeanDataPoint(now, bytesReadMean.Float64, fp, app, q)
		}
		if maxRetries.Valid {
			s.mb.RecordCockroachdbStatementRetriesMaxDataPoint(now, maxRetries.Int64, fp, app, q)
		}
		if errorCount.Valid {
			s.mb.RecordCockroachdbStatementErrorCountDataPoint(now, errorCount.Int64, fp, app, q, s.nullStr(lastErrorCode))
		}
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeTransactionStats(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryTransactionStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query transaction statistics: %w", err)
	}
	defer rows.Close()

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

		fp := s.nullStr(fingerprintID)
		app := s.nullStr(appName)

		if execCount.Valid {
			s.mb.RecordCockroachdbTransactionExecutionCountDataPoint(now, execCount.Int64, fp, app)
		}
		if svcLatMean.Valid {
			s.mb.RecordCockroachdbTransactionLatencyServiceMeanDataPoint(now, svcLatMean.Float64, fp, app)
		}
		if commitLatMean.Valid {
			s.mb.RecordCockroachdbTransactionLatencyCommitMeanDataPoint(now, commitLatMean.Float64, fp, app)
		}
		if retryLatMean.Valid {
			s.mb.RecordCockroachdbTransactionLatencyRetryMeanDataPoint(now, retryLatMean.Float64, fp, app)
		}
		if rowsReadMean.Valid {
			s.mb.RecordCockroachdbTransactionRowsReadMeanDataPoint(now, rowsReadMean.Float64, fp, app)
		}
		if rowsWrittenMean.Valid {
			s.mb.RecordCockroachdbTransactionRowsWrittenMeanDataPoint(now, rowsWrittenMean.Float64, fp, app)
		}
		if bytesReadMean.Valid {
			s.mb.RecordCockroachdbTransactionBytesReadMeanDataPoint(now, bytesReadMean.Float64, fp, app)
		}
		if maxRetries.Valid {
			s.mb.RecordCockroachdbTransactionRetriesMaxDataPoint(now, maxRetries.Int64, fp, app)
		}
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeIndexUsage(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryIndexUsageStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query index usage: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, indexName string
		var totalReads int64
		var secondsSinceLastRead sql.NullFloat64

		err := rows.Scan(&tableName, &indexName, &totalReads, &secondsSinceLastRead)
		if err != nil {
			s.logger.Error("Failed to scan index usage row", zap.Error(err))
			continue
		}

		s.mb.RecordCockroachdbIndexReadsTotalDataPoint(now, totalReads, tableName, indexName)
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeClusterQueries(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	var count int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM crdb_internal.cluster_queries").Scan(&count)
	if err != nil {
		return fmt.Errorf("query cluster queries count: %w", err)
	}

	s.mb.RecordCockroachdbClusterQueriesActiveDataPoint(now, count)
	return nil
}

func (s *cockroachScraper) scrapeClusterSessions(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryClusterSessions, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query cluster sessions: %w", err)
	}
	defer rows.Close()

	var sessionCount int64
	for rows.Next() {
		var sessionID, userName, appName sql.NullString
		var nodeID, allocBytes sql.NullInt64
		var sessionAge sql.NullFloat64

		err := rows.Scan(&sessionID, &nodeID, &userName, &appName, &allocBytes, &sessionAge)
		if err != nil {
			s.logger.Error("Failed to scan cluster sessions row", zap.Error(err))
			continue
		}

		sessionCount++

		if allocBytes.Valid {
			nodeStr := ""
			if nodeID.Valid {
				nodeStr = fmt.Sprintf("%d", nodeID.Int64)
			}
			s.mb.RecordCockroachdbSessionMemoryAllocatedDataPoint(now, allocBytes.Int64, nodeStr, s.nullStr(appName))
		}
	}

	s.mb.RecordCockroachdbClusterSessionsActiveDataPoint(now, sessionCount)
	return rows.Err()
}

func (s *cockroachScraper) scrapeClusterTransactions(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	var count int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM crdb_internal.cluster_transactions").Scan(&count)
	if err != nil {
		return fmt.Errorf("query cluster transactions count: %w", err)
	}

	s.mb.RecordCockroachdbClusterTransactionsActiveDataPoint(now, count)
	return nil
}

func (s *cockroachScraper) scrapeContentionMetrics(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	if err := s.scrapeContendedIndexes(ctx, db, now); err != nil {
		s.logger.Warn("Failed to scrape contended indexes", zap.Error(err))
	}

	if err := s.scrapeContendedTables(ctx, db, now); err != nil {
		s.logger.Warn("Failed to scrape contended tables", zap.Error(err))
	}

	if err := s.scrapeContentionEvents(ctx, db, now); err != nil {
		s.logger.Warn("Failed to scrape contention events", zap.Error(err))
	}

	return nil
}

func (s *cockroachScraper) scrapeContendedIndexes(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryClusterContendedIndexes, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, schemaName, tableName, indexName sql.NullString
		var numEvents sql.NullInt64

		err := rows.Scan(&dbName, &schemaName, &tableName, &indexName, &numEvents)
		if err != nil {
			continue
		}

		if numEvents.Valid {
			s.mb.RecordCockroachdbIndexContentionEventsDataPoint(now, numEvents.Int64,
				s.nullStr(dbName), s.nullStr(schemaName), s.nullStr(tableName), s.nullStr(indexName))
		}
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedTables(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryClusterContendedTables, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, schemaName, tableName sql.NullString
		var numEvents sql.NullInt64

		err := rows.Scan(&dbName, &schemaName, &tableName, &numEvents)
		if err != nil {
			continue
		}

		if numEvents.Valid {
			s.mb.RecordCockroachdbTableContentionEventsDataPoint(now, numEvents.Int64,
				s.nullStr(dbName), s.nullStr(schemaName), s.nullStr(tableName))
		}
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeContentionEvents(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryClusterContentionEvents, s.config.QueryLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	var totalEvents int64
	for rows.Next() {
		var tableID, indexID, numEvents sql.NullInt64
		var cumulativeTime sql.NullFloat64

		err := rows.Scan(&tableID, &indexID, &numEvents, &cumulativeTime)
		if err != nil {
			continue
		}

		if numEvents.Valid {
			totalEvents += numEvents.Int64
		}
	}

	s.mb.RecordCockroachdbContentionEventsTotalDataPoint(now, totalEvents)
	return rows.Err()
}

func (s *cockroachScraper) scrapeJobs(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryJobs, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	// Aggregate jobs by type and status.
	type jobKey struct {
		jobType string
		status  string
	}
	counts := make(map[jobKey]int64)

	for rows.Next() {
		var jobID int64
		var jobType, status, runningStatus sql.NullString
		var fractionCompleted sql.NullFloat64

		err := rows.Scan(&jobID, &jobType, &status, &runningStatus, &fractionCompleted)
		if err != nil {
			s.logger.Error("Failed to scan job row", zap.Error(err))
			continue
		}

		key := jobKey{
			jobType: s.nullStr(jobType),
			status:  s.nullStr(status),
		}
		counts[key]++
	}

	for key, count := range counts {
		s.mb.RecordCockroachdbJobsCountDataPoint(now, count, key.jobType, key.status)
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeSchemaChanges(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, querySchemaChanges, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query schema changes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, changeType, state string
		err := rows.Scan(&tableName, &changeType, &state)
		if err != nil {
			s.logger.Error("Failed to scan schema change row", zap.Error(err))
			continue
		}

		s.mb.RecordCockroachdbSchemaChangesActiveDataPoint(now, 1, tableName, changeType, state)
	}
	return rows.Err()
}

func (s *cockroachScraper) scrapeNodeMetrics(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	rows, err := db.QueryContext(ctx, queryGossipLiveness)
	if err != nil {
		return fmt.Errorf("query node liveness: %w", err)
	}
	defer rows.Close()

	var liveCount, totalCount int64
	for rows.Next() {
		var nodeID sql.NullInt64
		var isLive sql.NullInt64

		err := rows.Scan(&nodeID, &isLive)
		if err != nil {
			continue
		}

		totalCount++
		if isLive.Valid && isLive.Int64 == 1 {
			liveCount++
		}
	}

	s.mb.RecordCockroachdbNodesLiveDataPoint(now, liveCount)
	s.mb.RecordCockroachdbNodesTotalDataPoint(now, totalCount)
	return rows.Err()
}

func (s *cockroachScraper) scrapeRangeMetrics(ctx context.Context, db *sql.DB, now pcommon.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	row := db.QueryRowContext(ctx, queryRangesNoLeases)

	var totalRanges, underReplicatedRanges, unavailableRanges int64
	err := row.Scan(&totalRanges, &underReplicatedRanges, &unavailableRanges)
	if err != nil {
		return fmt.Errorf("query range metrics: %w", err)
	}

	s.mb.RecordCockroachdbRangesTotalDataPoint(now, totalRanges)
	s.mb.RecordCockroachdbRangesUnderReplicatedDataPoint(now, underReplicatedRanges)
	s.mb.RecordCockroachdbRangesUnavailableDataPoint(now, unavailableRanges)
	return nil
}
