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
	db, err := sql.Open("postgres", cfg.ConnectionString)
	if err != nil {
		settings.Logger.Error("Failed to open database", zap.Error(err))
		return nil
	}

	// Configure connection pool - these are already time.Duration
	db.SetMaxOpenConns(cfg.MaxOpenConnections)
	db.SetMaxIdleConns(cfg.MaxIdleConnections)
	db.SetConnMaxLifetime(cfg.ConnectionMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnectionMaxIdleTime)

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

	// QueryTimeout is already a time.Duration, use it directly
	ctx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "cockroachdb")
	attrs.PutStr("db.system", "cockroachdb")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/npcomplete777/cockroachdb-receiver")
	sm.Scope().SetVersion("1.0.0")

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

// CRITICAL: This function includes ALL dimensional data including query text
func (s *cockroachScraper) scrapeStatementStatistics(ctx context.Context, sm pmetric.ScopeMetrics) error {
	// Pass QueryLimit parameter
	rows, err := s.client.QueryContext(ctx, queryStatementStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query statement statistics: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			fingerprintID   string
			appName         sql.NullString
			database        sql.NullString
			query           sql.NullString
			stmtType        sql.NullString
			execCount       sql.NullInt64
			svcLatMean      sql.NullFloat64
			parseLatMean    sql.NullFloat64
			planLatMean     sql.NullFloat64
			runLatMean      sql.NullFloat64
			rowsReadMean    sql.NullFloat64
			rowsWrittenMean sql.NullFloat64
			bytesReadMean   sql.NullFloat64
			maxRetries      sql.NullInt64
			errorCount      sql.NullInt64
			lastErrorCode   sql.NullString
		)

		if err := rows.Scan(
			&fingerprintID,
			&appName,
			&database,
			&query,
			&stmtType,
			&execCount,
			&svcLatMean,
			&parseLatMean,
			&planLatMean,
			&runLatMean,
			&rowsReadMean,
			&rowsWrittenMean,
			&bytesReadMean,
			&maxRetries,
			&errorCount,
			&lastErrorCode,
		); err != nil {
			s.logger.Warn("Failed to scan statement statistics row", zap.Error(err))
			continue
		}

		// Helper function to create metrics with ALL dimensional data
		addMetric := func(name, description, unit string, isInt bool, value interface{}) {
			m := sm.Metrics().AppendEmpty()
			m.SetName(name)
			m.SetDescription(description)
			m.SetUnit(unit)
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)

			if isInt {
				if v, ok := value.(sql.NullInt64); ok && v.Valid {
					dp.SetIntValue(v.Int64)
				} else if v, ok := value.(int64); ok {
					dp.SetIntValue(v)
				}
			} else {
				if v, ok := value.(sql.NullFloat64); ok && v.Valid {
					dp.SetDoubleValue(v.Float64)
				} else if v, ok := value.(float64); ok {
					dp.SetDoubleValue(v)
				}
			}

			// Add ALL dimensional data as attributes
			dp.Attributes().PutStr("fingerprint_id", fingerprintID)
			if appName.Valid && appName.String != "" {
				dp.Attributes().PutStr("app_name", appName.String)
			} else {
				dp.Attributes().PutStr("app_name", "cockroachdb-internal")
			}
			if database.Valid {
				dp.Attributes().PutStr("database", database.String)
			}
			if stmtType.Valid {
				dp.Attributes().PutStr("statement_type", stmtType.String)
			}
			// CRITICAL: Add actual query text so you see SQL instead of hex IDs
			if query.Valid {
				dp.Attributes().PutStr("query", query.String)
			}
			if lastErrorCode.Valid && lastErrorCode.String != "" {
				dp.Attributes().PutStr("last_error_code", lastErrorCode.String)
			}
		}

		// Create metrics with full dimensional data
		if execCount.Valid {
			addMetric("cockroachdb.statement.execution.count", "Number of times this statement was executed", "1", true, execCount)
		}
		if errorCount.Valid {
			addMetric("cockroachdb.statement.error.count", "Number of errors for this statement", "1", true, errorCount)
		}
		if maxRetries.Valid {
			addMetric("cockroachdb.statement.retries.max", "Maximum number of retries for this statement", "1", true, maxRetries)
		}
		if svcLatMean.Valid {
			addMetric("cockroachdb.statement.latency.service.mean", "Average service latency", "s", false, svcLatMean)
		}
		if parseLatMean.Valid {
			addMetric("cockroachdb.statement.latency.parse.mean", "Average parsing latency", "s", false, parseLatMean)
		}
		if planLatMean.Valid {
			addMetric("cockroachdb.statement.latency.plan.mean", "Average planning latency", "s", false, planLatMean)
		}
		if runLatMean.Valid {
			addMetric("cockroachdb.statement.latency.run.mean", "Average execution latency", "s", false, runLatMean)
		}
		if rowsReadMean.Valid {
			addMetric("cockroachdb.statement.rows.read.mean", "Average rows read", "1", false, rowsReadMean)
		}
		if rowsWrittenMean.Valid {
			addMetric("cockroachdb.statement.rows.written.mean", "Average rows written", "1", false, rowsWrittenMean)
		}
		if bytesReadMean.Valid {
			addMetric("cockroachdb.statement.bytes.read.mean", "Average bytes read", "By", false, bytesReadMean)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeTransactionStatistics(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryTransactionStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			fingerprintID      sql.NullString
			appName            sql.NullString
			executionCount     sql.NullInt64
			serviceLatencyMean sql.NullFloat64
			commitLatencyMean  sql.NullFloat64
			retryLatencyMean   sql.NullFloat64
			rowsReadMean       sql.NullFloat64
			rowsWrittenMean    sql.NullFloat64
			bytesReadMean      sql.NullFloat64
			maxRetries         sql.NullInt64
		)

		if err := rows.Scan(
			&fingerprintID,
			&appName,
			&executionCount,
			&serviceLatencyMean,
			&commitLatencyMean,
			&retryLatencyMean,
			&rowsReadMean,
			&rowsWrittenMean,
			&bytesReadMean,
			&maxRetries,
		); err != nil {
			s.logger.Warn("Failed to scan transaction statistics row", zap.Error(err))
			continue
		}

		addMetric := func(name, description, unit string, isInt bool, value interface{}) {
			m := sm.Metrics().AppendEmpty()
			m.SetName(name)
			m.SetDescription(description)
			m.SetUnit(unit)
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)

			if isInt {
				if v, ok := value.(sql.NullInt64); ok && v.Valid {
					dp.SetIntValue(v.Int64)
				}
			} else {
				if v, ok := value.(sql.NullFloat64); ok && v.Valid {
					dp.SetDoubleValue(v.Float64)
				}
			}

			if fingerprintID.Valid {
				dp.Attributes().PutStr("fingerprint_id", fingerprintID.String)
			}
			if appName.Valid && appName.String != "" {
				dp.Attributes().PutStr("app_name", appName.String)
			} else {
				dp.Attributes().PutStr("app_name", "cockroachdb-internal")
			}
		}

		if executionCount.Valid {
			addMetric("cockroachdb.transaction.execution.count", "Transaction execution count", "1", true, executionCount)
		}
		if serviceLatencyMean.Valid {
			addMetric("cockroachdb.transaction.latency.service", "Average transaction service latency", "s", false, serviceLatencyMean)
		}
		if commitLatencyMean.Valid {
			addMetric("cockroachdb.transaction.latency.commit", "Average commit latency", "s", false, commitLatencyMean)
		}
		if retryLatencyMean.Valid {
			addMetric("cockroachdb.transaction.latency.retry", "Average retry latency", "s", false, retryLatencyMean)
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeIndexUsage(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryIndexUsageStatistics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			tableName              sql.NullString
			indexName              sql.NullString
			totalReads             sql.NullInt64
			secondsSinceLastRead   sql.NullFloat64
		)

		if err := rows.Scan(&tableName, &indexName, &totalReads, &secondsSinceLastRead); err != nil {
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

func (s *cockroachScraper) scrapeClusterQueries(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterQueries, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			queryID         sql.NullString
			nodeID          sql.NullInt64
			userName        sql.NullString
			appName         sql.NullString
			durationSeconds sql.NullFloat64
			query           sql.NullString
		)

		if err := rows.Scan(&queryID, &nodeID, &userName, &appName, &durationSeconds, &query); err != nil {
			s.logger.Warn("Failed to scan cluster queries row", zap.Error(err))
			continue
		}
		count++
	}

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
	rows, err := s.client.QueryContext(ctx, queryClusterSessions, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			sessionID       sql.NullString
			nodeID          sql.NullInt64
			userName        sql.NullString
			appName         sql.NullString
			allocBytes      sql.NullInt64
			sessionAgeSeconds sql.NullFloat64
		)

		if err := rows.Scan(&sessionID, &nodeID, &userName, &appName, &allocBytes, &sessionAgeSeconds); err != nil {
			s.logger.Warn("Failed to scan cluster sessions row", zap.Error(err))
			continue
		}
		count++

		if allocBytes.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.session.memory.allocated")
			m.SetDescription("Memory allocated by session")
			m.SetUnit("By")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(allocBytes.Int64)
			if sessionID.Valid {
				dp.Attributes().PutStr("session_id", sessionID.String)
			}
			if userName.Valid {
				dp.Attributes().PutStr("user", userName.String)
			}
			if appName.Valid && appName.String != "" {
				dp.Attributes().PutStr("app_name", appName.String)
			}
		}
	}

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
	rows, err := s.client.QueryContext(ctx, queryClusterTransactions, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			txnID           sql.NullString
			nodeID          sql.NullInt64
			appName         sql.NullString
			durationSeconds sql.NullFloat64
		)

		if err := rows.Scan(&txnID, &nodeID, &appName, &durationSeconds); err != nil {
			s.logger.Warn("Failed to scan cluster transactions row", zap.Error(err))
			continue
		}
		count++
	}

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

func (s *cockroachScraper) scrapeClusterLocks(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterLocks, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName      sql.NullString
			tableName         sql.NullString
			lockStrength      sql.NullString
			granted           sql.NullBool
			lockCount         sql.NullInt64
			maxDurationSeconds sql.NullFloat64
		)

		if err := rows.Scan(&databaseName, &tableName, &lockStrength, &granted, &lockCount, &maxDurationSeconds); err != nil {
			s.logger.Warn("Failed to scan cluster locks row", zap.Error(err))
			continue
		}

		if lockCount.Valid {
			m := sm.Metrics().AppendEmpty()
			m.SetName("cockroachdb.cluster.locks.count")
			m.SetDescription("Number of locks")
			m.SetUnit("1")
			g := m.SetEmptyGauge()
			dp := g.DataPoints().AppendEmpty()
			dp.SetTimestamp(timestamp)
			dp.SetIntValue(lockCount.Int64)
			if databaseName.Valid {
				dp.Attributes().PutStr("database", databaseName.String)
			}
			if tableName.Valid {
				dp.Attributes().PutStr("table", tableName.String)
			}
			if lockStrength.Valid {
				dp.Attributes().PutStr("lock_strength", lockStrength.String)
			}
			if granted.Valid {
				dp.Attributes().PutBool("granted", granted.Bool)
			}
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeContendedIndexes(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterContendedIndexes, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        sql.NullString
			schemaName          sql.NullString
			tableName           sql.NullString
			indexName           sql.NullString
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
			if databaseName.Valid {
				dp.Attributes().PutStr("database", databaseName.String)
			}
			if schemaName.Valid {
				dp.Attributes().PutStr("schema", schemaName.String)
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

func (s *cockroachScraper) scrapeContendedKeys(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterContendedKeys, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        sql.NullString
			schemaName          sql.NullString
			tableName           sql.NullString
			indexName           sql.NullString
			numContentionEvents sql.NullInt64
		)

		if err := rows.Scan(&databaseName, &schemaName, &tableName, &indexName, &numContentionEvents); err != nil {
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
			if databaseName.Valid {
				dp.Attributes().PutStr("database", databaseName.String)
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

func (s *cockroachScraper) scrapeContendedTables(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterContendedTables, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var (
			databaseName        sql.NullString
			schemaName          sql.NullString
			tableName           sql.NullString
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
			if databaseName.Valid {
				dp.Attributes().PutStr("database", databaseName.String)
			}
			if schemaName.Valid {
				dp.Attributes().PutStr("schema", schemaName.String)
			}
			if tableName.Valid {
				dp.Attributes().PutStr("table", tableName.String)
			}
		}
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeContentionEvents(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryClusterContentionEvents, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	totalEvents := 0

	for rows.Next() {
		var (
			tableID                     sql.NullInt64
			indexID                     sql.NullInt64
			numContentionEvents         sql.NullInt64
			cumulativeContentionSeconds sql.NullFloat64
		)

		if err := rows.Scan(&tableID, &indexID, &numContentionEvents, &cumulativeContentionSeconds); err != nil {
			s.logger.Warn("Failed to scan contention events row", zap.Error(err))
			continue
		}

		if numContentionEvents.Valid {
			totalEvents += int(numContentionEvents.Int64)
		}
	}

	m := sm.Metrics().AppendEmpty()
	m.SetName("cockroachdb.contention.events.total")
	m.SetDescription("Total contention events")
	m.SetUnit("1")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetIntValue(int64(totalEvents))

	return rows.Err()
}

func (s *cockroachScraper) scrapeTransactionContentionEvents(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryTransactionContentionEvents, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var (
			databaseName            sql.NullString
			tableName               sql.NullString
			contentionType          sql.NullString
			contentionDurationSeconds sql.NullFloat64
		)

		if err := rows.Scan(&databaseName, &tableName, &contentionType, &contentionDurationSeconds); err != nil {
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

func (s *cockroachScraper) scrapeRanges(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryRangesNoLeases)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var totalRanges, underReplicated, unavailable sql.NullInt64

	if rows.Next() {
		if err := rows.Scan(&totalRanges, &underReplicated, &unavailable); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	if totalRanges.Valid {
		m := sm.Metrics().AppendEmpty()
		m.SetName("cockroachdb.ranges.total")
		m.SetDescription("Total number of ranges")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(totalRanges.Int64)
	}

	if underReplicated.Valid {
		m := sm.Metrics().AppendEmpty()
		m.SetName("cockroachdb.ranges.under_replicated")
		m.SetDescription("Under-replicated ranges")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(underReplicated.Int64)
	}

	if unavailable.Valid {
		m := sm.Metrics().AppendEmpty()
		m.SetName("cockroachdb.ranges.unavailable")
		m.SetDescription("Unavailable ranges")
		m.SetUnit("1")
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(unavailable.Int64)
	}

	return rows.Err()
}

func (s *cockroachScraper) scrapeGossipLiveness(ctx context.Context, sm pmetric.ScopeMetrics) error {
	rows, err := s.client.QueryContext(ctx, queryGossipLiveness)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	liveNodes := 0
	for rows.Next() {
		var nodeID, isLive sql.NullInt64
		if err := rows.Scan(&nodeID, &isLive); err != nil {
			s.logger.Warn("Failed to scan gossip liveness row", zap.Error(err))
			continue
		}
		if isLive.Valid && isLive.Int64 == 1 {
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
	rows, err := s.client.QueryContext(ctx, queryJobs, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	jobsByStatus := make(map[string]int)

	for rows.Next() {
		var (
			jobID             sql.NullInt64
			jobType           sql.NullString
			status            sql.NullString
			runningStatus     sql.NullString
			fractionCompleted sql.NullFloat64
		)

		if err := rows.Scan(&jobID, &jobType, &status, &runningStatus, &fractionCompleted); err != nil {
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
	rows, err := s.client.QueryContext(ctx, querySchemaChanges, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var tableName, changeType, state sql.NullString
		if err := rows.Scan(&tableName, &changeType, &state); err != nil {
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
	rows, err := s.client.QueryContext(ctx, queryNodeMetrics, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for rows.Next() {
		var nodeID, storeID sql.NullInt64
		var name sql.NullString
		var value sql.NullFloat64

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
	rows, err := s.client.QueryContext(ctx, queryKVNodeStatus, s.config.QueryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var nodeID sql.NullInt64
		var p50, p99 sql.NullFloat64
		if err := rows.Scan(&nodeID, &p50, &p99); err != nil {
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
