package cockroachreceiver

import (
    "testing"
    "time"
    
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
)

func TestAddQueryMetrics(t *testing.T) {
    scraper := &cockroachScraper{}
    metrics := pmetric.NewMetrics()
    rm := metrics.ResourceMetrics().AppendEmpty()
    sm := rm.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    queryStats := []QueryStats{
        {
            Query:          "SELECT * FROM users WHERE id = $1",
            ExecutionCount: 100,
            MeanLatency:    0.025,
        },
        {
            Query:          "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
            ExecutionCount: 50,
            MeanLatency:    0.015,
        },
    }
    
    scraper.addQueryMetrics(sm, queryStats, now)
    
    // Should create 2 metrics: execution_count and latency
    if sm.Metrics().Len() != 2 {
        t.Errorf("Expected 2 metrics, got %d", sm.Metrics().Len())
    }
    
    // Check execution count metric
    countMetric := sm.Metrics().At(0)
    if countMetric.Name() != "cockroachdb.query.execution_count" {
        t.Errorf("Expected metric name 'cockroachdb.query.execution_count', got %q", countMetric.Name())
    }
    if countMetric.Gauge().DataPoints().Len() != 2 {
        t.Errorf("Expected 2 data points for execution count, got %d", countMetric.Gauge().DataPoints().Len())
    }
    
    // Check first data point
    dp := countMetric.Gauge().DataPoints().At(0)
    if dp.IntValue() != 100 {
        t.Errorf("Expected execution count 100, got %d", dp.IntValue())
    }
    
    // Check latency metric
    latencyMetric := sm.Metrics().At(1)
    if latencyMetric.Name() != "cockroachdb.query.latency" {
        t.Errorf("Expected metric name 'cockroachdb.query.latency', got %q", latencyMetric.Name())
    }
    if latencyMetric.Gauge().DataPoints().Len() != 2 {
        t.Errorf("Expected 2 data points for latency, got %d", latencyMetric.Gauge().DataPoints().Len())
    }
}

func TestAddLatencyPercentileMetrics(t *testing.T) {
    scraper := &cockroachScraper{}
    metrics := pmetric.NewMetrics()
    rm := metrics.ResourceMetrics().AppendEmpty()
    sm := rm.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    latencyStats := []QueryLatencyStats{
        {
            QueryFingerprint: "SELECT * FROM users WHERE id = _",
            P50Latency:      0.010,
            P95Latency:      0.025,
            P99Latency:      0.050,
            ErrorCount:      5,
        },
    }
    
    scraper.addLatencyPercentileMetrics(sm, latencyStats, now)
    
    // Should create 4 metrics: p50, p95, p99, errors
    if sm.Metrics().Len() != 4 {
        t.Errorf("Expected 4 metrics, got %d", sm.Metrics().Len())
    }
    
    expectedMetrics := []string{
        "cockroachdb.query.latency.p50",
        "cockroachdb.query.latency.p95",
        "cockroachdb.query.latency.p99",
        "cockroachdb.query.errors",
    }
    
    for i, expected := range expectedMetrics {
        metric := sm.Metrics().At(i)
        if metric.Name() != expected {
            t.Errorf("Metric %d: expected name %q, got %q", i, expected, metric.Name())
        }
    }
    
    // Verify p50 value
    p50Metric := sm.Metrics().At(0)
    if p50Metric.Gauge().DataPoints().Len() != 1 {
        t.Errorf("Expected 1 data point for p50, got %d", p50Metric.Gauge().DataPoints().Len())
    }
    if p50Metric.Gauge().DataPoints().At(0).DoubleValue() != 0.010 {
        t.Errorf("Expected p50 latency 0.010, got %f", p50Metric.Gauge().DataPoints().At(0).DoubleValue())
    }
}

func TestAddRangeHealthMetrics(t *testing.T) {
    scraper := &cockroachScraper{}
    metrics := pmetric.NewMetrics()
    rm := metrics.ResourceMetrics().AppendEmpty()
    sm := rm.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    rangeHealth := RangeHealthStats{
        TotalRanges:           1000,
        UnderReplicatedRanges: 10,
        UnavailableRanges:     0,
    }
    
    scraper.addRangeHealthMetrics(sm, rangeHealth, now)
    
    // Should create 3 metrics
    if sm.Metrics().Len() != 3 {
        t.Errorf("Expected 3 metrics, got %d", sm.Metrics().Len())
    }
    
    expectedMetrics := map[string]int64{
        "cockroachdb.ranges.total":            1000,
        "cockroachdb.ranges.under_replicated": 10,
        "cockroachdb.ranges.unavailable":      0,
    }
    
    for i := 0; i < sm.Metrics().Len(); i++ {
        metric := sm.Metrics().At(i)
        expectedValue, ok := expectedMetrics[metric.Name()]
        if !ok {
            t.Errorf("Unexpected metric name: %q", metric.Name())
            continue
        }
        
        if metric.Gauge().DataPoints().Len() != 1 {
            t.Errorf("Metric %q: expected 1 data point, got %d", metric.Name(), metric.Gauge().DataPoints().Len())
            continue
        }
        
        actualValue := metric.Gauge().DataPoints().At(0).IntValue()
        if actualValue != expectedValue {
            t.Errorf("Metric %q: expected value %d, got %d", metric.Name(), expectedValue, actualValue)
        }
    }
}

func TestAddContentionMetrics(t *testing.T) {
    scraper := &cockroachScraper{}
    metrics := pmetric.NewMetrics()
    rm := metrics.ResourceMetrics().AppendEmpty()
    sm := rm.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    contentionStats := []ContentionStats{
        {
            TableName:      "users",
            IndexName:      "primary",
            ContentionTime: 1.5,
            NumContention:  25,
        },
        {
            TableName:      "orders",
            IndexName:      "idx_user_id",
            ContentionTime: 0.8,
            NumContention:  10,
        },
    }
    
    scraper.addContentionMetrics(sm, contentionStats, now)
    
    // Should create 2 metrics: time and events
    if sm.Metrics().Len() != 2 {
        t.Errorf("Expected 2 metrics, got %d", sm.Metrics().Len())
    }
    
    // Check contention time metric
    timeMetric := sm.Metrics().At(0)
    if timeMetric.Name() != "cockroachdb.contention.time" {
        t.Errorf("Expected metric name 'cockroachdb.contention.time', got %q", timeMetric.Name())
    }
    if timeMetric.Gauge().DataPoints().Len() != 2 {
        t.Errorf("Expected 2 data points, got %d", timeMetric.Gauge().DataPoints().Len())
    }
    
    // Verify attributes are set
    dp := timeMetric.Gauge().DataPoints().At(0)
    tableAttr, exists := dp.Attributes().Get("table")
    if !exists {
        t.Error("Expected 'table' attribute to exist")
    }
    if tableAttr.Str() != "users" {
        t.Errorf("Expected table 'users', got %q", tableAttr.Str())
    }
}

func TestAddNodeStatusMetrics(t *testing.T) {
    scraper := &cockroachScraper{}
    metrics := pmetric.NewMetrics()
    rm := metrics.ResourceMetrics().AppendEmpty()
    sm := rm.ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    nodeStatus := []NodeStatus{
        {NodeID: 1, IsLive: true, Address: "n1"},
        {NodeID: 2, IsLive: true, Address: "n2"},
        {NodeID: 3, IsLive: false, Address: "n3"},
    }
    
    scraper.addNodeStatusMetrics(sm, nodeStatus, now)
    
    if sm.Metrics().Len() != 1 {
        t.Errorf("Expected 1 metric, got %d", sm.Metrics().Len())
    }
    
    metric := sm.Metrics().At(0)
    if metric.Name() != "cockroachdb.node.live" {
        t.Errorf("Expected metric name 'cockroachdb.node.live', got %q", metric.Name())
    }
    
    if metric.Gauge().DataPoints().Len() != 3 {
        t.Errorf("Expected 3 data points, got %d", metric.Gauge().DataPoints().Len())
    }
    
    // Check first node (live)
    dp1 := metric.Gauge().DataPoints().At(0)
    if dp1.IntValue() != 1 {
        t.Errorf("Expected live node to have value 1, got %d", dp1.IntValue())
    }
    
    // Check third node (dead)
    dp3 := metric.Gauge().DataPoints().At(2)
    if dp3.IntValue() != 0 {
        t.Errorf("Expected dead node to have value 0, got %d", dp3.IntValue())
    }
}

func TestMinFunction(t *testing.T) {
    tests := []struct {
        name     string
        a        int
        b        int
        expected int
    }{
        {"a smaller", 5, 10, 5},
        {"b smaller", 10, 5, 5},
        {"equal", 7, 7, 7},
        {"negative numbers", -5, -10, -10},
        {"zero and positive", 0, 5, 0},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := min(tt.a, tt.b)
            if result != tt.expected {
                t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
            }
        })
    }
}
