package cockroachreceiver

import (
    "testing"
    "time"
)

func TestNewCockroachClient_InvalidConnectionString(t *testing.T) {
    cfg := &Config{
        ConnectionString: "invalid://connection",
        QueryTimeout:     30 * time.Second,
        QueryLimit:       20,
        MaxOpenConns:     10,
        MaxIdleConns:     5,
        ConnMaxLifetime:  time.Hour,
        ConnMaxIdleTime:  10 * time.Minute,
    }
    
    // This will fail during Ping, which is expected
    client, err := newCockroachClient(cfg)
    if err == nil {
        if client != nil {
            client.Close()
        }
        t.Error("Expected error with invalid connection string, got nil")
    }
}

func TestCockroachClient_CloseNilDB(t *testing.T) {
    client := &cockroachClient{
        db: nil,
    }
    
    err := client.Close()
    if err != nil {
        t.Errorf("Close() with nil db should not error, got: %v", err)
    }
}

func TestQueryStatsStructure(t *testing.T) {
    stats := QueryStats{
        Query:          "SELECT * FROM users",
        ExecutionCount: 100,
        MeanLatency:    0.025,
    }
    
    if stats.Query != "SELECT * FROM users" {
        t.Errorf("Expected query 'SELECT * FROM users', got %q", stats.Query)
    }
    if stats.ExecutionCount != 100 {
        t.Errorf("Expected execution count 100, got %d", stats.ExecutionCount)
    }
    if stats.MeanLatency != 0.025 {
        t.Errorf("Expected mean latency 0.025, got %f", stats.MeanLatency)
    }
}

func TestIndexUsageStatsStructure(t *testing.T) {
    stats := IndexUsageStats{
        TableName:  "users",
        IndexName:  "idx_email",
        TotalReads: 5000,
    }
    
    if stats.TableName != "users" {
        t.Errorf("Expected table name 'users', got %q", stats.TableName)
    }
    if stats.IndexName != "idx_email" {
        t.Errorf("Expected index name 'idx_email', got %q", stats.IndexName)
    }
    if stats.TotalReads != 5000 {
        t.Errorf("Expected total reads 5000, got %d", stats.TotalReads)
    }
}

func TestContentionStatsStructure(t *testing.T) {
    stats := ContentionStats{
        TableName:      "orders",
        IndexName:      "primary",
        ContentionTime: 1.5,
        NumContention:  25,
    }
    
    if stats.TableName != "orders" {
        t.Errorf("Expected table name 'orders', got %q", stats.TableName)
    }
    if stats.ContentionTime != 1.5 {
        t.Errorf("Expected contention time 1.5, got %f", stats.ContentionTime)
    }
    if stats.NumContention != 25 {
        t.Errorf("Expected num contention 25, got %d", stats.NumContention)
    }
}

func TestRangeHealthStatsStructure(t *testing.T) {
    stats := RangeHealthStats{
        TotalRanges:           1000,
        UnderReplicatedRanges: 10,
        UnavailableRanges:     0,
    }
    
    if stats.TotalRanges != 1000 {
        t.Errorf("Expected total ranges 1000, got %d", stats.TotalRanges)
    }
    if stats.UnderReplicatedRanges != 10 {
        t.Errorf("Expected under-replicated ranges 10, got %d", stats.UnderReplicatedRanges)
    }
    if stats.UnavailableRanges != 0 {
        t.Errorf("Expected unavailable ranges 0, got %d", stats.UnavailableRanges)
    }
}

func TestNodeStatusStructure(t *testing.T) {
    node := NodeStatus{
        NodeID:  1,
        IsLive:  true,
        Address: "node1.cluster.local",
    }
    
    if node.NodeID != 1 {
        t.Errorf("Expected node ID 1, got %d", node.NodeID)
    }
    if !node.IsLive {
        t.Error("Expected node to be live")
    }
    if node.Address != "node1.cluster.local" {
        t.Errorf("Expected address 'node1.cluster.local', got %q", node.Address)
    }
}

func TestJobStatsStructure(t *testing.T) {
    job := JobStats{
        JobID:         12345,
        JobType:       "BACKUP",
        Status:        "running",
        RunningStatus: "backing up",
        Created:       "2025-10-01 12:00:00",
    }
    
    if job.JobID != 12345 {
        t.Errorf("Expected job ID 12345, got %d", job.JobID)
    }
    if job.JobType != "BACKUP" {
        t.Errorf("Expected job type 'BACKUP', got %q", job.JobType)
    }
    if job.Status != "running" {
        t.Errorf("Expected status 'running', got %q", job.Status)
    }
}

func TestChangefeedLagStructure(t *testing.T) {
    lag := ChangefeedLag{
        JobID:      999,
        LagSeconds: 120.5,
    }
    
    if lag.JobID != 999 {
        t.Errorf("Expected job ID 999, got %d", lag.JobID)
    }
    if lag.LagSeconds != 120.5 {
        t.Errorf("Expected lag seconds 120.5, got %f", lag.LagSeconds)
    }
}

func TestSchemaChangeStructure(t *testing.T) {
    change := SchemaChange{
        TableName: "users",
        Type:      "ADD COLUMN",
        State:     "running",
    }
    
    if change.TableName != "users" {
        t.Errorf("Expected table name 'users', got %q", change.TableName)
    }
    if change.Type != "ADD COLUMN" {
        t.Errorf("Expected type 'ADD COLUMN', got %q", change.Type)
    }
    if change.State != "running" {
        t.Errorf("Expected state 'running', got %q", change.State)
    }
}

func TestStatementErrorStructure(t *testing.T) {
    err := StatementError{
        Query:      "INSERT INTO users VALUES ($1)",
        ErrorCode:  "23505",
        ErrorCount: 10,
    }
    
    if err.Query != "INSERT INTO users VALUES ($1)" {
        t.Errorf("Expected query 'INSERT INTO users VALUES ($1)', got %q", err.Query)
    }
    if err.ErrorCode != "23505" {
        t.Errorf("Expected error code '23505', got %q", err.ErrorCode)
    }
    if err.ErrorCount != 10 {
        t.Errorf("Expected error count 10, got %d", err.ErrorCount)
    }
}
