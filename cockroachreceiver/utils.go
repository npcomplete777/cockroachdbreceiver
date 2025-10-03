package cockroachreceiver

import (
    "net/url"
    "strings"
)

// sanitizeConnectionString removes sensitive information from a PostgreSQL connection string
// Returns a safe string suitable for metrics/logs containing only host, port, and database
func sanitizeConnectionString(connStr string) string {
    // Handle postgresql:// and postgres:// schemes
    if !strings.HasPrefix(connStr, "postgresql://") && !strings.HasPrefix(connStr, "postgres://") {
        // If it's a key=value format or malformed, return a generic placeholder
        return "cockroachdb://[redacted]"
    }
    
    parsed, err := url.Parse(connStr)
    if err != nil {
        return "cockroachdb://[invalid]"
    }
    
    // Build sanitized string with just host, port, and database
    sanitized := "cockroachdb://"
    
    // Add host
    if parsed.Host != "" {
        sanitized += parsed.Host
    } else {
        sanitized += "[unknown]"
    }
    
    // Add database path if present
    if parsed.Path != "" && parsed.Path != "/" {
        sanitized += parsed.Path
    }
    
    return sanitized
}
