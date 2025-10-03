package cockroachreceiver

import (
    "strings"
    "testing"
)

func TestSanitizeConnectionString(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected string
    }{
        {
            name:     "full connection string with password",
            input:    "postgresql://user:password@localhost:26257/mydb",
            expected: "cockroachdb://localhost:26257/mydb",
        },
        {
            name:     "connection string with special chars in password",
            input:    "postgresql://admin:p@ssw0rd!@host.example.com:26257/production",
            expected: "cockroachdb://host.example.com:26257/production",
        },
        {
            name:     "connection string without database",
            input:    "postgresql://user:pass@localhost:26257",
            expected: "cockroachdb://localhost:26257",
        },
        {
            name:     "connection string with query parameters",
            input:    "postgresql://user:pass@localhost:26257/db?sslmode=require",
            expected: "cockroachdb://localhost:26257/db",
        },
        {
            name:     "postgres scheme instead of postgresql",
            input:    "postgres://user:pass@localhost:26257/db",
            expected: "cockroachdb://localhost:26257/db",
        },
        {
            name:     "malformed connection string",
            input:    "invalid://connection",
            expected: "cockroachdb://[redacted]",
        },
        {
            name:     "key-value format",
            input:    "host=localhost port=26257 user=admin password=secret",
            expected: "cockroachdb://[redacted]",
        },
        {
            name:     "empty string",
            input:    "",
            expected: "cockroachdb://[redacted]",
        },
        {
            name:     "connection with IPv6 host",
            input:    "postgresql://user:pass@[::1]:26257/db",
            expected: "cockroachdb://[::1]:26257/db",
        },
        {
            name:     "connection with domain and port",
            input:    "postgresql://user:pass@cockroach.example.com:26257/myapp",
            expected: "cockroachdb://cockroach.example.com:26257/myapp",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := sanitizeConnectionString(tt.input)
            if result != tt.expected {
                t.Errorf("sanitizeConnectionString(%q) = %q, want %q", tt.input, result, tt.expected)
            }
            
            // Verify no credentials are present in output
            if strings.Contains(result, "password") || strings.Contains(result, "pass") {
                t.Errorf("sanitizeConnectionString(%q) contains credentials: %q", tt.input, result)
            }
        })
    }
}

func TestSanitizeConnectionString_NoLeakedCredentials(t *testing.T) {
    // Additional test to ensure common password patterns don't leak
    dangerousInputs := []string{
        "postgresql://admin:MyP@ssw0rd!@localhost:26257/db",
        "postgresql://root:12345@host:26257/db",
        "postgresql://user:secret123@host:26257/db",
    }
    
    for _, input := range dangerousInputs {
        result := sanitizeConnectionString(input)
        
        // Check that result doesn't contain any part of the password
        if strings.Contains(result, "P@ssw0rd") || 
           strings.Contains(result, "12345") || 
           strings.Contains(result, "secret") ||
           strings.Contains(result, "admin") ||
           strings.Contains(result, "root") {
            t.Errorf("Credential leak detected in sanitized string: %q from input %q", result, input)
        }
    }
}
