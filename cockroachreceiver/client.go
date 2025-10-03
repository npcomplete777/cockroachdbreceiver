package cockroachreceiver

import (
	_ "github.com/lib/pq"
)

// PostgreSQL driver is imported for side effects
// All database connection logic is now in scraper.go
