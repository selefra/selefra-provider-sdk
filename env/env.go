package env

import (
	"fmt"
	"os"
)

const DatabaseDsn = "SELEFRA_DATABASE_DSN"

// GetDatabaseDsn read database dsn from environment for test
func GetDatabaseDsn() string {
	dsn := os.Getenv(DatabaseDsn)
	if dsn == "" {
		panic(fmt.Sprintf("test helper need env: %s, but not found it in environment", DatabaseDsn))
	}
	return dsn
}
