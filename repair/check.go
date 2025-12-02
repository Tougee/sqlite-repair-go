package repair

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func CheckIntegrity(dbPath string) error {
	if _, err := os.Stat(dbPath); err != nil {
		return fmt.Errorf("file check failed: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("integrity check failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("failed to scan integrity check result: %w", err)
		}
		if result != "ok" {
			return fmt.Errorf("integrity check returned: %s", result)
		}
	}

	return nil
}
