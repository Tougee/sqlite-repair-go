package backup

import (
	"database/sql"
	"encoding/json"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type TableSchema struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

func ExportSchema(dbPath string, backupPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query("SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return err
	}
	defer rows.Close()

	var schemas []TableSchema
	for rows.Next() {
		var s TableSchema
		if err := rows.Scan(&s.Name, &s.SQL); err != nil {
			return err
		}
		schemas = append(schemas, s)
	}

	data, err := json.MarshalIndent(schemas, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(backupPath, data, 0644)
}
