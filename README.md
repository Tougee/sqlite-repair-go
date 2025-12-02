# SQLite Repair Go

This is a Go implementation of the SQLite corruption recovery strategy inspired by [WCDB](https://github.com/Tencent/wcdb)'s Corrupt Recovery mechanism.

## Core Concepts

1.  **Schema Backup**: Proactively backup the `sqlite_master` table when the database is healthy.
2.  **Greedy Page Scanning**: When corruption occurs, instead of traversing the potentially broken B-Tree structure, the tool scans the database file page by page.
3.  **Pattern Matching & Recovery**: It identifies valid data pages (Leaf Table Pages), parses the Cells, and attempts to decode the data by matching it against the backed-up Schema.

## Project Structure

-   `backup/`: Contains logic for backing up database metadata (Schema).
-   `repair/`: Contains the core repair logic (Page scanning, Cell parsing, Schema matching).
-   `cmd/demo/`: A demo application showing the full cycle of corruption and recovery.

## Usage

### 1. Backup Schema

You should call this function during application startup or as a periodic task to ensure you have a recent copy of the table structures.

```go
import "github.com/tougee/sqlite-repair-go/backup"

// ...
err := backup.ExportSchema("path/to/db.sqlite", "path/to/schema_backup.json")
if err != nil {
    // handle error
}
```

### 2. Check Integrity (Optional)

You can check if a database file is corrupt before attempting repair.

```go
import "github.com/tougee/sqlite-repair-go/repair"

// ...
err := repair.CheckIntegrity("path/to/corrupt.sqlite")
if err != nil {
    fmt.Println("Database is corrupt:", err)
}
```

### 3. Execute Repair

When corruption is detected, run the repair process using the corrupted DB file and the backup schema.

```go
import "github.com/tougee/sqlite-repair-go/repair"

// ...
cfg := repair.RepairConfig{
    CorruptDBPath:    "path/to/corrupt.sqlite",
    BackupSchemaPath: "path/to/schema_backup.json",
    OutputDBPath:     "path/to/recovered.sqlite",
    PageSize:         4096, // Usually 4096 for SQLite
}

err := repair.Run(cfg)
if err != nil {
    // handle error
}
```
