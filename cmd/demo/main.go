package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tougee/sqlite-repair-go/backup"
	"github.com/tougee/sqlite-repair-go/repair"
)

func main() {
	originalDB := "original.db"
	backupPath := "schema.json"

	// Test 1 files
	corruptDBHeader := "corrupt_header.db"
	recoveredDBHeader := "recovered_header.db"

	// Test 2 files
	corruptDBMiddle := "corrupt_middle.db"
	recoveredDBMiddle := "recovered_middle.db"

	// Cleanup previous run
	os.Remove(originalDB)
	os.Remove(backupPath)
	os.Remove(corruptDBHeader)
	os.Remove(recoveredDBHeader)
	os.Remove(corruptDBMiddle)
	os.Remove(recoveredDBMiddle)

	// 1. Create and populate original DB
	log.Println("Step 1: Creating original database with more data...")
	createOriginalDB(originalDB)

	// 2. Backup Schema
	log.Println("Step 2: Backing up schema...")
	if err := backup.ExportSchema(originalDB, backupPath); err != nil {
		log.Fatalf("Backup failed: %v", err)
	}

	// --- Test Case 1: Header Corruption ---
	log.Println("\n=== Test Case 1: Header Corruption (Page 1 Lost) ===")

	log.Println("Step 3.1: Creating header-corrupted database...")
	if err := createCorruptDBHeader(originalDB, corruptDBHeader); err != nil {
		log.Fatalf("Corruption failed: %v", err)
	}

	log.Println("Step 3.2: Checking integrity...")
	if err := repair.CheckIntegrity(corruptDBHeader); err != nil {
		log.Printf("Confirmed: Database is corrupt: %v", err)
	}

	log.Println("Step 3.3: Repairing...")
	cfgHeader := repair.RepairConfig{
		CorruptDBPath:    corruptDBHeader,
		BackupSchemaPath: backupPath,
		OutputDBPath:     recoveredDBHeader,
		PageSize:         4096,
	}
	if err := repair.Run(cfgHeader); err != nil {
		log.Fatalf("Repair failed: %v", err)
	}

	log.Println("Step 3.4: Verifying result...")
	verifyDB(recoveredDBHeader)

	// --- Test Case 2: Middle Corruption ---
	log.Println("\n=== Test Case 2: Middle Corruption (Page 5 Lost) ===")

	log.Println("Step 4.1: Creating middle-corrupted database...")
	if err := createCorruptDBMiddle(originalDB, corruptDBMiddle); err != nil {
		log.Fatalf("Corruption failed: %v", err)
	}

	log.Println("Step 4.2: Checking integrity...")
	if err := repair.CheckIntegrity(corruptDBMiddle); err != nil {
		log.Printf("Confirmed: Database is corrupt: %v", err)
	}

	log.Println("Step 4.3: Repairing...")
	cfgMiddle := repair.RepairConfig{
		CorruptDBPath:    corruptDBMiddle,
		BackupSchemaPath: backupPath,
		OutputDBPath:     recoveredDBMiddle,
		PageSize:         4096,
	}
	if err := repair.Run(cfgMiddle); err != nil {
		log.Fatalf("Repair failed: %v", err)
	}

	log.Println("Step 4.4: Verifying result...")
	verifyDB(recoveredDBMiddle)
}

func createOriginalDB(path string) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			age INTEGER,
			bio BLOB
		);
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert enough data to ensure we use multiple pages.
	// With 1KB bio, each row is > 1KB. 4KB page can hold ~3 rows.
	// 50 rows will take ~17 pages.
	stmt, _ := db.Prepare("INSERT INTO users(name, age, bio) VALUES(?, ?, ?)")
	defer stmt.Close()

	for i := 1; i <= 50; i++ {
		name := fmt.Sprintf("User%d", i)
		bio := make([]byte, 1000) // 1KB blob
		// Fill bio with some data
		copy(bio, []byte(fmt.Sprintf("Bio data for user %d...", i)))

		_, err = stmt.Exec(name, 20+(i%50), bio)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func createCorruptDBHeader(src, dst string) error {
	return copyAndWipe(src, dst, 0) // Wipe Page 1 (Offset 0)
}

func createCorruptDBMiddle(src, dst string) error {
	// Wipe Page 5 (Offset 4096 * 4)
	// This should destroy some data rows, but not all.
	return copyAndWipe(src, dst, 4096*4)
}

func copyAndWipe(src, dst string, offset int64) error {
	// Copy file
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	if _, err = io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	out.Close()

	// Corrupt
	f, err := os.OpenFile(dst, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	zeros := make([]byte, 4096)
	if _, err := f.WriteAt(zeros, offset); err != nil {
		return err
	}
	return nil
}

func verifyDB(path string) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		log.Printf("Query failed: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		var age int
		if err := rows.Scan(&id, &name, &age); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		log.Printf("Recovered Row: ID=%d, Name=%s, Age=%d", id, name, age)
		count++
	}
	log.Printf("Total recovered rows: %d", count)
}
