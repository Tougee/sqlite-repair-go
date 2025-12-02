package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tougee/sqlite-repair-go/backup"
	"github.com/tougee/sqlite-repair-go/repair"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "backup":
		handleBackup()
	case "repair":
		handleRepair()
	case "check":
		handleCheck()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  sqlite-repair backup <db_path> <schema_output_path>")
	fmt.Println("  sqlite-repair repair -src <corrupt_db> -schema <schema_path> -out <output_db> [-pagesize 4096]")
	fmt.Println("  sqlite-repair check <db_path>")
}

func handleBackup() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: sqlite-repair backup <db_path> <schema_output_path>")
		os.Exit(1)
	}
	dbPath := os.Args[2]
	schemaPath := os.Args[3]

	if err := backup.ExportSchema(dbPath, schemaPath); err != nil {
		fmt.Printf("Backup failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Schema backup successful.")
}

func handleCheck() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: sqlite-repair check <db_path>")
		os.Exit(1)
	}
	dbPath := os.Args[2]
	if err := repair.CheckIntegrity(dbPath); err != nil {
		fmt.Printf("Integrity check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Integrity check passed.")
}

func handleRepair() {
	repairCmd := flag.NewFlagSet("repair", flag.ExitOnError)
	src := repairCmd.String("src", "", "Path to corrupt database")
	schema := repairCmd.String("schema", "", "Path to backup schema json")
	out := repairCmd.String("out", "", "Path to output recovered database")
	pageSize := repairCmd.Int("pagesize", 4096, "Page size of the database (default 4096)")

	repairCmd.Parse(os.Args[2:])

	if *src == "" || *schema == "" || *out == "" {
		repairCmd.Usage()
		os.Exit(1)
	}

	cfg := repair.RepairConfig{
		CorruptDBPath:    *src,
		BackupSchemaPath: *schema,
		OutputDBPath:     *out,
		PageSize:         *pageSize,
	}

	if err := repair.Run(cfg); err != nil {
		fmt.Printf("Repair failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Repair completed successfully.")
}
