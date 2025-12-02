package repair

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tougee/sqlite-repair-go/backup"
)

type RepairConfig struct {
	CorruptDBPath    string
	BackupSchemaPath string
	OutputDBPath     string
	PageSize         int // Usually 4096 for SQLite
}

type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

type ColumnInfo struct {
	Name string
	Type string
	Pk   int // 1 if PK
}

func Run(cfg RepairConfig) error {
	schemas, err := loadSchemas(cfg.BackupSchemaPath)
	if err != nil {
		return fmt.Errorf("failed to load schema: %v", err)
	}

	outDB, err := initOutputDB(cfg.OutputDBPath, schemas)
	if err != nil {
		return fmt.Errorf("failed to init output db: %v", err)
	}
	defer outDB.Close()

	var tables []TableInfo
	for _, s := range schemas {
		cols, err := getTableInfo(outDB, s.Name)
		if err != nil {
			return fmt.Errorf("failed to get table info for %s: %v", s.Name, err)
		}
		tables = append(tables, TableInfo{Name: s.Name, Columns: cols})
	}

	f, err := os.Open(cfg.CorruptDBPath)
	if err != nil {
		return fmt.Errorf("failed to open corrupt db: %v", err)
	}
	defer f.Close()

	buffer := make([]byte, cfg.PageSize)
	pageIndex := 0
	for {
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if n != cfg.PageSize {
			// incomplete page, stop processing
			break
		}

		err = processPage(buffer, pageIndex, tables, outDB)
		if err != nil {
			fmt.Printf("Page %d error: %v\n", pageIndex, err)
		}
		pageIndex++
	}

	return nil
}

func loadSchemas(path string) ([]backup.TableSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var schemas []backup.TableSchema
	err = json.Unmarshal(data, &schemas)
	return schemas, err
}

func initOutputDB(path string, schemas []backup.TableSchema) (*sql.DB, error) {
	os.Remove(path)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	for _, s := range schemas {
		_, err := db.Exec(s.SQL)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("create table %s failed: %v", s.Name, err)
		}
	}
	return db, nil
}

func getTableInfo(db *sql.DB, tableName string) ([]ColumnInfo, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt_value interface{}
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, ColumnInfo{Name: name, Type: ctype, Pk: pk})
	}
	return cols, nil
}

func processPage(pageData []byte, pageIndex int, tables []TableInfo, outDB *sql.DB) error {
	// SQLite B-Tree Leaf Table Page signature is 0x0D
	headerOffset := 0
	if pageIndex == 0 {
		headerOffset = 100
	}

	if headerOffset >= len(pageData) {
		return nil
	}

	flag := pageData[headerOffset]
	if flag != 0x0D {
		return nil // not a leaf table page
	}

	// parse header
	// 0: flag (1 byte)
	// 1: free block offset (2 bytes)
	// 3: number of cells (2 bytes)
	// 5: start of cell content area (2 bytes)
	// 7: fragmented free bytes (1 byte)
	numCells := binary.BigEndian.Uint16(pageData[headerOffset+3 : headerOffset+5])

	// traverse cells
	// Cell pointers start at headerOffset + 8
	for i := 0; i < int(numCells); i++ {
		ptrOffset := headerOffset + 8 + i*2
		if ptrOffset+2 > len(pageData) {
			break
		}
		cellOffset := binary.BigEndian.Uint16(pageData[ptrOffset : ptrOffset+2])

		if int(cellOffset) >= len(pageData) {
			continue
		}

		// parse cell
		// Cell format:
		// - Payload Size (Varint)
		// - Row ID (Varint)
		// - Payload (Record)
		cell := pageData[cellOffset:]
		payloadSize, n := readVarint(cell)
		if n == 0 {
			continue
		}
		rowID, n2 := readVarint(cell[n:])
		if n2 == 0 {
			continue
		}

		// check payload is complete within page (no overflow handling for now)
		totalHeaderSize := n + n2
		if int(payloadSize) > len(cell)-totalHeaderSize {
			// overflow page, skip for now
			continue
		}

		payload := cell[totalHeaderSize : totalHeaderSize+int(payloadSize)]
		values, err := parseRecord(payload)
		if err != nil {
			continue
		}

		// try match table and insert
		tryRecoverRow(rowID, values, tables, outDB)
	}

	return nil
}

func tryRecoverRow(rowID uint64, values []interface{}, tables []TableInfo, outDB *sql.DB) {
	for _, table := range tables {
		if len(table.Columns) != len(values) {
			continue
		}

		// prepare insert values
		insertValues := make([]interface{}, len(values))
		copy(insertValues, values)

		// handle INTEGER PRIMARY KEY alias
		// if table has INTEGER PRIMARY KEY, the corresponding column in Record is usually NULL, need to fill with rowID
		for i, col := range table.Columns {
			if col.Pk == 1 && strings.ToUpper(col.Type) == "INTEGER" {
				if insertValues[i] == nil {
					insertValues[i] = int64(rowID)
				}
			}
		}

		// build and execute insert statement
		placeholders := make([]string, len(values))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query := fmt.Sprintf("INSERT OR IGNORE INTO %s VALUES (%s)", table.Name, strings.Join(placeholders, ", "))

		_, err := outDB.Exec(query, insertValues...)
		if err == nil {
			// Successfully inserted, consider it a successful match (simple strategy)
			// If multiple tables have the same structure, this may cause false positives,
			// but for repair purposes, recovering data is the priority
			return
		}
	}
}
