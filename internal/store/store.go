package store

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgproto3/v2"
	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

func New(dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", dbPath)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) Ping() error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	return s.db.Ping()
}

func (s *Store) Execute(sql string) ([]pgproto3.FieldDescription, [][][]byte, error) {
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get column types: %w", err)
	}

	fieldDescriptions := make([]pgproto3.FieldDescription, len(columnTypes))
	for i, col := range columnTypes {
		fieldDescriptions[i] = pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          OIDText,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0, 
		}
	}

	numColumns := len(columnTypes)
	var resultRows [][][]byte

	for rows.Next() {
		values := make([]any, numColumns)
		scans := make([]any, numColumns)
		for i := range values {
			scans[i] = &values[i]
		}

		err = rows.Scan(scans...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		encodedRow := make([][]byte, numColumns)
		for i, val := range values {
			_, encoded, err := encodeValue(val)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to encode value at column %d: %w", i, err)
			}
			encodedRow[i] = encoded
		}

		resultRows = append(resultRows, encodedRow)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return fieldDescriptions, resultRows, nil
}
