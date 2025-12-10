package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
	_ "github.com/mattn/go-sqlite3"
)

type LogCommand struct {
	Type string
	SQL  string
}

type ApplyResponse struct {
	Error  error
	Result sql.Result
}

type Store struct {
	db   *sql.DB
	raft *raft.Raft
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

func (s *Store) Query(sql string) ([]pgproto3.FieldDescription, [][][]byte, error) {
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

func (s *Store) Apply(log *raft.Log) interface{} {
	var cmd LogCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &ApplyResponse{
			Error: fmt.Errorf("failed to unmarshal command: %w", err),
		}
	}

	if cmd.Type == "EXECUTE" {
		result, err := s.db.Exec(cmd.SQL)
		return &ApplyResponse{
			Error:  err,
			Result: result,
		}
	}

	return &ApplyResponse{
		Error: fmt.Errorf("unknown command type: %s", cmd.Type),
	}
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (s *Store) Restore(snapshot io.ReadCloser) error {
	return nil
}

func (s *Store) SetRaft(r *raft.Raft) {
	s.raft = r
}

func (s *Store) Replicate(sql string) error {
	if s.raft == nil {
		return fmt.Errorf("raft instance not initialized")
	}

	cmd := LogCommand{
		Type: "EXECUTE",
		SQL:  sql,
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	timeout := 10 * time.Second
	future := s.raft.Apply(bytes, timeout)

	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	response := future.Response()
	if response != nil {
		if applyResp, ok := response.(*ApplyResponse); ok {
			if applyResp.Error != nil {
				return fmt.Errorf("fsm execution failed: %w", applyResp.Error)
			}
		}
	}

	return nil
}
