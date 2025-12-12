package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
	_ "github.com/mattn/go-sqlite3"
)

type LogCommand struct {
	Type  string
	SQL   string
	Batch []string
}

type ApplyResponse struct {
	Error  error
	Result sql.Result
}

type Store struct {
	db     *sql.DB
	dbPath string
	raft   *raft.Raft
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

	return &Store{db: db, dbPath: dbPath}, nil
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

	if cmd.Type == "BATCH" {
		tx, err := s.db.Begin()
		if err != nil {
			return &ApplyResponse{
				Error: fmt.Errorf("failed to begin transaction: %w", err),
			}
		}

		for _, sql := range cmd.Batch {
			_, err = tx.Exec(sql)
			if err != nil {
				tx.Rollback()
				return &ApplyResponse{
					Error: fmt.Errorf("failed to execute batch statement: %w", err),
				}
			}
		}

		err = tx.Commit()
		return &ApplyResponse{
			Error: err,
		}
	}

	return &ApplyResponse{
		Error: fmt.Errorf("unknown command type: %s", cmd.Type),
	}
}

func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	tempID := fmt.Sprintf("%d", time.Now().UnixNano())
	snapshotPath := filepath.Join(filepath.Dir(s.dbPath), fmt.Sprintf("snapshot-%s.db", tempID))

	_, err := s.db.Exec(fmt.Sprintf("VACUUM INTO '%s'", snapshotPath))
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	file, err := os.Open(snapshotPath)
	if err != nil {
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}

	return &FSMSnapshot{
		file: file,
		path: snapshotPath,
	}, nil
}

func (s *Store) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	fmt.Println("[INFO] raft-fsm: starting snapshot restore...")

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close database before restore: %w", err)
	}

	tempID := fmt.Sprintf("%d", time.Now().UnixNano())
	tempPath := filepath.Join(filepath.Dir(s.dbPath), fmt.Sprintf("restore-%s.db", tempID))

	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp restore file: %w", err)
	}

	bytesWritten, err := io.Copy(tempFile, snapshot)
	tempFile.Close()
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to copy snapshot data: %w", err)
	}

	fmt.Printf("[INFO] raft-fsm: copied %d bytes from snapshot\n", bytesWritten)

	os.Remove(s.dbPath + "-shm")
	os.Remove(s.dbPath + "-wal")

	if err := os.Rename(tempPath, s.dbPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file to database path: %w", err)
	}

	fmt.Printf("[INFO] raft-fsm: restored snapshot to %s\n", s.dbPath)

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on", s.dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to re-open database after restore: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database after restore: %w", err)
	}

	s.db = db
	fmt.Println("[INFO] raft-fsm: snapshot restore complete!")
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

func (s *Store) ReplicateBatch(batch []string) error {
	if s.raft == nil {
		return fmt.Errorf("raft instance not initialized")
	}

	cmd := LogCommand{
		Type:  "BATCH",
		Batch: batch,
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal batch command: %w", err)
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

type FSMSnapshot struct {
	file *os.File
	path string
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer f.file.Close()
	defer os.Remove(f.path)

	_, err := io.Copy(sink, f.file)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to copy snapshot to sink: %w", err)
	}

	return sink.Close()
}

func (f *FSMSnapshot) Release() {
	if f.file != nil {
		f.file.Close()
	}
	os.Remove(f.path)
}
