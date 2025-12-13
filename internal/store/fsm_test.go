package store

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func TestApplyExecute(t *testing.T) {
	dbPath := "test_apply.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	createCmd := LogCommand{
		Type: "EXECUTE",
		SQL:  "CREATE TABLE test_apply (id INTEGER, value TEXT)",
	}
	createData, err := json.Marshal(createCmd)
	if err != nil {
		t.Fatalf("Failed to marshal create command: %v", err)
	}

	createLog := &raft.Log{
		Data: createData,
	}

	result := store.Apply(createLog)
	applyResp, ok := result.(*ApplyResponse)
	if !ok {
		t.Fatalf("Expected *ApplyResponse, got %T", result)
	}
	if applyResp.Error != nil {
		t.Fatalf("Failed to create table via Apply: %v", applyResp.Error)
	}
	t.Log("CREATE TABLE via Apply succeeded")

	insertCmd := LogCommand{
		Type: "EXECUTE",
		SQL:  "INSERT INTO test_apply (id, value) VALUES (42, 'test-value')",
	}
	insertData, err := json.Marshal(insertCmd)
	if err != nil {
		t.Fatalf("Failed to marshal insert command: %v", err)
	}

	insertLog := &raft.Log{
		Data: insertData,
	}

	result = store.Apply(insertLog)
	applyResp, ok = result.(*ApplyResponse)
	if !ok {
		t.Fatalf("Expected *ApplyResponse, got %T", result)
	}
	if applyResp.Error != nil {
		t.Fatalf("Failed to insert data via Apply: %v", applyResp.Error)
	}

	if applyResp.Result == nil {
		t.Fatal("Expected Result to be non-nil")
	}
	rowsAffected, err := applyResp.Result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}
	t.Log("INSERT via Apply succeeded")

	_, rows, err := store.Query("SELECT id, value FROM test_apply")
	if err != nil {
		t.Fatalf("Failed to query inserted data: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	if string(rows[0][0]) != "42" {
		t.Errorf("Expected id '42', got '%s'", string(rows[0][0]))
	}

	if string(rows[0][1]) != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", string(rows[0][1]))
	}
	t.Log("Data verified via Query")
}

func TestApplyInvalidJSON(t *testing.T) {
	dbPath := "test_apply_invalid.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	invalidLog := &raft.Log{
		Data: []byte("not valid json"),
	}

	result := store.Apply(invalidLog)
	applyResp, ok := result.(*ApplyResponse)
	if !ok {
		t.Fatalf("Expected *ApplyResponse, got %T", result)
	}

	if applyResp.Error == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
	t.Logf("Invalid JSON handled correctly: %v", applyResp.Error)
}

func TestApplyUnknownCommandType(t *testing.T) {
	dbPath := "test_apply_unknown.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	unknownCmd := LogCommand{
		Type: "UNKNOWN_TYPE",
		SQL:  "SELECT 1",
	}
	data, err := json.Marshal(unknownCmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	log := &raft.Log{
		Data: data,
	}

	result := store.Apply(log)
	applyResp, ok := result.(*ApplyResponse)
	if !ok {
		t.Fatalf("Expected *ApplyResponse, got %T", result)
	}

	if applyResp.Error == nil {
		t.Fatal("Expected error for unknown command type, got nil")
	}
	t.Logf("Unknown command type handled correctly: %v", applyResp.Error)
}

func TestApplySQLError(t *testing.T) {
	dbPath := "test_apply_sql_error.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cmd := LogCommand{
		Type: "EXECUTE",
		SQL:  "INSERT INTO nonexistent_table VALUES (1)",
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	log := &raft.Log{
		Data: data,
	}

	result := store.Apply(log)
	applyResp, ok := result.(*ApplyResponse)
	if !ok {
		t.Fatalf("Expected *ApplyResponse, got %T", result)
	}

	if applyResp.Error == nil {
		t.Fatal("Expected SQL error, got nil")
	}
	t.Logf("SQL error handled correctly: %v", applyResp.Error)
}

func TestSnapshotAndRestore(t *testing.T) {
	dbPath := "test_snapshot.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = store.db.Exec("INSERT INTO test VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if snapshot == nil {
		t.Fatal("Expected snapshot, got nil")
	}
	t.Log(" Snapshot created successfully")

	fsmSnapshot, ok := snapshot.(*FSMSnapshot)
	if !ok {
		t.Fatal("Snapshot is not an FSMSnapshot")
	}
	defer fsmSnapshot.Release()

	snapshotFile, err := os.Open(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}
	defer snapshotFile.Close()

	err = store.Restore(snapshotFile)
	if err != nil {
		t.Fatalf("Restore returned error: %v", err)
	}
	t.Log(" Restore completed successfully")

	var count int
	err = store.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query after restore: %v", err)
	}

	if count != 1 {
		t.Fatalf("Expected 1 row after restore, got %d", count)
	}
	t.Log(" Data verified after restore")
}
