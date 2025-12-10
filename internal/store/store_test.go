package store

import (
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	dbPath := "test.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	err = store.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	t.Log("Store created successfully")
	t.Log("Database connection is alive")
}

func TestClose(t *testing.T) {
	dbPath := "test_close.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}

	err = store.Ping()
	if err == nil {
		t.Fatal("Expected error when pinging closed database")
	}

	t.Log("Store closed successfully")
	t.Log("Ping fails after close as expected")
}

func TestStoreLifecycle(t *testing.T) {
	dbPath := "test_lifecycle.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Log("Store initialized")

	fieldDesc, rows, err := store.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if len(fieldDesc) != 0 {
		t.Logf("Warning: CREATE TABLE returned %d field descriptions (expected 0)", len(fieldDesc))
	}
	if len(rows) != 0 {
		t.Logf("Warning: CREATE TABLE returned %d rows (expected 0)", len(rows))
	}
	t.Log("Table created")

	fieldDesc, rows, err = store.Execute("INSERT INTO test VALUES (1, 'Aether')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Log("Data inserted")

	fieldDesc, rows, err = store.Execute("SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	t.Log("Got 1 row as expected")

	if len(fieldDesc) != 2 {
		t.Fatalf("Expected 2 field descriptions, got %d", len(fieldDesc))
	}
	if string(fieldDesc[0].Name) != "id" {
		t.Errorf("Expected first field 'id', got '%s'", string(fieldDesc[0].Name))
	}
	if string(fieldDesc[1].Name) != "name" {
		t.Errorf("Expected second field 'name', got '%s'", string(fieldDesc[1].Name))
	}
	t.Log("Field descriptions correct")

	if len(rows[0]) != 2 {
		t.Fatalf("Expected 2 columns in row, got %d", len(rows[0]))
	}
	if string(rows[0][0]) != "1" {
		t.Errorf("Expected id '1', got '%s'", string(rows[0][0]))
	}
	if string(rows[0][1]) != "Aether" {
		t.Errorf("Expected name 'Aether', got '%s'", string(rows[0][1]))
	}
	t.Log("Encoded bytes correct: id=1, name=Aether")

	err = store.Close()
	if err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}
	t.Log("Store closed")

	store, err = New(dbPath)
	if err != nil {
		t.Fatalf("Failed to re-open store: %v", err)
	}
	defer store.Close()
	t.Log("Store re-opened")

	fieldDesc, rows, err = store.Execute("SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("Failed to select data after re-open: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row after re-open, got %d", len(rows))
	}

	if len(rows[0]) != 2 {
		t.Fatalf("Expected 2 columns after re-open, got %d", len(rows[0]))
	}

	if string(rows[0][0]) != "1" {
		t.Errorf("Expected id '1' after re-open, got '%s'", string(rows[0][0]))
	}

	if string(rows[0][1]) != "Aether" {
		t.Errorf("Expected name 'Aether' after re-open, got '%s'", string(rows[0][1]))
	}
	t.Log("Data persisted correctly: id=1, name=Aether")

	t.Log("=== Store lifecycle test passed: Create, Insert, Select, Close, Re-open, Select ===")
}
