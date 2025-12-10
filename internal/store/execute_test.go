package store

import (
	"os"
	"testing"
)

func TestExecute(t *testing.T) {
	dbPath := "test_execute.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, _, err = store.Query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	t.Log("Table created")

	_, _, err = store.Query("INSERT INTO users (name, age) VALUES ('Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Log("Data inserted")

	_, _, err = store.Query("INSERT INTO users (name, age) VALUES ('Bob', 25)")
	if err != nil {
		t.Fatalf("Failed to insert second row: %v", err)
	}

	fieldDesc, rows, err := store.Query("SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	if len(fieldDesc) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(fieldDesc))
	}

	expectedColumns := []string{"id", "name", "age"}
	for i, col := range expectedColumns {
		if string(fieldDesc[i].Name) != col {
			t.Errorf("Expected column name %s, got %s", col, string(fieldDesc[i].Name))
		}
		if fieldDesc[i].DataTypeOID != OIDText {
			t.Errorf("Expected OID %d for column %s, got %d", OIDText, col, fieldDesc[i].DataTypeOID)
		}
	}
	t.Log("Field descriptions correct")

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	if len(rows[0]) != 3 {
		t.Fatalf("Expected 3 values in first row, got %d", len(rows[0]))
	}
	if string(rows[0][1]) != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", string(rows[0][1]))
	}
	if string(rows[0][2]) != "30" {
		t.Errorf("Expected age '30', got '%s'", string(rows[0][2]))
	}
	t.Log("First row correct: Alice, 30")

	if string(rows[1][1]) != "Bob" {
		t.Errorf("Expected name 'Bob', got '%s'", string(rows[1][1]))
	}
	if string(rows[1][2]) != "25" {
		t.Errorf("Expected age '25', got '%s'", string(rows[1][2]))
	}
	t.Log("Second row correct: Bob, 25")

	t.Log("=== Execute method works correctly ===")
}

func TestExecuteEmptyResult(t *testing.T) {
	dbPath := "test_execute_empty.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, _, err = store.Query("CREATE TABLE empty_table (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	fieldDesc, rows, err := store.Query("SELECT * FROM empty_table")
	if err != nil {
		t.Fatalf("Failed to query empty table: %v", err)
	}

	if len(fieldDesc) != 1 {
		t.Errorf("Expected 1 field description, got %d", len(fieldDesc))
	}

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(rows))
	}

	t.Log("Empty result set handled correctly")
}
