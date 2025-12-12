package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotAndRestoreIntegration(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}

	_, err = store1.db.Exec("CREATE TABLE snapshot_test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = store1.db.Exec("INSERT INTO snapshot_test VALUES (1, 'original')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	_, err = store1.db.Exec("INSERT INTO snapshot_test VALUES (2, 'data')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Log("Created store with test data")

	snapshot, err := store1.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Snapshot returned nil")
	}

	t.Log("Snapshot created successfully")

	fsmSnapshot, ok := snapshot.(*FSMSnapshot)
	if !ok {
		t.Fatal("Snapshot is not an FSMSnapshot")
	}

	if _, err := os.Stat(fsmSnapshot.path); os.IsNotExist(err) {
		t.Fatalf("Snapshot file does not exist: %s", fsmSnapshot.path)
	}

	t.Logf("Snapshot file created: %s", fsmSnapshot.path)

	_, err = store1.db.Exec("INSERT INTO snapshot_test VALUES (3, 'modified')")
	if err != nil {
		t.Fatalf("Failed to insert modified data: %v", err)
	}

	t.Log("Modified original data")

	snapshotFile, err := os.Open(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}
	defer snapshotFile.Close()

	err = store1.Restore(snapshotFile)
	if err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}

	t.Log("Restored from snapshot")

	rows, err := store1.db.Query("SELECT id, value FROM snapshot_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query after restore: %v", err)
	}
	defer rows.Close()

	var results []struct {
		id    int
		value string
	}

	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		results = append(results, struct {
			id    int
			value string
		}{id, value})
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 rows after restore, got %d", len(results))
	}

	if results[0].id != 1 || results[0].value != "original" {
		t.Errorf("Expected row 1: (1, 'original'), got (%d, '%s')", results[0].id, results[0].value)
	}

	if results[1].id != 2 || results[1].value != "data" {
		t.Errorf("Expected row 2: (2, 'data'), got (%d, '%s')", results[1].id, results[1].value)
	}

	t.Log("Data verified: original data restored correctly")

	store1.Close()

	fsmSnapshot.Release()
	if _, err := os.Stat(fsmSnapshot.path); !os.IsNotExist(err) {
		t.Logf("Warning: Snapshot file still exists after Release: %s", fsmSnapshot.path)
	} else {
		t.Log("Snapshot file cleaned up")
	}

	t.Log("========================================")
	t.Log("SNAPSHOT AND RESTORE TEST PASSED")
	t.Log("  - Snapshot created successfully")
	t.Log("  - Snapshot file exists on disk")
	t.Log("  - Restore replaces database")
	t.Log("  - Original data restored correctly")
	t.Log("  - Modified data was discarded")
	t.Log("  - Database connection re-opened")
	t.Log("========================================")
}

func TestSnapshotEmptyDatabase(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "empty.db")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot of empty database: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Snapshot returned nil")
	}

	fsmSnapshot := snapshot.(*FSMSnapshot)
	defer fsmSnapshot.Release()

	fileInfo, err := os.Stat(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to stat snapshot file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Fatal("Snapshot file is empty")
	}

	t.Logf("Empty database snapshot created: %d bytes", fileInfo.Size())
}

func TestRestoreNonexistentDatabase(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	store1, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}

	_, err = store1.db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	snapshot, err := store1.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	fsmSnapshot := snapshot.(*FSMSnapshot)

	snapshotFile, err := os.Open(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}
	defer snapshotFile.Close()

	dbPath2 := filepath.Join(tempDir, "test2.db")
	store2, err := New(dbPath2)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	err = store2.Restore(snapshotFile)
	if err != nil {
		t.Fatalf("Failed to restore: %v", err)
	}

	rows, err := store2.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
	if err != nil {
		t.Fatalf("Failed to query restored database: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Table 'test' not found in restored database")
	}

	t.Log("Restore to different database path successful")

	fsmSnapshot.Release()
}

func TestSnapshotWithLargeData(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "large.db")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.db.Exec("CREATE TABLE large_test (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 1000; i++ {
		_, err = store.db.Exec("INSERT INTO large_test VALUES (?, ?)", i, "data-"+string(rune(i)))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	t.Log("Inserted 1000 rows")

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	fsmSnapshot := snapshot.(*FSMSnapshot)
	defer fsmSnapshot.Release()

	fileInfo, err := os.Stat(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to stat snapshot file: %v", err)
	}

	t.Logf("Large database snapshot created: %d bytes", fileInfo.Size())

	snapshotFile, err := os.Open(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}
	defer snapshotFile.Close()

	err = store.Restore(snapshotFile)
	if err != nil {
		t.Fatalf("Failed to restore: %v", err)
	}

	var count int
	err = store.db.QueryRow("SELECT COUNT(*) FROM large_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 1000 {
		t.Fatalf("Expected 1000 rows after restore, got %d", count)
	}

	t.Log("Large database restored with all 1000 rows")
}

func TestSnapshotRestore(t *testing.T) {
	tempDir := t.TempDir()
	dbPath1 := filepath.Join(tempDir, "store1.db")

	store1, err := New(dbPath1)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}

	_, err = store1.db.Exec("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, err = store1.db.Exec("INSERT INTO test_data (id, value) VALUES (?, ?)", i, "value-"+string(rune(i)))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	t.Log("Store1 created with 100 rows")

	snapshot, err := store1.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	fsmSnapshot, ok := snapshot.(*FSMSnapshot)
	if !ok {
		t.Fatal("Snapshot is not an FSMSnapshot")
	}
	defer fsmSnapshot.Release()

	t.Logf("Snapshot created: %s", fsmSnapshot.path)

	dbPath2 := filepath.Join(tempDir, "store2.db")

	store2, err := New(dbPath2)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	t.Log("Store2 created at new path")

	snapshotFile, err := os.Open(fsmSnapshot.path)
	if err != nil {
		t.Fatalf("Failed to open snapshot file: %v", err)
	}

	err = store2.Restore(snapshotFile)
	if err != nil {
		t.Fatalf("Failed to restore snapshot to store2: %v", err)
	}

	t.Log("Snapshot restored to store2")

	var count2 int
	err = store2.db.QueryRow("SELECT COUNT(*) FROM test_data").Scan(&count2)
	if err != nil {
		t.Fatalf("Failed to count rows in store2: %v", err)
	}

	if count2 != 100 {
		t.Fatalf("Expected 100 rows in store2, got %d", count2)
	}

	t.Logf("Store2 has 100 rows after restore")

	var value string
	err = store2.db.QueryRow("SELECT value FROM test_data WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query data in store2: %v", err)
	}

	if value != "value-"+string(rune(1)) {
		t.Errorf("Expected value 'value-\x01', got '%s'", value)
	}

	var count1 int
	err = store1.db.QueryRow("SELECT COUNT(*) FROM test_data").Scan(&count1)
	if err != nil {
		t.Fatalf("Failed to query store1 after snapshot: %v", err)
	}

	if count1 != 100 {
		t.Fatalf("Expected 100 rows in store1, got %d", count1)
	}

	_, err = store1.db.Exec("INSERT INTO test_data (id, value) VALUES (?, ?)", 101, "new-value")
	if err != nil {
		t.Fatalf("Failed to insert into store1 after snapshot: %v", err)
	}

	err = store1.db.QueryRow("SELECT COUNT(*) FROM test_data").Scan(&count1)
	if err != nil {
		t.Fatalf("Failed to query store1 after insert: %v", err)
	}

	if count1 != 101 {
		t.Fatalf("Expected 101 rows in store1 after insert, got %d", count1)
	}

	t.Log("Store1 is still usable (can query and insert)")

	store1.Close()

	t.Log("========================================")
	t.Log("SNAPSHOT/RESTORE TEST PASSED")
	t.Log("  Store1 created with 100 rows")
	t.Log("  Snapshot created successfully")
	t.Log("  Store2 created at different path")
	t.Log("  Snapshot restored to Store2")
	t.Log("  Store2 has all 100 rows")
	t.Log("  Store1 remains usable after snapshot")
	t.Log("========================================")
}
