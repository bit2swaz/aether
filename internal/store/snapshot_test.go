package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

type mockSnapshotSink struct {
	bytes.Buffer
	cancelled bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.Buffer.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) Cancel() error {
	m.cancelled = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func TestSnapshotVersionPersist(t *testing.T) {
	dbPath := "test_snapshot_version.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.db.Exec("CREATE TABLE test_data (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = store.db.Exec("INSERT INTO test_data (id, value) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	sink := &mockSnapshotSink{}
	err = snapshot.Persist(sink)
	if err != nil {
		t.Fatalf("Failed to persist snapshot: %v", err)
	}

	snapshotBytes := sink.Bytes()
	if len(snapshotBytes) == 0 {
		t.Fatal("Snapshot is empty")
	}

	firstByte := snapshotBytes[0]
	if firstByte != SnapshotVersion {
		t.Errorf("Expected first byte to be %d (SnapshotVersion), got %d", SnapshotVersion, firstByte)
	}
	t.Logf("First byte correctly set to version %d", firstByte)

	dbPath2 := "test_snapshot_restore.db"
	defer os.Remove(dbPath2)
	defer os.Remove(dbPath2 + "-shm")
	defer os.Remove(dbPath2 + "-wal")

	store2, err := New(dbPath2)
	if err != nil {
		t.Fatalf("Failed to create second store: %v", err)
	}
	defer store2.Close()

	reader := io.NopCloser(bytes.NewReader(snapshotBytes))
	err = store2.Restore(reader)
	if err != nil {
		t.Fatalf("Failed to restore snapshot with valid version: %v", err)
	}

	_, rows, err := store2.Query("SELECT id, value FROM test_data")
	if err != nil {
		t.Fatalf("Failed to query restored data: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	if string(rows[0][0]) != "1" || string(rows[0][1]) != "test" {
		t.Errorf("Data mismatch after restore: got id=%s, value=%s", string(rows[0][0]), string(rows[0][1]))
	}

	t.Log("Snapshot persisted and restored successfully with correct version")
}

func TestSnapshotIncompatibleVersion(t *testing.T) {
	dbPath := "test_incompatible.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	testCases := []struct {
		name    string
		version byte
	}{
		{"version 0", 0x00},
		{"version 2", 0x02},
		{"version 99", 0x63},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath2 := fmt.Sprintf("test_invalid_%d.db", tc.version)
			defer os.Remove(dbPath2)
			defer os.Remove(dbPath2 + "-shm")
			defer os.Remove(dbPath2 + "-wal")

			store2, err := New(dbPath2)
			if err != nil {
				t.Fatalf("Failed to create store: %v", err)
			}
			defer store2.Close()

			invalidSnapshot := []byte{tc.version}
			validDBData := make([]byte, 100)
			for i := range validDBData {
				validDBData[i] = 0x00
			}
			invalidSnapshot = append(invalidSnapshot, validDBData...)

			reader := io.NopCloser(bytes.NewReader(invalidSnapshot))
			err = store2.Restore(reader)

			if err == nil {
				t.Fatalf("Expected error for incompatible version %d, but got nil", tc.version)
			}

			if !strings.Contains(err.Error(), "incompatible snapshot version") {
				t.Errorf("Expected error to contain 'incompatible snapshot version', got: %v", err)
			}

			expectedVersionInError := fmt.Sprintf("%d", tc.version)
			if !strings.Contains(err.Error(), expectedVersionInError) {
				t.Errorf("Expected error to contain version number %s, got: %v", expectedVersionInError, err)
			}

			t.Logf("Correctly rejected version %d with error: %v", tc.version, err)
		})
	}
}

func TestSnapshotMissingVersion(t *testing.T) {
	dbPath := "test_missing_version.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	emptySnapshot := []byte{}
	reader := io.NopCloser(bytes.NewReader(emptySnapshot))
	err = store.Restore(reader)

	if err == nil {
		t.Fatal("Expected error when reading version from empty snapshot, but got nil")
	}

	if !strings.Contains(err.Error(), "failed to read snapshot version") {
		t.Errorf("Expected error about reading version, got: %v", err)
	}

	t.Logf("Correctly rejected empty snapshot with error: %v", err)
}
