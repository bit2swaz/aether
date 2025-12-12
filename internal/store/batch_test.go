package store

import (
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestBatchTransaction(t *testing.T) {
	dbPath := "test_batch.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	dataDir := "test_batch_raft"
	defer os.RemoveAll(dataDir)

	config := raft.DefaultConfig()
	config.LocalID = "test-node"
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	_, transport := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:0"))
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("Failed to create raft: %v", err)
	}
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	f := r.BootstrapCluster(configuration)
	if err := f.Error(); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if r.State() == raft.Leader {
				goto leaderElected
			}
		case <-timeout:
			t.Fatal("Timeout waiting for leader election")
		}
	}

leaderElected:
	t.Log("Leader elected")

	store.SetRaft(r)

	batch := []string{
		"CREATE TABLE batch_test (id INTEGER, name TEXT, value INTEGER)",
		"INSERT INTO batch_test VALUES (1, 'alice', 100)",
		"INSERT INTO batch_test VALUES (2, 'bob', 200)",
		"UPDATE batch_test SET value = 150 WHERE id = 1",
	}

	err = store.ReplicateBatch(batch)
	if err != nil {
		t.Fatalf("Failed to replicate batch: %v", err)
	}

	t.Log("Batch replicated successfully")

	_, rows, err := store.Query("SELECT id, name, value FROM batch_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query batch_test: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	if string(rows[0][0]) != "1" || string(rows[0][1]) != "alice" || string(rows[0][2]) != "150" {
		t.Errorf("Row 1: expected (1, 'alice', 150), got (%s, %s, %s)",
			string(rows[0][0]), string(rows[0][1]), string(rows[0][2]))
	}

	if string(rows[1][0]) != "2" || string(rows[1][1]) != "bob" || string(rows[1][2]) != "200" {
		t.Errorf("Row 2: expected (2, 'bob', 200), got (%s, %s, %s)",
			string(rows[1][0]), string(rows[1][1]), string(rows[1][2]))
	}

	t.Log("Data verified: batch transaction committed correctly")
}

func TestBatchTransactionRollback(t *testing.T) {
	dbPath := "test_batch_rollback.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	dataDir := "test_batch_rollback_raft"
	defer os.RemoveAll(dataDir)

	config := raft.DefaultConfig()
	config.LocalID = "test-node"
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	_, transport := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:0"))
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("Failed to create raft: %v", err)
	}
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	f := r.BootstrapCluster(configuration)
	if err := f.Error(); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if r.State() == raft.Leader {
				goto leaderElected
			}
		case <-timeout:
			t.Fatal("Timeout waiting for leader election")
		}
	}

leaderElected:
	store.SetRaft(r)

	err = store.Replicate("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = store.Replicate("INSERT INTO rollback_test VALUES (1, 'initial')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	t.Log("Initial data inserted")

	batch := []string{
		"INSERT INTO rollback_test VALUES (2, 'valid')",
		"INSERT INTO rollback_test VALUES (1, 'duplicate')",
		"INSERT INTO rollback_test VALUES (3, 'also_valid')",
	}

	err = store.ReplicateBatch(batch)
	if err == nil {
		t.Fatal("Expected error for duplicate primary key, got nil")
	}

	t.Logf("Batch correctly failed: %v", err)

	_, rows, err := store.Query("SELECT id, value FROM rollback_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query rollback_test: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row after rollback, got %d", len(rows))
	}

	if string(rows[0][0]) != "1" || string(rows[0][1]) != "initial" {
		t.Errorf("Expected (1, 'initial'), got (%s, %s)",
			string(rows[0][0]), string(rows[0][1]))
	}

	t.Log("Transaction correctly rolled back - only initial data remains")
}

func TestEmptyBatch(t *testing.T) {
	dbPath := "test_empty_batch.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	dataDir := "test_empty_batch_raft"
	defer os.RemoveAll(dataDir)

	config := raft.DefaultConfig()
	config.LocalID = "test-node"
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 5 * time.Millisecond

	_, transport := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:0"))
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("Failed to create raft: %v", err)
	}
	defer r.Shutdown()

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	f := r.BootstrapCluster(configuration)
	if err := f.Error(); err != nil {
		t.Fatalf("Failed to bootstrap cluster: %v", err)
	}

	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if r.State() == raft.Leader {
				goto leaderElected
			}
		case <-timeout:
			t.Fatal("Timeout waiting for leader election")
		}
	}

leaderElected:
	store.SetRaft(r)

	batch := []string{}

	err = store.ReplicateBatch(batch)
	if err != nil {
		t.Fatalf("Empty batch should not error: %v", err)
	}

	t.Log("Empty batch handled correctly")
}
