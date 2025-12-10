package store

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestReplicate(t *testing.T) {
	dbPath := "test_replicate.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	dataDir := "test_replicate_raft"
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

	err = store.Replicate("CREATE TABLE replicate_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to replicate CREATE TABLE: %v", err)
	}
	t.Log("CREATE TABLE replicated successfully")

	err = store.Replicate("INSERT INTO replicate_test VALUES (1, 'test-data')")
	if err != nil {
		t.Fatalf("Failed to replicate INSERT: %v", err)
	}
	t.Log("INSERT replicated successfully")

	_, rows, err := store.Query("SELECT id, name FROM replicate_test")
	if err != nil {
		t.Fatalf("Failed to query replicated data: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	if string(rows[0][0]) != "1" {
		t.Errorf("Expected id '1', got '%s'", string(rows[0][0]))
	}

	if string(rows[0][1]) != "test-data" {
		t.Errorf("Expected name 'test-data', got '%s'", string(rows[0][1]))
	}
	t.Log("Replicated data verified via Query")
}

func TestReplicateWithoutRaft(t *testing.T) {
	dbPath := "test_replicate_no_raft.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	err = store.Replicate("CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Fatal("Expected error when replicating without Raft instance, got nil")
	}

	t.Logf("Correctly rejected replication without Raft: %v", err)
}

func TestReplicateInvalidSQL(t *testing.T) {
	dbPath := "test_replicate_invalid.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	dataDir := "test_replicate_invalid_raft"
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

	err = store.Replicate("INSERT INTO nonexistent_table VALUES (1)")
	if err == nil {
		t.Fatal("Expected error for invalid SQL, got nil")
	}

	t.Logf("Invalid SQL correctly rejected: %v", err)
}

func TestReplicateMarshalError(t *testing.T) {

	dbPath := "test_replicate_marshal.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	config := raft.DefaultConfig()
	config.LocalID = "test-node"
	_, transport := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:0"))
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("Failed to create raft: %v", err)
	}
	defer r.Shutdown()

	store.SetRaft(r)

	cmd := LogCommand{
		Type: "EXECUTE",
		SQL:  "SELECT 1",
	}
	_, err = json.Marshal(cmd)
	if err != nil {
		t.Errorf("Unexpected marshal error: %v", err)
	}

	t.Log("LogCommand marshals successfully")
}
