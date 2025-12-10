package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestClusterReplication(t *testing.T) {
	tempDir := t.TempDir()

	node1DataDir := filepath.Join(tempDir, "node1")
	node2DataDir := filepath.Join(tempDir, "node2")
	node3DataDir := filepath.Join(tempDir, "node3")

	if err := os.MkdirAll(node1DataDir, 0755); err != nil {
		t.Fatalf("Failed to create node1 directory: %v", err)
	}
	if err := os.MkdirAll(node2DataDir, 0755); err != nil {
		t.Fatalf("Failed to create node2 directory: %v", err)
	}
	if err := os.MkdirAll(node3DataDir, 0755); err != nil {
		t.Fatalf("Failed to create node3 directory: %v", err)
	}

	node1DBPath := filepath.Join(node1DataDir, "store.db")
	node2DBPath := filepath.Join(node2DataDir, "store.db")
	node3DBPath := filepath.Join(node3DataDir, "store.db")

	t.Logf("Created temp directories: %s", tempDir)

	store1, err := New(node1DBPath)
	if err != nil {
		t.Fatalf("Failed to create store1: %v", err)
	}
	defer store1.Close()

	raft1, transport1, err := createRaftNode("node1", "127.0.0.1:9000", node1DataDir, store1)
	if err != nil {
		t.Fatalf("Failed to create raft1: %v", err)
	}
	defer raft1.Shutdown()
	store1.SetRaft(raft1)

	t.Log("Node 1 created")

	store2, err := New(node2DBPath)
	if err != nil {
		t.Fatalf("Failed to create store2: %v", err)
	}
	defer store2.Close()

	raft2, transport2, err := createRaftNode("node2", "127.0.0.1:9001", node2DataDir, store2)
	if err != nil {
		t.Fatalf("Failed to create raft2: %v", err)
	}
	defer raft2.Shutdown()
	store2.SetRaft(raft2)

	t.Log("Node 2 created")

	store3, err := New(node3DBPath)
	if err != nil {
		t.Fatalf("Failed to create store3: %v", err)
	}
	defer store3.Close()

	raft3, transport3, err := createRaftNode("node3", "127.0.0.1:9002", node3DataDir, store3)
	if err != nil {
		t.Fatalf("Failed to create raft3: %v", err)
	}
	defer raft3.Shutdown()
	store3.SetRaft(raft3)

	t.Log("Node 3 created")

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      "node1",
				Address: transport1.LocalAddr(),
			},
		},
	}

	f := raft1.BootstrapCluster(configuration)
	if err := f.Error(); err != nil {
		t.Fatalf("Failed to bootstrap node1: %v", err)
	}

	t.Log("Node 1 bootstrapped")

	if err := waitForLeader([]*raft.Raft{raft1}, 10*time.Second); err != nil {
		t.Fatalf("Failed to elect initial leader: %v", err)
	}
	t.Logf("Node 1 became leader: %s", raft1.Leader())

	addFuture := raft1.AddVoter("node2", transport2.LocalAddr(), 0, 0)
	if err := addFuture.Error(); err != nil {
		t.Fatalf("Failed to add node2 as voter: %v", err)
	}
	t.Log("Node 2 added as voter")

	addFuture = raft1.AddVoter("node3", transport3.LocalAddr(), 0, 0)
	if err := addFuture.Error(); err != nil {
		t.Fatalf("Failed to add node3 as voter: %v", err)
	}
	t.Log("Node 3 added as voter")

	if err := waitForLeader([]*raft.Raft{raft1, raft2, raft3}, 10*time.Second); err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	leader := raft1.Leader()
	t.Logf("Leader elected: %s", leader)

	configFuture := raft1.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		t.Fatalf("Failed to get configuration: %v", err)
	}

	servers := configFuture.Configuration().Servers
	if len(servers) != 3 {
		t.Fatalf("Expected 3 servers in configuration, got %d", len(servers))
	}
	t.Logf("Cluster configuration verified: %d nodes", len(servers))

	err = store1.Replicate("CREATE TABLE test (val TEXT)")
	if err != nil {
		t.Fatalf("Failed to replicate CREATE TABLE: %v", err)
	}
	t.Log("CREATE TABLE replicated")

	err = store1.Replicate("INSERT INTO test VALUES ('synced')")
	if err != nil {
		t.Fatalf("Failed to replicate INSERT: %v", err)
	}
	t.Log("INSERT replicated")
	t.Log("Waiting 2 seconds for replication to complete...")
	time.Sleep(2 * time.Second)

	_, rows1, err := store1.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query node1: %v", err)
	}
	t.Logf("Node 1 query successful: %d rows", len(rows1))

	_, rows2, err := store2.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query node2: %v", err)
	}
	t.Logf("Node 2 query successful: %d rows", len(rows2))

	_, rows3, err := store3.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query node3: %v", err)
	}
	t.Logf("Node 3 query successful: %d rows", len(rows3))

	if len(rows1) != 1 {
		t.Fatalf("Node 1: Expected 1 row, got %d", len(rows1))
	}
	if string(rows1[0][0]) != "synced" {
		t.Fatalf("Node 1: Expected 'synced', got '%s'", string(rows1[0][0]))
	}
	t.Log("Node 1 data verified: 'synced'")

	if len(rows2) != 1 {
		t.Fatalf("Node 2: Expected 1 row, got %d", len(rows2))
	}
	if string(rows2[0][0]) != "synced" {
		t.Fatalf("Node 2: Expected 'synced', got '%s'", string(rows2[0][0]))
	}
	t.Log("Node 2 data verified: 'synced'")

	if len(rows3) != 1 {
		t.Fatalf("Node 3: Expected 1 row, got %d", len(rows3))
	}
	if string(rows3[0][0]) != "synced" {
		t.Fatalf("Node 3: Expected 'synced', got '%s'", string(rows3[0][0]))
	}
	t.Log("Node 3 data verified: 'synced'")

	t.Log("========================================")
	t.Log("CLUSTER REPLICATION TEST PASSED")
	t.Log("  - 3 nodes successfully formed cluster")
	t.Log("  - Leader elected")
	t.Log("  - CREATE TABLE replicated to all nodes")
	t.Log("  - INSERT replicated to all nodes")
	t.Log("  - Data consistency verified across cluster")
	t.Log("========================================")
}

func createRaftNode(nodeID, bindAddr, dataDir string, fsm raft.FSM) (*raft.Raft, *raft.NetworkTransport, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 50 * time.Millisecond

	addr, err := raft.NewTCPTransport(bindAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create transport: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, addr)
	if err != nil {
		addr.Close()
		return nil, nil, fmt.Errorf("failed to create raft: %w", err)
	}

	return r, addr, nil
}

func waitForLeader(rafts []*raft.Raft, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, r := range rafts {
				if r.Leader() != "" {
					return nil
				}
			}

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for leader election")
			}
		}
	}
}
