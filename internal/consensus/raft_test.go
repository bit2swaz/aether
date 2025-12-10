package consensus

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type mockFSM struct{}

func (m *mockFSM) Apply(log *raft.Log) interface{} {
	return nil
}

func (m *mockFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &mockSnapshot{}, nil
}

func (m *mockFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}

type mockSnapshot struct{}

func (m *mockSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (m *mockSnapshot) Release() {}

func TestNewConsensus(t *testing.T) {
	dataDir := "test_consensus_data"
	defer os.RemoveAll(dataDir)

	fsm := &mockFSM{}

	consensus, err := New("node1", "7000", dataDir, fsm)
	if err != nil {
		t.Fatalf("Failed to create consensus: %v", err)
	}
	defer consensus.Shutdown()

	if consensus.raft == nil {
		t.Fatal("Raft instance is nil")
	}

	t.Log("Consensus instance created successfully")
}

func TestWaitForLeader(t *testing.T) {
	dataDir := "test_leader_data"
	defer os.RemoveAll(dataDir)

	fsm := &mockFSM{}

	consensus, err := New("node1", "7001", dataDir, fsm)
	if err != nil {
		t.Fatalf("Failed to create consensus: %v", err)
	}
	defer consensus.Shutdown()

	t.Log("Consensus created, waiting for leader election...")

	err = consensus.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("Failed to elect leader: %v", err)
	}

	leader := consensus.raft.Leader()
	if leader == "" {
		t.Fatal("Leader is empty after WaitForLeader returned success")
	}

	t.Logf("Leader elected: %s", leader)

	if consensus.raft.State() != raft.Leader {
		t.Errorf("Expected node to be leader, got state: %s", consensus.raft.State())
	}

	t.Log("Node is in leader state")
}

func TestWaitForLeaderTimeout(t *testing.T) {
	dataDir := "test_timeout_data"
	defer os.RemoveAll(dataDir)

	fsm := &mockFSM{}

	consensus, err := New("node2", "7002", dataDir, fsm)
	if err != nil {
		t.Fatalf("Failed to create consensus: %v", err)
	}
	defer consensus.Shutdown()

	t.Log("Consensus created as node2 (no bootstrap)")

	err = consensus.WaitForLeader(500 * time.Millisecond)
	if err == nil {
		t.Fatal("Expected timeout error, but WaitForLeader succeeded")
	}

	t.Logf("WaitForLeader timed out as expected: %v", err)
}

func TestBootstrapOnlyForNode1(t *testing.T) {
	dataDir1 := "test_bootstrap_node1"
	defer os.RemoveAll(dataDir1)

	fsm1 := &mockFSM{}
	consensus1, err := New("node1", "7003", dataDir1, fsm1)
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer consensus1.Shutdown()

	err = consensus1.WaitForLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("Node1 failed to elect leader: %v", err)
	}
	t.Log("Node1 bootstrapped and elected leader")

	dataDir2 := "test_bootstrap_node2"
	defer os.RemoveAll(dataDir2)

	fsm2 := &mockFSM{}
	consensus2, err := New("node2", "7004", dataDir2, fsm2)
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer consensus2.Shutdown()

	err = consensus2.WaitForLeader(500 * time.Millisecond)
	if err == nil {
		t.Fatal("Node2 should not have a leader without joining a cluster")
	}
	t.Log("Node2 correctly did not bootstrap")
}
