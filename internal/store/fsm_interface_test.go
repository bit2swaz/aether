package store

import (
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func TestStoreFSMInterface(t *testing.T) {
	dbPath := "test_fsm_interface.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	var _ raft.FSM = store

	t.Log("Store implements raft.FSM interface")
}
