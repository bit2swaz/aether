package consensus
import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)
type Consensus struct {
	raft *raft.Raft
}
func New(nodeID, raftPort, raftAdvertise, dataDir string, fsm raft.FSM) (*Consensus, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 500
	config.TrailingLogs = 10  
	bindAddr := fmt.Sprintf("0.0.0.0:%s", raftPort)
	adv := raftAdvertise
	if adv == "" {
		adv = fmt.Sprintf("127.0.0.1:%s", raftPort)
	}
	advertiseAddr, err := net.ResolveTCPAddr("tcp", adv)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertise address: %w", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP transport: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	boltDBPath := filepath.Join(dataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	logStore := boltStore
	stableStore := boltStore
	snapshotDir := filepath.Join(dataDir, "snapshots")
	snapshotStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft instance: %w", err)
	}
	if nodeID == "node1" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress(adv),
				},
			},
		}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			if err != raft.ErrCantBootstrap {
				return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
			}
		}
	}
	return &Consensus{raft: r}, nil
}
func (c *Consensus) WaitForLeader(timeout time.Duration) error {
	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			leader := c.raft.Leader()
			if leader != "" {
				return nil
			}
			if time.Since(start) >= timeout {
				return fmt.Errorf("timeout waiting for leader election")
			}
		}
	}
}
func (c *Consensus) Raft() *raft.Raft {
	return c.raft
}
func (c *Consensus) Shutdown() error {
	if c.raft != nil {
		f := c.raft.Shutdown()
		return f.Error()
	}
	return nil
}
