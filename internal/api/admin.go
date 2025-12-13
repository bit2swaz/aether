package api
import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"github.com/bit2swaz/aether/internal/consensus"
	"github.com/hashicorp/raft"
)
type Server struct {
	consensus *consensus.Consensus
	nodeID    string
	port      int
}
func NewServer(consensus *consensus.Consensus, nodeID string, port int) *Server {
	return &Server{
		consensus: consensus,
		nodeID:    nodeID,
		port:      port,
	}
}
func (s *Server) Start() {
	http.HandleFunc("/join", s.handleJoin)
	http.HandleFunc("/status", s.handleStatus)
	addr := fmt.Sprintf(":%d", s.port)
	slog.Info("Starting admin HTTP server", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Admin server failed", "error", err)
	}
}
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to decode join request", "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Info("Received join request", "node_id", req.NodeID, "raft_addr", req.RaftAddr)
	f := s.consensus.Raft().AddVoter(
		raft.ServerID(req.NodeID),
		raft.ServerAddress(req.RaftAddr),
		0,
		0,
	)
	if err := f.Error(); err != nil {
		slog.Error("Failed to add voter", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("Successfully added voter", "node_id", req.NodeID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	leader := s.consensus.Raft().Leader()
	state := s.consensus.Raft().State()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id": s.nodeID,
		"leader":  string(leader),
		"state":   state.String(),
	})
}
func JoinCluster(leaderAddr, nodeID, raftAddr string) error {
	req := map[string]string{
		"node_id":   nodeID,
		"raft_addr": raftAddr,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal join request: %w", err)
	}
	joinURL := fmt.Sprintf("%s/join", leaderAddr)
	resp, err := http.Post(joinURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to send join request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join request failed with status: %d", resp.StatusCode)
	}
	return nil
}
