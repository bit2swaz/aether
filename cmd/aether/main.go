package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bit2swaz/aether/internal/consensus"
	"github.com/bit2swaz/aether/internal/store"
	"github.com/hashicorp/raft"
	"github.com/jackc/pgproto3/v2"
)

var (
	globalStore     *store.Store
	globalConsensus *consensus.Consensus
)

func main() {
	// Parse command-line flags
	nodeID := flag.String("id", "node1", "Node ID")
	pgPort := flag.Int("port", 5432, "PostgreSQL protocol port")
	raftPort := flag.Int("raft-port", 7000, "Raft consensus port")
	httpPort := flag.Int("http-port", 8080, "HTTP admin API port")
	joinAddr := flag.String("join", "", "Address of leader node to join (e.g., http://localhost:8080)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Starting Aether node",
		"node_id", *nodeID,
		"pg_port", *pgPort,
		"raft_port", *raftPort,
		"http_port", *httpPort,
		"join", *joinAddr)

	dbPath := fmt.Sprintf("%s.db", *nodeID)
	var err error
	globalStore, err = store.New(dbPath)
	if err != nil {
		panic(err)
	}
	defer globalStore.Close()

	slog.Info("Database initialized", "path", dbPath)

	dataDir := fmt.Sprintf("%s-data", *nodeID)
	globalConsensus, err = consensus.New(*nodeID, fmt.Sprintf("%d", *raftPort), dataDir, globalStore)
	if err != nil {
		panic(err)
	}
	defer globalConsensus.Shutdown()

	slog.Info("Consensus layer initialized")

	globalStore.SetRaft(globalConsensus.Raft())

	if *nodeID == "node1" {
		err = globalConsensus.WaitForLeader(30 * time.Second)
		if err != nil {
			panic(fmt.Sprintf("Failed to elect leader: %v", err))
		}
		slog.Info("Leader elected")
	}

	go startAdminServer(*httpPort, *nodeID, *raftPort)

	if *joinAddr != "" {
		err = joinCluster(*joinAddr, *nodeID, fmt.Sprintf("127.0.0.1:%d", *raftPort))
		if err != nil {
			slog.Error("Failed to join cluster", "error", err)
			panic(err)
		}
		slog.Info("Successfully joined cluster", "leader", *joinAddr)
	}

	pgAddr := fmt.Sprintf("0.0.0.0:%d", *pgPort)
	err = startServer(pgAddr)
	if err != nil {
		panic(err)
	}
}

func startServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	slog.Info("Listening on " + addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	slog.Info("New connection from " + remoteAddr)

	buf := make([]byte, 8)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		slog.Error("Failed to read initial message", "error", err)
		return
	}

	length := binary.BigEndian.Uint32(buf[0:4])
	code := binary.BigEndian.Uint32(buf[4:8])

	var backend *pgproto3.Backend

	if length == 8 && code == 80877103 {
		slog.Info("SSL request received, rejecting")
		_, err = conn.Write([]byte{'N'})
		if err != nil {
			slog.Error("Failed to send SSL rejection", "error", err)
			return
		}
		backend = pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	} else {
		slog.Info("No SSL request, processing startup message directly")

		bufferedReader := io.MultiReader(bytes.NewReader(buf), conn)
		backend = pgproto3.NewBackend(pgproto3.NewChunkReader(bufferedReader), conn)
	}

	startupMsgRaw, err := backend.ReceiveStartupMessage()
	if err != nil {
		slog.Error("Failed to receive startup message", "error", err)
		return
	}

	startupMsg, ok := startupMsgRaw.(*pgproto3.StartupMessage)
	if !ok {
		slog.Error("Expected StartupMessage but got different message type")
		return
	}

	slog.Info("Received startup message",
		"protocol_version", startupMsg.ProtocolVersion,
		"parameters", startupMsg.Parameters)

	err = backend.Send(&pgproto3.AuthenticationOk{})
	if err != nil {
		slog.Error("Failed to send AuthenticationOk", "error", err)
		return
	}

	err = backend.Send(&pgproto3.ParameterStatus{
		Name:  "server_version",
		Value: "14.0",
	})
	if err != nil {
		slog.Error("Failed to send server_version parameter", "error", err)
		return
	}

	err = backend.Send(&pgproto3.ParameterStatus{
		Name:  "client_encoding",
		Value: "UTF8",
	})
	if err != nil {
		slog.Error("Failed to send client_encoding parameter", "error", err)
		return
	}

	err = backend.Send(&pgproto3.ReadyForQuery{
		TxStatus: 'I',
	})
	if err != nil {
		slog.Error("Failed to send ReadyForQuery", "error", err)
		return
	}

	slog.Info("PostgreSQL handshake completed successfully")

	for {
		msg, err := backend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("Client disconnected")
				return
			}
			slog.Error("Error receiving message", "error", err)
			return
		}

		switch v := msg.(type) {
		case *pgproto3.Query:
			slog.Info("Received query", "sql", v.String)

			sqlUpper := strings.ToUpper(strings.TrimSpace(v.String))
			isWrite := strings.HasPrefix(sqlUpper, "INSERT") ||
				strings.HasPrefix(sqlUpper, "UPDATE") ||
				strings.HasPrefix(sqlUpper, "DELETE") ||
				strings.HasPrefix(sqlUpper, "CREATE") ||
				strings.HasPrefix(sqlUpper, "DROP") ||
				strings.HasPrefix(sqlUpper, "ALTER")

			var commandTag string

			if isWrite {
				slog.Info("Routing to Replicate (distributed write)")
				err = globalStore.Replicate(v.String)
				if err != nil {
					slog.Error("Failed to replicate command", "error", err)
					err = backend.Send(&pgproto3.ErrorResponse{
						Severity: "ERROR",
						Code:     "42000",
						Message:  err.Error(),
					})
					if err != nil {
						slog.Error("Failed to send ErrorResponse", "error", err)
					}
					err = backend.Send(&pgproto3.ReadyForQuery{
						TxStatus: 'I',
					})
					if err != nil {
						slog.Error("Failed to send ReadyForQuery", "error", err)
					}
					return
				}

				if strings.HasPrefix(sqlUpper, "INSERT") {
					commandTag = "INSERT 0 1"
				} else if strings.HasPrefix(sqlUpper, "UPDATE") {
					commandTag = "UPDATE 1"
				} else if strings.HasPrefix(sqlUpper, "DELETE") {
					commandTag = "DELETE 1"
				} else if strings.HasPrefix(sqlUpper, "CREATE") {
					commandTag = "CREATE TABLE"
				} else if strings.HasPrefix(sqlUpper, "DROP") {
					commandTag = "DROP TABLE"
				} else if strings.HasPrefix(sqlUpper, "ALTER") {
					commandTag = "ALTER TABLE"
				} else {
					commandTag = "OK"
				}

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte(commandTag),
				})
				if err != nil {
					slog.Error("Failed to send CommandComplete", "error", err)
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'I',
				})
				if err != nil {
					slog.Error("Failed to send ReadyForQuery", "error", err)
					return
				}
			} else {
				slog.Info("Routing to Query (local read)")
				fieldDescriptions, resultRows, err := globalStore.Query(v.String)
				if err != nil {
					slog.Error("Failed to execute query", "error", err)
					err = backend.Send(&pgproto3.ErrorResponse{
						Severity: "ERROR",
						Code:     "42000",
						Message:  err.Error(),
					})
					if err != nil {
						slog.Error("Failed to send ErrorResponse", "error", err)
					}
					err = backend.Send(&pgproto3.ReadyForQuery{
						TxStatus: 'I',
					})
					if err != nil {
						slog.Error("Failed to send ReadyForQuery", "error", err)
					}
					return
				}

				err = backend.Send(&pgproto3.RowDescription{
					Fields: fieldDescriptions,
				})
				if err != nil {
					slog.Error("Failed to send RowDescription", "error", err)
					return
				}

				for _, row := range resultRows {
					err = backend.Send(&pgproto3.DataRow{
						Values: row,
					})
					if err != nil {
						slog.Error("Failed to send DataRow", "error", err)
						return
					}
				}

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte("SELECT"),
				})
				if err != nil {
					slog.Error("Failed to send CommandComplete", "error", err)
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'I',
				})
				if err != nil {
					slog.Error("Failed to send ReadyForQuery", "error", err)
					return
				}
			}

		case *pgproto3.Terminate:
			slog.Info("Client closing connection")
			return

		default:
			slog.Warn("Unknown message type", "type", msg)
		}
	}
}

func startAdminServer(port int, nodeID string, raftPort int) {
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
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

		f := globalConsensus.Raft().AddVoter(
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
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		leader := globalConsensus.Raft().Leader()
		state := globalConsensus.Raft().State()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id": nodeID,
			"leader":  string(leader),
			"state":   state.String(),
		})
	})

	addr := fmt.Sprintf(":%d", port)
	slog.Info("Starting admin HTTP server", "addr", addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Admin server failed", "error", err)
	}
}

func joinCluster(leaderAddr, nodeID, raftAddr string) error {
	req := map[string]string{
		"node_id":   nodeID,
		"raft_addr": raftAddr,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal join request: %w", err)
	}

	joinURL := fmt.Sprintf("%s/join", leaderAddr)
	slog.Info("Sending join request", "url", joinURL, "node_id", nodeID, "raft_addr", raftAddr)

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
