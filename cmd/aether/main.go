package main

import (
	"bytes"
	"encoding/binary"
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

	"github.com/bit2swaz/aether/internal/api"
	"github.com/bit2swaz/aether/internal/consensus"
	"github.com/bit2swaz/aether/internal/metrics"
	"github.com/bit2swaz/aether/internal/store"
	"github.com/jackc/pgproto3/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Initialize Prometheus metrics
	metrics.Init()
	go startMetricsServer(9090)
	slog.Info("Metrics server starting on :9090")

	// Track Raft state and commit index
	go func() {
		// Give Raft a moment to initialize
		time.Sleep(500 * time.Millisecond)

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		// Update immediately on start
		updateMetrics := func() {
			state := globalConsensus.Raft().State()
			metrics.SetRaftState(int(state))

			commitIndex := globalConsensus.Raft().LastIndex()
			metrics.SetCommitIndex(commitIndex)
		}

		updateMetrics() // Initial update

		for range ticker.C {
			updateMetrics()
		}
	}()

	// Simple bootstrap wait
	if *nodeID == "node1" {
		// We don't strictly panic here because in a restart scenario,
		// we might wait for peers. Just a warning log is safer.
		go func() {
			err := globalConsensus.WaitForLeader(30 * time.Second)
			if err != nil {
				slog.Warn("Leader election taking a while...", "error", err)
			} else {
				slog.Info("Leader elected")
			}
		}()
	}

	adminServer := api.NewServer(globalConsensus, *nodeID, *httpPort)
	go adminServer.Start()

	// Give the admin server a moment to start
	if *nodeID == "node1" {
		time.Sleep(500 * time.Millisecond)
	}

	if *joinAddr != "" {
		// Retry logic for joining the cluster
		go func() {
			maxRetries := 20
			for i := 0; i < maxRetries; i++ {
				// Wait a bit before first attempt to let leader settle
				time.Sleep(time.Duration(1000) * time.Millisecond)

				err = api.JoinCluster(*joinAddr, *nodeID, fmt.Sprintf("127.0.0.1:%d", *raftPort))
				if err == nil {
					slog.Info("Successfully joined cluster", "leader", *joinAddr)
					return
				}

				slog.Warn("Join attempt failed, retrying...", "attempt", i+1, "error", err)
			}
			slog.Error("Failed to join cluster after max retries")
		}()
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

	metrics.IncConnection()
	defer metrics.DecConnection()

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
		TxStatus: 'I', // 'I' = Idle
	})
	if err != nil {
		slog.Error("Failed to send ReadyForQuery", "error", err)
		return
	}

	slog.Info("PostgreSQL handshake completed successfully")

	var txnBuffer []string
	inTxn := false

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

			// --- FIX: USE HASPREFIX FOR TRANSACTIONS ---
			if strings.HasPrefix(sqlUpper, "BEGIN") {
				inTxn = true
				txnBuffer = nil
				slog.Info("Transaction started")

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte("BEGIN"),
				})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'T', // 'T' = In Transaction block
				})
				if err != nil {
					return
				}
				continue
			}

			if strings.HasPrefix(sqlUpper, "ROLLBACK") {
				inTxn = false
				txnBuffer = nil
				slog.Info("Transaction rolled back")

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte("ROLLBACK"),
				})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'I', // 'I' = Idle
				})
				if err != nil {
					return
				}
				continue
			}

			if strings.HasPrefix(sqlUpper, "COMMIT") {
				slog.Info("Committing transaction", "statements", len(txnBuffer))

				if len(txnBuffer) > 0 {
					err = globalStore.ReplicateBatch(txnBuffer)
					if err != nil {
						slog.Error("Failed to replicate batch", "error", err)
						err = backend.Send(&pgproto3.ErrorResponse{
							Severity: "ERROR",
							Code:     "42000",
							Message:  err.Error(),
						})
						if err != nil {
							return
						}

						// On error, we are back to Idle
						err = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						if err != nil {
							return
						}
						return
					}
				}

				inTxn = false
				txnBuffer = nil

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte("COMMIT"),
				})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'I', // 'I' = Idle
				})
				if err != nil {
					return
				}
				continue
			}
			// --- FIX END ---

			// Handling buffered commands (inside transaction)
			if inTxn {
				txnBuffer = append(txnBuffer, v.String)
				slog.Info("Buffered statement in transaction", "count", len(txnBuffer))

				commandTag := getCommandTag(sqlUpper)

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte(commandTag),
				})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{
					TxStatus: 'T', // Keep status 'T'
				})
				if err != nil {
					return
				}
				continue
			}

			// Handling atomic commands (outside transaction)
			isWrite := strings.HasPrefix(sqlUpper, "INSERT") ||
				strings.HasPrefix(sqlUpper, "UPDATE") ||
				strings.HasPrefix(sqlUpper, "DELETE") ||
				strings.HasPrefix(sqlUpper, "CREATE") ||
				strings.HasPrefix(sqlUpper, "DROP") ||
				strings.HasPrefix(sqlUpper, "ALTER")

			if isWrite {
				slog.Info("Routing to Replicate (distributed write)")
				metrics.IncSQL("write")
				err = globalStore.Replicate(v.String)
				if err != nil {
					slog.Error("Failed to replicate command", "error", err)
					backend.Send(&pgproto3.ErrorResponse{
						Severity: "ERROR",
						Code:     "42000",
						Message:  err.Error(),
					})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					return
				}

				commandTag := getCommandTag(sqlUpper)

				err = backend.Send(&pgproto3.CommandComplete{
					CommandTag: []byte(commandTag),
				})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err != nil {
					return
				}

			} else {
				// READ Query
				slog.Info("Routing to Query (local read)")
				metrics.IncSQL("read")
				fieldDescriptions, resultRows, err := globalStore.Query(v.String)
				if err != nil {
					slog.Error("Failed to execute query", "error", err)
					backend.Send(&pgproto3.ErrorResponse{
						Severity: "ERROR",
						Code:     "42000",
						Message:  err.Error(),
					})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					return
				}

				err = backend.Send(&pgproto3.RowDescription{Fields: fieldDescriptions})
				if err != nil {
					return
				}

				for _, row := range resultRows {
					err = backend.Send(&pgproto3.DataRow{Values: row})
					if err != nil {
						return
					}
				}

				err = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
				if err != nil {
					return
				}

				err = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err != nil {
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

// Helper to guess command tags for Postgres
func getCommandTag(sql string) string {
	if strings.HasPrefix(sql, "INSERT") {
		return "INSERT 0 1"
	}
	if strings.HasPrefix(sql, "UPDATE") {
		return "UPDATE 1"
	}
	if strings.HasPrefix(sql, "DELETE") {
		return "DELETE 1"
	}
	if strings.HasPrefix(sql, "CREATE") {
		return "CREATE TABLE"
	}
	if strings.HasPrefix(sql, "DROP") {
		return "DROP TABLE"
	}
	if strings.HasPrefix(sql, "ALTER") {
		return "ALTER TABLE"
	}
	return "OK"
}

func startMetricsServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	http.Handle("/metrics", promhttp.Handler())

	slog.Info("Metrics server listening", "address", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("Metrics server failed", "error", err)
	}
}
