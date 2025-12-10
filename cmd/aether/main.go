package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/bit2swaz/aether/internal/store"
	"github.com/jackc/pgproto3/v2"
)

var globalStore *store.Store

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var err error
	globalStore, err = store.New("aether.db")
	if err != nil {
		panic(err)
	}
	defer globalStore.Close()

	slog.Info("Database initialized")

	err = startServer("0.0.0.0:5432")
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

			fieldDescriptions, resultRows, err := globalStore.Execute(v.String)
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

		case *pgproto3.Terminate:
			slog.Info("Client closing connection")
			return

		default:
			slog.Warn("Unknown message type", "type", msg)
		}
	}
}
