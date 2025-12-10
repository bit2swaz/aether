package main

import (
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/bit2swaz/aether/internal/store"
	"github.com/jackc/pgproto3/v2"
)

func TestHandshakeAndQuery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	defer os.Remove("test_handshake.db")
	defer os.Remove("test_handshake.db-shm")
	defer os.Remove("test_handshake.db-wal")

	var err error
	globalStore, err = store.New("test_handshake.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer globalStore.Close()

	serverAddr := "127.0.0.1:5433"

	go func() {
		err := startServer(serverAddr)
		if err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "postgres",
			"database": "postgres",
		},
	}
	err = frontend.Send(startupMsg)
	if err != nil {
		t.Fatalf("Failed to send StartupMessage: %v", err)
	}

	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive AuthenticationOk: %v", err)
	}
	if _, ok := msg.(*pgproto3.AuthenticationOk); !ok {
		t.Fatalf("Expected AuthenticationOk, got %T", msg)
	}
	t.Log("Received AuthenticationOk")

	receivedServerVersion := false
	receivedClientEncoding := false
	receivedReadyForQuery := false

	for !receivedReadyForQuery {
		msg, err = frontend.Receive()
		if err != nil {
			t.Fatalf("Failed to receive message: %v", err)
		}

		switch v := msg.(type) {
		case *pgproto3.ParameterStatus:
			t.Logf("Received ParameterStatus: %s = %s", v.Name, v.Value)
			if v.Name == "server_version" {
				receivedServerVersion = true
				if v.Value != "14.0" {
					t.Errorf("Expected server_version '14.0', got '%s'", v.Value)
				}
			}
			if v.Name == "client_encoding" {
				receivedClientEncoding = true
				if v.Value != "UTF8" {
					t.Errorf("Expected client_encoding 'UTF8', got '%s'", v.Value)
				}
			}
		case *pgproto3.ReadyForQuery:
			t.Logf("Received ReadyForQuery with TxStatus '%c'", v.TxStatus)
			if v.TxStatus != 'I' {
				t.Errorf("Expected TxStatus 'I', got '%c'", v.TxStatus)
			}
			receivedReadyForQuery = true
		default:
			t.Logf("Received unexpected message during handshake: %T", msg)
		}
	}

	if !receivedServerVersion {
		t.Error("Did not receive server_version ParameterStatus")
	}
	if !receivedClientEncoding {
		t.Error("Did not receive client_encoding ParameterStatus")
	}

	t.Log("Handshake completed successfully")

	query := &pgproto3.Query{
		String: "SELECT 1",
	}
	err = frontend.Send(query)
	if err != nil {
		t.Fatalf("Failed to send Query: %v", err)
	}
	t.Log("Sent query: SELECT 1")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive RowDescription: %v", err)
	}
	rowDesc, ok := msg.(*pgproto3.RowDescription)
	if !ok {
		t.Fatalf("Expected RowDescription, got %T", msg)
	}
	if len(rowDesc.Fields) != 1 {
		t.Fatalf("Expected 1 field in RowDescription, got %d", len(rowDesc.Fields))
	}
	if string(rowDesc.Fields[0].Name) != "1" {
		t.Errorf("Expected field name '1', got '%s'", string(rowDesc.Fields[0].Name))
	}
	if rowDesc.Fields[0].DataTypeOID != 25 {
		t.Errorf("Expected DataTypeOID 25 (Text), got %d", rowDesc.Fields[0].DataTypeOID)
	}
	t.Log("Received RowDescription")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive DataRow: %v", err)
	}
	dataRow, ok := msg.(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("Expected DataRow, got %T", msg)
	}
	if len(dataRow.Values) != 1 {
		t.Fatalf("Expected 1 value in DataRow, got %d", len(dataRow.Values))
	}
	if string(dataRow.Values[0]) != "1" {
		t.Errorf("Expected '1', got '%s'", string(dataRow.Values[0]))
	}
	t.Log("Received DataRow with value '1'")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive CommandComplete: %v", err)
	}
	cmdComplete, ok := msg.(*pgproto3.CommandComplete)
	if !ok {
		t.Fatalf("Expected CommandComplete, got %T", msg)
	}
	if string(cmdComplete.CommandTag) != "SELECT" {
		t.Errorf("Expected CommandTag 'SELECT', got '%s'", string(cmdComplete.CommandTag))
	}
	t.Log("Received CommandComplete")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive ReadyForQuery: %v", err)
	}
	readyMsg, ok := msg.(*pgproto3.ReadyForQuery)
	if !ok {
		t.Fatalf("Expected ReadyForQuery, got %T", msg)
	}
	if readyMsg.TxStatus != 'I' {
		t.Errorf("Expected TxStatus 'I', got '%c'", readyMsg.TxStatus)
	}
	t.Log("Received ReadyForQuery after query")

	terminate := &pgproto3.Terminate{}
	err = frontend.Send(terminate)
	if err != nil {
		t.Fatalf("Failed to send Terminate: %v", err)
	}
	t.Log("Sent Terminate message")

	conn.Close()
	t.Log("Connection closed")

	t.Log("=== Test passed: Full handshake and query cycle with real database ===")
}

func TestRealDatabaseQuery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	defer os.Remove("test_integration.db")
	defer os.Remove("test_integration.db-shm")
	defer os.Remove("test_integration.db-wal")

	var err error
	globalStore, err = store.New("test_integration.db")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer globalStore.Close()

	_, _, err = globalStore.Query("CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, _, err = globalStore.Query("INSERT INTO test_table VALUES (1, 'Aether')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	serverAddr := "127.0.0.1:5434"

	go func() {
		err := startServer(serverAddr)
		if err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "postgres",
			"database": "postgres",
		},
	}
	err = frontend.Send(startupMsg)
	if err != nil {
		t.Fatalf("Failed to send StartupMessage: %v", err)
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("Failed during handshake: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	t.Log("Handshake completed")

	query := &pgproto3.Query{
		String: "SELECT * FROM test_table",
	}
	err = frontend.Send(query)
	if err != nil {
		t.Fatalf("Failed to send Query: %v", err)
	}

	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive RowDescription: %v", err)
	}
	rowDesc, ok := msg.(*pgproto3.RowDescription)
	if !ok {
		t.Fatalf("Expected RowDescription, got %T", msg)
	}
	if len(rowDesc.Fields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(rowDesc.Fields))
	}
	if string(rowDesc.Fields[0].Name) != "id" {
		t.Errorf("Expected first column 'id', got '%s'", string(rowDesc.Fields[0].Name))
	}
	if string(rowDesc.Fields[1].Name) != "name" {
		t.Errorf("Expected second column 'name', got '%s'", string(rowDesc.Fields[1].Name))
	}
	t.Log("Received RowDescription with correct columns")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive DataRow: %v", err)
	}
	dataRow, ok := msg.(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("Expected DataRow, got %T", msg)
	}
	if len(dataRow.Values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(dataRow.Values))
	}
	if string(dataRow.Values[0]) != "1" {
		t.Errorf("Expected id '1', got '%s'", string(dataRow.Values[0]))
	}
	if string(dataRow.Values[1]) != "Aether" {
		t.Errorf("Expected name 'Aether', got '%s'", string(dataRow.Values[1]))
	}
	t.Log("Received DataRow with correct data: id=1, name=Aether")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive CommandComplete: %v", err)
	}
	if _, ok := msg.(*pgproto3.CommandComplete); !ok {
		t.Fatalf("Expected CommandComplete, got %T", msg)
	}
	t.Log("Received CommandComplete")

	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("Failed to receive ReadyForQuery: %v", err)
	}
	if _, ok := msg.(*pgproto3.ReadyForQuery); !ok {
		t.Fatalf("Expected ReadyForQuery, got %T", msg)
	}
	t.Log("Received ReadyForQuery")

	terminate := &pgproto3.Terminate{}
	err = frontend.Send(terminate)
	if err != nil {
		t.Fatalf("Failed to send Terminate: %v", err)
	}

	conn.Close()
	t.Log("=== Real database query test passed ===")
}
