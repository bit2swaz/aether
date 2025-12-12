package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "postgres://admin:password@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Disable connection pooling to ensure each statement is separate
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS flood (id SERIAL, data TEXT);")
	if err != nil {
		panic(err)
	}

	fmt.Println("🌊 Flooding Aether (Memory-Safe Mode)...")
	fmt.Println("Inserting 500 rows one at a time, NO transactions...")

	// Memory-safe: 500 rows, one at a time, NO TRANSACTIONS
	totalRows := 500

	for i := 1; i <= totalRows; i++ {
		// Each INSERT is its own Raft log entry
		_, err := db.Exec("INSERT INTO flood (data) VALUES ('payload');")
		if err != nil {
			panic(err)
		}

		// Progress update every 50 rows
		if i%50 == 0 {
			fmt.Printf("\rInserted %d/%d rows...", i, totalRows)
		}

		// Delay every 25 rows to give system time to breathe
		if i%25 == 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	fmt.Println("\n✅ Done. With threshold=50, snapshot should have triggered multiple times.")
}
