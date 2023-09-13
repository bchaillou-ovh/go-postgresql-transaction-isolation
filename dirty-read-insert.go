package main

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func dirtyReadInsert(conn1, conn2 *pgxpool.Pool, isolationLevel string) {
	tx, err := conn1.Begin(ctx)
	if err != nil {
		panic(err)
	}

	// Set isolation level for first connection
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)
	if err != nil {
		fmt.Printf("[Conn1] Err set isolation level: %v\n", err)
	} else {
		fmt.Printf("[Conn1] Isolation level: %s\n", isolationLevel)
	}

	// Insert with first connection
	_, err = tx.Exec(ctx, "INSERT INTO users (name, balance) VALUES ('toto', 10)")
	if err != nil {
		fmt.Printf("[Conn1] Failed to insert toto: %v\n", err)
	}

	// Select with first connection
	var balance int
	row := tx.QueryRow(ctx, "SELECT balance FROM users WHERE name='toto'")
	err = row.Scan(&balance)
	if err != nil {
		fmt.Printf("[Conn1] Err scan select: %v\n", err)
	} else {
		fmt.Printf("[Conn1] toto balance after insert: %d\n", balance)
	}

	// Set isolation level for second connection
	rows, err := conn2.Query(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)
	if err != nil {
		fmt.Printf("[Conn2] Err set isolation level: %v\n", err)
	} else {
		fmt.Printf("[Conn2] Isolation level: %s\n", isolationLevel)
	}
	rows.Close()

	// Select with second connection
	var balance2 int
	row = conn2.QueryRow(ctx, "SELECT balance FROM users WHERE name='toto'")
	err = row.Scan(&balance2)
	if err != nil {
		fmt.Printf("[Conn2] Err scan select: %v\n", err)
	} else {
		fmt.Printf("[Conn2] toto balance after insert: %d\n", balance2)
	}

	// Commit insert of first connection
	if err := tx.Commit(ctx); err != nil {
		fmt.Printf("[Conn1] Failed to commit: %v\n", err)
	}
}
