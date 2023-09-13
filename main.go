package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "tx-demo"
)

var ctx context.Context

func main() {
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	ctx = context.Background()

	conn1, err := pgxpool.New(ctx, connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn1.Close()

	conn2, err := pgxpool.New(ctx, connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn2.Close()

	type ReadPhenomena struct {
		name            string
		isolationLevels []string
		testFunction    func(conn1, conn2 *pgxpool.Pool, isolationLevel string)
	}

	phenomenas := []ReadPhenomena{
		{
			name:            "Dirty read (insert)",
			isolationLevels: []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"},
			testFunction:    dirtyReadInsert,
		},
		{
			name:            "Dirty read (update)",
			isolationLevels: []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"},
			testFunction:    dirtyReadUpdate,
		},
	}

	for _, phenomena := range phenomenas {
		fmt.Printf("%s\n", phenomena.name)
		for _, isolationLevel := range phenomena.isolationLevels {
			fmt.Printf("\nIsolation level - %s\n", isolationLevel)
			seedDb(conn1)
			phenomena.testFunction(conn1, conn2, isolationLevel)
			printTable(conn1)
		}
		fmt.Printf("\n---\n\n")
	}
}

func printTable(conn *pgxpool.Pool) {
	fmt.Printf("Final table state:\n")
	rows, _ := conn.Query(ctx, "SELECT id, name, balance, group_id FROM users ORDER BY id")
	for rows.Next() {
		var name []byte
		var id, balance, group_id int
		_ = rows.Scan(&id, &name, &balance, &group_id)
		fmt.Printf("%2d | %10s | %5d | %d\n", id, name, balance, group_id)
	}
}
