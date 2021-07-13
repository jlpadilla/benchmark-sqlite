package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/mattn/go-sqlite3"
)

const TOTAL_RECORDS int = 500000 // How many records you want to simulate.

func main() {
	fmt.Printf("Started. Generating %d records.\n", TOTAL_RECORDS)
	database, _ :=
		// sql.Open("sqlite3", "./bogo.db")  // Switch this if you want to save to a file
		sql.Open("sqlite3", ":memory:")
	statement, _ :=
		database.Prepare("CREATE TABLE IF NOT EXISTS resource (id INTEGER PRIMARY KEY, name TEXT, data TEXT)")
	statement.Exec()
	database.Prepare("BEGIN TRANSACTION")
	statement, _ =
		database.Prepare("INSERT INTO resource (name, data) VALUES (?, ?)")
	start := time.Now()

	// valueStrings := make([]string, 0) //[]string{}
	for i := 0; i < TOTAL_RECORDS; i++ {

		// TODO: marshall JSON from map[string]interface{}
		statement.Exec(fmt.Sprintf("name-%d", i),
			fmt.Sprintf("{ \"kind\": \"%s\", \"counter\": %d, \"number\": %d, \"boolean\": %t, \"beer\": \"%s\", \"car\": \"%s\", \"color\": \"%s\", \"city\": \"%s\" ,\"label\" : [\"aaa\", \"bbb\"]}",
				gofakeit.Color(), i, gofakeit.Number(1, 999999), gofakeit.Bool(), gofakeit.BeerName(), gofakeit.CarModel(), gofakeit.Color(), gofakeit.City()))

	}
	// sql := fmt.Sprintf("INSERT INTO resource (name, namespace) VALUES %s", strings.Join(valueStrings, ","))

	database.Prepare("COMMIT TRANSACTION")
	fmt.Printf("Insert %d records took %v \n\n", TOTAL_RECORDS, time.Since(start))

	// Query
	startQuery := time.Now()
	rows, queryError :=
		// database.Query("SELECT id, data FROM resource WHERE id=?", gofakeit.Number(1, TOTAL_RECORDS))

		// database.Query("SELECT id, data from resource where json_extract(data, \"$.counter\")<=5")
		// database.Query("SELECT id, data from resource where json_extract(data, \"$.color\")='Green' LIMIT 10")
		database.Query("SELECT id, data from resource where json_extract(data, \"$.city\") LIKE 'New%' LIMIT 10")

	if queryError != nil {
		fmt.Println("Query Error", queryError)
	}
	var id int
	fmt.Println("Query JSON key took: ", time.Since(startQuery))
	var data string
	for rows.Next() {
		rows.Scan(&id, &data)
		fmt.Println(strconv.Itoa(id) + ": " + " " + data)
	}

	fmt.Println("\nWon't exit so I can mesure memory.")
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
