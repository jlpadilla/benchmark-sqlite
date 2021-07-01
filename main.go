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
		statement.Exec(fmt.Sprintf("name-%d", i), fmt.Sprintf("kind: %s, counter: %d, number: %d, boolean: %t, beer: %s, car: %s, color: %s, city: %s , property1: value1, property2: value2, property3: value3, property4: value4  ",
			gofakeit.Color(), i, gofakeit.Number(1, 999999), gofakeit.Bool(), gofakeit.BeerName(), gofakeit.CarModel(), gofakeit.Color(), gofakeit.City()))
		// valueStrings = append(valueStrings, " (myName, myNamespace)")
	}
	// sql := fmt.Sprintf("INSERT INTO resource (name, namespace) VALUES %s", strings.Join(valueStrings, ","))

	database.Prepare("COMMIT TRANSACTION")
	fmt.Printf("Insert %d records took %v \n", TOTAL_RECORDS, time.Since(start))

	// Query
	startQuery := time.Now()
	rows, _ :=
		database.Query("SELECT id, data FROM resource WHERE id=?", gofakeit.Number(1, TOTAL_RECORDS))
	var id int
	fmt.Println("Query by primary key took: ", time.Since(startQuery))
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
