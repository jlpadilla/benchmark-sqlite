package main

import (
	"database/sql"
	"fmt"
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
		database.Prepare("CREATE TABLE IF NOT EXISTS resources (id TEXT, name TEXT, data TEXT)")
	statement.Exec()
	database.Prepare("BEGIN TRANSACTION")
	statement, _ =
		database.Prepare("INSERT INTO resources (id, name, data) VALUES (?, ?, ?)")
	var uid string
	start := time.Now()

	for i := 0; i < TOTAL_RECORDS; i++ {
		// Remember UID to use in query.
		uid = gofakeit.UUID()

		// This is hard to read but faster than JSON marshal.
		_, err := statement.Exec(uid, fmt.Sprintf("name-%d", i),
			fmt.Sprintf(`{"kind":%q,"counter":%d,"number":%d,"boolean":%t,"beer":%q,"car":%q,"color":%q,"city":%q,"label":%s}`,
				gofakeit.Color(),
				i,
				gofakeit.Number(1, 999999),
				gofakeit.Bool(),
				gofakeit.BeerName(),
				gofakeit.CarModel(),
				gofakeit.Color(),
				gofakeit.City(),
				`["label1=value1","label2=value2","label3=value3","label4=value4","label5=value5"]`,
			))

		// ALTERNATIVE: This is easier to read but JSON marshal makes it slower.
		//
		// record := map[string]interface{}{
		// 	"kind":    gofakeit.Color(),
		// 	"counter": i,
		// 	"number":  gofakeit.Number(1, 9999),
		// 	"bool":    gofakeit.Bool(),
		// 	"beer":    gofakeit.BeerName(),
		// 	"car":     gofakeit.CarModel(),
		// 	"color":   gofakeit.Color(),
		// 	"city":    gofakeit.City(),
		// 	"company": gofakeit.Company(),
		// 	"label": []string{"label1=value1", "label2=value2", "label3=value3", "label4=value4", "label5=value5", gofakeit.Fruit()},
		// }
		// jsonData, _ := json.Marshal(record)
		// statement.Exec(i, fmt.Sprintf("name-%d", i), jsonData)

		if err != nil {
			fmt.Println("Error inserting:", err)
		}
	}

	database.Prepare("COMMIT TRANSACTION")
	fmt.Printf("Insert %d records took %v \n\n", TOTAL_RECORDS, time.Since(start))

	// Benchmark queries
	fmt.Println("DESCRIPTION: Find a record using the UID")
	benchmarkQuery(database, fmt.Sprintf("SELECT id, data FROM resources WHERE id='%s'", uid))

	fmt.Println("\nDESCRIPTION: Find records with counter less than 5")
	benchmarkQuery(database, "SELECT id, data from resources where json_extract(data, \"$.counter\")<=5")

	fmt.Println("\nDESCRIPTION: Find records with a city name containing `New`")
	benchmarkQuery(database, "SELECT id, data from resources where json_extract(data, \"$.city\") LIKE 'New%' LIMIT 10")

	fmt.Println("\nDESCRIPTION: Find all the values for the field 'color'")
	benchmarkQuery(database, "SELECT DISTINCT json_extract(resources.data, '$.color') as color from resources ORDER BY color ASC")

	fmt.Println("\nWon't exit so I can mesure memory.  TODO: Print memory stats before exiting.")
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

const PRINT_RESULTS bool = false

func benchmarkQuery(database *sql.DB, q string) {
	startQuery := time.Now()
	rows, queryError := database.Query(q)
	if queryError != nil {
		// FIXME: I see this error, but rows has the correct results.
		fmt.Println("Error executing query: ", queryError)
	}

	fmt.Println("QUERY      : ", q)
	fmt.Println("TIME       : ", time.Since(startQuery))
	if PRINT_RESULTS {
		fmt.Println("RESULTS    :")
		var data, id string
		for rows.Next() {
			err := rows.Scan(&id, &data)
			if err != nil {
				rows.Scan(&data)
			}
			fmt.Println("\t", id, data)
			// } else {
			// 	fmt.Println(id)
			// }
		}
		rows.Close()
	} else {
		fmt.Println("RESULTS    :  To include serults set PRINT_RESULTS=true")
	}

}
