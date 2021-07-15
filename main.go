package main

import (
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/mattn/go-sqlite3"
)

const TOTAL_RECORDS int = 500000 // How many records you want to simulate.
const PRINT_RESULTS bool = false
const REPEAT int = 3

func main() {
	fmt.Printf("Generating %d records.\n\n", TOTAL_RECORDS)

	os.Remove("./benchmark.db") // Delete database file if it exists.
	database, _ :=
		sql.Open("sqlite3", "./benchmark.db") // Switch this if you want to save to a file
		// sql.Open("sqlite3", ":memory:")

	database.Exec("CREATE TABLE IF NOT EXISTS resources (id TEXT, name TEXT, data TEXT)")
	// IMPORTANT: With BEGIN and COMMIT TRANSACTION saving inserts to a file is comparable to in memory.
	database.Exec("BEGIN TRANSACTION")
	statement, _ := database.Prepare("INSERT INTO resources (id, name, data) VALUES (?, ?, ?)")

	var uid string
	start := time.Now()

	for i := 0; i < TOTAL_RECORDS; i++ {
		uid = gofakeit.UUID() // Remember this UID to use later in the query.

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
			fmt.Println("Error inserting record:", err)
		}
	}

	database.Exec("COMMIT TRANSACTION")
	fmt.Printf("Insert %d resource table records took %v \n", TOTAL_RECORDS, time.Since(start))
	PrintMemUsage()

	fmt.Println("Adding index to resources table")
	start = time.Now()
	database.Exec("CREATE UNIQUE INDEX idx_resources_name ON resources (name)")
	database.Exec("COMMIT TRANSACTION")

	fmt.Printf("Adding index to resource table with %d records took %v \n\n", TOTAL_RECORDS, time.Since(start))

	database.Exec("CREATE TABLE IF NOT EXISTS relations (id TEXT, relatedID TEXT)")
	database.Exec("COMMIT TRANSACTION")

	database.Exec("BEGIN TRANSACTION")
	statement, _ = database.Prepare("INSERT INTO relations (id, relatedID) VALUES (?, ?)")

	start = time.Now()
	relations := [11]int{-5, 2, 2, 2, 1, 1, 4, -6, -7, -8, 0}
	for i := 21; i <= TOTAL_RECORDS; i++ {
		j := (i % 10)
		// fmt.Println("i: ", i, " j: ", j, "relations[j]: ", relations[j])
		uid := lookUpUID(database, fmt.Sprintf("name-%d", i))
		relatedID := lookUpUID(database, fmt.Sprintf("name-%d", (i+relations[j])))
		// fmt.Println("i: ", i, " uid: ", uid, "relatedID: ", relatedID)

		_, err := statement.Exec(uid, relatedID)

		if err != nil {
			fmt.Println("Error inserting record:", err)
		}
	}
	fmt.Printf("Inserting relations table with %d records took %v \n", TOTAL_RECORDS-20, time.Since(start))
	database.Exec("DROP INDEX IF EXISTS idx_resources_name")

	database.Exec("COMMIT TRANSACTION")

	// Benchmark queries
	fmt.Println("BENCHMARK QUERIES")
	fmt.Println("\nDESCRIPTION: Find a record using the UID")
	benchmarkQuery(database, fmt.Sprintf("SELECT id, data FROM resources WHERE id='%s'", uid))

	fmt.Println("\nDESCRIPTION: Find records with counter less than 5")
	benchmarkQuery(database, "SELECT id, data from resources where json_extract(data, \"$.counter\") <= 5 LIMIT 5")

	fmt.Println("\nDESCRIPTION: Find records with a city name containing `New`")
	benchmarkQuery(database, "SELECT id, data from resources where json_extract(data, \"$.city\") LIKE 'new%' LIMIT 10")

	fmt.Println("\nDESCRIPTION: Find all the values for the field 'color'")
	benchmarkQuery(database, "SELECT DISTINCT json_extract(resources.data, '$.color') as color from resources ORDER BY color ASC")

	fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind'")
	benchmarkQuery(database, "SELECT json_extract(resources.data, '$.kind') as kind , count(json_extract(resources.data, '$.kind')) as count FROM resources GROUP BY kind ORDER BY count DESC")

	fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' using subquery")
	benchmarkQuery(database, "SELECT kind, count(*) as count FROM (SELECT json_extract(resources.data, '$.kind') as kind FROM resources) GROUP BY kind ORDER BY count DESC")

	PrintMemUsage()
	fmt.Println("\nWon't exit so I can get memory usage from OS.")
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func lookUpUID(database *sql.DB, name string) string {
	q := fmt.Sprintf("SELECT id FROM resources WHERE name = '%s'", name)

	rows, queryError := database.Query(q)
	if queryError != nil {
		fmt.Println("Error executing query: ", queryError)
	}
	defer rows.Close()
	var id string
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			rows.Scan(&id)
		}
	}
	return id
}
func benchmarkQuery(database *sql.DB, q string) {

	times := make([]time.Duration, REPEAT)
	var sum time.Duration
	sum = 0
	// startQuery := time.Now()

	var rows *sql.Rows
	var queryError error
	for i := 0; i < REPEAT; i++ {
		startQuery := time.Now()
		rows, queryError = database.Query(q)
		if queryError != nil {
			fmt.Println("Error executing query: ", queryError)
		}
		times[i] = time.Duration(time.Since(startQuery))

		sum = sum + times[i]
		if i == REPEAT-1 {
			defer rows.Close()
		} else {
			rows.Close()
		}
	}

	fmt.Println("QUERY      : ", q)
	fmt.Println("Average TIME       : ", time.Duration(sum.Nanoseconds()/int64(len(times))))
	// fmt.Println("TIMES       : ", times)

	if PRINT_RESULTS {
		fmt.Println("RESULTS    :")
		var data, id string
		for rows.Next() {
			err := rows.Scan(&id, &data)
			if err != nil {
				rows.Scan(&data)
			}
			fmt.Println("\t", id, data)
		}
	} else {
		fmt.Println("RESULTS    :  To print results set PRINT_RESULTS=true")
	}
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Println("\nMEMORY USAGE:")
	fmt.Printf("\tAlloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\n\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\n\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\n\tNumGC = %v\n\n", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
