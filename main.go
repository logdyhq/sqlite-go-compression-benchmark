package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/corpix/uarand"
	"github.com/ddosify/go-faker/faker"
	"github.com/schollz/progressbar/v3"

	sqlitezstd "github.com/jtarchie/sqlitezstd"
	_ "github.com/mattn/go-sqlite3"
)

// LogEntry represents a single NGINX log entry formatted as JSON
type LogEntry struct {
	TimeLocal            string `json:"time_local"`
	RemoteAddr           string `json:"remote_addr"`
	Request              string `json:"request"`
	Status               int    `json:"status"`
	BodyBytesSent        int    `json:"body_bytes_sent"`
	HTTPReferer          string `json:"http_referer"`
	HTTPUserAgent        string `json:"http_user_agent"`
	RequestTime          string `json:"request_time"`
	UpstreamResponseTime string `json:"upstream_response_time"`
	UpstreamAddr         string `json:"upstream_addr"`
	UpstreamStatus       int    `json:"upstream_status"`
}

var methods = []string{"GET", "POST", "PUT", "DELETE"}
var referers = []string{"https://google.com", "https://example.com", "https://referrer.com", ""}
var statuses = []int{200, 201, 301, 400, 401, 403, 404, 500, 502, 503}

func randomChoice[T any](choices []T) T {
	return choices[rand.Intn(len(choices))]
}

func randomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}

func randomDuration() string {
	return fmt.Sprintf("%.3f", rand.Float64()*2)
}

func generateLogEntry() LogEntry {
	faker := faker.NewFaker()
	return LogEntry{
		TimeLocal:            randomTime(),
		RemoteAddr:           randomIP(),
		Request:              fmt.Sprintf("%s %s HTTP/1.1", randomChoice(methods), faker.RandomUrl()),
		Status:               randomChoice(statuses),
		BodyBytesSent:        rand.Intn(10000) + 500,
		HTTPReferer:          randomChoice(referers),
		HTTPUserAgent:        uarand.GetRandom(),
		RequestTime:          randomDuration(),
		UpstreamResponseTime: randomDuration(),
		UpstreamAddr:         randomIP(),
		UpstreamStatus:       randomChoice(statuses),
	}
}

var name = "access_log"

func generateFile() {
	var max = 1_000_000
	f, err := os.OpenFile(name+".json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	bar := progressbar.Default(int64(max))

	for i := 0; i < max; i++ { // Generates 10 random log entries
		entry := generateLogEntry()
		jsonEntry, err := json.Marshal(entry)
		if err != nil {
			fmt.Println("Error generating JSON:", err)
			continue
		}

		bar.Add(1)

		if _, err := f.Write(jsonEntry); err != nil {
			log.Fatal(err)
		}
		if _, err := f.Write([]byte("\n")); err != nil {
			log.Fatal(err)
		}
	}

	bar.Close()
}

func randomTime() string {
	// Define the time span: 6 days ago until now
	now := time.Now()
	sixDaysAgo := now.AddDate(0, 0, -6)

	// Choose a random day within the last 6 days
	randomDays := rand.Intn(7) // 0 to 6 days ago
	chosenDay := sixDaysAgo.AddDate(0, 0, randomDays)

	// Define the 1-hour time window, e.g., 3:00 PM to 4:00 PM
	// Adjust as per your desired time span
	startHour := 9 // 3:00 PM
	startTime := time.Date(chosenDay.Year(), chosenDay.Month(), chosenDay.Day(), startHour, 0, 0, 0, time.Local)
	endTime := startTime.Add(time.Hour) // 1 hour later

	// Generate a random time within that 1-hour span
	randomTime := startTime.Add(time.Duration(rand.Int63n(int64(endTime.Sub(startTime)))))
	return randomTime.Format(time.RFC3339)
}

func readFile(fn func(le LogEntry), maxCount int) {
	file, err := os.Open(name + ".json")
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	// Create a new scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	bar := progressbar.Default(int64(maxCount))
	i := 0
	// Iterate over each line
	for scanner.Scan() {
		i++
		bar.Add(1)
		var logEntry LogEntry
		line := scanner.Text()

		// Parse the JSON line into the struct
		if err := json.Unmarshal([]byte(line), &logEntry); err != nil {
			fmt.Printf("Failed to parse JSON: %v\n", err)
			continue
		}

		fn(logEntry)

		if i == maxCount {
			return
		}
	}

	// Check for any scanner errors
	if err := scanner.Err(); err != nil {
		fmt.Printf("Scanner error: %v\n", err)
	}
}

func createTableColumned(db *sql.DB, addIndexes bool) error {
	query := `
	CREATE TABLE IF NOT EXISTS nginx_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		time_local TEXT,
		remote_addr TEXT,
		request TEXT,
		status INTEGER,
		body_bytes_sent INTEGER,
		http_referer TEXT,
		http_user_agent TEXT,
		request_time TEXT,
		upstream_response_time TEXT,
		upstream_addr TEXT,
		upstream_status INTEGER
	);
	`

	if addIndexes {
		query = query + `
	CREATE INDEX IF NOT EXISTS status_idx
		ON nginx_logs (status);
	CREATE INDEX IF NOT EXISTS time_local_idx
		ON nginx_logs (time_local);`
	}
	_, err := db.Exec(query)
	return err
}

func createTableJson(db *sql.DB, addIndexes bool) error {
	query := `
	CREATE TABLE IF NOT EXISTS nginx_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		json_log TEXT
	);
	`

	if addIndexes {
		query = query + `
		
		alter table nginx_logs
		add column status TEXT
		as (json_extract(json_log, '$.status'));
	alter table nginx_logs
		add column time_local TEXT
		as (json_extract(json_log, '$.time_local'));

CREATE INDEX IF NOT EXISTS status_idx
		  ON nginx_logs (status);
	CREATE INDEX IF NOT EXISTS time_local_idx
		  ON nginx_logs (time_local);
	`
	}

	_, err := db.Exec(query)
	return err
}

func insertLogEntries(db *sql.DB, entries []LogEntry, jsonFormat bool) {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	query := `
	INSERT INTO nginx_logs (
		time_local, remote_addr, request, status, body_bytes_sent,
		http_referer, http_user_agent, request_time, upstream_response_time,
		upstream_addr, upstream_status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	if jsonFormat {
		query = `
		INSERT INTO nginx_logs (
			json_log
		) VALUES (?);`
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback() // Rollback transaction on error
		log.Fatal(err)
	}
	defer stmt.Close()

	// Iterate over the entries and execute the prepared statement for each entry
	for _, entry := range entries {
		if jsonFormat {
			b, _ := json.Marshal(entry)
			_, err = stmt.Exec(string(b))
		} else {
			_, err = stmt.Exec(entry.TimeLocal, entry.RemoteAddr, entry.Request, entry.Status, entry.BodyBytesSent,
				entry.HTTPReferer, entry.HTTPUserAgent, entry.RequestTime, entry.UpstreamResponseTime,
				entry.UpstreamAddr, entry.UpstreamStatus)
		}
		if err != nil {
			tx.Rollback() // Rollback transaction on error
			log.Fatal(err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func insertLogEntry(db *sql.DB, entry LogEntry) {
	query := `
	INSERT INTO nginx_logs (
		time_local, remote_addr, request, status, body_bytes_sent,
		http_referer, http_user_agent, request_time, upstream_response_time,
		upstream_addr, upstream_status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	_, err := db.Exec(query, entry.TimeLocal, entry.RemoteAddr, entry.Request, entry.Status, entry.BodyBytesSent,
		entry.HTTPReferer, entry.HTTPUserAgent, entry.RequestTime, entry.UpstreamResponseTime,
		entry.UpstreamAddr, entry.UpstreamStatus)

	if err != nil {
		panic(err)
	}
}

func openDbAndLoad(dbPath string, json bool, addIndexes bool, dbPageSize int) *sql.DB {
	log.Println("Opening DB", dbPath)
	db, err := sql.Open("sqlite3", "./"+dbPath)
	if err != nil {
		log.Fatal(err)
	}

	db.Exec(`
	PRAGMA page_size = ` + strconv.Itoa(dbPageSize) + `;
	PRAGMA journal_mode = OFF;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = OFF;
	PRAGMA cache_size = 1000000000;
	PRAGMA foreign_keys = false;
	PRAGMA temp_store = memory;
	`)
	// db.SetMaxOpenConns(1)
	// Create table

	if json {
		if err := createTableJson(db, addIndexes); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := createTableColumned(db, addIndexes); err != nil {
			log.Fatal(err)
		}
	}

	return db
}

func readFileAndInsert(db *sql.DB, numRecords int, json bool) {
	leng := 150
	count := 0
	bulk := []LogEntry{}
	readFile(func(le LogEntry) {

		count++
		bulk = append(bulk, le)
		if len(bulk) == leng {
			insertLogEntries(db, bulk, json)
			bulk = []LogEntry{}
		}

	}, numRecords)

	insertLogEntries(db, bulk, json)
}

func rewriteJson() {
	f, err := os.OpenFile(name+".txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	readFile(func(le LogEntry) {
		if _, err := f.Write([]byte(
			le.TimeLocal + "|" +
				le.RemoteAddr + "|" +
				le.Request + "|" +
				strconv.Itoa(le.Status) + "|" +
				strconv.Itoa(le.BodyBytesSent) + "|" +
				le.HTTPReferer + "|" +
				le.HTTPUserAgent + "|" +
				le.RequestTime + "|" +
				le.UpstreamResponseTime + "|" +
				le.UpstreamAddr + "|" +
				strconv.Itoa(le.UpstreamStatus) +
				"\n",
		)); err != nil {
			log.Fatal(err)
		}
	}, 1_000_000)
}

func openCompressedDb(dbPath string) *sql.DB {
	log.Println("Opening compressed DB", dbPath)
	initErr := sqlitezstd.Init()
	if initErr != nil {
		panic(fmt.Sprintf("Failed to initialize SQLiteZSTD: %s", initErr))
	}

	db, err := sql.Open("sqlite3", dbPath+".zst?vfs=zstd")
	if err != nil {
		panic(fmt.Sprintf("Failzed to open database: %s", err))
	}

	return db
}

type queryResult struct {
	uncompressed time.Duration
	compressed   time.Duration
	timesSlower  float64
	query        string
}

func benchmark(db *sql.DB, dbCompressed *sql.DB, json bool, addIndexes bool, queryRepeat int) []queryResult {

	log.Println("Optimizing DB")
	db.Exec("PRAGMA optimize;")

	log.Println("Optimizing compressed DB")
	dbCompressed.Exec("PRAGMA optimize;")

	queries := []string{
		"select count(*), 1 from nginx_logs",
		"select count(*), 1 from nginx_logs where status = 200",
		"select status, count(*) from nginx_logs group by status",
		"select status, count(*) from nginx_logs where status = 200 group by status",
	}

	if json {

		statusCol := `json_log->'status'`
		if addIndexes {
			statusCol = `status`
		}

		queries = []string{
			"select count(*), 1 from nginx_logs",
			"select count(*), 1 from nginx_logs where " + statusCol + " = 200",
			"select json_log->'status', count(*) from nginx_logs group by " + statusCol + "",
			"select json_log->'status', count(*) from nginx_logs where " + statusCol + " = 200 group by " + statusCol + "",
		}
	}

	log.Println("Querying")
	results := []queryResult{}
	for _, q := range queries {

		var sum time.Duration
		var sumCompressed time.Duration
		i := queryRepeat
		for i > 0 {
			var r1 int
			var r2 int
			var r3 int
			var r4 int

			res2, err := db.Query(q)
			if err != nil {
				panic(err)
			}
			resCompressed2, err := dbCompressed.Query("PRAGMA temp_store = memory; " + q)
			if err != nil {
				panic(err)
			}

			t1 := time.Now()
			if res2.Next() {
				res2.Scan(&r1, &r2)
			}
			sum = sum + time.Since(t1)

			if res2.Err() != nil {
				panic(res2.Err())
			}

			t2 := time.Now()
			if resCompressed2.Next() {
				resCompressed2.Scan(&r3, &r4)
			}
			sumCompressed = sumCompressed + time.Since(t2)

			// log.Println(r1, r2, r3, r4)

			if resCompressed2.Err() != nil {
				panic(resCompressed2.Err())
			}
			i--
		}
		res1 := sum / time.Duration(queryRepeat)
		res2 := sumCompressed / time.Duration(queryRepeat)
		log.Printf("query: %s, uncompressed db: %s, compressed db: %s, times slower: x%f", q, res1, res2, float64(res2)/float64(res1))
		results = append(results, queryResult{
			query:        q,
			uncompressed: res1,
			compressed:   res2,
			timesSlower:  float64(res2) / float64(res1),
		})
	}

	return results

}

type config struct {
	numRecords        []int
	jsonFormat        []bool
	indexes           []bool
	compressionLvl    []int
	compressionChunks []string
	sqlitePageSize    []int
	queryRepeat       int
}

func writeRow(row []string) {
	file, err := os.OpenFile("results.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	file.WriteString(strings.Join(row, ",") + "\n")
	file.Close()
}

func getFileSize(file string) float64 {
	fi, err := os.Stat(file)
	if err != nil {
		panic(err)
	}
	// get the size
	return float64(fi.Size()) / float64(1024) / float64(1024)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg := config{
		numRecords:        []int{1_000_000},
		jsonFormat:        []bool{true},
		indexes:           []bool{true},
		sqlitePageSize:    []int{1},
		compressionLvl:    []int{10},
		compressionChunks: []string{"1024:2048:4096"},
		queryRepeat:       5,
		// numRecords:        []int{1_000_000},
		// jsonFormat:        []bool{true, false},
		// indexes:           []bool{false, true},
		// sqlitePageSize:    []int{4096, 16384, 65536},
		// compressionLvl:    []int{1, 5, 10},
		// compressionChunks: []string{"16:32:64", "64:128:256", "1024:2048:4096"},
	}

	os.WriteFile("results.csv", []byte{}, 0644)
	writeRow([]string{"numRecords", "jsonFormat", "indexes",
		"sqlitePageSize",
		"compressionLvl", "compressionChunks",
		"uncompressedSize", "compressedSize",
		"query", "uncompressedTime", "compressedTime", "timeSlower",
	})

	// generateFile()
	// rewriteJson()

	done := 0
	total := len(cfg.numRecords) * len(cfg.jsonFormat) * len(cfg.indexes) * len(cfg.compressionLvl) * len(cfg.compressionChunks) * len(cfg.sqlitePageSize)
	dbPath := "nginx_logs_columns.db"
	for _, numRecords := range cfg.numRecords {
		for _, jsonFormat := range cfg.jsonFormat {
			for _, indexes := range cfg.indexes {
				for _, sqlitePageSize := range cfg.sqlitePageSize {
					os.Remove(dbPath)
					db := openDbAndLoad(dbPath, jsonFormat, indexes, sqlitePageSize)
					readFileAndInsert(db, numRecords, jsonFormat)

					for _, compressionLvl := range cfg.compressionLvl {
						for _, compressionChunks := range cfg.compressionChunks {

							os.Remove(dbPath + ".zst")
							compressDb(dbPath, compressionLvl, compressionChunks)

							dbCompressed := openCompressedDb(dbPath)

							results := benchmark(db, dbCompressed, jsonFormat, indexes, cfg.queryRepeat)

							for i, res := range results {
								row := []string{
									strconv.Itoa(numRecords), strconv.FormatBool(jsonFormat), strconv.FormatBool(indexes),
									strconv.Itoa(sqlitePageSize),
									strconv.Itoa(compressionLvl), compressionChunks,
									fmt.Sprintf("%.2f", getFileSize(dbPath)), fmt.Sprintf("%.2f", getFileSize(dbPath+".zst")),
									strconv.Itoa(i), strconv.Itoa(int(res.uncompressed.Milliseconds())), strconv.Itoa(int(res.compressed.Milliseconds())),
									fmt.Sprintf("%.2f", res.timesSlower),
								}
								writeRow(row)
								log.Println(done, total, row)
							}
							done++
						}
					}
				}
			}
		}
	}

}

func compressDb(filename string, compressionLvl int, compressionChunks string) {
	// compress

	log.Println("Compressing DB", filename)
	// Build the command
	cmd := exec.Command("go", "run", "github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek",
		"-c", compressionChunks,
		"-q", strconv.Itoa(compressionLvl),
		"-f", filename,
		"-o", filename+".zst")

	// Run the command and capture the output
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Error executing command: %v\n", err)
		return
	}

	// Print the output of the command
	fmt.Printf("Command output: %s\n", string(output))

}
