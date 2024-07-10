package tsb

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	"tsb/worker"

	"github.com/cespare/xxhash"
	"github.com/joho/godotenv"

	_ "github.com/lib/pq"
)

var workerCount int
var filename	string
var wg sync.WaitGroup
var dbconfig dbconfiguration


type Record struct {
	Host	string
	Start	string
	End		string
}

type dbconfiguration struct {
	database string
	user string
	password string
	host string
	dbname string
	sslmode string
}

func init() {
	flag.IntVar(&workerCount, "w", 1, "number of workers to create for executing queries")

	flag.Parse()

	filename = flag.Arg(0)

	if err := godotenv.Load(); err != nil {
		log.Fatalf("failed to load environment: %v", err)
	}

	dbconfig = dbconfiguration{
		os.Getenv("DB_DATABASE"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_SSL_MODE"),
	}
}

func Execute() {
	connstr := fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=%s",
		dbconfig.user,dbconfig.password,dbconfig.host,dbconfig.dbname,dbconfig.sslmode)
	db, err := sql.Open(os.Getenv("DB_DATABASE"), connstr)
	if err != nil {
		log.Fatal("error opening db connection: ",err)
	}

	db.SetMaxOpenConns(workerCount)

	defer db.Close()

	completedJobs := make(chan time.Duration, 2)

	wp := worker.NewWorkerPool[Record, time.Duration](workerCount, completedJobs)

	wp.InitWorkers(func (r Record) time.Duration {
		startTime := time.Now()
		ExecuteTSQuery(db, r.Start, r.End, r.Host)
		return time.Since(startTime)
	})

	start := time.Now()	//	TEMP; seeing how long executions take w different options

	csvRecords := make(chan Record)

	go parseCSVFile(filename, csvRecords)

	for r := range csvRecords {
		workerId := int(xxhash.Sum64String(r.Host) % uint64(workerCount))
		go wp.SendJob(workerId, r)
		wg.Add(1)
	}

	go func() {
		wg.Wait()
		wp.Close()
		close(completedJobs)
	}()

	DisplayResults(completedJobs, &wg)
	fmt.Println(time.Since(start))
}

func parseCSVFile(filename string, records chan Record) {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("error reading file: ", err)
	}

	defer f.Close()

	r := csv.NewReader(f)
	r.Read()	//	TEMP: read off headers from first line-find better way?

	// read file, dispatching routines for each line
	for {
		r, err := r.Read()
		if err != nil {
			fmt.Printf("error reading next record %v\n",err)
			break
		}

		record := Record {
			Host: r[0],
			Start: r[1],
			End: r[2],
		}

		records <- record
	}
	close(records)
}

func DisplayResults(c <-chan time.Duration, waitgroup *sync.WaitGroup) {

	var totalQueries int
	var totalTime time.Time
	var min time.Duration
	var max time.Duration

	for dur := range c {
		totalQueries++
		totalTime = totalTime.Add(dur)

		if dur < min {
			min = dur
		}
		if dur > max {
			max = dur
		}
		waitgroup.Done()
	}

	fmt.Printf(`
			Total Queries: %d
			Total Execution Duration: %v
			Max Execution Time (single query): %v
			Min Execution Time (single query): %v
		`,
		totalQueries,
		totalTime,
		max,
		min,
	)
}

func ExecuteTSQuery(db *sql.DB, start, end, host string) {
	ExecuteQuery(db,
	`with minutes as (
    select
        generate_series(
            timestamp '`+start+`',
            timestamp '`+end+`' - interval '60 second',
            '60 second'::interval
        ) as minute
    )
    select
        minutes.minute as start,
        minutes.minute + interval '60 second' as end,
        min(usage),
        max(usage) from minutes
        left join cpu_usage cu
        on cu.ts >= minute and cu.ts <= minute + interval '60 second'
        where host = '`+host+`'
        group by minute;`)
}

func ExecuteQuery(db *sql.DB, q string) {
	var start time.Time
	var end time.Time
	var min float64
	var max float64

	rows, err := db.Query(q)

	if err != nil {
		fmt.Println("query failed: ", err)
	}

	for rows.Next() {
		err = rows.Scan(&start, &end, &min, &max)
		if err != nil {
			fmt.Println("failed to parse row: ", err)
		}

		// fmt.Printf("\nStart: %v \nEnd: %s, \nMin: %v \nMax: %v\n\n\n", start, end, min, max)
	}
}
