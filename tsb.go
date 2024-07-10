package tsb

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
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
var wg sync.WaitGroup
var db *sql.DB

type Record struct {
	Host  string
	Start string
	End   string
}

// parse cli arguments
func init() {
	flag.IntVar(&workerCount, "w", 1, "number of workers to create for executing queries")

	flag.Parse()
}

// initialize database connections
func init() {
	var err error

	if err = godotenv.Load(); err != nil {
		log.Fatalf("failed to load environment: %v", err)
	}

	connstr := fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_SSL_MODE"),
	)

	if db, err = sql.Open(os.Getenv("DB_DATABASE"), connstr); err != nil {
		log.Fatal("could not connect to database: ", err)
	}

	db.SetMaxOpenConns(workerCount)
}

func Execute() {
	errors := make(chan error)
	completedJobs := make(chan time.Duration, 2)

	wp := worker.NewWorkerPool[Record, time.Duration](workerCount, completedJobs)
	wp.InitWorkers(func(r Record) time.Duration {
		startTime := time.Now()
		ExecuteTSQuery(r.Start, r.End, r.Host)
		return time.Since(startTime)
	})

	csvRecords := make(chan Record)
	go parseCSVFile(errors, flag.Arg(0), csvRecords)

	for r := range csvRecords {
		workerId := int(xxhash.Sum64String(r.Host) % uint64(workerCount))
		go wp.SendJob(workerId, r)
		wg.Add(1)
	}

	go func() {
		wg.Wait()
		wp.Close()
		db.Close()
		close(completedJobs)
	}()

	DisplayResults(completedJobs, &wg)
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

func parseCSVFile(ec chan error, filename string, records chan Record) {
	f, err := os.Open(filename)
	if err != nil {
		ec <- err
	}

	defer f.Close()
	defer close(records)

	r := csv.NewReader(f)
	r.Read() //	TEMP: read off headers from first line-find better way?

	for {
		r, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				ec <- err
			}
		}

		record := Record{
			Host:  r[0],
			Start: r[1],
			End:   r[2],
		}

		records <- record
	}
}

func ExecuteTSQuery(start, end, host string) {
	var qstart time.Time
	var qend time.Time
	var min float64
	var max float64

	query := `with minutes as (
		select
			generate_series(
				$1::timestamp,
				$2::timestamp - '60 second'::interval,
				'60 second'::interval
			) as minute
		)
		select
			minutes.minute as start,
			minutes.minute + '60 second'::interval as end,
			min(usage),
			max(usage) from minutes
			left join cpu_usage cu
			on cu.ts >= minute and cu.ts <= minute + '60 second'::interval
			where host = $3
			group by minute;`

	rows, err := db.Query(query, start, end, host)

	if err != nil {
		fmt.Println("query failed: ", err)
	}

	for rows.Next() {
		err = rows.Scan(&qstart, &qend, &min, &max)
		if err != nil {
			fmt.Println("failed to parse row: ", err)
		}

	}
}
