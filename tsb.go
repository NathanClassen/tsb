package tsb

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync"
	"time"
	"tsb/worker"
	"tsb/utils"

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

type queryTimes struct {
	start time.Time
	end time.Time
	duration time.Duration
}

type StatResult struct {
	queryCount int
	totalTime int
	cumulativeTime int
	minQueryTime int
	maxQueryTime int
	medianTime int
	avgTime int
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
	completedJobs := make(chan queryTimes, 2)

	ctx, cancel := context.WithCancel(context.Background())

	wp := worker.NewWorkerPool[Record, queryTimes](workerCount, completedJobs)
	wp.InitWorkers(ctx, func(r Record) queryTimes {
		queryTime, err := executeTSQuery(r.Start, r.End, r.Host)
		if err != nil {
			errors <- err
		}
		return queryTime
	})

	csvRecords := make(chan Record)
	go parseCSVFile(errors, flag.Arg(0), csvRecords)

	for r := range csvRecords {
		workerId := int(xxhash.Sum64String(r.Host) % uint64(workerCount))
		go wp.SendJob(workerId, r)
		wg.Add(1)
	}

	go func(ctx context.Context, errors chan error) {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errors:
				log.Println("got ERRORR")
				cancel()
				wp.Close()
				db.Close()
				close(errors)
				close(completedJobs)
				log.Fatalf("encountered error: %v", err)
			}
		}
	}(ctx, errors)

	result := make(chan StatResult)
	go calculateResult(completedJobs, result, &wg)

	wg.Wait()

	cancel()
	wp.Close()
	db.Close()
	close(errors)
	close(completedJobs)

	res := <-result
	fmt.Println(formattedResult(res))
}

func calculateResult(times <-chan queryTimes, res chan StatResult, waitgroup *sync.WaitGroup) {

	result := StatResult{}

	var durations []int

	beginning := time.Now()
	ending := time.Now()

	for qt := range times {
		durations = append(durations, int(qt.duration.Milliseconds()))

		if qt.start.Before(beginning) {
			beginning = qt.start
		}

		if qt.end.After(ending) {
			ending = qt.end
		}

		waitgroup.Done()
	}

	slices.Sort(durations)

	result.totalTime		= int(ending.Sub(beginning).Milliseconds())
	result.queryCount		= len(durations)
	result.medianTime		= utils.CalculateMedian(durations)
	result.avgTime			= utils.CalculateAvg(durations)
	result.cumulativeTime	= utils.Sum(durations) 
	result.minQueryTime		= durations[0]
	result.maxQueryTime		= durations[result.queryCount - 1]

	res <- result
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
				return
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

func executeTSQuery(start, end, host string) (queryTimes, error) {

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

	startT := time.Now()
	rows, err := db.Query(query, start, end, host)
	endT := time.Now()

	if err != nil {
		return queryTimes{}, err
	}

	rows.Close() //	not using the result

	return queryTimes{startT, endT, endT.Sub(startT)}, nil
}

func formattedResult(stats StatResult) string {
	format := func(ms int) string {
		sec := ms / 1000
		if sec > 0 {
			return fmt.Sprintf("%ds %dms", sec, ms % 1000)
		}

		return fmt.Sprintf("%dms", ms)
	}

	return fmt.Sprintf(`
	total queries:               %d
	total time:                  %s
	cumulative processing time:  %s
	minimum single query time:   %s
	maximum single query time:   %s
	median query time:           %s
	average query time:          %s
	`,
			stats.queryCount,
			format(stats.totalTime),
			format(stats.cumulativeTime),
			format(stats.minQueryTime),
			format(stats.maxQueryTime),
			format(stats.medianTime),
			format(stats.avgTime))


}
