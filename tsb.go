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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"tsb/utils"
	"tsb/worker"

	"github.com/cespare/xxhash"
	"github.com/joho/godotenv"

	_ "github.com/lib/pq"
)

var workerCount int
var dbConns int
var wg sync.WaitGroup
var db *sql.DB

type Record struct {
	Host  string
	Start time.Time
	End   time.Time
}

type queryTimes struct {
	start    time.Time
	end      time.Time
	duration time.Duration
}

type StatResult struct {
	queryCount     int
	totalTime      int
	cumulativeTime int
	minQueryTime   int
	maxQueryTime   int
	medianTime     int
	avgTime        int
}

// parse cli arguments
func init() {
	flag.IntVar(&workerCount, "w", 1, "number of workers to create for executing queries")
	flag.IntVar(&dbConns, "d", 1, "number of workers to create for executing queries")

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

	db.SetMaxOpenConns(dbConns)
}

func Execute() {

	errorChannel := make(chan error)
	csvRecords := make(chan Record)
	completedJobs := make(chan queryTimes)
	result := make(chan StatResult)

	ctx, cancel := context.WithCancel(context.Background())

	wp := worker.NewWorkerPool[Record, queryTimes](workerCount, completedJobs)
	wp.InitWorkers(ctx, func(r Record) queryTimes {
		queryTime, err := executeTSQuery(r.Start, r.End, r.Host)
		if err != nil {
			errorChannel <- err
		}
		return queryTime
	})

	go monitorErrors(ctx, cancel, errorChannel, completedJobs, csvRecords, wp, db)

	go parseCSVFile(errorChannel, flag.Arg(0), csvRecords)

	for r := range csvRecords {
		workerId := int(xxhash.Sum64String(r.Host) % uint64(workerCount))
		go wp.SendJob(workerId, r)
		wg.Add(1)
	}

	go calculateResult(completedJobs, result, &wg)

	time.Sleep(1 * time.Second)
	wg.Wait()

	cancel()
	wp.Close()
	db.Close()
	close(errorChannel)
	close(completedJobs)

	res := <-result
	fmt.Println(formattedResult(res))
}

func monitorErrors(ctx context.Context, canc context.CancelFunc, errorc chan error, jobc chan queryTimes, csvc chan Record, wp *worker.WorkerPool[Record, queryTimes], db *sql.DB) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errorc:
			canc()
			wp.Close()
			db.Close()
			close(errorc)
			close(jobc)
			close(csvc)
			log.Fatalf("encountered error: %v", err)
		}
	}
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

	if len(durations) > 0 {
		slices.Sort(durations)
		fmt.Println("durations: ", durations)

		result.totalTime = int(ending.Sub(beginning).Milliseconds())
		result.queryCount = len(durations)
		result.medianTime = utils.CalculateMedian(durations)
		result.avgTime = utils.CalculateAvg(durations)
		result.cumulativeTime = utils.Sum(durations)
		result.minQueryTime = durations[0]
		result.maxQueryTime = durations[result.queryCount-1]
	}

	res <- result
}

func parseCSVFile(ec chan error, filename string, records chan Record) { // TODO: probably dont close records in here, let error watcher do that.. of do it here and not there?
	var err error
	var f *os.File

	const TIME_FORMAT = "2006-01-02 15:04:05"
	if filename == "" {
		f = os.Stdin
	} else {
		// Check if the file has a .csv extension
		if ext := strings.ToLower(filepath.Ext(filename)); ext != ".csv" {
			ec <- fmt.Errorf("file %s is not a CSV file", filename)
			return
		}

		// Open the file
		f, err = os.Open(filename)
		if err != nil {
			ec <- err
			return
		}
		defer f.Close()
	}

	r := csv.NewReader(f)

	headers, err := r.Read()
	if err != nil {
		ec <- fmt.Errorf("error reading CSV headers: %v", err)
		return
	}
	if len(headers) != 3 {
		ec <- fmt.Errorf("CSV headers are invalid.\n\tGot: %v\n\tExpected: [hostname start_time end_time]", headers)
		return
	}

	for {
		r, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				ec <- fmt.Errorf("error reading CSV record: %v", err)
				return
			}
		}

		if len(r) != 3 {
			ec <- fmt.Errorf("invalid record length: %v", r)
			return
		}

		startTime, err := time.Parse(TIME_FORMAT, r[1])
		if err != nil {
			ec <- fmt.Errorf("invalid start timestamp: %v", r[1])
			return
		}

		endTime, err := time.Parse(TIME_FORMAT, r[2])
		if err != nil {
			ec <- fmt.Errorf("invalid end timestamp: %v", r[2])
			return
		}

		records <- Record{
			Host:  r[0],
			Start: startTime,
			End:   endTime,
		}
	}
	close(records)
}

func executeTSQuery(start, end time.Time, host string) (queryTimes, error) {

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
			return fmt.Sprintf("%ds %dms", sec, ms%1000)
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
