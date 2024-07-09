package tsb

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
	"tsb/worker"

	"github.com/cespare/xxhash"
	_ "github.com/lib/pq"
)

var MAX_WORKERS uint64 = 20
var FILE_NAME = "/Users/nathanclassen/Desktop/TimescaleDB_coding_assignment-RD_eng_setup/query_params.csv"

var workerCount int
var filename	string

type Record struct {
	Host	string
	Start	string
	End		string
}

func init() {
	flag.IntVar(&workerCount, "w", 1, "number of workers to create for executing queries")

	flag.Parse()

	filename = flag.Arg(0)
}

func Execute() {
	//	todo:	read cmmd input
	db, err := sql.Open("postgres", "user=postgres password=password host=localhost dbname=homework sslmode=disable")
	if err != nil {
		fmt.Println("error opening db connection: ",err)
	}

	defer db.Close()

	db.SetMaxOpenConns(int(MAX_WORKERS))

	wg := sync.WaitGroup{}

	completedJobs := make(chan time.Duration, 2)

	wp := worker.NewWorkerPool[Record, time.Duration](int(MAX_WORKERS), completedJobs)

	wp.InitWorkers(func (r Record) time.Duration {
		startTime := time.Now()
		ExecuteTSQuery(db, r.Start, r.End, r.Host)
		return time.Since(startTime)
	})

	start := time.Now()	// temp; seeing how long executions take w different options

	f, err := os.Open(FILE_NAME)
	if err != nil {
		fmt.Println("error reading file: ", err)
	}

	defer f.Close()

	r := csv.NewReader(f)
	r.Read()	//	read off headers from first line

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

		go func() {
			
			hash := int(xxhash.Sum64String(record.Host) % MAX_WORKERS)

			wc, found := wp.WorkerMap[hash]
			if !found {
				fmt.Printf("could not find work channel in %v for %d\n\n",wp.WorkerMap,hash)
			}
			wg.Add(1)
			wc <- record
		}()
		
	}

	go func() {
		wg.Wait()

		wp.Close()
		close(completedJobs)
	}()


	 //	current problems is that the task given to each worker is decrementing the wg.
	//	however, the wg ma be decrememented before the result is put onto the done channel.

	/*
		create orchestrate func and call aggregation function as last call. It will loop over the completed channel
		and calculate the latest stats. This loop will last as long as the channel is open.

		before calling it, start a background routine that will await the wg. the wg should gurantee that stats have been collected.
		once the wg is empty, the background routine will close the complted channel, thus telling the aggregate func to stop looping 
		and finally display it's calculations. Can also close the work group.
	*/
	DisplayResults(completedJobs, &wg)
	end := time.Now()
	fmt.Println(end.Sub(start))
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
