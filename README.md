# Timescale Benchmarker (tsb)

`tsb` benchmarks performance of SQL queries executed across multiple workers. Query parameters are provided in CSV format either by providing the path to a CSV file or through STDIN

## Setup guide

### Build `tsb`

Make sure you have Go installed: `go version`

Run `make build` to build the cli tool. There will now be a binary called `tsb` at the root of the project.

### Environment

To connect to the example database `tsb` relies on the environment variables found in `.env-example`.

EITHER:
- rename this file to `.env`, or
- create a new `.env` file with different credentials if you want to connect to a different database. 

### Database

`tsb` needs to connect to a database to run the queries.

To create the example TimescaleDB instance and populate with sample data:

1. run `docker compose up -d db` to start the _homework_ database on port 5432
2. _after_ the database service has been started with docker compose, run `. db/import-data.sh ` to populate with data.

## Usage:

    tsb [OPTIONS] [FILENAME]

    OPTIONS

    -w 
        Specify the number of workers to create. Defaults to 1 if not provided.

    -d
        Specify the maximum number of database connections to use. Defaults to 1 if not provided.

    FILENAME
        The name of the CSV file to be processed. If no filename is provided, tsb will read from stdin.

    Example:

        ./tsb -w 50 -d 25 /path/to/myParams.csv

`tsb` will operate on CSV input that follows this pattern:

    hostname,start_time,end_time
    host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
    some_host_23,2017-01-02 13:02:02,2017-01-02 14:02:02
    another-host,2017-01-02 18:50:28,2017-01-02 19:50:28
    ...

