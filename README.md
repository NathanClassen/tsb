# Timescale Benchmarker (tsb)

`tsb` benchmarks performance of SQL queries executed across multiple workers. Query parameters are provided in CSV format either by providing the path to a CSV file or through STDIN

## Setup guide

### Prerequisites

You must have [Go](https://go.dev/) installed: `go version`

You must have [Docker](https://www.docker.com/) installed and running

### Build `tsb`

Run `make build` to build the cli tool. There will now be a binary called `tsb` at the root of the project.

### Environment

To connect to the example database `tsb` requires a dotenv file to hold the environment variables for the database connection.

An example of these can be found in `.env-example`. The values there will work for the example database. To make the environment available:

EITHER:
- rename the `.env-example` file to `.env`, **or**
- create a new `.env` with the appropriate variables set

### Database

`tsb` needs to connect to a database to run the queries.

To create start the example TimescaleDB instance and populate with sample data:

run `docker compose up -d` to start the _homework_ database on port 5432

## Test run:

The **db** directory has a sample of query parameters in a CSV file called `query_params.csv`. After following the Setup guide, you can use this file to test the application by running the following command from the root of the project:

`./tsb -w 50 -d 25 db/query_params.csv`

This command tells `tsb` to create 50 workers which will share a pool of 25 database connections, to read each line of `db/query_params.csv`, executing a SELECT query for each one.

After a few moments it will output stats about the performance of the query. For example:

    starting tsb with 50 workers and a max of 10 open database connections


            total queries:               200
            total time:                  5s 22ms
            cumulative processing time:  17s 682ms
            minimum single query time:   60ms
            maximum single query time:   175ms
            median query time:           83ms
            average query time:          88ms

## Usage:

    tsb [OPTIONS] [FILENAME]

    OPTIONS

    -w 
        Specify the number of workers to create. Defaults to 1 if not provided.

    -d
        Specify the maximum number of database connections to use. Defaults to 10 if not provided.

    FILENAME
        The name of the CSV file to be processed. If no filename is provided, tsb will read from stdin.

    Example:

        ./tsb /path/to/myParams.csv
        ./tsb -w 50 /path/to/myParams.csv
        ./tsb -w 50 /path/to/myParams.csv

`tsb` will operate on CSV input that follows this pattern:

The CSV must have 3 headers corresponding to a hostname string, followed by two timestamps

    hostname,start_time,end_time
    host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
    some_host_23,2017-01-02 13:02:02,2017-01-02 14:02:02
    another-host,2017-01-02 18:50:28,2017-01-02 19:50:28
    ...

