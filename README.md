# Stock Market Data Generator for CockroachDB

This is a simple Go program to generate and load into CockroachDB some stock market oriented data.
It was built in response to a question about how many rows of this type of data could be loaded
within a specified time window.

## Build
```
$ go get github.com/google/uuid
$ go get github.com/jackc/pgx/v4
$ go build
```

## Run

* Edit the [environment file](./env.sh), setting the various DB connection parameters as well
  as:
  - `BATCH_SIZE`: the number of rows copied into the table at a time (defaults to 128)
  - `N_THREADS`: the number of parallel goroutines used to parallelize the work (defaults to 4).
    NOTE: each of these consumes one DB connection

* Dump the table DDL and use the output to create the table to hold this generated data:
  `$ ./gen_data --dump-ddl`

* Generate and load the data:
  `$ ./gen_data 100000`

## Notes

* The [data](./data) directory contains some data files used as reference here since it seemed like
"stock market data".

