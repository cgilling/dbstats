# dbstats
[![Build Status](https://travis-ci.org/cgilling/dbstats.svg?branch=master)](https://travis-ci.org/cgilling/dbstats)

A golang database/sql driver wrapper that provides stats and hooks around database operations

## Alpha Notice

The api for retrieving stats will most likely change in the near future.

## Usage
`dbstats` provides a wrapper for a `database/sql/driver.Driver` that allows for retrieving statistics with regards to its usage. This is done by wrapping the Driver's `Open` function and then registering the new wrapped driver with `database/sql`.

```go
import (
  "database/sql"
  
  "github.com/cgilling/dbstats"
  "github.com/lib/pq"
)

var pqStats dbstats.Driver

func init() {
  pqStats = dbstats.New(pq.Open)
  sql.Register("pqstats", pqStats)
}

func main() {
  db, err := sql.Open("pqstats", "dbname=pqgotest"); // use the normal database connection string
  ... // use db as normal
  fmt.Printf("%d queries and %d execs\n", pqStats.Queries(), pqStats.Execs())
}

```
