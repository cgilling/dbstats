# dbstats
[![Build Status](https://travis-ci.org/cgilling/dbstats.svg?branch=master)](https://travis-ci.org/cgilling/dbstats)
[![GoDoc](https://godoc.org/github.com/cgilling/dbstats?status.svg)](https://godoc.org/github.com/cgilling/dbstats)

A golang database/sql driver wrapper that provides hooks around database operations in order to gather usage/performance statistics.

## Usage
`dbstats` provides a wrapper for a `database/sql/driver.Driver` . This is done by wrapping the Driver's `Open` function and then registering the new wrapped driver with `database/sql`. Once the driver has been wrapped, `Hook`s can be registered in order to gather various statistics. A very basic Hook `CounterHook` is provided by this package.

```go
import (
  "database/sql"
  
  "github.com/cgilling/dbstats"
  "github.com/lib/pq"
)

var pqStats dbstats.CounterHook

func init() {
  s := dbstats.New(pq.Open)
  s.AddHook(pqStats)
  sql.Register("pqstats", s)
}

func main() {
  db, err := sql.Open("pqstats", "dbname=pqgotest"); // use the normal database connection string
  ... // use db as normal
  fmt.Printf("%d queries and %d execs\n", pqStats.Queries(), pqStats.Execs())
}

```
