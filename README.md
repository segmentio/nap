# Nap

> **Note**
> Segment has paused maintenance on this project, but may return it to an active status in the future. Issues and pull requests from external contributors are not being considered, although internal contributions may appear from time to time. The project remains available under its open source license for anyone to use.

Nap is a library that abstracts access to primary-replica physical SQL servers topologies as a single logical database mimicking the standard `sql.DB` APIs.

## Install
```shell
$ go get github.com/tsenart/nap
```

## Usage
```go
package main

import (
  "log"

  "github.com/tsenart/nap"
  _ "github.com/go-sql-driver/mysql" // Any sql.DB works
)

func main() {
  // The first DSN is assumed to be the primary and all
  // other to be replicas
  dsns := "tcp://user:password@primary/dbname;"
  dsns += "tcp://user:password@replica01/dbname;"
  dsns += "tcp://user:password@replica02/dbname"

  db, err := nap.Open("mysql", dsns)
  if err != nil {
    log.Fatal(err)
  }

  if err := db.Ping(); err != nil {
    log.Fatalf("Some physical database is unreachable: %s", err)
  }

  // Read queries are directed to replicas with Query and QueryRow.
  // Always use Query or QueryRow for SELECTS
  // Load distribution is round-robin only for now.
  var count int
  err = db.QueryRow("SELECT COUNT(*) FROM sometable").Scan(&count)
  if err != nil {
    log.Fatal(err)
  }

  // Write queries are directed to the primary with Exec.
  // Always use Exec for INSERTS, UPDATES
  err = db.Exec("UPDATE sometable SET something = 1")
  if err != nil {
    log.Fatal(err)
  }

  // Prepared statements are aggregates. If any of the underlying
  // physical databases fails to prepare the statement, the call will
  // return an error. On success, if Exec is called, then the
  // primary is used, if Query or QueryRow are called, then a replica
  // is used.
  stmt, err := db.Prepare("SELECT * FROM sometable WHERE something = ?")
  if err != nil {
    log.Fatal(err)
  }

  // Transactions always use the primary
  tx, err := db.Begin()
  if err != nil {
    log.Fatal(err)
  }
  // Do something transactional ...
  if err = tx.Commit(); err != nil {
    log.Fatal(err)
  }

  // If needed, one can access the primary or a replica explicitly.
  primary, replica := db.Primary(), db.Replica()
}
```

## Todo
* Support other replica load balancing algorithms.

## License
See [LICENSE](LICENSE)
