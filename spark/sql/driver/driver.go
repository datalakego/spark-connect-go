// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package driver implements `database/sql`'s driver interfaces over
// Spark Connect, so every Go tool that speaks `database/sql` — goose,
// sqlc, pgx consumers, ad-hoc test harnesses, script code — can target
// a Spark-backed lakehouse without learning the native client API.
//
// The package registers itself on import as the driver name "spark":
//
//	import (
//	    "database/sql"
//	    _ "github.com/datalakego/spark-connect-go/spark/sql/driver"
//	)
//
//	db, err := sql.Open("spark", "sc://localhost:15002?format=iceberg")
//
// DSN grammar is the plain Spark Connect URL with optional query
// parameters:
//
//	sc://host:port
//	sc://host:port?token=<bearer>
//	sc://host:port?format=iceberg
//	sc://host:port?format=delta
//	sc://host:port?token=<bearer>&format=delta
//
// The `format` parameter is consumed by downstream tools (such as
// goose-spark) that need to emit format-specific DDL; the driver
// itself doesn't interpret it.
//
// v0 scope: sufficient for goose to create its version table, insert
// applied-migration rows, and select them back. Specifically:
//
//   - Exec / Query against a Spark Connect endpoint via session.Sql.
//   - No statement preparation (every call is `NumInput() == 0`;
//     callers that pass arguments get an error so SQL injection
//     isn't possible through this driver, and migrations author their
//     own literal SQL anyway).
//   - No-op transactions — Begin / Commit succeed silently, Rollback
//     warns but does not error. Lakehouse commit semantics live in
//     Iceberg / Delta at the per-statement level; wrapping statements
//     in a `database/sql` Tx doesn't buy atomicity here. Documented
//     in the package comment so a caller who expects real transaction
//     semantics isn't surprised.
//
// v1+ adds: parameter binding via the Spark Connect parameterised-
// query proto, prepared-statement caching, catalog introspection for
// row metadata (column names + types) in the Rows wrapper.
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"

	sparksql "github.com/datalakego/spark-connect-go/spark/sql"
)

// init registers the driver under the name "spark". Consumers that
// only want the typed DataFrame surface don't have to import this
// package; consumers that use database/sql do.
func init() {
	sql.Register("spark", &sparkDriver{})
}

// sparkDriver is the database/sql.Driver implementation. Satisfies
// both the legacy Driver interface (Open) and the modern
// DriverContext interface (OpenConnector).
type sparkDriver struct{}

// Open opens a new connection using the given DSN. Implements
// database/sql/driver.Driver.
//
// database/sql calls Open for every new connection the pool wants.
// Keeping this lightweight — parse the DSN, return a Connector,
// defer actual session construction to Connect — matches how the
// standard library's pool expects drivers to behave.
func (d *sparkDriver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector returns a Connector ready to produce Conns.
// Implements database/sql/driver.DriverContext.
func (d *sparkDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{driver: d, cfg: cfg}, nil
}

// connector wraps a parsed DSN and produces Conns on demand.
type connector struct {
	driver *sparkDriver
	cfg    *dsnConfig
}

// Connect opens a Spark Connect session and returns a driver.Conn
// wrapping it. Implements database/sql/driver.Connector.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	session, err := sparksql.NewSessionBuilder().Remote(c.cfg.sparkDSN).Build(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{session: session, cfg: c.cfg}, nil
}

// Driver returns the owning driver. Implements
// database/sql/driver.Connector.
func (c *connector) Driver() driver.Driver { return c.driver }
