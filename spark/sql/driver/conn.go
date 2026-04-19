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

package driver

import (
	"context"
	"database/sql/driver"
	"errors"

	sparksql "github.com/datalakego/spark-connect-go/spark/sql"
)

// conn is the per-logical-connection state database/sql keeps in its
// pool. Each Conn holds one Spark Connect session; Close stops it.
//
// Implements:
//   - driver.Conn (legacy Prepare / Close / Begin)
//   - driver.ConnPrepareContext
//   - driver.ConnBeginTx
//   - driver.ExecerContext
//   - driver.QueryerContext
//   - driver.Pinger (Ping via session round-trip)
type conn struct {
	session sparksql.SparkSession
	cfg     *dsnConfig
	closed  bool
}

// Prepare wraps the SQL in a driver.Stmt. Legacy (non-context)
// variant of PrepareContext. No server-side preparation — Spark
// Connect doesn't expose a prepare primitive. Each Exec/Query
// re-sends the statement; v1+ adds a plan cache if latency matters.
//
// Implements database/sql/driver.Conn.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext is the context-aware Prepare. Implements
// database/sql/driver.ConnPrepareContext.
func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &stmt{conn: c, query: query}, nil
}

// Close releases the underlying Spark Connect session. Idempotent.
// Implements database/sql/driver.Conn.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.session == nil {
		return nil
	}
	return c.session.Stop()
}

// Begin returns a no-op transaction. Implements
// database/sql/driver.Conn (legacy; ConnBeginTx below is preferred).
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx returns a no-op transaction. Lakehouse commit semantics
// (Iceberg snapshot commits, Delta transaction log appends) happen
// at the per-statement level inside Spark, not at a SQL-layer Tx
// boundary. Wrapping statements in a database/sql Tx doesn't buy
// atomicity for lakehouse writes, so this driver's Tx is a no-op
// that lets tools (goose, sqlc) that default to transactional
// execution not break.
//
// Implements database/sql/driver.ConnBeginTx.
func (c *conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &tx{}, nil
}

// ExecContext runs a statement that returns no rows. Forwards to
// the Stmt path so the NumInput-based argument check lives in one
// place.
//
// Implements database/sql/driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return (&stmt{conn: c, query: query}).ExecContext(ctx, args)
}

// QueryContext runs a SELECT returning rows. Implements
// database/sql/driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return (&stmt{conn: c, query: query}).QueryContext(ctx, args)
}

// Ping checks the Spark Connect session is alive. Implements
// database/sql/driver.Pinger.
func (c *conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	_, err := c.session.Sql(ctx, "SELECT 1")
	return err
}

// tx is the no-op transaction described on BeginTx.
type tx struct{}

// Commit succeeds silently. Implements database/sql/driver.Tx.
func (tx) Commit() error { return nil }

// Rollback returns an error because lakehouse rollback isn't a
// meaningful concept at the database/sql layer. Callers that expect
// Rollback to un-apply statements see the error and can choose to
// handle it; tools that default to rollback-on-error (goose with
// failing migrations) get a signal that rollback didn't happen.
// Implements database/sql/driver.Tx.
func (tx) Rollback() error {
	return errors.New("spark driver: transactional rollback is not supported; lakehouse commit semantics are per-statement")
}
