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
)

// stmt wraps one query string against a conn. The driver doesn't
// cache a prepared plan on the server; every Exec/Query re-sends
// the statement. Works for goose's load (dozens of statements per
// migration run) and keeps the implementation simple; v1+ adds a
// server-side plan cache if latency-sensitive callers appear.
//
// Implements:
//   - driver.Stmt (legacy Close / NumInput / Exec / Query)
//   - driver.StmtExecContext
//   - driver.StmtQueryContext
type stmt struct {
	conn  *conn
	query string
}

// Close is a no-op; the statement holds no server-side state.
// Implements database/sql/driver.Stmt.
func (*stmt) Close() error { return nil }

// NumInput reports -1 — "driver doesn't know the number of
// placeholders." The renderer parses `$N` tokens inline at Exec /
// Query time, so the sql package's up-front arg-count check doesn't
// apply. Returning -1 tells database/sql to hand any arg count
// through unchanged.
//
// Implements database/sql/driver.Stmt.
func (*stmt) NumInput() int { return -1 }

// Exec is the legacy (non-context) Exec entry. Implements
// database/sql/driver.Stmt.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), named)
}

// Query is the legacy (non-context) Query entry. Implements
// database/sql/driver.Stmt.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), named)
}

// ExecContext runs the statement and discards rows. Returns a
// Result with RowsAffected=-1 because Spark Connect doesn't surface
// that metric reliably at the session.Sql layer. database/sql
// allows -1 per the driver.Result docs.
//
// Arguments are rendered into the query at $N placeholders before
// the session call fires; Spark Connect's protocol-level parameter
// binding doesn't round-trip reliably across every supported Spark
// version, so client-side rendering is the compatible path.
//
// Implements database/sql/driver.StmtExecContext.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	q, err := render(s.query, args)
	if err != nil {
		return nil, err
	}
	if _, err := s.conn.session.Sql(ctx, q); err != nil {
		return nil, err
	}
	return result{}, nil
}

// QueryContext runs the statement and returns rows. The underlying
// DataFrame is materialised via Collect and walked by the Rows
// wrapper. For small result sets (the version-table SELECTs goose
// fires) this is right-sized; large result sets should bypass the
// driver and use the native DataFrame / iter.Seq2 path directly.
//
// Arguments are rendered into the query at $N placeholders before
// the session call fires — see ExecContext for rationale.
//
// Implements database/sql/driver.StmtQueryContext.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	q, err := render(s.query, args)
	if err != nil {
		return nil, err
	}
	df, err := s.conn.session.Sql(ctx, q)
	if err != nil {
		return nil, err
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, err
	}
	return newRows(rows), nil
}
