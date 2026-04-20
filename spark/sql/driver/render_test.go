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
	"database/sql/driver"
	"strings"
	"testing"
	"time"
)

func namedArgs(vs ...any) []driver.NamedValue {
	out := make([]driver.NamedValue, len(vs))
	for i, v := range vs {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

func TestRender_NoArgsPassesThrough(t *testing.T) {
	got, err := render("SELECT 1", nil)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got != "SELECT 1" {
		t.Errorf("got %q, want unchanged", got)
	}
}

func TestRender_Int64AndBool(t *testing.T) {
	// goose's Insert always passes (version int64, is_applied bool).
	// Verify the exact shape it emits renders to valid Spark SQL.
	q := `INSERT INTO goose_db_version (version_id, is_applied) VALUES ($1, $2)`
	got, err := render(q, namedArgs(int64(20260419000001), true))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := `INSERT INTO goose_db_version (version_id, is_applied) VALUES (20260419000001, TRUE)`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRender_StringEscapesEmbeddedQuotes(t *testing.T) {
	got, err := render(`SELECT * FROM t WHERE name = $1`, namedArgs("O'Brien"))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "'O''Brien'") {
		t.Errorf("got %q, want doubled-quote escape", got)
	}
}

func TestRender_LeavesLiteralInsideStringAlone(t *testing.T) {
	// A `$1` that lives inside a quoted literal must stay literal.
	// Otherwise something like `WHERE s = '$1'` would try to
	// substitute into the user-intended string.
	got, err := render(`SELECT '$1 then $2' WHERE id = $1`, namedArgs(int64(42)))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "'$1 then $2'") {
		t.Errorf("got %q, string-literal $-tokens should stay untouched", got)
	}
	if !strings.Contains(got, "id = 42") {
		t.Errorf("got %q, outer $1 should have been replaced with 42", got)
	}
}

func TestRender_NilBecomesSQLNull(t *testing.T) {
	got, err := render(`SELECT $1`, namedArgs(nil))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if got != "SELECT NULL" {
		t.Errorf("got %q, want SELECT NULL", got)
	}
}

func TestRender_TimeRendersAsTimestampLiteral(t *testing.T) {
	ts := time.Date(2026, 4, 19, 12, 34, 56, 0, time.UTC)
	got, err := render(`SELECT $1`, namedArgs(ts))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(got, "TIMESTAMP '2026-04-19 12:34:56") {
		t.Errorf("got %q, want TIMESTAMP literal", got)
	}
}

func TestRender_UnsupportedTypeErrors(t *testing.T) {
	type custom struct{ N int }
	_, err := render(`SELECT $1`, namedArgs(custom{N: 5}))
	if err == nil || !strings.Contains(err.Error(), "unsupported arg type") {
		t.Errorf("err = %v, want unsupported-arg-type", err)
	}
}

func TestRender_MissingOrdinalErrors(t *testing.T) {
	// $3 referenced but only two args supplied — surfaces at render
	// time rather than on the wire, so the error points at the query.
	_, err := render(`INSERT INTO t VALUES ($1, $2, $3)`, namedArgs(int64(1), int64(2)))
	if err == nil || !strings.Contains(err.Error(), "$3") {
		t.Errorf("err = %v, want missing-ordinal error naming $3", err)
	}
}
