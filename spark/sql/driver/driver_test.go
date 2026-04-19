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
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/datalake-go/spark-connect-go/spark/sql/types"
)

// --- DSN parsing ---------------------------------------------------

func TestParseDSN_AcceptsPlainSc(t *testing.T) {
	cfg, err := parseDSN("sc://localhost:15002")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.sparkDSN != "sc://localhost:15002" {
		t.Errorf("sparkDSN = %q", cfg.sparkDSN)
	}
	if cfg.format != "" {
		t.Errorf("format = %q, want empty", cfg.format)
	}
}

func TestParseDSN_ParsesFormatParameter(t *testing.T) {
	cfg, err := parseDSN("sc://localhost:15002?format=iceberg")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.format != "iceberg" {
		t.Errorf("format = %q, want iceberg", cfg.format)
	}
}

func TestParseDSN_NormalisesFormatCase(t *testing.T) {
	cfg, err := parseDSN("sc://localhost:15002?format=DELTA")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.format != "delta" {
		t.Errorf("format = %q, want delta (lowercased)", cfg.format)
	}
}

func TestParseDSN_RejectsUnknownFormat(t *testing.T) {
	_, err := parseDSN("sc://localhost:15002?format=duckdb")
	if err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Errorf("err = %v, want unsupported-format error", err)
	}
}

func TestParseDSN_RejectsMissingSchemePrefix(t *testing.T) {
	_, err := parseDSN("localhost:15002")
	if err == nil || !strings.Contains(err.Error(), "sc://") {
		t.Errorf("err = %v, want sc:// required", err)
	}
}

func TestParseDSN_RejectsEmpty(t *testing.T) {
	_, err := parseDSN("")
	if err == nil || !strings.Contains(err.Error(), "DSN is required") {
		t.Errorf("err = %v, want DSN-required error", err)
	}
}

func TestParseDSN_PreservesTokenInSparkDSN(t *testing.T) {
	// The token parameter is meaningful to the downstream session
	// builder; the driver itself doesn't interpret it but must not
	// strip it from the DSN forwarded to NewSessionBuilder().Remote.
	cfg, err := parseDSN("sc://host:15002?token=secret&format=iceberg")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if !strings.Contains(cfg.sparkDSN, "token=secret") {
		t.Errorf("sparkDSN should carry token unchanged; got %q", cfg.sparkDSN)
	}
}

// --- sql.Register side-effect ---------------------------------------

func TestDriver_RegistersAsSparkOnImport(t *testing.T) {
	// Importing this package (the test binary already does) triggers
	// init() which calls sql.Register("spark", ...). Verify the
	// driver list contains "spark".
	found := false
	for _, name := range sql.Drivers() {
		if name == "spark" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("driver list = %v; expected to contain \"spark\"", sql.Drivers())
	}
}

func TestDriver_OpenConnectorInvalidDSN(t *testing.T) {
	_, err := (&sparkDriver{}).OpenConnector("bogus://nope")
	if err == nil {
		t.Error("OpenConnector should reject non-sc:// DSN")
	}
}

// --- stmt argument rejection ---------------------------------------
//
// v0 doesn't implement parameter binding. Callers that pass args
// should see the errArgsUnsupported sentinel before any server call
// fires.

func TestStmt_ExecContextRejectsArgs(t *testing.T) {
	s := &stmt{conn: &conn{}, query: "SELECT 1"}
	_, err := s.ExecContext(context.Background(), []driver.NamedValue{
		{Ordinal: 1, Value: 42},
	})
	if err == nil || !errors.Is(err, errArgsUnsupported) {
		t.Errorf("ExecContext with args err = %v, want errArgsUnsupported", err)
	}
}

func TestStmt_QueryContextRejectsArgs(t *testing.T) {
	s := &stmt{conn: &conn{}, query: "SELECT 1"}
	_, err := s.QueryContext(context.Background(), []driver.NamedValue{
		{Ordinal: 1, Value: "x"},
	})
	if err == nil || !errors.Is(err, errArgsUnsupported) {
		t.Errorf("QueryContext with args err = %v, want errArgsUnsupported", err)
	}
}

// --- Rows wrapper --------------------------------------------------

// rowFake is a minimal types.Row satisfying every method the
// interface declares. Only FieldNames + Values + Len + At get
// exercised from rows.go; the rest are required by the interface
// shape.
type rowFake struct {
	names []string
	vals  []any
}

func (r rowFake) FieldNames() []string { return r.names }
func (r rowFake) Values() []any        { return r.vals }
func (r rowFake) Len() int             { return len(r.vals) }
func (r rowFake) At(i int) any {
	if i < 0 || i >= len(r.vals) {
		return nil
	}
	return r.vals[i]
}

func (r rowFake) Value(name string) any {
	for i, n := range r.names {
		if n == name {
			return r.vals[i]
		}
	}
	return nil
}
func (r rowFake) ToJsonString() (string, error) { return "", nil }

func TestRows_ColumnsReportsFieldNamesFromFirstRow(t *testing.T) {
	fakes := []types.Row{
		rowFake{names: []string{"id", "name"}, vals: []any{int64(1), "alice"}},
		rowFake{names: []string{"id", "name"}, vals: []any{int64(2), "bob"}},
	}
	r := newRows(fakes)
	got := r.Columns()
	if len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Errorf("Columns = %v, want [id name]", got)
	}
}

func TestRows_ColumnsEmptyOnEmptyResult(t *testing.T) {
	r := newRows(nil)
	if len(r.Columns()) != 0 {
		t.Errorf("Columns = %v on empty result, want []", r.Columns())
	}
}

func TestRows_NextWalksResultAndStopsOnEOF(t *testing.T) {
	fakes := []types.Row{
		rowFake{names: []string{"id", "name"}, vals: []any{int64(1), "alice"}},
		rowFake{names: []string{"id", "name"}, vals: []any{int64(2), "bob"}},
	}
	r := newRows(fakes)
	dest := make([]driver.Value, 2)

	if err := r.Next(dest); err != nil {
		t.Fatalf("Next row 1: %v", err)
	}
	if dest[0].(int64) != 1 || dest[1].(string) != "alice" {
		t.Errorf("row 1 = %v, want [1 alice]", dest)
	}

	if err := r.Next(dest); err != nil {
		t.Fatalf("Next row 2: %v", err)
	}
	if dest[0].(int64) != 2 || dest[1].(string) != "bob" {
		t.Errorf("row 2 = %v, want [2 bob]", dest)
	}

	if err := r.Next(dest); err != io.EOF {
		t.Errorf("Next past end = %v, want io.EOF", err)
	}
}

// --- no-op transaction --------------------------------------------

func TestTx_CommitIsNoop(t *testing.T) {
	var transaction tx
	if err := transaction.Commit(); err != nil {
		t.Errorf("Commit err = %v, want nil", err)
	}
}

func TestTx_RollbackErrors(t *testing.T) {
	var transaction tx
	err := transaction.Rollback()
	if err == nil || !strings.Contains(err.Error(), "rollback is not supported") {
		t.Errorf("Rollback err = %v, want rollback-not-supported error", err)
	}
}

// --- result --------------------------------------------------------

func TestResult_RowsAffectedReturnsUnknown(t *testing.T) {
	r := result{}
	n, err := r.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected err = %v, want nil", err)
	}
	if n != -1 {
		t.Errorf("RowsAffected = %d, want -1 (unknown)", n)
	}
}

func TestResult_LastInsertIdErrors(t *testing.T) {
	r := result{}
	_, err := r.LastInsertId()
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("LastInsertId err = %v, want not-supported error", err)
	}
}
