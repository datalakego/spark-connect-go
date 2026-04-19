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

package sql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// stubSession returns pre-canned DataFrames / errors from Sql and
// Table so the SqlAs / TableAs call-paths can be exercised without a
// live Spark Connect endpoint. Only Sql and Table need to do anything;
// the other SparkSession methods never fire on these paths.
type stubSession struct {
	SparkSession // embed for default no-op behaviour
	sqlFn        func(ctx context.Context, query string) (DataFrame, error)
	tableFn      func(name string) (DataFrame, error)
}

func (s *stubSession) Sql(ctx context.Context, query string) (DataFrame, error) {
	return s.sqlFn(ctx, query)
}
func (s *stubSession) Table(name string) (DataFrame, error) { return s.tableFn(name) }

func TestSqlAs_ForwardsQueryAndWrapsInDataFrameOf(t *testing.T) {
	var seen string
	session := &stubSession{
		sqlFn: func(_ context.Context, query string) (DataFrame, error) {
			seen = query
			return nil, nil // no actual DataFrame; TypedDataFrame still builds the plan
		},
	}
	const q = "SELECT id, email FROM users"
	ds, err := SqlAs[typedUser](context.Background(), session, q)
	if err != nil {
		t.Fatalf("SqlAs: %v", err)
	}
	if seen != q {
		t.Errorf("session.Sql got %q, want %q", seen, q)
	}
	if ds == nil {
		t.Fatalf("SqlAs returned nil Dataset on success")
	}
	if ds.plan == nil {
		t.Error("SqlAs returned Dataset without a cached plan")
	}
}

func TestSqlAs_PropagatesSessionError(t *testing.T) {
	wantErr := errors.New("connection refused")
	session := &stubSession{
		sqlFn: func(_ context.Context, _ string) (DataFrame, error) { return nil, wantErr },
	}
	_, err := SqlAs[typedUser](context.Background(), session, "SELECT 1")
	if !errors.Is(err, wantErr) && err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

func TestSqlAs_RejectsNonStructT(t *testing.T) {
	session := &stubSession{
		sqlFn: func(_ context.Context, _ string) (DataFrame, error) { return nil, nil },
	}
	_, err := SqlAs[int](context.Background(), session, "SELECT 1")
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("want struct-required error, got %v", err)
	}
}

func TestTableAs_ForwardsTableNameAndWrapsInDataFrameOf(t *testing.T) {
	var seen string
	session := &stubSession{
		tableFn: func(name string) (DataFrame, error) {
			seen = name
			return nil, nil
		},
	}
	const tbl = "users"
	ds, err := TableAs[typedUser](context.Background(), session, tbl)
	if err != nil {
		t.Fatalf("TableAs: %v", err)
	}
	if seen != tbl {
		t.Errorf("session.Table got %q, want %q", seen, tbl)
	}
	if ds == nil || ds.plan == nil {
		t.Error("TableAs returned empty Dataset on success")
	}
}

func TestTableAs_PropagatesSessionError(t *testing.T) {
	wantErr := errors.New("table not found")
	session := &stubSession{
		tableFn: func(_ string) (DataFrame, error) { return nil, wantErr },
	}
	_, err := TableAs[typedUser](context.Background(), session, "missing")
	if !errors.Is(err, wantErr) && err != wantErr {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

func TestSqlTyped_DeprecatedAliasEqualsSqlAs(t *testing.T) {
	// SqlTyped is retained as a deprecated alias that delegates to
	// SqlAs. Verify it forwards identically — same query reaches the
	// session, same plan is built.
	var seen string
	session := &stubSession{
		sqlFn: func(_ context.Context, query string) (DataFrame, error) {
			seen = query
			return nil, nil
		},
	}
	const q = "SELECT id FROM users"
	ds, err := SqlTyped[typedUser](context.Background(), session, q)
	if err != nil {
		t.Fatalf("SqlTyped: %v", err)
	}
	if seen != q {
		t.Errorf("SqlTyped forwarded %q, want %q", seen, q)
	}
	if ds == nil {
		t.Fatalf("SqlTyped returned nil Dataset")
	}
}
