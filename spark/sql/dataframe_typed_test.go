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
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedUser struct {
	ID        string    `spark:"id"`
	Email     string    `spark:"email"`
	Country   string    // no tag — falls back to snake_case of field name
	CreatedAt time.Time `spark:"created_at"`
	Secret    string    `spark:"-"` // skipped
	ignored   string    //nolint:unused // unexported → skipped
}

func TestBuildRowPlan_TagsAndSnakeCase(t *testing.T) {
	plan, err := buildRowPlan(reflect.TypeOf(typedUser{}))
	require.NoError(t, err)

	names := make([]string, 0, len(plan.fields))
	for _, f := range plan.fields {
		names = append(names, f.name)
	}
	assert.Equal(t, []string{"id", "email", "country", "created_at"}, names,
		"tag takes precedence; untagged fields snake_case; `-` and unexported skipped")
}

func TestBind_MatchesColumns(t *testing.T) {
	plan, _ := buildRowPlan(reflect.TypeOf(typedUser{}))

	// Columns may arrive in a different order than fields and can
	// include extras the struct doesn't care about; the binder
	// should pick the right indices and ignore the stranger.
	cols := []string{"created_at", "extra_col", "email", "id", "country"}
	bindings, err := plan.bind(cols)
	require.NoError(t, err)

	want := []int{3, -1, 1, 0, 2}
	for i, b := range bindings {
		assert.Equalf(t, want[i], b.planIndex,
			"column %q bound to wrong plan index", cols[i])
	}
}

func TestBind_SchemaDriftErrorsEarly(t *testing.T) {
	plan, _ := buildRowPlan(reflect.TypeOf(typedUser{}))
	// email column missing — the struct wants it, the result doesn't
	// have it. Bind must surface this, not defer to per-row decode.
	_, err := plan.bind([]string{"id", "country", "created_at"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "email",
		"schema-drift error should name the missing column")
}

func TestDecodeRow_ArrowTimestampToTime(t *testing.T) {
	plan, _ := buildRowPlan(reflect.TypeOf(typedUser{}))
	cols := []string{"id", "email", "country", "created_at"}
	bindings, err := plan.bind(cols)
	require.NoError(t, err)

	when := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	// arrow.Timestamp is an int64 of microseconds since epoch. Build
	// one directly so the decode path's Microsecond branch fires.
	ts := arrow.Timestamp(when.UnixMicro())

	values := []any{"abc", "alice@example.com", "UK", ts}

	var out typedUser
	require.NoError(t, decodeRow(plan, values, bindings, &out))

	assert.Equal(t, "abc", out.ID)
	assert.Equal(t, "alice@example.com", out.Email)
	assert.Equal(t, "UK", out.Country)
	assert.True(t, out.CreatedAt.Equal(when), "TIMESTAMP micros should round-trip to time.Time")
}

func TestDecodeRow_AssignableAndConvertible(t *testing.T) {
	type row struct {
		N int    `spark:"n"` // assignable from int
		S string `spark:"s"` // assignable from string
		F int    `spark:"f"` // float64 → int is convertible
	}
	plan, err := buildRowPlan(reflect.TypeOf(row{}))
	require.NoError(t, err)
	bindings, err := plan.bind([]string{"n", "s", "f"})
	require.NoError(t, err)

	var out row
	require.NoError(t, decodeRow(plan, []any{int(42), "hello", float64(7)}, bindings, &out))
	assert.Equal(t, 42, out.N)
	assert.Equal(t, "hello", out.S)
	assert.Equal(t, 7, out.F)
}

func TestDecodeRow_NilIsZero(t *testing.T) {
	type row struct {
		N int    `spark:"n"`
		S string `spark:"s"`
	}
	plan, _ := buildRowPlan(reflect.TypeOf(row{}))
	bindings, _ := plan.bind([]string{"n", "s"})

	var out row
	require.NoError(t, decodeRow(plan, []any{nil, nil}, bindings, &out))
	assert.Zero(t, out.N)
	assert.Zero(t, out.S)
}

func TestTypedDataFrame_RejectsNonStruct(t *testing.T) {
	// TypedDataFrame is supposed to surface the misuse at construction
	// time, not at Collect. A map / slice / primitive should fail
	// clearly with a pointer back at the caller's T.
	type notAStruct = map[string]string
	_, err := TypedDataFrame[notAStruct](nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "T must be a struct")
}

func TestSnakeCase_CommonShapes(t *testing.T) {
	cases := map[string]string{
		"ID":          "id",
		"Email":       "email",
		"CreatedAt":   "created_at",
		"HTTPServer":  "http_server",
		"JSONPayload": "json_payload",
		"A":           "a",
		"":            "",
	}
	for in, want := range cases {
		assert.Equalf(t, want, snakeCase(in), "snakeCase(%q)", in)
	}
}
