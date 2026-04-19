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
	"fmt"
	"iter"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
)

// DataFrameOf[T] is an edge-typing wrapper around a regular DataFrame.
// Its role is narrow: cache the reflected row plan so a caller who
// already knows the result shape can materialise into []T / iter /
// *T without re-validating T on every call.
//
// The wrapper deliberately has no transformation methods (no Where,
// Limit, OrderBy, Select, Join, GroupBy). Transformations change row
// shape and make the type parameter lie. Callers compose
// transformations on the untyped DataFrame, then re-type at the edge
// via As[T] when ready to collect.
//
// Column binding uses struct tags in the same shape sqlx / parquet-go
// already use:
//
//	type User struct {
//	    ID      string    `spark:"id"`
//	    Email   string    `spark:"email"`
//	    Created time.Time `spark:"created_at"`
//	}
//
// Fields without a `spark:"..."` tag are mapped by snake_case'd field
// name, so a plain Go struct works without any tags at all. Fields
// tagged `spark:"-"` are skipped. Columns in the DataFrame that
// don't match any field are ignored — typical of projections narrower
// than the struct.
//
// Schema drift (a struct field that the result's projection doesn't
// contain) surfaces at the first Collect / Stream / First call as a
// single error rather than per-row panics.
type DataFrameOf[T any] struct {
	df   DataFrame
	plan *rowPlan
}

// As wraps df in the typed surface. T must be a struct; the row plan
// is computed and cached once so subsequent Collect / Stream / First
// calls don't re-reflect. Schema compatibility with T is validated
// lazily — the first materialisation surfaces drift, not As itself.
//
// This is the only supported constructor for DataFrameOf[T]. Callers
// hold a DataFrame (from session.Sql, session.Table, a chain of
// untyped transformations) and hand it here at the point the result
// shape is known.
func As[T any](df DataFrame) (*DataFrameOf[T], error) {
	var zero T
	rt := reflect.TypeOf(zero)
	if rt == nil || rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("DataFrameOf[T]: T must be a struct, got %v", rt)
	}
	plan, err := buildRowPlan(rt)
	if err != nil {
		return nil, err
	}
	return &DataFrameOf[T]{df: df, plan: plan}, nil
}

// DataFrame returns the underlying untyped DataFrame. Escape hatch
// for transformations the typed surface deliberately doesn't carry
// — GroupBy, joins, window functions, Where, Limit, OrderBy. Chain
// on the untyped handle, then call As[T] again when the output
// shape is known.
func (d *DataFrameOf[T]) DataFrame() DataFrame { return d.df }

// Collect materialises every row into a []T. Holds the whole table
// on the heap for the duration of the call — callers with large
// result sets should project narrower on the SQL side or use Stream
// for constant-memory iteration.
func (d *DataFrameOf[T]) Collect(ctx context.Context) ([]T, error) {
	rows, err := d.df.Collect(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	bindings, err := d.plan.bind(rows[0].FieldNames())
	if err != nil {
		return nil, err
	}
	out := make([]T, len(rows))
	for i, r := range rows {
		if err := decodeRow(d.plan, r.Values(), bindings, &out[i]); err != nil {
			return nil, fmt.Errorf("DataFrameOf[T].Collect: row %d: %w", i, err)
		}
	}
	return out, nil
}

// Stream yields typed rows one at a time with constant memory. Uses
// the untyped DataFrame.All streaming primitive underneath. Consumers
// range with Go 1.23's iter.Seq2:
//
//	for row, err := range ds.Stream(ctx) {
//	    if err != nil { break }
//	    // use row
//	}
func (d *DataFrameOf[T]) Stream(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		var bindings []columnBinding
		for row, rerr := range d.df.All(ctx) {
			if rerr != nil {
				yield(zero, rerr)
				return
			}
			if bindings == nil {
				b, berr := d.plan.bind(row.FieldNames())
				if berr != nil {
					yield(zero, berr)
					return
				}
				bindings = b
			}
			var out T
			if derr := decodeRow(d.plan, row.Values(), bindings, &out); derr != nil {
				yield(zero, derr)
				return
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

// First returns a pointer to the first row as T, or ErrNotFound when
// the DataFrame produces zero rows. Runs Collect underneath at v0;
// a LIMIT 1 pushdown lands alongside the Spark Connect plan-cache
// work in a future cycle.
func (d *DataFrameOf[T]) First(ctx context.Context) (*T, error) {
	rows, err := d.Collect(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// rowPlan caches the reflected structure of T so Collect doesn't
// reflect on every row. Built once per DataFrameOf[T].
type rowPlan struct {
	goType reflect.Type
	fields []plannedField
}

type plannedField struct {
	name  string // column name from tag or snake_case'd field name
	index []int  // reflect.FieldByIndex path
	gotyp reflect.Type
}

// columnBinding maps a result-set column position to the field slot
// in the plan that should receive it. A column that the struct
// doesn't describe has planIndex = -1 and is skipped.
type columnBinding struct {
	planIndex int
}

var rowPlanCache sync.Map // reflect.Type -> *rowPlan

func buildRowPlan(rt reflect.Type) (*rowPlan, error) {
	if cached, ok := rowPlanCache.Load(rt); ok {
		return cached.(*rowPlan), nil
	}
	plan := &rowPlan{goType: rt}
	if err := walkPlan(rt, nil, plan); err != nil {
		return nil, err
	}
	rowPlanCache.Store(rt, plan)
	return plan, nil
}

func walkPlan(rt reflect.Type, parent []int, plan *rowPlan) error {
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		idx := append(append([]int{}, parent...), i)
		tag := sf.Tag.Get("spark")
		if tag == "-" {
			continue
		}
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct && tag == "" {
			if err := walkPlan(sf.Type, idx, plan); err != nil {
				return err
			}
			continue
		}
		name := tag
		if name == "" {
			name = snakeCase(sf.Name)
		}
		plan.fields = append(plan.fields, plannedField{
			name:  name,
			index: idx,
			gotyp: sf.Type,
		})
	}
	return nil
}

// bind aligns the plan's fields with a concrete set of result
// columns. Result columns that don't map to any planned field are
// tagged planIndex = -1 and dropped at decode time. A planned field
// that doesn't appear in the columns is a schema-drift error — the
// SQL changed in a way the struct doesn't describe and we want the
// caller to know at plan time, not per row.
func (p *rowPlan) bind(columns []string) ([]columnBinding, error) {
	byName := make(map[string]int, len(p.fields))
	for i, f := range p.fields {
		byName[f.name] = i
	}
	bindings := make([]columnBinding, len(columns))
	seen := make(map[int]bool, len(p.fields))
	for ci, name := range columns {
		if idx, ok := byName[name]; ok {
			bindings[ci] = columnBinding{planIndex: idx}
			seen[idx] = true
		} else {
			bindings[ci] = columnBinding{planIndex: -1}
		}
	}
	var missing []string
	for i, f := range p.fields {
		if !seen[i] {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("DataFrameOf[T]: struct field(s) not in result schema: %s",
			strings.Join(missing, ", "))
	}
	return bindings, nil
}

// decodeRow writes values into *T using the bindings. dest is a
// pointer to T; we take *T (not T) so the caller can write each
// element of a pre-allocated []T slice without paying for a
// reflect.Value per row.
func decodeRow[T any](plan *rowPlan, values []any, bindings []columnBinding, dest *T) error {
	dv := reflect.ValueOf(dest).Elem()
	for ci := 0; ci < len(values) && ci < len(bindings); ci++ {
		b := bindings[ci]
		if b.planIndex < 0 {
			continue
		}
		pf := &plan.fields[b.planIndex]
		target := fieldByIndex(dv, pf.index)
		if err := assignTypedValue(target, values[ci]); err != nil {
			return fmt.Errorf("column %d (%s): %w", ci, pf.name, err)
		}
	}
	return nil
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	cur := v
	for _, i := range index {
		for cur.Kind() == reflect.Ptr {
			if cur.IsNil() {
				cur.Set(reflect.New(cur.Type().Elem()))
			}
			cur = cur.Elem()
		}
		cur = cur.Field(i)
	}
	return cur
}

// assignTypedValue writes src into the reflect field, doing the
// small set of conversions Spark's row values need — nil → zero for
// optional fields, arrow.Timestamp → time.Time for TIMESTAMP
// columns, assignable/convertible for primitives. Anything rarer
// surfaces as an explicit error so callers can tighten their struct
// to match.
func assignTypedValue(dst reflect.Value, src any) error {
	if src == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	dt := dst.Type()
	isPtr := dt.Kind() == reflect.Ptr
	innerType := dt
	if isPtr {
		innerType = dt.Elem()
	}
	if ts, ok := src.(arrow.Timestamp); ok && innerType == reflect.TypeOf(time.Time{}) {
		setTypedValue(dst, reflect.ValueOf(ts.ToTime(arrow.Microsecond)), isPtr, innerType)
		return nil
	}
	sv := reflect.ValueOf(src)
	if sv.Type().AssignableTo(innerType) {
		setTypedValue(dst, sv, isPtr, innerType)
		return nil
	}
	if sv.Type().ConvertibleTo(innerType) {
		setTypedValue(dst, sv.Convert(innerType), isPtr, innerType)
		return nil
	}
	return fmt.Errorf("cannot assign %T to %v", src, dt)
}

func setTypedValue(dst, src reflect.Value, isPtr bool, inner reflect.Type) {
	if isPtr {
		p := reflect.New(inner)
		p.Elem().Set(src)
		dst.Set(p)
		return
	}
	dst.Set(src)
}

// snakeCase converts a Go field name to its snake_case form. Matches
// the convention used by sqlx / gorm / jackc so a plain Go struct
// with no tags lines up with columns that follow standard SQL naming.
func snakeCase(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) {
			prev := runes[i-1]
			if unicode.IsLower(prev) || (i+1 < len(runes) && unicode.IsLower(runes[i+1])) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
