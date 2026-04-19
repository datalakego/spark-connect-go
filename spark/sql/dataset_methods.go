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
	"iter"

	"github.com/datalakego/spark-connect-go/spark/sql/column"
	"github.com/datalakego/spark-connect-go/spark/sql/functions"
)

// Where adds a filter predicate. Lazy: the condition is applied to
// the underlying DataFrame when Collect / Stream / First materialises
// the Dataset. Chainable — each call narrows the projection further.
//
// sqlStr is a Spark SQL fragment (e.g. "country = 'UK'" or
// "id IN ('a', 'b', 'c')"). args is accepted for API compatibility
// with dorm.Query's signature but currently ignored — the underlying
// DataFrame.Where takes a bare string; callers interpolate values
// with fmt.Sprintf or build the fragment via the functions package.
func (d *DataFrameOf[T]) Where(sqlStr string, args ...any) *DataFrameOf[T] {
	_ = args
	cp := d.clone()
	cp.ops = append(cp.ops, func(ctx context.Context, df DataFrame) (DataFrame, error) {
		return df.Where(ctx, sqlStr)
	})
	return cp
}

// Limit caps the number of rows materialised. Chainable; repeated
// calls each produce their own Limit relation in the underlying plan,
// and Spark's optimiser collapses them to the minimum.
func (d *DataFrameOf[T]) Limit(n int) *DataFrameOf[T] {
	cp := d.clone()
	cp.ops = append(cp.ops, func(ctx context.Context, df DataFrame) (DataFrame, error) {
		return df.Limit(ctx, int32(n)), nil
	})
	return cp
}

// OrderBy adds an ascending sort by one or more columns. Callers who
// need descending order, null-ordering modifiers, or expression-based
// sort keys drop to DataFrame() and invoke Sort directly with
// column.Convertible values.
func (d *DataFrameOf[T]) OrderBy(columns ...string) *DataFrameOf[T] {
	cp := d.clone()
	cp.ops = append(cp.ops, func(ctx context.Context, df DataFrame) (DataFrame, error) {
		cols := make([]column.Convertible, 0, len(columns))
		for _, name := range columns {
			cols = append(cols, functions.Col(name))
		}
		return df.Sort(ctx, cols...)
	})
	return cp
}

// First returns a pointer to the first row as T. Applies Limit(1)
// under the hood before materialising. Returns ErrNotFound when the
// (possibly filtered) DataFrame has zero rows.
func (d *DataFrameOf[T]) First(ctx context.Context) (*T, error) {
	rows, err := d.Limit(1).Collect(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// Stream yields typed rows one at a time with constant memory,
// honouring any queued Where / Limit / OrderBy. Uses the untyped
// DataFrame.All streaming primitive underneath. Consumers range with
// Go 1.23's iter.Seq2:
//
//	for row, err := range ds.Stream(ctx) {
//	    if err != nil { break }
//	    // use row
//	}
func (d *DataFrameOf[T]) Stream(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		df, err := d.resolveDataFrame(ctx)
		if err != nil {
			yield(zero, err)
			return
		}
		var bindings []columnBinding
		for row, rerr := range df.All(ctx) {
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
