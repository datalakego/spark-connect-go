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
	"fmt"
	"iter"
	"reflect"
)

// Dataset is a type alias for DataFrameOf[T]. Matches the Scala/Java
// Dataset[T] naming; DataFrameOf[T] remains as the original name and
// is fully interchangeable.
type Dataset[T any] = DataFrameOf[T]

// ErrNotFound is returned by First when the DataFrame produces zero
// rows.
var ErrNotFound = errors.New("spark: no rows returned")

// Collect materialises every row of df into a []T by wrapping df in
// the typed surface and calling Collect. Equivalent to
// TypedDataFrame[T](df).Collect(ctx) but written as a one-liner for
// callers who already hold a DataFrame.
func Collect[T any](ctx context.Context, df DataFrame) ([]T, error) {
	typed, err := TypedDataFrame[T](df)
	if err != nil {
		return nil, err
	}
	return typed.Collect(ctx)
}

// Stream yields typed rows one at a time using the untyped
// DataFrame's streaming primitive underneath. Constant memory
// regardless of result size. Schema binding happens on the first row;
// a subsequent row whose schema diverges from the first surfaces the
// error through the iterator.
//
// Consumers range over the return value with Go 1.23's iter.Seq2:
//
//	for row, err := range sql.Stream[User](ctx, df) {
//	    if err != nil { break }
//	    // use row
//	}
func Stream[T any](ctx context.Context, df DataFrame) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		typed, err := TypedDataFrame[T](df)
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
				b, berr := typed.plan.bind(row.FieldNames())
				if berr != nil {
					yield(zero, berr)
					return
				}
				bindings = b
			}
			var out T
			if derr := decodeRow(typed.plan, row.Values(), bindings, &out); derr != nil {
				yield(zero, derr)
				return
			}
			if !yield(out, nil) {
				return
			}
		}
	}
}

// First returns the first row of df decoded as T, or ErrNotFound if
// df produced no rows. Runs Collect underneath at v0; the DataFrame
// LIMIT optimisation lands when Dataset[T].Limit stabilises.
func First[T any](ctx context.Context, df DataFrame) (*T, error) {
	rows, err := Collect[T](ctx, df)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

// As wraps df in the typed surface. Alias for TypedDataFrame[T],
// named to match CLAUDE_CODE_BOOTSTRAP.md and Scala's Encoder-flavoured
// naming. Schema compatibility with T is validated lazily — the first
// call to Collect or Stream surfaces drift, not As itself.
func As[T any](df DataFrame) (*DataFrameOf[T], error) {
	return TypedDataFrame[T](df)
}

// Into scans df into dst where dst is a pointer to either a slice
// (populated with every row) or a single struct (the first row;
// ErrNotFound on empty). Non-generic variant for cases where T is
// not known at compile time — typical of code-generated consumers
// or reflection-heavy DSLs.
func Into(ctx context.Context, df DataFrame, dst any) error {
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("spark.Into: dst must be a non-nil pointer, got %T", dst)
	}
	elem := rv.Elem()
	switch elem.Kind() {
	case reflect.Slice:
		return intoSlice(ctx, df, elem)
	case reflect.Struct:
		return intoStruct(ctx, df, elem)
	default:
		return fmt.Errorf("spark.Into: dst must point to a slice or struct, got %v", elem.Kind())
	}
}

func intoSlice(ctx context.Context, df DataFrame, sliceValue reflect.Value) error {
	elemType := sliceValue.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("spark.Into: slice element type must be a struct, got %v", elemType)
	}
	plan, err := buildRowPlan(elemType)
	if err != nil {
		return err
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))
		return nil
	}
	bindings, err := plan.bind(rows[0].FieldNames())
	if err != nil {
		return err
	}
	out := reflect.MakeSlice(sliceValue.Type(), len(rows), len(rows))
	for i, r := range rows {
		if err := decodeRowReflect(plan, r.Values(), bindings, out.Index(i)); err != nil {
			return fmt.Errorf("spark.Into: row %d: %w", i, err)
		}
	}
	sliceValue.Set(out)
	return nil
}

func intoStruct(ctx context.Context, df DataFrame, structValue reflect.Value) error {
	plan, err := buildRowPlan(structValue.Type())
	if err != nil {
		return err
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	bindings, err := plan.bind(rows[0].FieldNames())
	if err != nil {
		return err
	}
	return decodeRowReflect(plan, rows[0].Values(), bindings, structValue)
}

// decodeRowReflect is the non-generic variant of decodeRow used by
// Into. Mirrors decodeRow's logic but walks the destination via
// reflect.Value rather than *T, so the caller can populate one slot
// of an already-allocated []T without instantiating T.
func decodeRowReflect(plan *rowPlan, values []any, bindings []columnBinding, dest reflect.Value) error {
	for ci := 0; ci < len(values) && ci < len(bindings); ci++ {
		b := bindings[ci]
		if b.planIndex < 0 {
			continue
		}
		pf := &plan.fields[b.planIndex]
		target := fieldByIndex(dest, pf.index)
		if err := assignTypedValue(target, values[ci]); err != nil {
			return fmt.Errorf("column %d (%s): %w", ci, pf.name, err)
		}
	}
	return nil
}
