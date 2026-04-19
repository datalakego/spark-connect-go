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
	"iter"
)

// ErrNotFound is returned by First when the DataFrame produces zero
// rows.
var ErrNotFound = errors.New("spark: no rows returned")

// Collect materialises every row of df into a []T. Thin wrapper over
// As[T] + (*DataFrameOf[T]).Collect for callers who hold an untyped
// DataFrame and want the result in one call.
func Collect[T any](ctx context.Context, df DataFrame) ([]T, error) {
	typed, err := As[T](df)
	if err != nil {
		return nil, err
	}
	return typed.Collect(ctx)
}

// Stream yields typed rows one at a time using the untyped
// DataFrame's streaming primitive underneath. Constant memory
// regardless of result size. Schema binding happens on the first
// row; a subsequent row whose schema diverges from the first
// surfaces the error through the iterator.
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
		typed, err := As[T](df)
		if err != nil {
			yield(zero, err)
			return
		}
		for row, rerr := range typed.Stream(ctx) {
			if !yield(row, rerr) {
				return
			}
		}
	}
}

// First returns the first row of df decoded as T, or ErrNotFound if
// df produced no rows.
func First[T any](ctx context.Context, df DataFrame) (*T, error) {
	typed, err := As[T](df)
	if err != nil {
		return nil, err
	}
	return typed.First(ctx)
}
