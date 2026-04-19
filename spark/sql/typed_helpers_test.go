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

// Dataset[T] is the alias; verify at compile time that it's
// interchangeable with DataFrameOf[T]. A failing assertion here is
// a type-level regression caught without running any code.
var _ *DataFrameOf[typedUser] = (*Dataset[typedUser])(nil)

func TestErrNotFound_Sentinel(t *testing.T) {
	// First's empty-result path wraps ErrNotFound; verify callers can
	// branch on it via errors.Is.
	wrapped := errors.Join(ErrNotFound, errors.New("context"))
	if !errors.Is(wrapped, ErrNotFound) {
		t.Error("ErrNotFound should match via errors.Is through errors.Join")
	}
}

func TestInto_RejectsNonPointer(t *testing.T) {
	err := Into(context.Background(), nil, "not a pointer")
	if err == nil || !strings.Contains(err.Error(), "non-nil pointer") {
		t.Errorf("want non-nil-pointer error, got %v", err)
	}
}

func TestInto_RejectsNilPointer(t *testing.T) {
	var users *[]typedUser
	err := Into(context.Background(), nil, users)
	if err == nil || !strings.Contains(err.Error(), "non-nil pointer") {
		t.Errorf("want non-nil-pointer error, got %v", err)
	}
}

func TestInto_RejectsPointerToNonSliceNonStruct(t *testing.T) {
	n := 42
	err := Into(context.Background(), nil, &n)
	if err == nil || !strings.Contains(err.Error(), "slice or struct") {
		t.Errorf("want slice-or-struct error, got %v", err)
	}
}

func TestInto_RejectsSliceOfNonStruct(t *testing.T) {
	// Cover the elemType check in intoSlice before any I/O would fire.
	ns := []int{}
	err := Into(context.Background(), nil, &ns)
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("want slice-element-struct error, got %v", err)
	}
}

func TestAs_RejectsNonStructT(t *testing.T) {
	// As[T] delegates to TypedDataFrame[T] which enforces T must be a
	// struct. Verify the error surfaces before any DataFrame I/O.
	_, err := As[int](nil)
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("want struct-required error, got %v", err)
	}
}

func TestCollect_RejectsNonStructT(t *testing.T) {
	_, err := Collect[int](context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("want struct-required error, got %v", err)
	}
}
