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

func TestErrNotFound_Sentinel(t *testing.T) {
	// First's empty-result path wraps ErrNotFound; verify callers can
	// branch on it via errors.Is.
	wrapped := errors.Join(ErrNotFound, errors.New("context"))
	if !errors.Is(wrapped, ErrNotFound) {
		t.Error("ErrNotFound should match via errors.Is through errors.Join")
	}
}

func TestAs_RejectsNonStructT(t *testing.T) {
	// As[T] is the sole constructor for DataFrameOf[T]; T must be a
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

func TestFirst_RejectsNonStructT(t *testing.T) {
	_, err := First[int](context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Errorf("want struct-required error, got %v", err)
	}
}
