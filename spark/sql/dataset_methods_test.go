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
	"reflect"
	"testing"
)

// Where / Limit / OrderBy are lazy — they queue an op on the
// DataFrameOf without touching the underlying DataFrame. Verify the
// queue state, not the materialised output (the latter requires a
// Spark Connect endpoint and lives in the integration suite).

func newTestDataset(t *testing.T) *DataFrameOf[typedUser] {
	t.Helper()
	plan, err := buildRowPlan(reflect.TypeOf(typedUser{}))
	if err != nil {
		t.Fatalf("buildRowPlan: %v", err)
	}
	return &DataFrameOf[typedUser]{plan: plan}
}

func TestDataset_WhereQueuesOp(t *testing.T) {
	ds := newTestDataset(t).Where("country = 'UK'")
	if len(ds.ops) != 1 {
		t.Fatalf("expected 1 op after Where, got %d", len(ds.ops))
	}
}

func TestDataset_LimitQueuesOp(t *testing.T) {
	ds := newTestDataset(t).Limit(10)
	if len(ds.ops) != 1 {
		t.Fatalf("expected 1 op after Limit, got %d", len(ds.ops))
	}
}

func TestDataset_OrderByQueuesOp(t *testing.T) {
	ds := newTestDataset(t).OrderBy("created_at", "id")
	if len(ds.ops) != 1 {
		t.Fatalf("expected 1 op after OrderBy, got %d", len(ds.ops))
	}
}

func TestDataset_ChainableWhereLimitOrderBy(t *testing.T) {
	ds := newTestDataset(t).
		Where("country = 'UK'").
		OrderBy("created_at").
		Limit(10)
	if len(ds.ops) != 3 {
		t.Fatalf("expected 3 ops after chain, got %d", len(ds.ops))
	}
}

func TestDataset_CloneIsolatesOps(t *testing.T) {
	parent := newTestDataset(t).Where("a = 1")
	child := parent.Where("b = 2")
	// parent should still have exactly 1 op; child 2.
	if len(parent.ops) != 1 {
		t.Errorf("parent mutated: %d ops, want 1", len(parent.ops))
	}
	if len(child.ops) != 2 {
		t.Errorf("child ops: %d, want 2", len(child.ops))
	}
}

func TestDataset_ResolveWithNoOpsReturnsUnderlying(t *testing.T) {
	// A dataset with no queued ops should resolve to its own df field
	// unchanged. Use a sentinel DataFrame that errors on any method;
	// resolve must not touch it.
	var called bool
	fake := &sentinelDataFrame{mark: &called}
	plan, _ := buildRowPlan(reflect.TypeOf(typedUser{}))
	ds := &DataFrameOf[typedUser]{df: fake, plan: plan}
	got, err := ds.resolveDataFrame(context.Background())
	if err != nil {
		t.Fatalf("resolveDataFrame: %v", err)
	}
	if got != fake {
		t.Errorf("expected underlying df returned unchanged")
	}
	if called {
		t.Errorf("no-op resolve should not invoke any DataFrame method")
	}
}

// sentinelDataFrame is a fake DataFrame whose only job is to be
// returned by resolve-with-no-ops. Methods panic or flag a marker —
// used to prove resolveDataFrame isn't touching the underlying.
type sentinelDataFrame struct {
	DataFrame
	mark *bool
}
