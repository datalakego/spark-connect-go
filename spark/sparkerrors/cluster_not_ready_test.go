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

package sparkerrors

import (
	"errors"
	"fmt"
	"testing"
)

func TestNewClusterNotReady_DetectsCanonicalDatabricksPattern(t *testing.T) {
	// Shape ported verbatim from production Databricks responses.
	raw := errors.New(
		"rpc error: code = FailedPrecondition desc = " +
			"[FailedPrecondition] cluster with id=0313-xyz is in state Pending " +
			"[state=PENDING] (requestId=abc-123)",
	)
	got := NewClusterNotReady(raw)
	if got == nil {
		t.Fatalf("NewClusterNotReady returned nil on canonical Databricks error: %v", raw)
	}
	if got.State != "PENDING" {
		t.Errorf("State = %q, want PENDING", got.State)
	}
	if got.RequestID != "abc-123" {
		t.Errorf("RequestID = %q, want abc-123", got.RequestID)
	}
	if got.Cause != raw {
		t.Errorf("Cause should be the original error unchanged")
	}
	if !got.IsRetryable() {
		t.Errorf("IsRetryable should be true")
	}
}

func TestNewClusterNotReady_RejectsUnrelatedErrors(t *testing.T) {
	cases := []error{
		nil,
		errors.New("connection refused"),
		errors.New("rpc error: code = Internal desc = server error"),
		errors.New("[FailedPrecondition] lock held"), // no state Pending marker
	}
	for _, e := range cases {
		if got := NewClusterNotReady(e); got != nil {
			t.Errorf("NewClusterNotReady(%v) = %v, want nil", e, got)
		}
	}
}

func TestIsClusterNotReady_MatchesTypedAndSentinel(t *testing.T) {
	raw := errors.New(
		"[FailedPrecondition] state Pending starting up",
	)
	typed := NewClusterNotReady(raw)
	if !IsClusterNotReady(typed) {
		t.Errorf("IsClusterNotReady on typed ClusterNotReady should be true")
	}
	// Wrap the typed error — IsClusterNotReady should still match
	// because errors.As unwinds wrapping.
	wrapped := fmt.Errorf("retrying: %w", typed)
	if !IsClusterNotReady(wrapped) {
		t.Errorf("IsClusterNotReady on wrapped typed error should be true")
	}
	// A bare sentinel wrap also matches for callers that don't
	// produce the typed struct.
	sentinelWrap := fmt.Errorf("context: %w", ErrClusterNotReady)
	if !IsClusterNotReady(sentinelWrap) {
		t.Errorf("IsClusterNotReady on %%w-wrapped sentinel should be true")
	}
}

func TestIsClusterNotReady_FalseOnNilAndUnrelated(t *testing.T) {
	if IsClusterNotReady(nil) {
		t.Errorf("IsClusterNotReady(nil) should be false")
	}
	if IsClusterNotReady(errors.New("some other error")) {
		t.Errorf("IsClusterNotReady on unrelated error should be false")
	}
}

func TestClusterNotReady_IsMatchesSentinel(t *testing.T) {
	// errors.Is should match the sentinel through the typed form's
	// own Is method (tested independently of the convenience
	// IsClusterNotReady helper).
	typed := &ClusterNotReady{State: "PENDING"}
	if !errors.Is(typed, ErrClusterNotReady) {
		t.Errorf("errors.Is(typed, ErrClusterNotReady) should be true")
	}
}

func TestClusterNotReady_UnwrapExposesCause(t *testing.T) {
	cause := errors.New("underlying grpc error")
	typed := &ClusterNotReady{Cause: cause}
	if got := errors.Unwrap(typed); got != cause {
		t.Errorf("Unwrap returned %v, want %v", got, cause)
	}
}
