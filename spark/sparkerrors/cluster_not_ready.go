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
	"strings"
)

// ErrClusterNotReady is returned when a Spark cluster is in a
// warm-up state and the RPC is safe to retry after backoff. The
// canonical case is Databricks clusters returning
// `[FailedPrecondition]` with `state Pending` during cold start,
// but the pattern also fires for self-managed clusters whose
// drivers are still resolving JARs.
//
// Wrap the typed ClusterNotReady with %w so errors.Is lookups keep
// working:
//
//	if errors.Is(err, sparkerrors.ErrClusterNotReady) { ... }
//	if sparkerrors.IsClusterNotReady(err) { ... }
var ErrClusterNotReady = errors.New("sparkerrors: cluster not ready")

// ClusterNotReady is the typed form of ErrClusterNotReady. Carries
// enough context (state string, request ID, original cause) for an
// operator to correlate with server-side logs.
type ClusterNotReady struct {
	State     string // e.g. "Pending", "PENDING"
	RequestID string
	Message   string
	Cause     error
}

func (e *ClusterNotReady) Error() string {
	return fmt.Sprintf("cluster not ready (state=%s): %s", e.State, e.Message)
}

func (e *ClusterNotReady) Unwrap() error { return e.Cause }

// Is reports whether target is ErrClusterNotReady. Lets callers
// branch via errors.Is regardless of whether they hold the sentinel
// or the typed form.
func (e *ClusterNotReady) Is(target error) bool { return target == ErrClusterNotReady }

// IsRetryable marks the error as safe to retry after backoff.
// Callers that drive their own retry loops can branch on this
// without pulling in the package's retry machinery.
func (e *ClusterNotReady) IsRetryable() bool { return true }

// IsClusterNotReady is the convenience wrapper around errors.As for
// the ClusterNotReady type. Returns true whether err is the typed
// form or wraps the sentinel; either way a caller's retry loop
// makes the right call.
func IsClusterNotReady(err error) bool {
	if err == nil {
		return false
	}
	var typed *ClusterNotReady
	if errors.As(err, &typed) {
		return true
	}
	return errors.Is(err, ErrClusterNotReady)
}

// NewClusterNotReady inspects err for the canonical
// `[FailedPrecondition]` + `state Pending` pattern and returns a
// typed ClusterNotReady if it matches, else nil. The string-matching
// looks fragile but is the detection pattern that's held across
// multiple Databricks runtime versions and is the shape self-managed
// Spark clusters emit too.
//
// Callers typically invoke this in an RPC error path:
//
//	resp, err := cli.ExecutePlan(ctx, req)
//	if cn := sparkerrors.NewClusterNotReady(err); cn != nil {
//	    // retry with backoff
//	}
func NewClusterNotReady(err error) *ClusterNotReady {
	if err == nil {
		return nil
	}
	errStr := err.Error()

	if !strings.Contains(errStr, "[FailedPrecondition]") ||
		(!strings.Contains(errStr, "state Pending") &&
			!strings.Contains(errStr, "state PENDING")) {
		return nil
	}

	state := "PENDING"
	if idx := strings.Index(errStr, "[state="); idx != -1 {
		endIdx := strings.Index(errStr[idx:], "]")
		if endIdx != -1 {
			state = errStr[idx+7 : idx+endIdx]
		}
	}

	var requestID string
	if idx := strings.Index(errStr, "(requestId="); idx != -1 {
		endIdx := strings.Index(errStr[idx:], ")")
		if endIdx != -1 {
			requestID = errStr[idx+11 : idx+endIdx]
		}
	}

	return &ClusterNotReady{
		State:     state,
		RequestID: requestID,
		Message:   "The cluster is starting up. Please retry your request in a few moments.",
		Cause:     err,
	}
}
