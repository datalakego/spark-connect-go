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

package driver

import (
	"errors"
	"fmt"
	"strings"
)

// dsnConfig is the parsed DSN. The driver is a boring transport
// layer — it stays out of table-format concerns (Iceberg vs Delta
// vs parquet) and keeps the original URL intact for the Spark
// Connect session builder to interpret.
type dsnConfig struct {
	// sparkDSN is what gets passed to NewSessionBuilder().Remote(...).
	// Includes any `?token=` or other query fragment verbatim so the
	// upstream builder sees the URL exactly as the caller wrote it.
	sparkDSN string
}

// parseDSN accepts the `sc://host:port[?token=...]` Spark Connect
// URL form. The driver does not interpret query parameters beyond
// validating the scheme — any `token`, `user`, or future Spark
// Connect flag rides through in `sparkDSN` untouched.
//
// Table format selection (Iceberg, Delta, parquet) is explicitly
// NOT a driver concern. Migrations and application SQL pick format
// via Spark's native `USING <format>` DDL clause; the transport
// layer stays boring and reusable.
func parseDSN(dsn string) (*dsnConfig, error) {
	if dsn == "" {
		return nil, errors.New("spark driver: DSN is required")
	}
	if !strings.HasPrefix(dsn, "sc://") {
		return nil, fmt.Errorf("spark driver: DSN must start with sc://, got %q", dsn)
	}
	return &dsnConfig{sparkDSN: dsn}, nil
}
