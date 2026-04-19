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
	"net/url"
	"strings"
)

// dsnConfig is the parsed DSN. Kept minimal — the Spark Connect
// session builder wants the original URL verbatim, so the config
// mostly exists to surface query-parameter knobs (format, token)
// that downstream tools want to read without re-parsing the DSN.
type dsnConfig struct {
	// sparkDSN is what gets passed to NewSessionBuilder().Remote(...).
	// Includes any `?token=` fragment if present.
	sparkDSN string

	// format is the value of the `format` query parameter ("iceberg"
	// / "delta" / empty). Driver ignores it; consumers that need
	// dialect-aware DDL read it via the exported Connector.Format()
	// accessor.
	format string
}

// parseDSN accepts the sc:// Spark Connect URL form with optional
// query parameters. Returns an error for malformed input.
//
// Recognised query parameters:
//
//   - token: bearer token forwarded to the Spark Connect server in
//     the Authorization header. Preserved in sparkDSN verbatim so
//     the session builder picks it up.
//   - format: lakehouse table format ("iceberg" | "delta"). Driver-
//     layer passthrough; used by consumers like goose-spark to pick
//     a dialect-appropriate CREATE TABLE.
//
// Unrecognised parameters are ignored (preserved in the DSN as-is)
// so the driver doesn't fight with future Spark Connect flags.
func parseDSN(dsn string) (*dsnConfig, error) {
	if dsn == "" {
		return nil, errors.New("spark driver: DSN is required")
	}
	if !strings.HasPrefix(dsn, "sc://") {
		return nil, fmt.Errorf("spark driver: DSN must start with sc://, got %q", dsn)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("spark driver: parse DSN: %w", err)
	}

	cfg := &dsnConfig{
		sparkDSN: dsn,
		format:   strings.ToLower(u.Query().Get("format")),
	}
	if cfg.format != "" && cfg.format != "iceberg" && cfg.format != "delta" {
		return nil, fmt.Errorf(
			"spark driver: unsupported format %q; expected iceberg or delta", cfg.format)
	}
	return cfg, nil
}
