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
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// render interpolates driver.NamedValue arguments into a query
// string at `$N` placeholders ($1, $2, ...). Values are quoted as
// Spark SQL literals on the way through.
//
// Spark Connect's parameterised-query proto exists but doesn't
// round-trip reliably across every Spark version the driver
// supports. Client-side rendering keeps the driver compatible with
// the full 3.4+ range and mirrors what the native Spark driver in
// datalakego/lakeorm already does for the same reason.
//
// Accepts `$N` placeholders only. `?` is reserved for a future cycle
// if a caller asks for it — most database/sql-adjacent codegen
// (sqlc, goose's own dialects, pgx patterns) emits `$N`, so the
// narrower set suffices and keeps the renderer simple.
//
// Supported types are the stdlib-sql defaults: nil, bool, int64,
// float64, string, []byte, time.Time. Anything rarer surfaces as an
// error so the caller tightens their query before seeing a silently
// mis-interpolated value on the wire.
func render(query string, args []driver.NamedValue) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	byOrdinal := make(map[int]driver.Value, len(args))
	for _, a := range args {
		byOrdinal[a.Ordinal] = a.Value
	}

	var b strings.Builder
	b.Grow(len(query) + 32)
	for i := 0; i < len(query); {
		c := query[i]

		// Pass single-quoted string literals through untouched, so a
		// `'$1'` in the caller's query stays literal rather than
		// getting parameter-substituted.
		if c == '\'' {
			b.WriteByte(c)
			i++
			for i < len(query) {
				if query[i] == '\'' {
					// Doubled '' is an escaped quote; keep scanning.
					if i+1 < len(query) && query[i+1] == '\'' {
						b.WriteByte('\'')
						b.WriteByte('\'')
						i += 2
						continue
					}
					b.WriteByte('\'')
					i++
					break
				}
				b.WriteByte(query[i])
				i++
			}
			continue
		}

		if c == '$' && i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			ord, _ := strconv.Atoi(query[i+1 : j])
			v, ok := byOrdinal[ord]
			if !ok {
				return "", fmt.Errorf("spark driver: no argument for placeholder $%d", ord)
			}
			lit, err := sqlLiteral(v)
			if err != nil {
				return "", fmt.Errorf("spark driver: placeholder $%d: %w", ord, err)
			}
			b.WriteString(lit)
			i = j
			continue
		}

		b.WriteByte(c)
		i++
	}
	return b.String(), nil
}

// sqlLiteral renders a single Go value as a Spark SQL literal token.
// Strings are single-quoted with embedded quotes doubled. Time values
// render as TIMESTAMP literals in UTC microsecond precision — matches
// Spark's default timestamp parsing.
func sqlLiteral(v driver.Value) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'", nil
	case time.Time:
		return "TIMESTAMP '" + x.UTC().Format("2006-01-02 15:04:05.000000") + "'", nil
	default:
		return "", fmt.Errorf("unsupported arg type %T", v)
	}
}
