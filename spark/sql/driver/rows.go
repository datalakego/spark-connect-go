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
	"io"

	"github.com/datalake-go/spark-connect-go/spark/sql/types"
)

// rows wraps a materialised slice of sparksql types.Row values so
// they satisfy database/sql/driver.Rows. The SELECT path in stmt.go
// collects a DataFrame first and hands the result here — fine for
// the small reads goose makes against the version table; larger
// scans should bypass the database/sql driver and use the native
// DataFrame / iter.Seq2 path instead.
type rows struct {
	rows  []types.Row
	cols  []string
	index int
}

// newRows builds a Rows handle from a slice of sparksql rows.
// Columns are read from the first row's FieldNames; if the slice
// is empty, the column list is empty and Next immediately reports
// io.EOF.
func newRows(collected []types.Row) *rows {
	var cols []string
	if len(collected) > 0 {
		cols = collected[0].FieldNames()
	}
	return &rows{rows: collected, cols: cols}
}

// Columns returns the column names. Implements
// database/sql/driver.Rows.
func (r *rows) Columns() []string { return r.cols }

// Close releases any driver-held resources. The slice is already
// fully materialised; this is a no-op.
// Implements database/sql/driver.Rows.
func (r *rows) Close() error { return nil }

// Next advances to the next row and writes its values into dest.
// Returns io.EOF when the slice is exhausted. Implements
// database/sql/driver.Rows.
func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	values := r.rows[r.index].Values()
	for i := range dest {
		if i >= len(values) {
			dest[i] = nil
			continue
		}
		dest[i] = values[i]
	}
	r.index++
	return nil
}
