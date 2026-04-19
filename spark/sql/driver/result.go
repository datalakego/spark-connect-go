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

import "errors"

// result is the driver.Result returned by every Exec / ExecContext.
// Spark Connect's session.Sql doesn't surface a row-count the way
// Postgres' CommandComplete message does, so we return -1 per the
// driver.Result docs: "RowsAffected returns the number of rows
// affected by an update, insert, or delete. Not every database or
// database driver may support this."
type result struct{}

// LastInsertId is not supported — lakehouse tables don't have
// auto-increment semantics. Returns an error so callers that reach
// for it see the intent clearly rather than getting a misleading
// zero.
func (result) LastInsertId() (int64, error) {
	return 0, errors.New("spark driver: LastInsertId is not supported; lakehouse tables have no auto-increment sequence")
}

// RowsAffected returns -1 to signal "unknown." Matches the
// convention several other drivers (notably snowflake, clickhouse)
// follow when the underlying engine doesn't emit the metric.
func (result) RowsAffected() (int64, error) {
	return -1, nil
}
