package types

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
)

// NewRowSequence flattens record batches to a sequence of rows stream.
func NewRowSequence(ctx context.Context, recordSeq iter.Seq2[arrow.Record, error]) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		for rec, recErr := range recordSeq {
			select {
			case <-ctx.Done():
				_ = yield(nil, ctx.Err())
				return
			default:
			}

			// Treat io.EOF as clean stream termination. Some Spark
			// implementations (notably Databricks clusters as of 05/2025)
			// yield EOF as an error value instead of ending the sequence.
			if errors.Is(recErr, io.EOF) {
				return
			}
			if recErr != nil {
				// forward upstream error once, then stop
				_ = yield(nil, recErr)
				return
			}
			if rec == nil {
				_ = yield(nil, errors.New("expected arrow.Record to contain non-nil Rows, got nil"))
				return
			}

			rows, err := func() ([]Row, error) {
				defer rec.Release()
				return ReadArrowRecordToRows(rec)
			}()
			if err != nil {
				_ = yield(nil, err)
				return
			}
			for _, row := range rows {
				if !yield(row, nil) {
					return
				}
			}
		}
	}
}
