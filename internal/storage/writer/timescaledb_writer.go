package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type TimescaleDBWriter struct {
	db *pgxpool.Pool
}

func NewTimescaleDBWriter(db *pgxpool.Pool) *TimescaleDBWriter {
	return &TimescaleDBWriter{db: db}
}

func (w *TimescaleDBWriter) Write(ctx context.Context, metrics []*pb.Metric) error {
	rows := make([][]any, len(metrics))

	for i, m := range metrics {
		ts := time.Unix(0, m.TimestampUnixNano)

		attrsJSON, err := json.Marshal(m.Attributes)
		if err != nil {
			return fmt.Errorf("failed to marshal attributes: %w", err)
		}

		var val float64
		if gauge := m.GetGauge(); gauge != nil {
			val = gauge.Value
		} else if counter := m.GetCounter(); counter != nil {
			val = float64(counter.Total)
		}

		rows[i] = []any{ts, m.Name, val, attrsJSON}
	}

	copyCount, err := w.db.CopyFrom(
		ctx,
		pgx.Identifier{"metrics"},
		[]string{"timestamp", "name", "value", "attributes"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("failed to copy metrics to timescaledb: %w", err)
	}

	if int(copyCount) != len(metrics) {
		return fmt.Errorf("copyFrom mismatch: expected %d, got %d", len(metrics), int(copyCount))
	}

	return nil
}
