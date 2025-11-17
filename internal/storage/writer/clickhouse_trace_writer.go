package writer

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type ClickHouseTraceWriter struct {
	conn clickhouse.Conn
}

func NewClickHouseTraceWriter(conn clickhouse.Conn) *ClickHouseTraceWriter {
	return &ClickHouseTraceWriter{conn: conn}
}

func (w *ClickHouseTraceWriter) Write(ctx context.Context, spans []*pb.Span) error {
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO aura.traces")
	if err != nil {
		return err
	}

	for _, span := range spans {
		startTime := time.Unix(0, span.StartTimeUnixNano)
		duration := uint64(span.EndTimeUnixNano - span.StartTimeUnixNano)

		traceID := padBytes(span.TraceId, 16)
		spanID := padBytes(span.SpanId, 8)
		parentSpanID := padBytes(span.ParentSpanId, 8)

		err := batch.Append(
			startTime,
			traceID,
			spanID,
			parentSpanID,
			span.Name,
			duration,
			span.Attributes,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

func padBytes(b []byte, length int) []byte {
	if len(b) == length {
		return b
	}
	if len(b) > length {
		return b[:length]
	}

	padded := make([]byte, length)
	copy(padded, b)
	return padded
}
