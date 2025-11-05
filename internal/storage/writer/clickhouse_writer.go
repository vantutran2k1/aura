package writer

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type ClickHouseWriter struct {
	conn clickhouse.Conn
}

func NewClickHouseWriter(conn clickhouse.Conn) *ClickHouseWriter {
	return &ClickHouseWriter{conn: conn}
}

func (w *ClickHouseWriter) Write(ctx context.Context, logs []*pb.Log) error {
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO aura.logs")
	if err != nil {
		return err
	}

	for _, log := range logs {
		err = batch.Append(
			time.Unix(0, log.TimestampUnixNano),
			log.Id,
			log.ServiceName,
			log.Level,
			log.Message,
			log.Attributes,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}
