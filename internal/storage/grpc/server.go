package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

const logQueryTemplate = "SELECT timestamp, id, service_name, level, message, attributes FROM aura.logs WHERE timestamp >= toDateTime64(%d, 9) AND timestamp <= toDateTime64(%d, 9) AND (message LIKE '%%%s%%' OR service_name LIKE '%%%s%%' OR level LIKE '%%%s%%') ORDER BY timestamp DESC LIMIT %d"

type Server struct {
	pb.UnimplementedStorageServiceServer
	chConn clickhouse.Conn
}

func NewServer(ch clickhouse.Conn) *Server {
	return &Server{
		chConn: ch,
	}
}

func (s *Server) QueryLogs(ctx context.Context, req *pb.QueryLogsRequest) (*pb.QueryLogsResponse, error) {
	log.Printf("received query logs request: %s", req.Query)

	// TODO: handle query parser properly using ANTLR

	query := fmt.Sprintf(
		logQueryTemplate,
		req.StartTimeUnixNano/1e9,
		req.EndTimeUnixNano/1e9,
		req.Query,
		req.Query,
		req.Query,
		req.Limit,
	)

	rows, err := s.chConn.Query(ctx, query)
	if err != nil {
		log.Printf("clickhouse query error: %v", err)
		return nil, fmt.Errorf("failed to query click house: %w", err)
	}
	defer rows.Close()

	var results []*pb.Log
	for rows.Next() {
		var (
			logMsg     pb.Log
			timestamp  time.Time
			attributes map[string]string
		)

		if err := rows.Scan(
			&timestamp,
			&logMsg.Id,
			&logMsg.ServiceName,
			&logMsg.Level,
			&logMsg.Message,
			&attributes,
		); err != nil {
			log.Printf("clickhouse scan error: %v", err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		logMsg.TimestampUnixNano = timestamp.UnixNano()
		logMsg.Attributes = attributes
		results = append(results, &logMsg)
	}

	log.Printf("query successful, found %d logs", len(results))

	return &pb.QueryLogsResponse{Logs: results}, nil
}
