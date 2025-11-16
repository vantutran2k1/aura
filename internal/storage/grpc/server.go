package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

const logQueryTemplate = "SELECT timestamp, id, service_name, level, message, attributes FROM aura.logs WHERE timestamp >= toDateTime64(%d, 9) AND timestamp <= toDateTime64(%d, 9) AND (message LIKE '%%%s%%' OR service_name LIKE '%%%s%%' OR level LIKE '%%%s%%') ORDER BY timestamp DESC LIMIT %d"

type Server struct {
	pb.UnimplementedStorageServiceServer
	chConn clickhouse.Conn
	dbPool *pgxpool.Pool
}

func NewServer(ch clickhouse.Conn, db *pgxpool.Pool) *Server {
	return &Server{
		chConn: ch,
		dbPool: db,
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

func (s *Server) QueryMetrics(ctx context.Context, req *pb.QueryMetricsRequest) (*pb.QueryMetricsResponse, error) {
	log.Printf("[metrics] received query metrics request: %s", req.Query)

	query := `
	SELECT timestamp, name, value, attributes
	FROM metrics
	WHERE timestamp >= $1 AND timestamp <= $2
	AND name LIKE $3 OR attributes::text LIKE $3
	ORDER BY timestamp DESC
	LIMIT 100
	`

	likeQuery := "%" + req.Query + "%"
	startTime := time.Unix(0, req.StartTimeUnixNano)
	endTime := time.Unix(0, req.EndTimeUnixNano)

	rows, err := s.dbPool.Query(ctx, query, startTime, endTime, likeQuery)
	if err != nil {
		log.Printf("[metrics] timescaledb query error: %v", err)
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	var results []*pb.Metric
	for rows.Next() {
		var (
			m         pb.Metric
			ts        time.Time
			val       float64
			attrsJSON []byte
		)

		if err := rows.Scan(&ts, &m.Name, &val, &attrsJSON); err != nil {
			log.Printf("[metrics] timescaledb scan error: %v", err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		m.TimestampUnixNano = ts.UnixNano()
		// TODO: we lose distinction between sum and gauge
		m.DataPoint = &pb.Metric_Gauge{
			Gauge: &pb.Gauge{Value: val},
		}

		if err := json.Unmarshal(attrsJSON, &m.Attributes); err != nil {
			log.Printf("[metrics] failed to unmarshal attributes: %v", err)
		}

		results = append(results, &m)
	}

	log.Printf("[metrics] query successful, found %d data points", len(results))

	return &pb.QueryMetricsResponse{
		Metrics: results,
	}, nil
}
