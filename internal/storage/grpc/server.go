package grpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
	"github.com/vantutran2k1/aura/pkg/queryparser"
)

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

	baseQuery := "SELECT timestamp, id, service_name, level, message, attributes FROM aura.logs"
	var whereClause string
	var queryArgs []any

	if req.Query != "" {
		ast, err := queryparser.Parse(req.Query)
		if err != nil {
			log.Printf("query parse error: %v", err)
			return nil, fmt.Errorf("invalid query syntax: %w", err)
		}

		clause, args, err := queryparser.BuildSQL(ast)
		if err != nil {
			log.Printf("query build error: %v", err)
			return nil, fmt.Errorf("invalid query: %w", err)
		}

		whereClause = "WHERE " + clause
		queryArgs = args
	}

	timeClause := "timestamp >= ? AND timestamp <= ?"
	queryArgs = append(queryArgs, time.Unix(0, req.StartTimeUnixNano))
	queryArgs = append(queryArgs, time.Unix(0, req.EndTimeUnixNano))

	if whereClause == "" {
		whereClause = "WHERE " + timeClause
	} else {
		whereClause = whereClause + " AND " + timeClause
	}

	finalQuery := fmt.Sprintf("%s %s ORDER BY timestamp DESC LIMIT %d", baseQuery, whereClause, req.Limit)

	log.Printf("executing sql: %s", finalQuery)
	log.Printf("with args: %v", queryArgs...)

	rows, err := s.chConn.Query(ctx, finalQuery, queryArgs...)
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

	var whereClause string
	var queryArgs []any

	if req.Query != "" {
		ast, err := queryparser.Parse(req.Query)
		if err != nil {
			log.Printf("[metrics] query parse error: %v", err)
			return nil, fmt.Errorf("invalid query syntax: %w", err)
		}

		baseClause, baseArgs, err := queryparser.BuildSQL(ast)
		if err != nil {
			return nil, fmt.Errorf("invalid query: %w", err)
		}

		n := 0
		whereClause = "WHERE " + rebind(baseClause, &n)
		queryArgs = baseArgs
	}

	timeClause := "timestamp >= $1 AND timestamp <= $2"
	if whereClause != "" {
		timeClause = "timstamp >= $3 AND timestamp <= $4"
		queryArgs = append(queryArgs, time.Unix(0, req.StartTimeUnixNano), time.Unix(0, req.EndTimeUnixNano))
	} else {
		whereClause = "WHERE " + timeClause
		queryArgs = []any{time.Unix(0, req.StartTimeUnixNano), time.Unix(0, req.EndTimeUnixNano)}
	}

	finalQuery := fmt.Sprintf(
		"SELECT timestamp, name, value, attributes FROM metrics %s ORDER BY timestamp DESC LIMIT 100",
		whereClause,
	)

	log.Printf("executing sql: %s", finalQuery)
	log.Printf("with args: %v", queryArgs)

	rows, err := s.dbPool.Query(ctx, finalQuery, queryArgs...)
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

func (s *Server) QueryTraces(ctx context.Context, req *pb.QueryTracesRequest) (*pb.QueryTracesResponse, error) {
	log.Printf("[traces] received query traces request for ID: %s", req.TraceIdHex)

	traceIDBytes, err := hex.DecodeString(req.TraceIdHex)
	if err != nil {
		log.Printf("[traces] invalid trace ID hex: %v", err)
		return nil, fmt.Errorf("invalid trace_id_hex: %w", err)
	}
	paddedTraceID := padBytes(traceIDBytes, 16)

	query := `
		SELECT timestamp, trace_id, span_id, parent_span_id, name, duration_ns, attributes
		FROM aura.traces
		WHERE trace_id = ?
		ORDER BY timestamp ASC
	`

	rows, err := s.chConn.Query(ctx, query, string(paddedTraceID))
	if err != nil {
		log.Printf("[traces] clickhouse query error: %v", err)
		return nil, fmt.Errorf("failed to query traces: %w", err)
	}
	defer rows.Close()

	var results []*pb.Span
	for rows.Next() {
		var (
			span     pb.Span
			ts       time.Time
			duration uint64
		)

		if err := rows.Scan(
			&ts,
			&span.TraceId,
			&span.SpanId,
			&span.ParentSpanId,
			&span.Name,
			&duration,
			&span.Attributes,
		); err != nil {
			log.Printf("[traces] clickhouse scan error: %v", err)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		span.StartTimeUnixNano = ts.UnixNano()
		span.EndTimeUnixNano = ts.UnixNano() + int64(duration)

		results = append(results, &span)
	}

	log.Printf("[traces] query successful, found %d spans", len(results))

	return &pb.QueryTracesResponse{
		Spans: results,
	}, nil
}

func rebind(query string, n *int) string {
	var b strings.Builder
	for _, r := range query {
		if r == '?' {
			*n++
			b.WriteString(fmt.Sprintf("$%d", *n))
		} else {
			b.WriteRune(r)
		}
	}

	return b.String()
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
