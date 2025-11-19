# **Aura Observability Pipeline**

**Aura** is a high-performance, distributed microservices platform built entirely in **Golang** for ingesting, processing, and querying the three pillars of observability:

* **Logs**
* **Metrics**
* **Traces (Spans)**

This project emphasizes **high throughput**, **resilience**, and **clean, decoupled architecture**, making it suitable for production workloads and large-scale monitoring environments.

---

## 🌟 **Features Overview**

| Feature                                  | Description                                                              | Golang Concepts                                  |
| ---------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------ |
| **Complete Data Pipelines**              | Full ingestion and query path implemented for Logs, Metrics, and Traces. | Concurrency (errgroup, Channels), Protobuf/gRPC  |
| **High-Performance Storage**             | Decoupled storage adapters for different data types.                     | clickhouse-go/v2, pgx/v5 (TimescaleDB), Batching |
| **Centralized Alerting**                 | Stateful service running user-defined queries and firing webhooks.       | Schedulers, PostgreSQL/TimescaleDB               |
| **Structured Query Language**            | Custom safe query parser (e.g., `level=ERROR AND service_name="api"`).   | participle, SQL Parameterization                 |
| **Full Observability (Self-Monitoring)** | Platform monitors itself via tracing, metrics, profiling.                | OpenTelemetry (OTel), Prometheus/pprof           |
| **Clean Architecture**                   | "App Struct" pattern for simplifying service initialization.             | Struct Embedding, Interfaces, DI                 |
| **Production Hardening**                 | Viper-based configuration and API Key security.                          | Middleware, viper                                |
| **Web UI**                               | Dashboard using Alpine.js + Tailwind served via Go BFF.                  | Reverse Proxy, Static Serving                    |

---

## 🛠️ **Architecture**

Aura is composed of **four main Go microservices** plus supporting components, communicating via:

* **NATS** for asynchronous ingestion
* **gRPC** for synchronous queries

---

### **Go Microservices**

| Service           | Responsibility                                                  | Public Interface   |
| ----------------- | --------------------------------------------------------------- | ------------------ |
| **aura-ingestor** | Collector: receives logs/metrics/traces and publishes to NATS.  | HTTP/REST (8080)   |
| **aura-router**   | Processor: parses raw payloads and forwards processed messages. | NATS Subscriber    |
| **aura-storage**  | Persistor: writes batches to DB and exposes gRPC for reads.     | gRPC (50051)       |
| **aura-query**    | Public Query API: REST → gRPC translator with Redis cache.      | HTTP/REST (8081)   |
| **aura-alerter**  | Scheduler: evaluates alert rules and fires webhooks.            | Internal Scheduler |
| **aura-ui**       | Frontend Dashboard + API reverse proxy.                         | HTTP/REST (8082)   |

---

### **External Systems**

| System          | Role                  | Connection |
| --------------- | --------------------- | ---------- |
| **NATS**        | Message Bus           | TCP 4222   |
| **ClickHouse**  | Logs & Traces storage | TCP 9000   |
| **TimescaleDB** | Metrics storage       | TCP 5432   |
| **Redis**       | Query caching         | TCP 6379   |
| **Jaeger**      | Tracing visualization | gRPC 4317  |
| **Prometheus**  | Metrics collection    | HTTP 9090  |

---

## 🚀 **Getting Started**

### **Prerequisites**

* Go **1.20+**
* Docker & Docker Compose
* Optional: `grpcurl`

---

### **1. Install Dependencies**

```bash
go mod tidy

# Protobuf plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

### **2. Start Infrastructure**

```bash
docker-compose up -d
```

This launches all databases, NATS, Jaeger, Prometheus, etc.

---

### **3. Generate Protobuf Files**

```bash
protoc -I=proto --go_out=. --go-grpc_out=. proto/aura/v1/*.proto
```

---

### **4. Run All Services**

```bash
# Ingestion Path
go run ./cmd/aura-ingestor/
go run ./cmd/aura-router/

# Storage & Query
go run ./cmd/aura-storage/
go run ./cmd/aura-query/
go run ./cmd/aura-alerter/

# Webhook Receiver & Frontend
go run ./cmd/aura-webhook-receiver/
go run ./cmd/aura-ui/   # http://localhost:8082
```

---

## 🧪 **Verification and Testing**

### **Query UI**

Open:
**[http://localhost:8082](http://localhost:8082)**

### **Send Log**

```bash
curl -H "Authorization: Bearer aura-write-key-secret-123" \
  -X POST http://localhost:8080/v1/logs \
  -d '{"service_name":"test-svc","level":"INFO","message":"hello"}'
```

Expected: processed and stored.

---

### **Query Logs**

```bash
curl -H "Authorization: Bearer aura-read-key-secret-456" \
"http://localhost:8081/v1/logs?q=service_name=\"test-svc\""
```

---

### **Send Metric**

```bash
curl -H "Authorization: Bearer aura-write-key-secret-123" \
  -X POST http://localhost:8080/v1/metrics \
  -d '{"resourceMetrics":[...]}'
```

Stored in TimescaleDB.

---

### **Check Tracing**

Open Jaeger:
**[http://localhost:16686](http://localhost:16686)**

---

## ⚙️ **Configuration**

Configuration is managed through **config.yaml** (overridable by environment variables via Viper).

### Example: Change Ingestor Port

**In config.yaml:**

```yaml
ingestor:
  api: ":8088"
```

**Or override with environment variable:**

```bash
AURA_INGESTOR_API=":8088" go run ./cmd/aura-ingestor/
```

---
