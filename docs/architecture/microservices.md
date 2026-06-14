# Microservices Architecture

## Introduction

Thunders-BigData-System is decomposed into a set of loosely coupled, independently deployable microservices, each owning a bounded context within the big data platform's domain. This document describes the service decomposition strategy, individual service responsibilities, inter-service communication patterns, discovery mechanisms, the API Gateway pattern, and resilience strategies including circuit breakers and bulkheads.

The microservices architecture enables independent scaling, deployment, and evolution of each subsystem. Teams can iterate on the Ingestion Service without affecting the Analytics Service, and the system can allocate resources precisely where they are needed based on workload characteristics.

---

## Service Decomposition Strategy

### Bounded Contexts

The system follows Domain-Driven Design (DDD) principles to identify bounded contexts — cohesive areas of responsibility with well-defined boundaries. Each microservice maps to one bounded context:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Thunders-BigData-System Domain                        │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │  Ingestion   │  │  Processing  │  │   Storage    │  │  Analytics  │ │
│  │  Context     │  │  Context     │  │   Context    │  │  Context    │ │
│  │              │  │              │  │              │  │             │ │
│  │  • Ingest    │  │  • Transform │  │  • Persist   │  │  • Query    │ │
│  │  • Validate  │  │  • Aggregate │  │  • Index     │  │  • Visual   │ │
│  │  • Route     │  │  • Enrich    │  │  • Compact   │  │  • Forecast │ │
│  │  • Schema    │  │  • Window    │  │  • Replicate │  │  • Report   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘ │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │  ML          │  │  Governance  │  │  Gateway     │  │  Security   │ │
│  │  Context     │  │  Context     │  │  Context     │  │  Context    │ │
│  │              │  │              │  │              │  │             │ │
│  │  • Train     │  │  • Catalog   │  │  • Route     │  │  • Auth     │ │
│  │  • Serve     │  │  • Lineage   │  │  • Limit     │  │  • AuthZ    │ │
│  │  • Evaluate  │  │  • Quality   │  │  • Transform │  │  • Encrypt  │ │
│  │  • Registry  │  │  • Policy    │  │  • Aggregate │  │  • Audit    │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

### Decomposition Principles

1. **Single Responsibility**: Each service owns exactly one bounded context with a well-defined data model
2. **Data Ownership**: Each service is the authoritative source for its data; other services access it through published APIs or events
3. **Independent Deployability**: Services can be deployed, scaled, and versioned independently
4. **Interface Segregation**: Services expose thin, purpose-driven APIs rather than monolithic interfaces
5. **Event-Driven Coupling**: Services communicate asynchronously through events (Kafka topics) where possible, minimizing temporal coupling

---

## Core Microservices

### 1. Ingestion Service

**Responsibility**: Accept, validate, and route incoming data from external sources into the processing pipeline.

**Technology Stack**: Go 1.22+ (Thunder Gateway), Python 3.11+ (Connectors), Apache Kafka

**Owned Data**:
- Ingestion metadata (source, timestamp, trace IDs)
- Schema definitions and versions
- Dead letter queue records

**Published Events**:
- `thunders.ingestion.record_validated` — Record passed validation and was written to Kafka
- `thunders.ingestion.record_rejected` — Record failed validation; routed to DLQ

**API Endpoints**:

```yaml
ingestion_service:
  grpc:
    port: 9090
    endpoints:
      - Ingest(request: IngestRequest) -> IngestResponse
      - IngestStream(stream: IngestRequest) -> IngestResponse
      - GetSchema(request: SchemaRequest) -> SchemaResponse
      - RegisterSchema(request: SchemaRegistration) -> SchemaResponse
  rest:
    port: 8080
    endpoints:
      - POST   /api/v1/ingest                    # Ingest records
      - POST   /api/v1/ingest/batch               # Bulk file ingestion
      - GET    /api/v1/schemas                    # List schemas
      - POST   /api/v1/schemas                    # Register schema
      - GET    /api/v1/schemas/{id}/versions       # Schema version history
      - GET    /api/v1/dlq/{dataset}              # Dead letter queue records
      - POST   /api/v1/dlq/{dataset}/replay       # Replay DLQ records
```

**Service Definition (Go)**:

```go
// Ingestion Service — core ingestion handler
package ingestion

import (
    "context"
    "time"

    pb "github.com/thunders/bigdata/api/ingestion/v1"
)

type IngestionService struct {
    pb.UnimplementedIngestionServiceServer
    validator    SchemaValidator
    producer     KafkaProducer
    schemaReg    SchemaRegistry
    metrics      MetricsRecorder
    dlqWriter    DLQWriter
}

func (s *IngestionService) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
    start := time.Now()
    defer func() {
        s.metrics.RecordLatency("ingest", time.Since(start))
    }()

    // Validate schema exists and is compatible
    schema, err := s.schemaReg.GetLatestSchema(ctx, req.SchemaId)
    if err != nil {
        return nil, fmt.Errorf("schema not found: %w", err)
    }

    var accepted, rejected int32
    var rejections []*pb.RejectionDetail

    for i, record := range req.Records {
        // Validate against schema
        if err := s.validator.Validate(record.Payload, schema); err != nil {
            rejected++
            rejections = append(rejections, &pb.RejectionDetail{
                RecordIndex:  int32(i),
                ErrorCode:    "SCHEMA_VALIDATION_FAILED",
                ErrorMessage: err.Error(),
            })
            // Route to dead letter queue
            s.dlqWriter.Write(ctx, DLQRecord{
                Dataset:     req.Dataset,
                Record:      record,
                Error:       err.Error(),
                Timestamp:   time.Now(),
            })
            continue
        }

        // Enrich with metadata
        enriched := s.enrichRecord(record, ctx)

        // Write to Kafka with partitioning
        if err := s.producer.Produce(ctx, &KafkaMessage{
            Topic:     fmt.Sprintf("thunders.%s", req.Dataset),
            Key:       record.Key,
            Value:     enriched,
            Headers:   map[string]string{
                "schema-id":  req.SchemaId,
                "source":     req.Source,
                "trace-id":   extractTraceID(ctx),
            },
        }); err != nil {
            return nil, fmt.Errorf("failed to produce record: %w", err)
        }
        accepted++
    }

    s.metrics.RecordCounter("ingest.accepted", int64(accepted))
    s.metrics.RecordCounter("ingest.rejected", int64(rejected))

    return &pb.IngestResponse{
        AcceptedCount: accepted,
        RejectedCount: rejected,
        Rejections:    rejections,
    }, nil
}
```

### 2. Processing Service

**Responsibility**: Execute real-time and batch data transformations, aggregations, and enrichment.

**Technology Stack**: Apache Flink 1.18+ (stream), Apache Spark 3.5+ (batch), Java 17+, Python 3.11+

**Owned Data**:
- Pipeline definitions and configurations
- Checkpoint/savepoint metadata
- Processing metrics and lineage

**Published Events**:
- `thunders.processing.batch_started` — Batch job initiated
- `thunders.processing.batch_completed` — Batch job finished
- `thunders.processing.stream_checkpoint` — Stream checkpoint completed

**API Endpoints**:

```yaml
processing_service:
  rest:
    port: 8081
    endpoints:
      - POST   /api/v1/pipelines                     # Create pipeline
      - GET    /api/v1/pipelines                      # List pipelines
      - GET    /api/v1/pipelines/{id}                 # Get pipeline details
      - PUT    /api/v1/pipelines/{id}                 # Update pipeline
      - DELETE /api/v1/pipelines/{id}                 # Delete pipeline
      - POST   /api/v1/pipelines/{id}/start           # Start pipeline
      - POST   /api/v1/pipelines/{id}/stop            # Stop pipeline
      - POST   /api/v1/pipelines/{id}/savepoint       # Trigger savepoint
      - GET    /api/v1/pipelines/{id}/metrics          # Pipeline metrics
      - GET    /api/v1/jobs/{id}/status               # Job execution status
  grpc:
    port: 9091
    endpoints:
      - SubmitPipeline(request: PipelineRequest) -> PipelineResponse
      - GetPipelineStatus(request: StatusRequest) -> StatusResponse
```

### 3. Storage Service

**Responsibility**: Manage data persistence across the multi-tier storage architecture, including data lifecycle, compaction, and replication.

**Technology Stack**: Apache Druid, ClickHouse, Apache Iceberg, Redis, Rust 1.75+ (high-performance engine), C++ 20+ (columnar database)

**Owned Data**:
- Storage tier configurations
- Data placement and routing rules
- Compaction and retention policies
- Storage metrics (size, segment count, merge rate)

**Published Events**:
- `thunders.storage.segment_created` — New Druid segment created
- `thunders.storage.compaction_completed` — Compaction job finished
- `thunders.storage.tier_transition` — Data moved between storage tiers

**API Endpoints**:

```yaml
storage_service:
  rest:
    port: 8082
    endpoints:
      - GET    /api/v1/datasets                       # List datasets
      - GET    /api/v1/datasets/{id}/stats            # Dataset statistics
      - PUT    /api/v1/datasets/{id}/retention        # Update retention policy
      - POST   /api/v1/datasets/{id}/compact          # Trigger compaction
      - GET    /api/v1/storage/health                  # Storage tier health
      - GET    /api/v1/storage/capacity                 # Storage capacity metrics
```

### 4. Analytics Service

**Responsibility**: Provide interactive SQL analytics, OLAP cubes, statistical analysis, and visualization capabilities.

**Technology Stack**: Java 17+ (Query Engine), TypeScript (Dashboard), R (Statistics), Julia 1.10+ (Scientific Computing)

**Owned Data**:
- Saved queries and dashboards
- OLAP cube definitions
- Materialized view specifications

**Published Events**:
- `thunders.analytics.query_executed` — Query completed with performance metrics
- `thunders.analytics.dashboard_updated` — Dashboard configuration changed

**API Endpoints**:

```yaml
analytics_service:
  rest:
    port: 8083
    endpoints:
      - POST   /api/v1/query/sql                     # Execute SQL query
      - POST   /api/v1/query/batch                   # Submit batch of queries
      - GET    /api/v1/query/{id}/result              # Get query result
      - POST   /api/v1/query/cancel/{id}              # Cancel running query
      - GET    /api/v1/dashboards                      # List dashboards
      - POST   /api/v1/dashboards                      # Create dashboard
      - PUT    /api/v1/dashboards/{id}                # Update dashboard
      - GET    /api/v1/cubes                           # List OLAP cubes
      - POST   /api/v1/cubes/refresh                   # Refresh cube data
  websocket:
    port: 8084
    endpoints:
      - /ws/v1/subscribe/{dataset}                    # Subscribe to real-time updates
      - /ws/v1/query/stream                           # Streaming query results
```

### 5. ML Service

**Responsibility**: Manage the full machine learning lifecycle — feature engineering, model training, evaluation, serving, and monitoring.

**Technology Stack**: Python 3.11+, MLflow, Spark MLlib, TensorFlow, Feast (Feature Store)

**Owned Data**:
- Feature definitions and feature vectors
- Model artifacts and metadata
- Training job configurations and results
- Model performance metrics and drift statistics

**Published Events**:
- `thunders.ml.model_registered` — New model version registered
- `thunders.ml.model_deployed` — Model deployed to serving endpoint
- `thunders.ml.drift_detected` — Model drift detected in production

**API Endpoints**:

```yaml
ml_service:
  rest:
    port: 8085
    endpoints:
      - POST   /api/v1/features/define                # Define feature
      - GET    /api/v1/features/{entity_id}           # Get feature vector
      - POST   /api/v1/models/register                # Register model
      - GET    /api/v1/models                          # List models
      - GET    /api/v1/models/{id}/versions            # Model versions
      - POST   /api/v1/models/{id}/deploy              # Deploy model
      - POST   /api/v1/models/{id}/predict             # Online prediction
      - POST   /api/v1/training/jobs                   # Submit training job
      - GET    /api/v1/training/jobs/{id}              # Training job status
      - GET    /api/v1/monitoring/drift/{model_id}     # Drift metrics
  grpc:
    port: 9095
    endpoints:
      - Predict(request: PredictRequest) -> PredictResponse
      - GetFeatures(request: FeatureRequest) -> FeatureResponse
```

### 6. Gateway Service

**Responsibility**: Serve as the single entry point for all external client requests, handling routing, rate limiting, authentication, request transformation, and response aggregation.

**Technology Stack**: Go 1.22+ (API Gateway), Envoy Proxy (Istio), Kong (optional)

**Capabilities**:
- **Request Routing**: Route requests to the appropriate backend service based on URL path, headers, or query parameters
- **Rate Limiting**: Enforce per-tenant and per-API rate limits
- **Authentication**: Validate JWT/OAuth2 tokens and inject identity headers
- **Request Transformation**: Translate between REST/gRPC protocols
- **Response Aggregation**: Combine responses from multiple services into a single response
- **Circuit Breaking**: Open circuits to failing backends to prevent cascade failures

**API Gateway Configuration**:

```yaml
gateway_service:
  rest:
    port: 8443
    tls:
      enabled: true
      cert_secret: thunders-gateway-tls

  routes:
    - path: /api/v1/ingest/**
      service: ingestion-service
      port: 8080
      methods: [POST]
      rate_limit:
        requests_per_second: 10000
        burst: 50000
      auth: required
      timeout_ms: 5000

    - path: /api/v1/pipelines/**
      service: processing-service
      port: 8081
      methods: [GET, POST, PUT, DELETE]
      rate_limit:
        requests_per_second: 100
      auth: required
      timeout_ms: 10000

    - path: /api/v1/query/**
      service: analytics-service
      port: 8083
      methods: [POST, GET]
      rate_limit:
        requests_per_second: 500
      auth: required
      timeout_ms: 30000

    - path: /api/v1/models/**
      service: ml-service
      port: 8085
      methods: [GET, POST]
      rate_limit:
        requests_per_second: 200
      auth: required
      timeout_ms: 60000

  aggregation:
    - name: "dashboard_summary"
      path: /api/v1/dashboard/summary
      calls:
        - service: analytics-service
          path: /api/v1/query/sql
          method: POST
        - service: ml-service
          path: /api/v1/monitoring/drift
          method: GET
        - service: storage-service
          path: /api/v1/storage/capacity
          method: GET
```

---

## Inter-Service Communication

### Communication Patterns

Thunders-BigData-System employs three primary communication patterns, selected based on the interaction requirements:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Communication Patterns                                │
│                                                                          │
│  SYNCHRONOUS (REST/gRPC)          ASYNCHRONOUS (Event-Driven)          │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐         ┌──────────┐   │
│  │ Service A │───►│ Service B│    │ Service A │──► Kafka ──►│ Service B│   │
│  │ (caller)  │◄───│ (responder│   │ (producer)│    Topic    │(consumer)│   │
│  └──────────┘    └──────────┘    └──────────┘              └──────────┘   │
│                                                                          │
│  Use: Queries, CRUD ops           Use: Events, notifications,           │
│  Latency: ms                      data pipeline flow                    │
│  Coupling: Temporal               Latency: seconds                      │
│  Scale: Request-driven            Coupling: Loose (event schema only)   │
│                                   Scale: Consumer-driven                │
│                                                                          │
│  HYBRID (CQRS)                                                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                          │
│  │ Command  │───►│ Event    │───►│ Read     │                          │
│  │ (write)  │    │ Store    │    │ Model    │                          │
│  └──────────┘    └──────────┘    └──────────┘                          │
│                                                                          │
│  Use: Write-heavy workloads with separate read optimization             │
└────────────────────────────────────────────────────────────────────────┘
```

### REST Communication

Used for synchronous request-response interactions where the caller needs an immediate result:

```go
// Gateway calls Analytics Service for SQL query execution
func (g *Gateway) ExecuteQuery(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
    // Add authentication context
    ctx = metadata.AppendToOutgoingContext(ctx,
        "authorization", extractAuthToken(ctx),
        "x-request-id", generateRequestID(),
        "x-tenant-id", extractTenantID(ctx),
    )

    // Call analytics service
    resp, err := g.analyticsClient.ExecuteSQL(ctx, &pb.SQLRequest{
        Query:    req.Query,
        Format:   req.Format,
        Timeout:  req.Timeout,
    })
    if err != nil {
        return nil, fmt.Errorf("analytics service error: %w", err)
    }
    return &QueryResponse{
        Columns: resp.Columns,
        Rows:    resp.Rows,
        Stats:   resp.Stats,
    }, nil
}
```

### gRPC Communication

Used for high-throughput, low-latency inter-service communication where strong typing and streaming are beneficial:

```protobuf
// Inter-service gRPC definition for ML predictions
syntax = "proto3";
package thunders.ml.v1;

service MLPredictionService {
  // Unary: Single prediction
  rpc Predict(PredictRequest) returns (PredictResponse);

  // Server streaming: Batch prediction with streaming results
  rpc PredictBatch(PredictBatchRequest) returns (stream PredictResponse);

  // Bidirectional streaming: Real-time feature lookup + prediction
  rpc PredictStream(stream PredictRequest) returns (stream PredictResponse);
}

message PredictRequest {
  string model_id = 1;
  string model_version = 2;
  map<string, FeatureValue> features = 3;
  PredictOptions options = 4;
}

message PredictResponse {
  string prediction_id = 1;
  double score = 2;
  map<string, double> probabilities = 3;
  ModelMetadata model = 4;
  int64 latency_us = 5;
}
```

### Event-Driven Communication

Used for asynchronous, decoupled communication between services. Events flow through Kafka topics and services react to them independently:

```python
# Event-driven communication: Processing Service publishes, Analytics Service consumes
from thunders.events import EventProducer, EventConsumer, Event

# Producer: Processing Service publishes completion event
producer = EventProducer(bootstrap_servers="kafka:9092")

producer.publish(Event(
    topic="thunders.processing.batch_completed",
    key=f"{pipeline_id}:{job_id}",
    value={
        "pipeline_id": pipeline_id,
        "job_id": job_id,
        "status": "SUCCESS",
        "records_processed": 15_000_000,
        "duration_seconds": 3600,
        "output_datasets": ["thunders.analytics.daily_summary"],
        "snapshot_id": "snap-abc123",
        "completed_at": "2024-01-15T02:00:00Z"
    },
    headers={
        "event_type": "batch_completed",
        "schema_version": "v2",
        "trace_id": trace_id
    }
))

# Consumer: Analytics Service reacts to completion event
consumer = EventConsumer(
    bootstrap_servers="kafka:9092",
    group_id="analytics-refresh-consumer",
    topics=["thunders.processing.batch_completed"]
)

for event in consumer.stream():
    if event.value["status"] == "SUCCESS":
        for dataset in event.value["output_datasets"]:
            refresh_materialized_views(dataset)
            invalidate_cache(dataset)
            notify_subscribers(dataset)
```

### Communication Pattern Selection Guide

| Scenario | Pattern | Protocol | Rationale |
|---|---|---|---|
| User submits a query | Synchronous | REST | Client needs immediate result |
| ML prediction request | Synchronous | gRPC | Low latency, strong typing, streaming |
| Data pipeline completion | Asynchronous | Kafka event | Decoupled, multiple consumers |
| Schema registration | Synchronous | REST + Kafka event | Immediate ack + async propagation |
| Real-time dashboard update | Asynchronous | WebSocket + Kafka | Push-based, low latency |
| Cross-service data replication | Asynchronous | Kafka event | Durable, ordered, replayable |

---

## Service Discovery and Registration

### Kubernetes-Native Service Discovery

Services are registered as Kubernetes Services and discovered via DNS:

```yaml
# Service definition for Ingestion Service
apiVersion: v1
kind: Service
metadata:
  name: ingestion-service
  namespace: thunders
  labels:
    app: ingestion-service
    version: v3.2.1
spec:
  type: ClusterIP
  ports:
    - name: http
      port: 8080
      targetPort: 8080
    - name: grpc
      port: 9090
      targetPort: 9090
  selector:
    app: ingestion-service
```

**DNS Resolution**: Services are accessible via `<service-name>.<namespace>.svc.cluster.local` (e.g., `ingestion-service.thunders.svc.cluster.local`)

### Istio Service Registry

Istio maintains a real-time service registry that includes:
- Service endpoints (pods) with health status
- Traffic routing rules (VirtualService)
- Load balancing policies (DestinationRule)
- Circuit breaker configurations

```yaml
# Istio DestinationRule for Processing Service
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: processing-service
  namespace: thunders
spec:
  host: processing-service
  trafficPolicy:
    connectionPool:
      tcp:
        maxConnections: 200
        connectTimeout: 5s
      http:
        h2UpgradePolicy: UPGRADE
        maxRequestsPerConnection: 1000
    outlierDetection:
      consecutive5xxErrors: 5
      interval: 30s
      baseEjectionTime: 60s
      maxEjectionPercent: 50
  subsets:
    - name: v3
      labels:
        version: v3.2.1
    - name: v2
      labels:
        version: v2.8.0
```

### Health Check Endpoints

Every service exposes three health check endpoints:

```go
// Standardized health check handlers
mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
    // Liveness: Is the process running?
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
})

mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
    // Readiness: Can the service handle requests?
    if svc.IsReady() {
        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
    } else {
        w.WriteHeader(http.StatusServiceUnavailable)
        json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
    }
})

mux.HandleFunc("/health/started", func(w http.ResponseWriter, r *http.Request) {
    // Startup: Has the service finished initialization?
    if svc.IsStarted() {
        w.WriteHeader(http.StatusOK)
    } else {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
})
```

---

## API Gateway Pattern

### Gateway Architecture

The API Gateway serves as the system's single entry point, implementing the Backend-for-Frontend (BFF) pattern where different client types (web, mobile, API) may have tailored gateway configurations:

```
                          ┌─────────────────────────┐
                          │      Load Balancer       │
                          │    (AWS ALB / Nginx)     │
                          └────────────┬─────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
           ┌───────▼──────┐  ┌───────▼──────┐  ┌───────▼──────┐
           │  Web BFF     │  │  Mobile BFF  │  │  API BFF     │
           │  Gateway     │  │  Gateway     │  │  Gateway     │
           │  (REST+WS)   │  │  (REST+gRPC) │  │  (gRPC)      │
           └───────┬──────┘  └───────┬──────┘  └───────┬──────┘
                   │                 │                  │
                   └─────────────────┼──────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │   Service Mesh       │
                          │   (Istio / Envoy)    │
                          └──────────┬──────────┘
                                     │
              ┌──────────┬───────────┼───────────┬──────────┐
              │          │           │           │          │
        ┌─────▼───┐ ┌───▼───┐ ┌───▼───┐ ┌───▼───┐ ┌───▼───┐
        │Ingestion│ │Process│ │Storage│ │Analytics│ │  ML   │
        │ Service │ │Service│ │Service│ │ Service │ │Service│
        └─────────┘ └───────┘ └───────┘ └─────────┘ └───────┘
```

### Gateway Middleware Stack

The gateway processes each request through a chain of middleware:

```go
// Gateway middleware chain
func (g *Gateway) MiddlewareChain(next http.Handler) http.Handler {
    return chain(
        // 1. Request logging
        loggingMiddleware(g.logger),
        // 2. Request tracing (OpenTelemetry)
        tracingMiddleware(g.tracer),
        // 3. CORS handling
        corsMiddleware(g.allowedOrigins),
        // 4. TLS termination (if not handled by load balancer)
        tlsMiddleware(g.certManager),
        // 5. Rate limiting
        rateLimitMiddleware(g.rateLimiter),
        // 6. Authentication
        authMiddleware(g.authProvider),
        // 7. Tenant resolution
        tenantMiddleware(g.tenantResolver),
        // 8. Request validation
        validationMiddleware(g.validator),
        // 9. Request transformation
        transformMiddleware(g.transformer),
        // 10. Circuit breaker
        circuitBreakerMiddleware(g.breaker),
        // 11. Route to backend
        next,
    )
}
```

---

## Circuit Breaker and Resilience Patterns

### Circuit Breaker Pattern

Each service-to-service call is protected by a circuit breaker that prevents cascade failures:

```go
// Circuit breaker implementation for inter-service calls
package resilience

import (
    "context"
    "time"
    "github.com/sony/gobreaker"
)

type ServiceClient struct {
    breaker *gobreaker.CircuitBreaker
    client  *grpc.ClientConn
}

func NewServiceClient(serviceName string, conn *grpc.ClientConn) *ServiceClient {
    cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
        Name:        serviceName,
        MaxRequests: 5,                          // Half-open: allow 5 test requests
        Interval:    60 * time.Second,           // Closed: counting window
        Timeout:     30 * time.Second,           // Open → Half-open transition
        ReadyToTrip: func(counts gobreaker.Counts) bool {
            failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
            return counts.Requests >= 10 && failureRatio >= 0.6
        },
        OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
            log.Warn("circuit breaker state change",
                "service", name,
                "from", from.String(),
                "to", to.String(),
            )
        },
    })

    return &ServiceClient{
        breaker: cb,
        client:  conn,
    }
}

func (sc *ServiceClient) Call(ctx context.Context, method string, req interface{}) (interface{}, error) {
    result, err := sc.breaker.Execute(func() (interface{}, error) {
        return sc.invokeMethod(ctx, method, req)
    })
    if err != nil {
        if err == gobreaker.ErrOpenState {
            // Circuit is open — return cached/fallback response
            return sc.getFallback(ctx, method, req)
        }
        return nil, err
    }
    return result, nil
}
```

### Resilience Patterns Summary

| Pattern | Implementation | Use Case |
|---|---|---|
| **Circuit Breaker** | `gobreaker` (Go), Istio `outlierDetection` | Prevent cascade failures when a backend is unhealthy |
| **Retry with Backoff** | Exponential backoff with jitter | Handle transient network failures |
| **Bulkhead** | Separate connection pools per service, Kubernetes resource limits | Isolate failures to one service from affecting others |
| **Timeout** | Per-request deadlines via `context.WithTimeout` | Prevent hanging requests from consuming resources |
| **Fallback** | Cached responses, degraded functionality | Provide useful response when backend is unavailable |
| **Rate Limiting** | Token bucket algorithm per tenant/API | Prevent individual consumers from overwhelming the system |
| **Graceful Degradation** | Feature flags, optional dependency checks | Reduce functionality under load rather than failing completely |

### Bulkhead Isolation

```yaml
# Kubernetes resource limits for bulkhead isolation
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-service
  namespace: thunders
spec:
  template:
    spec:
      containers:
        - name: ingestion-service
          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
          env:
            - name: MAX_CONCURRENT_REQUESTS
              value: "1000"
            - name: CONNECTION_POOL_SIZE
              value: "50"          # Bulkhead: limit connections to each backend
            - name: KAFKA_PRODUCER_POOL
              value: "3"           # Separate pool for Kafka writes
```

### Retry Configuration

```yaml
# Retry policies for inter-service communication
retry_policies:
  ingestion_to_processing:
    max_attempts: 3
    initial_delay_ms: 100
    max_delay_ms: 5000
    backoff_multiplier: 2
    jitter: true
    retryable_errors:
      - "UNAVAILABLE"
      - "DEADLINE_EXCEEDED"
      - "RESOURCE_EXHAUSTED"
    non_retryable_errors:
      - "INVALID_ARGUMENT"
      - "NOT_FOUND"
      - "PERMISSION_DENIED"

  gateway_to_analytics:
    max_attempts: 2
    initial_delay_ms: 200
    max_delay_ms: 2000
    backoff_multiplier: 2
    jitter: true
    retryable_errors:
      - "UNAVAILABLE"
      - "INTERNAL"
```

---

## Deployment Architecture

### Service Deployment Matrix

| Service | Replicas (Min) | Replicas (Max) | CPU | Memory | Priority |
|---|---|---|---|---|---|
| Gateway Service | 3 | 20 | 2 cores | 4 GB | Critical |
| Ingestion Service | 3 | 15 | 4 cores | 8 GB | Critical |
| Processing Service | 2 | 10 | 2 cores | 4 GB | High |
| Storage Service | 3 | 10 | 4 cores | 8 GB | High |
| Analytics Service | 3 | 15 | 4 cores | 8 GB | High |
| ML Service | 2 | 8 | 8 cores | 16 GB | Medium |

### Service Dependency Graph

```
Gateway ──► Ingestion ──► Processing ──► Storage ──► Analytics
   │              │              │            │            │
   │              │              │            │            └──► ML Service
   │              │              │            │
   │              └──────────────┴────────────┘
   │                     (Kafka events)
   │
   └──► Security Service (all services depend on this)
```

---

## Related Documentation

- [System Architecture](system_architecture.md) — Comprehensive architecture and technology stack
- [Data Flow](data_flow.md) — End-to-end data movement and processing
- [Distributed Systems Design](distributed_systems.md) — Distributed computing patterns and consensus
- [Scalability Design](scalability_design.md) — Scaling strategies and load balancing
- [REST API](../api/rest_api.md) — API endpoint reference
