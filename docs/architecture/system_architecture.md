# System Architecture

## Introduction

Thunders-BigData-System implements a hybrid Lambda/Kappa architecture that unifies real-time stream processing and batch analytics under a single, coherent platform. This document provides a comprehensive deep dive into the system's architectural foundations, core component design, technology stack, governing design principles, and infrastructure requirements. While the [System Overview](system_overview.md) provides a high-level introduction, this document focuses on the architectural decisions, trade-offs, and structural patterns that shape the platform.

The architecture is deliberately decomposed into five logical layers — Ingestion, Processing, Storage, Serving, and Analytics — each with clearly defined responsibilities, well-specified interfaces, and independent scaling characteristics. This decomposition enables teams to evolve, deploy, and operate each layer autonomously while maintaining system-wide consistency guarantees.

---

## High-Level Architecture Overview

### Lambda Architecture Foundation

The system adopts a modified Lambda Architecture that preserves the strengths of both the speed and batch layers while mitigating the traditional drawback of maintaining two separate codebases. The key innovation is the **Unified Processing API** — a thin abstraction layer that normalizes streaming and batch computations into a single programming model. Developers write transformation logic once; the runtime determines whether to execute it in streaming mode (Flink), batch mode (Spark), or both based on latency requirements and data freshness SLAs.

```
                              ┌─────────────────────────────────┐
                              │         Data Sources             │
                              │  (IoT, Logs, DBs, APIs, Files)  │
                              └──────────────┬──────────────────┘
                                             │
                              ┌──────────────▼──────────────────┐
                              │        INGESTION LAYER           │
                              │  Kafka · Connectors · Gateway    │
                              │  Schema Registry · DLQ · WAL     │
                              └──────────────┬──────────────────┘
                                             │
                        ┌────────────────────┼────────────────────┐
                        │                    │                    │
              ┌─────────▼──────────┐ ┌──────▼───────────┐ ┌─────▼──────────────┐
              │   Speed Layer      │ │  Batch Layer     │ │  ML Pipeline       │
              │   (Apache Flink)   │ │ (Apache Spark)   │ │ (MLflow + Spark)   │
              │                    │ │                   │ │                     │
              │  • CEP             │ │  • ETL Jobs       │ │  • Feature Store    │
              │  • Windowed Agg    │ │  • ML Training    │ │  • Model Registry   │
              │  • Stateful Proc   │ │  • Backfill       │ │  • A/B Serving      │
              │  • < 1s Latency    │ │  • Large Joins    │ │  • Drift Detection  │
              └─────────┬──────────┘ └──────┬───────────┘ └─────┬──────────────┘
                        │                    │                    │
                        └────────────────────┼────────────────────┘
                                             │
                              ┌──────────────▼──────────────────┐
                              │         STORAGE LAYER            │
                              │  Hot: Druid + Redis              │
                              │  Warm: ClickHouse                │
                              │  Cold: Iceberg/Parquet on S3     │
                              └──────────────┬──────────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
          ┌─────────▼──────────┐  ┌─────────▼──────────┐  ┌─────────▼──────────┐
          │   SERVING LAYER    │  │  ANALYTICS LAYER   │  │  GOVERNANCE LAYER  │
          │   REST · gRPC      │  │  SQL Engine        │  │  Catalog · Lineage │
          │   WebSocket · JDBC │  │  BI Dashboards     │  │  Quality · Policy  │
          └────────────────────┘  └────────────────────┘  └────────────────────┘
```

### Kappa Architecture Enhancement

For use cases where the batch layer provides no additional value (e.g., event replay can reconstruct any state), the system supports a pure Kappa mode. In Kappa mode, all data flows exclusively through the streaming pipeline, and historical reprocessing is achieved by resetting Kafka consumer offsets and replaying events through the same Flink topology. This mode reduces operational complexity at the cost of higher compute consumption during reprocessing windows.

The choice between Lambda and Kappa modes is per-dataset and configurable:

```yaml
dataset: "user_events"
architecture_mode: lambda    # Options: lambda, kappa

lambda_config:
  speed_layer:
    enabled: true
    processor: flink
    latency_target_ms: 500
  batch_layer:
    enabled: true
    processor: spark
    schedule: "0 2 * * *"    # Daily at 2 AM
    lookback_days: 7

kappa_config:
  reprocessing:
    method: offset_reset
    parallelism_multiplier: 4    # 4x parallelism during reprocessing
    completion_detection: checkpoint_alignment
```

---

## Core Components

### 1. Ingestion Layer

The Ingestion Layer is the system's front door — responsible for reliably capturing data from heterogeneous sources, validating it against registered schemas, and routing it to the appropriate processing pipelines. It is designed to absorb bursty traffic patterns without data loss and provides exactly-once delivery semantics end-to-end.

**Sub-components:**

| Sub-component | Responsibility | Technology |
|---|---|---|
| Thunder Gateway | Accept HTTP/gRPC ingestion requests, TLS termination, rate limiting | Go 1.22+ |
| Stream Collector | Durable, partitioned message queuing with exactly-once semantics | Apache Kafka 3.6+ |
| Batch Importer | Bulk loading of historical files from object storage | Apache Spark 3.5+ |
| Schema Registry | Centralized schema management with evolution and compatibility checks | Confluent Schema Registry |
| Dead Letter Queue | Capture and isolate invalid or unprocessable records | Kafka DLQ Topics |

**Key design decisions:**

- **Write-ahead logging** before acknowledgment ensures no data is lost even during gateway crashes
- **Backpressure propagation** from Kafka through the gateway to clients prevents memory exhaustion
- **Schema-first design** — all data must conform to a registered schema (Avro, Protobuf, or JSON Schema) before entering the pipeline
- **Multi-protocol support** — REST for simplicity, gRPC for throughput, Kafka Connect for CDC, file upload for bulk loads

### 2. Processing Layer

The Processing Layer transforms raw ingested data into analytics-ready datasets. It comprises two complementary processing engines — Flink for real-time and Spark for batch — unified by a common abstraction layer.

**Stream Processor (Apache Flink):**

The stream processor handles continuous, stateful computations with sub-second latency. It supports rich windowing semantics (tumbling, sliding, session, custom), Complex Event Processing (CEP) for pattern detection, and stateful transformations with RocksDB-backed state backends that can grow beyond memory limits.

Critical capabilities:
- Exactly-once processing via aligned checkpoint barriers (every 30 seconds by default)
- Event-time processing with configurable watermark strategies for late-arriving data
- Savepoints for stateful job upgrades and A/B testing of pipeline logic
- Async I/O operators for enriching events from external services without blocking the main pipeline

**Batch Processor (Apache Spark):**

The batch processor executes scheduled ETL jobs, large-scale data transformations, and ML model training. It leverages Spark's adaptive query execution (AQE) and dynamic partition pruning for optimal performance on large datasets.

Critical capabilities:
- Iceberg-based table operations with ACID guarantees and time-travel
- Delta Lake integration for merge (upsert) operations on slowly-changing dimensions
- Dynamic resource allocation scales executors up/down based on workload
- Cost-based optimizer leverages table statistics for join reordering and filter pushdown

**Query Engine:**

Built on Apache Calcite, the custom query engine provides SQL-92 compatible access with extensions for time-series (window functions, time-weighted averages), geospatial (ST_* functions), and nested data types. The optimizer supports rule-based and cost-based optimization, with pluggable rules for domain-specific query rewrites.

### 3. Storage Layer

The Storage Layer implements a multi-tier strategy that places data on the optimal storage medium based on access patterns, latency requirements, and cost constraints.

**Tier Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Storage Tiers                                │
│                                                                     │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────────────┐ │
│  │   HOT TIER    │  │   WARM TIER   │  │      COLD TIER          │ │
│  │               │  │               │  │                         │ │
│  │  Apache Druid │  │  ClickHouse   │  │  Iceberg/Parquet on S3  │ │
│  │  + Redis      │  │               │  │  + HDFS (optional)      │ │
│  │               │  │               │  │                         │ │
│  │  Latency:     │  │  Latency:     │  │  Latency:               │ │
│  │  < 100ms      │  │  < 1s         │  │  Minutes                │ │
│  │               │  │               │  │                         │ │
│  │  Cost: High   │  │  Cost: Medium │  │  Cost: Low              │ │
│  │               │  │               │  │                         │ │
│  │  Data: Recent │  │  Data: Weeks  │  │  Data: Months/Years     │ │
│  │  (0-7 days)   │  │  (7-90 days)  │  │  (90+ days)            │ │
│  └───────────────┘  └───────────────┘  └─────────────────────────┘ │
│                                                                     │
│  Lifecycle: Hot ──────► Warm ──────► Cold ──────► Delete/Archive   │
│  Trigger:   Age/Size     Age          Age/Policy      Compliance    │
└─────────────────────────────────────────────────────────────────────┘
```

**Data Compaction:**

Each tier implements continuous compaction to optimize query performance:
- **Druid**: Segment compaction merges small segments during off-peak hours; column-oriented encoding with dictionary compression reduces storage footprint
- **ClickHouse**: Background merge processes combine data parts per `MergeTree` engine settings; TTL-driven moves between storage volumes
- **Parquet/Iceberg**: Spark-based compaction rewrites small files into larger, bloom-filter-enhanced files; Iceberg's `rewrite_data_files` procedure maintains optimal file sizes (512 MB target)

### 4. Serving Layer

The Serving Layer exposes processed data to downstream consumers through multiple protocols optimized for different access patterns.

| Interface | Protocol | Best For | Latency |
|---|---|---|---|
| REST API | HTTP/2 + JSON | General-purpose queries, administration | < 200ms |
| gRPC Service | HTTP/2 + Protobuf | Low-latency, high-throughput programmatic access | < 50ms |
| WebSocket | WS/WSS | Real-time dashboard updates, live monitoring | < 100ms |
| JDBC/ODBC | Native | BI tool integration (Tableau, Superset, Metabase) | < 500ms |
| GraphQL | HTTP/2 | Flexible, client-driven query composition | < 300ms |

**Query Execution Flow:**

1. **Parse & Validate** — SQL query parsed by Calcite, validated against schema registry
2. **Plan & Optimize** — Cost-based optimizer generates physical plan with join ordering, filter pushdown, and predicate rewriting
3. **Route** — Query router determines target storage tier(s) based on time range and data characteristics
4. **Execute** — For federated queries spanning multiple tiers, sub-queries execute in parallel with result merging
5. **Cache** — Results cached in Redis with TTL proportional to dataset update frequency
6. **Respond** — Results returned in requested format (JSON, Apache Arrow, Parquet)

### 5. Analytics Layer

The Analytics Layer provides higher-level analytical capabilities beyond raw data access:

- **SQL Analytics Engine** — Interactive SQL with support for window functions, CTEs, and user-defined aggregate functions (UDAFs)
- **OLAP Cubes** — Pre-computed aggregation cubes for sub-second drill-down queries on dimensional data
- **Statistical Analysis** — Integration with R and Julia runtimes for advanced statistical modeling
- **Visualization Engine** — TypeScript/JavaScript-based charting library with real-time streaming data support
- **Anomaly Detection** — Built-in anomaly detection using statistical process control and ML-based methods

---

## Technology Stack

### Infrastructure Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Container Orchestration | Kubernetes | 1.28+ | Cluster management, scheduling, auto-scaling |
| Service Mesh | Istio | 1.20+ | mTLS, traffic management, observability |
| Container Runtime | containerd | 1.7+ | OCI-compliant container execution |
| Image Registry | Harbor | 2.10+ | Private container registry with vulnerability scanning |
| Secret Management | HashiCorp Vault | 1.15+ | Dynamic secrets, encryption as a service |
| CI/CD | ArgoCD + GitHub Actions | - | GitOps-based continuous deployment |

### Data Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| Message Bus | Apache Kafka | 3.6+ | Event streaming, durable log, decoupling |
| Stream Processing | Apache Flink | 1.18+ | Real-time data processing, CEP |
| Batch Processing | Apache Spark | 3.5+ | Large-scale ETL, ML training, analytics |
| OLAP Engine (Hot) | Apache Druid | 28.x | Real-time OLAP queries, time-series |
| OLAP Engine (Warm) | ClickHouse | 24.x | Historical analytics, columnar storage |
| Caching | Redis Cluster | 7.x | Low-latency result caching, session store |
| Object Storage | MinIO / S3 | - | Data lake storage, archival |
| Table Format | Apache Iceberg | 1.5+ | ACID transactions, schema evolution, time-travel |
| Schema Registry | Confluent Schema Registry | 7.6+ | Schema management, compatibility enforcement |

### Application Stack

| Component | Technology | Purpose |
|---|---|---|
| API Server | Go 1.22+ | REST/gRPC gateway, high-concurrency handling |
| Query Engine | Java 17+ | SQL parsing, optimization, and execution |
| ML Pipeline | Python 3.11+ / MLflow | Model training, registry, and serving |
| Dashboard | TypeScript + React | Interactive data visualization |
| High-Performance Engine | Rust 1.75+ | Query executor, compression, memory management |
| Scientific Computing | Julia 1.10+ | Advanced analytics and optimization |
| Columnar Database | C++ 20+ | Storage engine, vectorized query execution |
| SDK | Python / Java / Go / Scala | Client libraries for pipeline development |

### Observability Stack

| Component | Technology | Purpose |
|---|---|---|
| Metrics | Prometheus + Grafana | Time-series metrics collection and dashboards |
| Logging | ELK Stack (Elasticsearch, Logstash, Kibana) | Centralized log aggregation and search |
| Tracing | Jaeger + OpenTelemetry | Distributed request tracing across services |
| Alerting | Alertmanager | Rule-based alert routing and notification |
| Profiling | Pyroscope | Continuous profiling for performance optimization |

---

## Design Principles

### 1. Scalability First

Every component is designed to scale horizontally. The system has no single point of bottleneck — stateless services scale via Kubernetes HPA, stateful services scale via partition/shard addition, and the storage layer scales by adding nodes to the respective clusters.

**Key patterns:**
- Partition-based parallelism at every layer (Kafka partitions, Spark partitions, ClickHouse shards)
- Elastic resource allocation that adapts to workload patterns
- Decoupled scaling: each layer can scale independently based on its own load characteristics

### 2. Fault Tolerance

The system assumes failure is inevitable and designs for resilience at every level:

- **Data durability**: Kafka replication factor of 3, min.insync.replicas=2
- **Processing resilience**: Flink checkpoint-based recovery with < 30s RPO; Spark job retry with idempotent writes
- **Storage redundancy**: Druid segment replication, ClickHouse ReplicatedMergeTree, S3 cross-AZ replication
- **Service availability**: Kubernetes auto-restart, Istio circuit breakers, graceful degradation
- **Disaster recovery**: Cross-region replication with < 15 min RPO via Kafka MirrorMaker 2.0

### 3. Data Consistency

The system provides configurable consistency guarantees:

| Consistency Level | Scope | Use Case |
|---|---|---|
| Exactly-once | Processing | Stream and batch processing ensure no duplicate output |
| Eventual | Serving | Cross-tier queries may see slight delays in data propagation |
| Strong | Single-tier queries | Queries within one storage tier see fully consistent data |
| Causal | Event processing | Events from the same source are processed in order |

**Mechanisms:**
- Kafka transactions and idempotent producers for exactly-once ingestion
- Flink aligned checkpoints for exactly-once stream processing
- Iceberg ACID transactions for atomic batch writes
- Two-phase commit for cross-system consistency (e.g., Flink → Kafka → Druid)

### 4. Observability

All components emit metrics (Prometheus), logs (structured JSON), and traces (OpenTelemetry) in a consistent format:

```yaml
observability:
  metrics:
    format: prometheus
    scrape_interval: 15s
    retention: 30d
  logging:
    format: json
    level: INFO
    output: stdout    # Collected by Fluentd/Logstash
  tracing:
    format: opentelemetry
    sampling_rate: 0.1    # 10% of requests traced in production
    export: jaeger
```

### 5. Security

Security is built into every layer, not bolted on:

- **In-transit**: All inter-service communication encrypted via Istio mTLS
- **At-rest**: S3 server-side encryption (SSE-KMS), LUKS for local disk encryption
- **Authentication**: OAuth2/OIDC for user-facing APIs, mTLS for service-to-service
- **Authorization**: RBAC with dataset-level and operation-level granularity
- **Audit**: Every data access and mutation is logged with user identity, timestamp, and operation details

### 6. Extensibility

The platform is designed for extension through well-defined plugin interfaces:

- **Custom Processors**: Implement the `Processor` interface to add new transformation logic
- **Custom Sinks**: Implement the `Sink` interface to write to new storage systems
- **Query Extensions**: Register custom Calcite UDFs and UDAFs for domain-specific logic
- **Connectors**: Build Kafka Connect connectors for new data sources

---

## System Requirements

### Development Environment

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 8 cores | 16 cores |
| Memory | 32 GB RAM | 64 GB RAM |
| Storage | 500 GB SSD | 1 TB NVMe SSD |
| Network | 1 Gbps | 10 Gbps |
| OS | Ubuntu 20.04+ / macOS 12+ | Ubuntu 22.04 LTS |

### Staging Environment

| Component | Count | Instance Type | Storage |
|---|---|---|---|
| Kubernetes Nodes | 5 | m5.2xlarge | 200 GB SSD |
| Kafka Brokers | 3 | i3.xlarge | 1 TB SSD |
| Flink Task Managers | 4 | c5.2xlarge | - |
| Druid Cluster | 2 historical + 1 broker | r5.2xlarge | 2 TB SSD |
| ClickHouse | 3 nodes | r5.2xlarge | 4 TB SSD |

### Production Environment (10 TB/day)

| Component | Count | Instance Type | Storage |
|---|---|---|---|
| Kubernetes Nodes | 10+ | m5.4xlarge | 500 GB SSD |
| Kafka Brokers | 5+ | i3.2xlarge | 2 TB SSD each |
| Flink Task Managers | 10+ | c5.4xlarge | 16 GB heap each |
| Druid Cluster | 4 historical + 2 broker | r5.4xlarge | 16 TB SSD |
| ClickHouse | 6 nodes (2 shards × 3 replicas) | r5.4xlarge | 32 TB SSD |
| Redis Cluster | 6 nodes | r5.xlarge | - |
| Monitoring Stack | 3 nodes | m5.xlarge | 500 GB SSD |

### Software Prerequisites

| Software | Version | Purpose |
|---|---|---|
| Docker | 24.0+ | Container runtime for local development |
| Kubernetes | 1.28+ | Production orchestration |
| Helm | 3.12+ | Kubernetes package management |
| Java (OpenJDK) | 17 | Flink, Spark, Query Engine |
| Python | 3.11+ | ML pipelines, SDK |
| Go | 1.22+ | API server, gateway services |
| kubectl | 1.28+ | Kubernetes CLI |

---

## Related Documentation

- [Data Flow](data_flow.md) — End-to-end data flow from ingestion to serving
- [Microservices Architecture](microservices.md) — Service decomposition and inter-service communication
- [Distributed Systems Design](distributed_systems.md) — Distributed computing patterns and consensus
- [Scalability Design](scalability_design.md) — Scaling strategies and capacity planning
- [System Overview](system_overview.md) — High-level component descriptions
