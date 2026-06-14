# Data Flow Documentation

## Introduction

Understanding how data moves through Thunders-BigData-System is essential for operators, developers, and architects who need to reason about latency, correctness, and reliability. This document provides an exhaustive reference for every data flow path in the system — from the moment a record is produced at an external source to the instant it becomes queryable through the serving layer. It covers batch flows, real-time streaming flows, inter-layer transitions, serialization formats, and configuration examples.

This document complements the [System Architecture](system_architecture.md) by focusing specifically on data movement, transformation semantics, and the guarantees provided at each stage.

---

## End-to-End Data Flow Overview

Data traverses five logical layers as it moves from source to consumer. At each boundary, specific contracts govern delivery guarantees, ordering, and error handling.

```
  SOURCE          INGESTION           PROCESSING           STORAGE            SERVING
┌─────────┐   ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐   ┌──────────────┐
│ IoT     │   │ Thunder      │   │ Flink Stream     │   │ Druid (Hot)  │   │ REST API     │
│ Sensors │──►│ Gateway      │──►│ Processor        │──►│ ClickHouse   │──►│ gRPC Service │
│ DB CDC  │   │              │   │                  │   │ (Warm)       │   │ WebSocket    │
│ Log     │   │ Kafka        │   │ Spark Batch      │   │ S3/Iceberg   │   │ JDBC/ODBC    │
│ Streams │   │ Connectors   │   │ Processor        │   │ (Cold)       │   │ GraphQL      │
│ Files   │   │ Schema       │   │                  │   │              │   │              │
│ APIs    │   │ Registry     │   │ ML Pipeline      │   │ Redis Cache  │   │              │
└─────────┘   └──────────────┘   └──────────────────┘   └──────────────┘   └──────────────┘

   Guarantees:   At-least-once      Exactly-once          Durable             Consistent
                 Schema-valid        Ordered (per-key)     Replicated          Cached
                 Enriched            Windowed              Compacted           Low-latency
```

### Flow Guarantees by Layer

| Boundary | Delivery Guarantee | Ordering | Latency | Error Handling |
|---|---|---|---|---|
| Source → Ingestion | At-least-once (idempotent dedup) | Per-source | < 10ms (gRPC) / < 50ms (REST) | DLQ for invalid records |
| Ingestion → Processing | Exactly-once (Kafka transactions) | Per-partition | < 5ms (Kafka consumer lag) | Consumer rollback & retry |
| Processing → Storage | Exactly-once (2PC / idempotent writes) | Per-key | < 100ms (stream) / Minutes (batch) | Sink retry + DLQ |
| Storage → Serving | Read-committed | N/A (read path) | < 100ms (hot) / < 5s (cold) | Cache fallback, graceful degradation |

---

## Batch Data Flow Pipeline

The batch pipeline handles large-scale, scheduled data transformations that do not require real-time latency. It is the primary path for historical data reprocessing, complex joins across large datasets, and ML model training.

### Batch Flow Architecture

```
┌──────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐    ┌──────────────┐
│  Source   │    │  Batch       │    │  Spark        │    │  Iceberg     │    │  Serving     │
│  Files    │───►│  Importer    │───►│  Processor    │───►│  Table       │───►│  Layer       │
│  (S3/HDFS)│    │  (Spark)     │    │  (ETL/ML)     │    │  (Data Lake) │    │  (Query)     │
└──────────┘    └──────────────┘    └───────────────┘    └──────────────┘    └──────────────┘
                      │                     │                    │
                      │              ┌──────▼──────┐     ┌──────▼──────┐
                      │              │  Feature    │     │  ClickHouse │
                      │              │  Store      │     │  (Warm)     │
                      │              │  (Feast)    │     └─────────────┘
                      │              └─────────────┘
                      │
                ┌─────▼──────┐
                │  Schema     │
                │  Validation │
                │  & Register │
                └────────────┘
```

### Batch Flow Configuration

```yaml
# Batch pipeline configuration
batch_pipeline:
  name: "daily_analytics_etl"
  schedule: "0 2 * * *"           # Cron: Daily at 2 AM UTC
  timeout_minutes: 360             # 6-hour timeout
  retry:
    max_attempts: 3
    backoff_multiplier: 2
    initial_delay_seconds: 60

  source:
    type: "iceberg"
    tables:
      - "thunders.raw.user_events"
      - "thunders.raw.transactions"
      - "thunders.raw.customer_profiles"
    time_range:
      lookback_days: 1             # Process yesterday's data
      overlap_hours: 2             # 2-hour overlap for late-arriving data

  processing:
    engine: "spark"
    config:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.sql.shuffle.partitions: "200"
      spark.sql.catalog.thunders: "org.apache.iceberg.spark.SparkCatalog"
      spark.sql.catalog.thunders.warehouse: "s3a://thunders-data/warehouse"
      spark.sql.catalog.thunders.catalog-impl: "org.apache.iceberg.aws.glue.GlueCatalog"
    dynamic_allocation:
      enabled: true
      min_executors: 4
      max_executors: 50
      idle_timeout_seconds: 120

  sink:
    type: "multi"
    targets:
      - type: "iceberg"
        table: "thunders.analytics.daily_summary"
        write_mode: "upsert"
        partition_by: ["event_date"]
      - type: "clickhouse"
        table: "thunders.daily_summary"
        write_mode: "replace_partition"
      - type: "feature_store"
        store: "feast"
        feature_view: "daily_user_features"
```

### Batch ETL Job Example

```python
# Batch ETL: Daily customer analytics aggregation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, date_trunc, collect_set
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("thunders-daily-customer-analytics") \
    .config("spark.sql.catalog.thunders", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Read raw events from Iceberg data lake
events = spark.table("thunders.raw.user_events") \
    .filter(col("event_date") == "2024-01-15")

transactions = spark.table("thunders.raw.transactions") \
    .filter(col("transaction_date") == "2024-01-15")

customers = spark.table("thunders.raw.customer_profiles")

# Join and aggregate
daily_summary = events \
    .join(transactions, "user_id", "left") \
    .join(customers, "user_id", "left") \
    .groupBy("user_id", "customer_tier", "region") \
    .agg(
        count("event_id").alias("total_events"),
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_revenue"),
        avg("session_duration").alias("avg_session_duration"),
        collect_set("event_type").alias("event_types")
    )

# Write to Iceberg with ACID guarantees
daily_summary.writeTo("thunders.analytics.daily_customer_summary") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.parquet.compression-codec", "zstd") \
    .partitionedBy(col("event_date")) \
    .option("merge-schema", "true") \
    .append()

# Also write to ClickHouse for interactive queries
daily_summary.write \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://clickhouse:8123/thunders") \
    .option("dbtable", "daily_customer_summary") \
    .option("isolationLevel", "NONE") \
    .mode("append") \
    .save()
```

---

## Real-Time Streaming Data Flow

The streaming pipeline handles continuous, low-latency data processing. Every record is processed within seconds of ingestion, enabling real-time dashboards, alerting, and operational intelligence.

### Streaming Flow Architecture

```
┌──────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐    ┌──────────────┐
│  Source   │    │  Kafka       │    │  Flink        │    │  Druid       │    │  WebSocket   │
│  (Push)   │───►│  Topic       │───►│  Stream       │───►│  Realtime    │───►│  Clients     │
│           │    │  (Partitioned)│   │  Processor    │    │  Node        │    │              │
└──────────┘    └──────────────┘    └───────┬───────┘    └──────────────┘    └──────────────┘
                                           │
                                    ┌──────▼───────┐
                                    │  Kafka Sink   │──► Downstream consumers
                                    │  (Output)     │
                                    └──────┬───────┘
                                           │
                              ┌────────────┼────────────┐
                              │            │            │
                       ┌──────▼──┐  ┌─────▼────┐  ┌───▼──────┐
                       │ Click-  │  │ Iceberg  │  │ Feature  │
                       │ House   │  │ Sink     │  │ Store    │
                       │ (Warm)  │  │ (Cold)   │  │ (Feast)  │
                       └─────────┘  └──────────┘  └──────────┘
```

### Streaming Job Configuration

```yaml
# Streaming pipeline configuration
streaming_pipeline:
  name: "realtime-event-analytics"
  flink:
    parallelism: 16
    max_parallelism: 256
    state_backend: rocksdb
    checkpointing:
      interval_ms: 30000
      mode: EXACTLY_ONCE
      timeout_ms: 60000
      min_pause_ms: 10000
      externalized: RETAIN_ON_CANCELLATION

    watermark:
      strategy: bounded_out_of_orderness
      max_out_of_orderness_ms: 5000    # 5-second tolerance for late events
      idle_timeout_ms: 30000           # 30-second idle source timeout

  sources:
    - topic: "thunders.user_events"
      consumer_group: "thunders-event-processor"
      start_from: latest
      deserialization: avro
      schema_id: "user_events_v3"

  operators:
    - name: "enrich_geo"
      type: async_io
      lookup_service: "geo_ip_service"
      timeout_ms: 100
      capacity: 100

    - name: "session_window"
      type: window
      window_type: session
      gap_minutes: 30
      allowed_lateness_minutes: 5

    - name: "anomaly_score"
      type: flat_map
      model_endpoint: "mlflow://anomaly-detector/v1"

  sinks:
    - name: "druid_realtime"
      type: druid
      index_service: "druid/overlord"
      segment_granularity: hour

    - name: "clickhouse"
      type: jdbc
      table: "thunders.event_sessions"
      batch_size: 10000
      flush_interval_ms: 5000

    - name: "iceberg"
      type: iceberg
      table: "thunders.processed.event_sessions"
      commit_interval_ms: 60000

    - name: "kafka_output"
      type: kafka
      topic: "thunders.processed.events"
      serialization: protobuf
```

### Streaming Job Code Example

```java
// Real-time event sessionization with enrichment and anomaly detection
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60000);
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);

// Source: Kafka with Avro deserialization
KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("thunders.user_events")
    .setGroupId("thunders-event-processor")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new AvroDeserializer<>(UserEvent.class))
    .build();

DataStream<UserEvent> events = env.fromSource(
    source,
    WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, ts) -> event.getTimestamp())
        .withIdleness(Duration.ofSeconds(30)),
    "user-events"
);

// Async enrichment: GeoIP lookup
DataStream<EnrichedEvent> enriched = AsyncDataStream.unorderedWait(
    events,
    new GeoIpAsyncFunction("http://geo-ip-service:8080/lookup"),
    100, TimeUnit.MILLISECONDS,  // Timeout per request
    100                          // Max concurrent requests
);

// Session window aggregation
DataStream<SessionSummary> sessions = enriched
    .keyBy(EnrichedEvent::getUserId)
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .allowedLateness(Time.minutes(5))
    .sideOutputLateData(lateOutputTag)
    .aggregate(new SessionAggregator());

// Multi-sink output
sessions.sinkTo(createDruidSink());
sessions.sinkTo(createClickHouseSink());
sessions.addSink(createIcebergSink());

// Late events to DLQ
env.getSideOutput(lateOutputTag)
    .sinkTo(createKafkaDlqSink("thunders.dlq.late_events"));

env.execute("thunders-realtime-event-analytics");
```

---

## Data Flow Between Layers

### Ingestion → Processing

Data transitions from the ingestion layer to the processing layer through Kafka topics. This boundary is the most critical in the system because it decouples producers from consumers while providing durability and ordering guarantees.

**Key behaviors at this boundary:**

1. **Consumer Group Management**: Each Flink consumer group maintains its own offset, enabling independent consumption at different rates
2. **Backpressure Propagation**: If Flink cannot keep up, Kafka consumer lag increases; Flink's credit-based flow control prevents internal buffer overflow
3. **Schema Evolution**: The Schema Registry ensures that consumers can read data written with compatible schema versions (backward, forward, or full compatibility)

```yaml
# Ingestion-to-processing bridge configuration
ingestion_to_processing:
  kafka:
    consumer_config:
      auto.offset.reset: latest
      enable.auto.commit: false         # Flink manages commits via checkpoints
      max.poll.records: 500
      max.poll.interval.ms: 300000
      session.timeout.ms: 30000
      heartbeat.interval.ms: 10000

    topic_config:
      num_partitions: 64
      replication_factor: 3
      min.insync.replicas: 2
      retention_ms: 604800000           # 7 days
      cleanup.policy: delete
      compression.type: lz4

  schema_registry:
    url: "http://schema-registry:8081"
    compatibility: BACKWARD
    subject_naming: "${topic}-value"
```

### Processing → Storage

Data is written from processing engines to the storage tier(s) determined by the storage routing configuration. This boundary implements exactly-once semantics through a combination of idempotent writes and two-phase commit protocols.

**Flink → Druid**: Real-time indexing via Druid's Kafka indexing service or tranquility; Flink writes to an intermediate Kafka topic that Druid consumes
**Flink → ClickHouse**: JDBC sink with batch accumulation and idempotent inserts using `INSERT INTO ... ON DUPLICATE KEY UPDATE`
**Flink → Iceberg**: Iceberg sink with commit coordination; Flink's `IcebergFilesCommitter` ensures atomic table commits
**Spark → Iceberg**: Native Iceberg writes with ACID guarantees; row-level operations (MERGE INTO) for upsert scenarios

### Storage → Serving

The serving layer reads from storage tiers based on query characteristics. A query router determines the optimal tier:

```python
# Storage tier routing logic
class StorageRouter:
    def route_query(self, query: Query) -> List[StorageTarget]:
        targets = []

        if query.time_range:
            # Hot tier: last 7 days with sub-second latency requirement
            if query.time_range.end > now() - timedelta(days=7):
                if query.latency_target_ms and query.latency_target_ms < 1000:
                    targets.append(StorageTarget.DRUID)

            # Warm tier: 7-90 days
            if query.time_range.start < now() - timedelta(days=7):
                targets.append(StorageTarget.CLICKHOUSE)

            # Cold tier: older than 90 days
            if query.time_range.start < now() - timedelta(days=90):
                targets.append(StorageTarget.ICEBERG_S3)

        # If no time range, query all tiers and merge
        if not query.time_range:
            targets = [StorageTarget.DRUID, StorageTarget.CLICKHOUSE]

        return targets
```

---

## Data Serialization Formats

The choice of serialization format impacts storage efficiency, query performance, and schema evolution capabilities. Thunders-BigData-System uses different formats at different stages of the data lifecycle.

### Format Comparison

| Property | Avro | Protobuf | Parquet | JSON |
|---|---|---|---|---|
| Primary Use | Kafka messages, Schema Registry | gRPC, high-throughput ingestion | Storage files, columnar access | REST API, debugging |
| Schema Evolution | Excellent (backward/forward/full) | Good (with conventions) | Limited (Iceberg handles evolution) | None (schemaless) |
| Compression | Medium (block-level) | High (varint encoding) | Excellent (columnar + encoding) | Poor (text-based) |
| Read Pattern | Row-oriented (full deserialization) | Row-oriented (full deserialization) | Columnar (predicate pushdown) | Row-oriented |
| Query Support | Via Calcite deserialization | Via Calcite deserialization | Native Spark/Flink pushdown | Via JSON path |
| Typing | Strong, nullable | Strong, optional/repeated | Strong, nullable | Weak, dynamic |

### Avro Schema (Kafka Messages)

Avro is the primary format for Kafka topics due to its excellent schema evolution support and compact binary encoding:

```json
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "io.thunders.events",
  "doc": "User interaction event from web and mobile applications",
  "fields": [
    {"name": "event_id", "type": "string", "doc": "Unique event identifier (UUID)"},
    {"name": "user_id", "type": "string", "doc": "User identifier"},
    {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": ["PAGE_VIEW", "CLICK", "PURCHASE", "ADD_TO_CART", "SEARCH"]}},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis", "doc": "Event timestamp in epoch milliseconds"},
    {"name": "properties", "type": {"type": "map", "values": "string"}, "default": {}},
    {"name": "session_id", "type": ["null", "string"], "default": null},
    {"name": "device", "type": {"type": "record", "name": "DeviceInfo", "fields": [
      {"name": "type", "type": {"type": "enum", "name": "DeviceType", "symbols": ["MOBILE", "DESKTOP", "TABLET"]}},
      {"name": "os", "type": "string"},
      {"name": "browser", "type": "string"}
    ]}},
    {"name": "geo", "type": ["null", {"type": "record", "name": "GeoInfo", "fields": [
      {"name": "country", "type": "string"},
      {"name": "city", "type": "string"},
      {"name": "latitude", "type": "double"},
      {"name": "longitude", "type": "double"}
    ]}], "default": null}
  ]
}
```

### Protobuf Schema (gRPC / High-Throughput)

Protobuf is used for gRPC service definitions and high-throughput ingestion paths where smaller wire size is critical:

```protobuf
syntax = "proto3";
package thunders.ingestion.v1;

option java_package = "io.thunders.ingestion.v1";

service IngestionService {
  rpc Ingest(IngestRequest) returns (IngestResponse);
  rpc IngestStream(stream IngestRequest) returns (IngestResponse);
}

message IngestRequest {
  string dataset = 1;
  string schema_id = 2;
  repeated Record records = 3;
}

message Record {
  string key = 1;
  bytes payload = 2;
  int64 timestamp = 3;
  map<string, string> metadata = 4;
}

message IngestResponse {
  int32 accepted_count = 1;
  int32 rejected_count = 2;
  repeated RejectionDetail rejections = 3;
}

message RejectionDetail {
  int32 record_index = 1;
  string error_code = 2;
  string error_message = 3;
}
```

### Parquet File Configuration (Storage)

Parquet is the columnar storage format used in Iceberg tables and S3 archival, optimized for analytical query workloads:

```yaml
# Parquet write configuration for Iceberg tables
iceberg:
  write:
    format: parquet
    parquet:
      compression_codec: zstd        # Best compression/performance ratio
      compression_level: 3            # ZSTD level (1-22, 3 is default)
      row_group_size: 134217728       # 128 MB row groups
      page_size: 1048576              # 1 MB pages
      dictionary_encoding: true
      dictionary_page_size: 1048576   # 1 MB dictionary pages
      bloom_filter_enabled: true
      bloom_filter_columns:           # Columns to generate bloom filters for
        - user_id
        - session_id
        - transaction_id
      bloom_filter_fpp: 0.01          # 1% false positive rate
      bloom_filter_ndv: 1000000       # Estimated distinct values
```

---

## Data Flow Configuration Examples

### Complete Pipeline Definition

```yaml
# Full pipeline configuration from source to sink
pipeline:
  name: "customer-event-analytics"
  version: "3.2.1"
  owner: "data-platform-team"

  # Source definition
  source:
    type: "kafka"
    topic: "thunders.raw.customer_events"
    bootstrap_servers: "kafka-0:9092,kafka-1:9092,kafka-2:9092"
    consumer_group: "customer-event-analytics-v3"
    schema:
      format: avro
      subject: "customer_events-value"
      compatibility: BACKWARD
    starting_offset: latest
    rate_limit:
      records_per_second: 50000
      bytes_per_second: 52428800    # 50 MB/s

  # Processing steps
  processing:
    - step: validate
      type: schema_validation
      schema_subject: "customer_events-value"
      on_failure: route_to_dlq

    - step: normalize
      type: transformation
      transformations:
        - field: "timestamp"
          operation: "to_utc"
        - field: "email"
          operation: "hash"
          algorithm: "sha256"
        - field: "amount"
          operation: "cast"
          target_type: "decimal(18,2)"

    - step: enrich_geo
      type: async_lookup
      service: "geo_ip_service"
      input_field: "ip_address"
      output_fields: ["country", "city", "region", "latitude", "longitude"]
      timeout_ms: 100
      cache_ttl_seconds: 3600

    - step: enrich_customer
      type: async_lookup
      service: "customer_profile_service"
      input_field: "customer_id"
      output_fields: ["tier", "lifetime_value", "account_age_days"]
      timeout_ms: 200
      cache_ttl_seconds: 300

    - step: sessionize
      type: window
      window_type: session
      key_field: "customer_id"
      gap_minutes: 30
      allowed_lateness_minutes: 5

    - step: aggregate
      type: aggregate
      group_by: ["customer_id", "event_type"]
      measures:
        event_count: "count"
        total_amount: "sum(amount)"
        avg_duration: "avg(session_duration)"
        unique_pages: "count_distinct(page_url)"

  # Sink definitions
  sinks:
    - name: "druid_realtime"
      type: "druid"
      index_service: "druid/overlord"
      segment_granularity: "HOUR"
      query_granularity: "MINUTE"
      max_rows_in_memory: 500000

    - name: "clickhouse_analytics"
      type: "clickhouse"
      table: "thunders.customer_sessions"
      engine: "ReplicatedMergeTree"
      partition_by: "toYYYYMM(event_date)"
      order_by: "(customer_id, timestamp)"
      batch_size: 10000
      flush_interval_ms: 5000

    - name: "iceberg_data_lake"
      type: "iceberg"
      table: "thunders.processed.customer_sessions"
      write_format: parquet
      commit_interval_ms: 60000

    - name: "feature_store"
      type: "feast"
      feature_view: "customer_realtime_features"
      online_store: "redis"
      offline_store: "iceberg"

  # Error handling
  error_handling:
    dlq_topic: "thunders.dlq.customer_events"
    retry:
      max_attempts: 3
      backoff_ms: [100, 500, 2000]
    circuit_breaker:
      failure_threshold: 10
      reset_timeout_ms: 30000

  # Monitoring
  monitoring:
    metrics:
      - "records_consumed_per_second"
      - "records_produced_per_second"
      - "processing_latency_ms_p99"
      - "checkpoint_duration_ms"
      - "consumer_lag_records"
    alerts:
      - metric: "consumer_lag_records"
        threshold: 1000000
        severity: warning
      - metric: "checkpoint_duration_ms"
        threshold: 60000
        severity: critical
```

---

## Monitoring Data Flow Health

### Key Metrics by Layer

| Layer | Metric | Description | Alert Threshold |
|---|---|---|---|
| Ingestion | `ingest.records_in.total` | Total records received | < 10% of expected |
| Ingestion | `ingest.validation_error_rate` | Schema validation failure rate | > 1% |
| Ingestion | `ingest.backpressure_ratio` | Backpressure on gateway | > 0.7 |
| Processing | `flink.consumer.lag` | Kafka consumer lag in records | > 1M records |
| Processing | `flink.checkpoint.duration` | Checkpoint completion time | > 60s |
| Processing | `flink.backpressure` | Operator backpressure ratio | > 0.5 |
| Processing | `spark.job.duration` | Batch job execution time | > 2x baseline |
| Storage | `druid.segment.count` | Total Druid segments | Sudden spike/drop |
| Storage | `clickhouse.merge.queue` | Pending merge operations | > 100 |
| Storage | `iceberg.snapshot.count` | Table snapshot count | > 100 (needs cleanup) |
| Serving | `query.latency.p99` | P99 query latency | > 5s |
| Serving | `query.error_rate` | Query failure rate | > 0.1% |
| Serving | `cache.hit_rate` | Redis cache hit rate | < 80% |

### Data Flow Tracing

Every record that enters the system receives a trace ID that propagates through all processing stages. This enables end-to-end visibility:

```
Trace: trace-abc123
├── [ingest] Received via gRPC at 10:30:00.100 (latency: 2ms)
├── [validate] Schema validation passed at 10:30:00.105 (latency: 5ms)
├── [enrich] GeoIP enrichment at 10:30:00.115 (latency: 10ms)
├── [process] Session aggregation at 10:30:00.200 (latency: 85ms)
├── [store] Written to Druid at 10:30:00.350 (latency: 150ms)
├── [store] Written to ClickHouse at 10:30:00.400 (latency: 200ms)
└── [serve] Queryable at 10:30:00.500 (total: 400ms)
```

---

## Related Documentation

- [System Architecture](system_architecture.md) — Comprehensive architecture and technology stack
- [Microservices Architecture](microservices.md) — Service decomposition and communication patterns
- [Distributed Systems Design](distributed_systems.md) — Distributed computing and consensus patterns
- [System Overview](system_overview.md) — High-level component descriptions
- [REST API](../api/rest_api.md) — API endpoints for ingestion and querying
