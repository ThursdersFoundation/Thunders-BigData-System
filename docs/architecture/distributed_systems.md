# Distributed Systems Design

## Introduction

Thunders-BigData-System is fundamentally a distributed system — it runs across dozens to thousands of nodes, processes petabytes of data, and must maintain correctness, availability, and performance in the face of partial failures, network partitions, and concurrent operations. This document covers the distributed computing patterns, data partitioning strategies, consistency models, fault tolerance mechanisms, consensus protocols, caching strategies, and network topology that underpin the platform's reliability and performance.

While the [Distributed Architecture](distributed_architecture.md) document covers cluster deployment and node-level configuration, this document focuses on the algorithmic and theoretical foundations of distributed systems design as applied within the Thunders platform.

---

## Distributed Computing Patterns

### MapReduce Pattern

Although modern big data processing has moved beyond the original MapReduce paradigm, the fundamental pattern — decompose a large computation into parallel map tasks followed by a reduce/aggregation phase — remains the backbone of both Spark and Flink execution engines.

**How Thunders uses MapReduce:**

Apache Spark internally compiles DataFrame and Dataset operations into a DAG of map, shuffle, and reduce stages. When a user submits a batch job like:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg

spark = SparkSession.builder.appName("thunders-mapreduce-example").getOrCreate()

# This logical operation compiles to a MapReduce-style execution plan
result = spark.table("thunders.raw.transactions") \
    .filter("transaction_date = '2024-01-15'")       # Map: filter (parallel)
    .groupBy("merchant_category")                      # Shuffle: repartition by key
    .agg(
        sum("amount").alias("total_revenue"),          # Reduce: aggregate
        count("transaction_id").alias("tx_count"),
        avg("amount").alias("avg_tx_value")
    ) \
    .orderBy("total_revenue", ascending=False)         # Final sort
```

**Execution Plan:**

```
Stage 0 (Map):     Read Parquet files ──► Filter rows ──► Partial aggregate
                                                                       │
Stage 1 (Shuffle): ──────────────────────────────────────────────────►  Repartition
                                                   by merchant_category │
                                                                       │
Stage 2 (Reduce):  Final aggregate ◄──────────────────────────────────┘
                          │
Stage 3 (Sort):    Sort by total_revenue
                          │
                    Write to output table
```

**MapReduce optimizations in Thunders:**
- **Combiner functions**: Pre-aggregate on the map side to reduce shuffle data volume
- **Locality-aware scheduling**: Schedule map tasks on nodes holding the input data (HDFS/DataLake locality)
- **Speculative execution**: Detect straggler tasks and launch redundant copies; take the first result

### DAG Execution Pattern

Both Flink and Spark represent computation as a Directed Acyclic Graph (DAG), where nodes are operators and edges represent data dependencies. The DAG execution model enables:

1. **Pipeline parallelism**: Operators connected by forward edges can execute in a pipelined fashion (no materialization)
2. **Shuffle boundaries**: Operators requiring data repartitioning create stage boundaries with materialization
3. **Parallelism per operator**: Each operator can have its own parallelism, independent of upstream/downstream operators

**Flink DAG Example:**

```java
// Flink streaming DAG with multiple branches and joins
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Source nodes (parallelism: 8)
DataStream<OrderEvent> orders = env
    .fromSource(kafkaOrderSource, watermarkStrategy, "orders")
    .setParallelism(8);

DataStream<ClickEvent> clicks = env
    .fromSource(kafkaClickSource, watermarkStrategy, "clicks")
    .setParallelism(8);

// Processing branches
DataStream<EnrichedOrder> enrichedOrders = orders
    .keyBy(OrderEvent::getUserId)
    .asyncEnrich(customerLookup)          // Parallelism: 8
    .setParallelism(16);

DataStream<ClickSummary> clickSummary = clicks
    .keyBy(ClickEvent::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new ClickAggregator())     // Parallelism: 8
    .setParallelism(16);

// Join branch (requires shuffle by userId)
DataStream<ConversionEvent> conversions = enrichedOrders
    .keyBy(EnrichedOrder::getUserId)
    .intervalJoin(clickSummary.keyBy(ClickSummary::getUserId))
    .between(Time.minutes(-5), Time.minutes(0))
    .process(new ConversionDetector())    // Parallelism: 16
    .setParallelism(32);

// Sink nodes
conversions.sinkTo(druidSink).setParallelism(16);
conversions.sinkTo(clickHouseSink).setParallelism(16);
```

**DAG Visualization:**

```
                    [Kafka: orders]         [Kafka: clicks]
                         │                       │
                    Source (p=8)            Source (p=8)
                         │                       │
                    keyBy(userId)           keyBy(userId)
                         │                       │
                    AsyncEnrich (p=16)     Window+Aggregate (p=16)
                         │                       │
                    keyBy(userId)           keyBy(userId)
                         │                       │
                         └─────── Interval ───────┘
                                  Join (p=32)
                                    │
                            ┌───────┴───────┐
                            │               │
                     Druid Sink (p=16)  ClickHouse Sink (p=16)
```

### Stream Processing Pattern

Stream processing in Thunders follows the continuous operator model, where records flow through a network of stateful operators. Key design considerations include:

- **State Management**: Operator state is partitioned by key and backed by RocksDB for durability beyond memory limits
- **Watermark Propagation**: Watermarks flow through the DAG, indicating the progress of event time; operators use watermarks to trigger window computations
- **Backpressure**: Credit-based flow control ensures that fast producers cannot overwhelm slow consumers; if an operator falls behind, backpressure propagates upstream to the source

```yaml
# Stream processing configuration
stream_processing:
  state_backend:
    type: rocksdb
    config:
      local_dir: /mnt/ephemeral/rocksdb
      timer_service_factory: ROCKSDB
      predefined_options: SPARK_SHELL_MEDIUM
      block_cache_size: 256MB
      write_buffer_size: 64MB
      max_open_files: -1

  checkpointing:
    interval_ms: 30000
    mode: EXACTLY_ONCE
    timeout_ms: 120000
    min_pause_ms: 10000
    max_concurrent: 1
    externalized: RETAIN_ON_CANCELLATION
    incremental: true
    aligned_barriers: true

  watermark:
    strategy: bounded_out_of_orderness
    max_out_of_orderness_ms: 5000
    idle_timeout_ms: 30000
    watermark_interval_ms: 200
```

---

## Partitioning Strategies

Data partitioning is the fundamental mechanism that enables parallelism, scalability, and data locality in a distributed system. Thunders supports multiple partitioning strategies, each optimized for different data characteristics and access patterns.

### Hash Partitioning

Records are assigned to partitions by applying a hash function to one or more key fields. This provides uniform distribution when the hash key has high cardinality.

**Best for**: Evenly distributed data with no natural ordering requirement, point lookups by key

**Used by**: Kafka topics, ClickHouse distributed tables, Spark shuffles

```python
# Hash partitioning implementation
import mmh3  # MurmurHash3 — fast, well-distributed hash function

def hash_partition(key: str, num_partitions: int) -> int:
    """Assign a key to a partition using MurmurHash3.

    Properties:
    - Deterministic: same key always maps to the same partition
    - Uniform: keys are distributed approximately evenly across partitions
    - Consistent: adding/removing partitions causes minimal key remapping
      (when using consistent hashing variant)
    """
    hash_value = mmh3.hash64(key, signed=False)
    return hash_value % num_partitions

# Example: 64 partitions
partition = hash_partition("user-12345", 64)  # → 23
```

**Consistent Hashing Extension:**

For scenarios where partition counts change dynamically (e.g., Kafka partition expansion), Thunders uses consistent hashing to minimize data movement:

```go
// Consistent hashing for dynamic partition assignment
type ConsistentHashRing struct {
    ring       map[uint32]string
    sortedKeys []uint32
    virtualNodes int
}

func (c *ConsistentHashRing) GetPartition(key string) string {
    hash := mmh3.Hash32([]byte(key))
    idx := sort.Search(len(c.sortedKeys), func(i int) bool {
        return c.sortedKeys[i] >= hash
    })
    if idx == len(c.sortedKeys) {
        idx = 0  // Wrap around
    }
    return c.ring[c.sortedKeys[idx]]
}
```

### Range Partitioning

Records are assigned to partitions based on value ranges of the partition key, typically a timestamp. This preserves ordering within partitions and enables efficient range scans.

**Best for**: Time-series data, range queries, ordered data processing

**Used by**: ClickHouse partitioning, Druid segment granularity, Iceberg partitioning

```yaml
# Range partitioning configuration
dataset: "iot_metrics"
partitioning:
  strategy: range
  key: ["timestamp"]
  ranges:
    - start: "2024-01-01T00:00:00Z"
      end: "2024-02-01T00:00:00Z"
      partitions: 8
    - start: "2024-02-01T00:00:00Z"
      end: "2024-03-01T00:00:00Z"
      partitions: 8
  # Or use automatic granularity-based partitioning
  granularity: daily        # One partition per day
  sub_partitioning:
    key: ["device_type"]
    strategy: hash
    num_sub_partitions: 4
```

**Range Partitioning in ClickHouse:**

```sql
-- ClickHouse: Range partitioning by month with primary key ordering
CREATE TABLE thunders.iot_metrics ON CLUSTER '{cluster}' (
    device_id String,
    metric_name String,
    value Float64,
    timestamp DateTime,
    tags Map(String, String),
    metric_date Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/iot_metrics', '{replica}')
PARTITION BY toYYYYMM(metric_date)           -- Range partition by month
ORDER BY (device_id, metric_name, timestamp)  -- Primary key for sparse index
TTL metric_date + INTERVAL 1 YEAR
SETTINGS
    index_granularity = 8192,
    min_bytes_for_wide_part = '10M',
    min_rows_for_wide_part = '100K';
```

### Composite Partitioning

Combines two partitioning strategies — typically hash for the primary dimension and range for the secondary (temporal) dimension. This is the most common strategy for multi-tenant systems.

**Best for**: Multi-tenant data with time-series characteristics

**Used by**: Iceberg table partitioning, Druid data sources

```yaml
# Composite partitioning: hash by tenant, range by time
dataset: "tenant_events"
partitioning:
  strategy: composite
  primary:
    key: ["tenant_id"]
    strategy: hash
    num_partitions: 32
    hash_function: murmur3_128
  secondary:
    key: ["timestamp"]
    strategy: range
    granularity: daily       # Daily partitions within each tenant bucket

# Iceberg table definition
iceberg:
  table: "thunders.tenant_events"
  partitioning:
    - "bucket(32, tenant_id)"
    - "days(timestamp)"
  sort_order:
    - "tenant_id"
    - "timestamp"
```

### Partitioning Strategy Selection Matrix

| Data Characteristic | Hash | Range | Composite |
|---|---|---|---|
| Uniform key distribution | Excellent | Good | Excellent |
| Skewed key distribution | Poor (hot spots) | Good (if range is even) | Good (hash spreads skew) |
| Point lookups by key | Excellent | Poor | Good |
| Range scans by time | Poor (all partitions) | Excellent | Excellent |
| Multi-tenant isolation | Good | Poor | Excellent |
| Ordered processing | Poor | Excellent | Good |
| Join co-location | Excellent (same key) | Poor | Good |

---

## Consistency Models

Distributed systems must make explicit trade-offs between consistency, availability, and partition tolerance (CAP theorem). Thunders-BigData-System provides different consistency guarantees at different layers and for different operations.

### Consistency Model Hierarchy

```
                    Strong Consistency
                    (Linearizability)
                         │
                         │  ← Serializability (transaction isolation)
                         │
                    Causal Consistency
                         │
                         │  ← Session Consistency (read-your-writes)
                         │
                    Eventual Consistency
                         │
                         │  ← Weak Consistency (best-effort)
                         │
                    No Guarantees
```

### Consistency by Layer and Operation

| Operation | Consistency Level | Mechanism | Latency Impact |
|---|---|---|---|
| Kafka write | Strong (per-partition) | Leader + ISR acknowledgment | Low (in-sync replicas) |
| Flink checkpoint | Strong (aligned barriers) | Two-phase commit with checkpoint barriers | Medium (barrier alignment) |
| Iceberg table write | Strong (ACID) | Atomic snapshot commit | Low (metadata-only commit) |
| Druid real-time query | Eventual | Segment handoff delay | None (read from real-time + historical) |
| ClickHouse query | Eventual (async replication) | ReplicatedMergeTree async log | None (read from local replica) |
| Redis cache read | Eventual | TTL-based invalidation | None (stale reads possible) |
| Cross-tier query | Eventual | Federated query with merge | Medium (tier reconciliation) |

### Eventual Consistency in Practice

The most common consistency model in Thunders is eventual consistency, applied between the processing and serving layers. Data written to storage becomes queryable within a bounded delay:

```
Timeline:
────────────────────────────────────────────────────────────────►

Source:     Record produced ─────────────────────────────────────
                  │
Kafka:            └── Written to partition (STRONG) ────────────
                          │
Flink:                    └── Processed, checkpointed (STRONG) ─
                                  │                    │
Druid:                            └── Indexing ───────┘
                                        │ (delay: ~1 min)
ClickHouse:                             └── Inserted ──► (delay: ~5s)
                                                              │
S3/Iceberg:                                                  └── Committed
                                                                   │ (delay: ~1 min)

Client Query:                                            ◄─── Sees data here
```

### Strong Consistency for Critical Operations

For operations requiring strong consistency (e.g., schema registration, configuration updates), Thunders uses the Raft consensus protocol:

```go
// Strong consistency for schema registration via Raft
func (sr *SchemaRegistry) RegisterSchema(ctx context.Context, req *SchemaRequest) error {
    // Propose schema registration through Raft — requires majority acknowledgment
    proposal := SchemaProposal{
        Operation:   "REGISTER",
        Subject:     req.Subject,
        Version:     req.Version,
        Schema:      req.Schema,
        Compatibility: req.Compatibility,
    }

    data, _ := json.Marshal(proposal)
    future := sr.raftNode.Apply(data, 10*time.Second)

    if err := future.Error(); err != nil {
        return fmt.Errorf("raft consensus failed: %w", err)
    }

    // After Raft commit, the schema is durably stored on a majority of nodes
    // and will be readable by any follower after the next heartbeat
    return nil
}
```

---

## Fault Tolerance and Recovery Mechanisms

### Failure Classification

Thunders classifies failures into several categories, each with distinct detection and recovery strategies:

| Failure Type | Example | Detection Time | Recovery Strategy |
|---|---|---|---|
| Transient Network | Packet loss, timeout | Seconds | Automatic retry with backoff |
| Node Crash | VM failure, OOM kill | 10-30s (heartbeat timeout) | Restart on different node (stateless) or recover from checkpoint (stateful) |
| Disk Failure | SSD wearout, corruption | Minutes (I/O errors) | Rebuild from replicas |
| Network Partition | Subnet outage, misconfiguration | Minutes (quorum loss) | Partition mode: minority stops writes |
| Software Bug | Incorrect output, infinite loop | Variable (monitoring) | Rollback deployment, manual investigation |
| Data Center Outage | Power failure, network cut | Seconds (health checks) | Failover to DR region |

### Checkpoint-Based Recovery (Flink)

Flink's checkpoint mechanism provides exactly-once processing guarantees by periodically snapshotting operator state:

```
Checkpoint Process:
─────────────────────────────────────────────────────────────

  Source-1 ──[barrier]──► Operator-A ──[barrier]──► Sink-1
  Source-2 ──[barrier]──► Operator-B ──[barrier]──► Sink-2

  1. Checkpoint coordinator injects barriers into sources
  2. Operators save state when they receive barriers from ALL inputs
  3. Sinks acknowledge checkpoint completion
  4. Coordinator marks checkpoint as completed
  5. State is stored in: s3://thunders-savepoints/checkpoint-<id>/

Recovery Process:
─────────────────────────────────────────────────────────────

  1. Detect failure (task manager heartbeat timeout)
  2. Restart affected task managers
  3. Restore operator state from latest completed checkpoint
  4. Reset Kafka consumer offsets to checkpoint position
  5. Resume processing — records between checkpoint and failure are reprocessed
  6. Idempotent sinks ensure reprocessing produces correct results
```

### Write-Ahead Log Recovery (Kafka)

Kafka provides durability through replicated write-ahead logs:

```yaml
# Kafka fault tolerance configuration
kafka:
  replication:
    factor: 3                           # 3 copies of each partition
    min_insync_replicas: 2              # Writes require 2 acknowledgments
    unclean_leader_election: false      # Never elect an out-of-sync replica as leader

  producer:
    acks: all                           # Wait for all in-sync replicas
    enable_idempotence: true            # Prevent duplicate writes
    max_in_flight_requests: 5           # Limit in-flight requests per connection
    retries: 2147483647                 # Retry indefinitely (idempotent)

  consumer:
    auto_offset_reset: latest
    enable_auto_commit: false           # Let Flink manage offsets via checkpoints
    isolation_level: read_committed     # Only read committed transactions

  log:
    retention_ms: 604800000             # 7-day retention
    segment_bytes: 1073741824           # 1 GB segments
    cleanup_policy: delete
    compression_type: lz4
```

### Idempotent Write Pattern

To ensure exactly-once semantics across the processing-to-storage boundary, all sink operations must be idempotent:

```python
# Idempotent write pattern for ClickHouse sink
def write_to_clickhouse(batch: List[Record], session_id: str):
    """Write a batch of records idempotently using the ReplacingMergeTree engine.

    Each record includes a _version field derived from the Flink checkpoint ID.
    ClickHouse's ReplacingMergeTree keeps only the latest version of each row,
    effectively deduplicating on background merge.
    """
    for record in batch:
        record['_version'] = checkpoint_id    # Monotonically increasing
        record['_session_id'] = session_id     # Identifies the write session

    client.execute(
        "INSERT INTO thunders.event_sessions VALUES",
        [(r['event_id'], r['user_id'], r['timestamp'],
          r['session_id'], r['_version'], r['_session_id'])
         for r in batch]
    )
    # Background compaction will eventually deduplicate rows with same event_id,
    # keeping only the one with the highest _version
```

---

## Leader Election and Consensus

### ZooKeeper-Based Coordination

Several Thunders components rely on ZooKeeper for distributed coordination. ZooKeeper uses the ZAB (ZooKeeper Atomic Broadcast) consensus protocol to maintain a consistent replicated state machine.

**Components using ZooKeeper:**

| Component | ZooKeeper Use |
|---|---|
| Apache Kafka | Controller election, partition leader election, ISR management, cluster metadata |
| Apache Flink | Job manager leader election, checkpoint coordination, dispatcher metadata |
| Apache Druid | Coordinator leader election, overlord leader election, segment allocation |

```yaml
# ZooKeeper ensemble configuration for production
zookeeper:
  replicas: 5                             # Odd number for quorum (3.6+ recommended)
  resources:
    requests:
      cpu: "2"
      memory: "8Gi"
    limits:
      cpu: "4"
      memory: "16Gi"
  persistence:
    size: 50Gi
    storageClass: ssd
  configuration:
    tickTime: 2000
    initLimit: 10
    syncLimit: 5
    maxClientCnxns: 200
    maxSessionTimeout: 40000
    autoPurge:
      snapRetainCount: 5
      purgeInterval: 1
    metricsProvider:
      className: org.apache.zookeeper.metrics.prometheus.PrometheusMetricsProvider
      httpPort: 7000
```

### Raft Consensus Implementation

For application-level consensus needs (schema registry, configuration management, distributed locks), Thunders implements the Raft consensus protocol. Raft provides a simpler, more understandable alternative to Paxos while offering the same safety guarantees.

**Raft Protocol Phases:**

```
1. Leader Election:
   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
   │ Node A  │    │ Node B  │    │ Node C  │    │ Node D  │    │ Node E  │
   │Follower │    │Follower │    │Candidate│    │Follower │    │Follower │
   └─────────┘    └─────────┘    └────┬────┘    └─────────┘    └─────────┘
                                     │
                          ┌──────────┴──────────┐
                          │ RequestVote (term 7) │
                          └──────────┬──────────┘
                                     │
                     ┌───────────────┼───────────────┐
                     │               │               │
              Vote (grant)    Vote (grant)    Vote (grant)
                     │               │               │
                     └───────────────┼───────────────┘
                                     │
                              Majority achieved
                                     │
                              ┌──────▼──────┐
                              │ Node C:     │
                              │ LEADER      │
                              └─────────────┘

2. Log Replication:
   Client ──► Leader ──┬──► Follower A (append)
                       ├──► Follower B (append)
                       └──► Follower C (append)
                              │
                       Majority replicated → Committed
                              │
                       Leader responds to client
                       All followers eventually committed
```

**Raft Implementation (Go):**

```go
// Raft-based distributed configuration manager
package consensus

import (
    "context"
    "encoding/json"
    "time"

    "github.com/hashicorp/raft"
)

type ConfigManager struct {
    raftNode  *raft.Raft
    fsm       *ConfigFSM
    transport *raft.InmemTransport
    peers     []string
}

type ConfigCommand struct {
    Op    string `json:"op"`     // SET, DELETE
    Key   string `json:"key"`
    Value string `json:"value"`
}

type ConfigFSM struct {
    data map[string]string
    mu   sync.RWMutex
}

// Apply applies a Raft log entry to the FSM
func (f *ConfigFSM) Apply(l *raft.Log) interface{} {
    var cmd ConfigCommand
    if err := json.Unmarshal(l.Data, &cmd); err != nil {
        return err
    }

    f.mu.Lock()
    defer f.mu.Unlock()

    switch cmd.Op {
    case "SET":
        f.data[cmd.Key] = cmd.Value
    case "DELETE":
        delete(f.data, cmd.Key)
    }
    return nil
}

// UpdateConfig proposes a configuration change through Raft consensus
func (cm *ConfigManager) UpdateConfig(key, value string) error {
    cmd := ConfigCommand{
        Op:    "SET",
        Key:   key,
        Value: value,
    }
    data, _ := json.Marshal(cmd)

    // Apply with timeout — this blocks until the entry is committed on a majority
    future := cm.raftNode.Apply(data, 10*time.Second)
    if err := future.Error(); err != nil {
        return fmt.Errorf("raft apply failed: %w", err)
    }

    return nil
}

// GetConfig reads the current config (locally consistent after apply)
func (cm *ConfigManager) GetConfig(key string) (string, error) {
    cm.fsm.mu.RLock()
    defer cm.fsm.mu.RUnlock()

    val, ok := cm.fsm.data[key]
    if !ok {
        return "", fmt.Errorf("key not found: %s", key)
    }
    return val, nil
}

// ReadConsistent reads with linearizable consistency (goes through Raft leader)
func (cm *ConfigManager) ReadConsistent(key string) (string, error) {
    // Verify this node is the leader
    if cm.raftNode.State() != raft.Leader {
        return "", fmt.Errorf("not the leader — redirect to %s", cm.raftNode.Leader())
    }

    // Read index protocol: ensure we read from a leader that hasn't been superseded
    readIdx := cm.raftNode.GetConfiguration().Index()
    future := cm.raftNode.VerifyLeader()
    if err := future.Error(); err != nil {
        return "", fmt.Errorf("leader verification failed: %w", err)
    }

    return cm.GetConfig(key)
}
```

### Paxos Comparison

While Thunders uses Raft for its custom consensus needs, the underlying infrastructure (ZooKeeper) uses ZAB, and Kafka uses its own Raft-based KRaft protocol (starting from Kafka 3.3+). Here is a comparison:

| Property | Raft | Multi-Paxos | ZAB |
|---|---|---|---|
| Understandability | High (designed for clarity) | Low (complex to implement correctly) | Medium |
| Leader election | Strong leader (always goes through leader) | Rotated proposer | Similar to Raft |
| Log replication | Leader sends AppendEntries | Proposer sends Accept | Leader sends TRUNC + DIFF |
| Use in Thunders | Custom consensus (config, schema) | — | ZooKeeper ensemble |
| Kafka migration | KRaft (Raft-based, no ZooKeeper) | — | Legacy (ZooKeeper mode) |

---

## Distributed Caching Strategies

### Multi-Level Caching Architecture

Thunders employs a multi-level caching strategy to minimize query latency while maintaining acceptable data freshness:

```
┌───────────────────────────────────────────────────────────────────┐
│                     Caching Architecture                          │
│                                                                    │
│  Level 1: Client-Side Cache (Browser / SDK)                       │
│  ┌───────────────────────────────────────────────────────────┐    │
│  │ TTL: 30s-5min (configurable per dataset)                  │    │
│  │ Storage: LocalStorage / In-Memory                          │    │
│  └───────────────────────────────────────────────────────────┘    │
│                          │ Cache MISS                              │
│  Level 2: API Gateway Cache (Envoy / Go Gateway)                  │
│  ┌───────────────────────────────────────────────────────────┐    │
│  │ TTL: 10s-60s                                              │    │
│  │ Storage: In-process LRU cache                             │    │
│  │ Capacity: 10K entries per gateway replica                 │    │
│  └───────────────────────────────────────────────────────────┘    │
│                          │ Cache MISS                              │
│  Level 3: Distributed Cache (Redis Cluster)                       │
│  ┌───────────────────────────────────────────────────────────┐    │
│  │ TTL: 1min-1hr (based on dataset update frequency)         │    │
│  │ Storage: Redis Cluster (6 nodes, 3 masters + 3 replicas) │    │
│  │ Capacity: 64 GB total                                     │    │
│  │ Eviction: allkeys-lru                                     │    │
│  └───────────────────────────────────────────────────────────┘    │
│                          │ Cache MISS                              │
│  Level 4: Storage Tier (Druid / ClickHouse / Iceberg)             │
│  ┌───────────────────────────────────────────────────────────┐    │
│  │ Native caching within each storage engine                  │    │
│  │ Druid: Query cache, segment cache                         │    │
│  │ ClickHouse: Mark cache, uncompressed cache                │    │
│  └───────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────┘
```

### Cache Invalidation Strategies

Maintaining cache consistency in a distributed system is one of the hardest problems. Thunders uses a combination of strategies:

| Strategy | Implementation | Use Case |
|---|---|---|
| **TTL Expiration** | Set per-key TTL based on dataset freshness SLA | Default strategy for most datasets |
| **Event-Driven Invalidation** | Listen to Kafka events for data changes; invalidate affected cache keys | Real-time dashboards, high-freshness datasets |
| **Version-Based Invalidation** | Include data version in cache key; new version = new key | Iceberg snapshot-based queries |
| **Write-Through** | Update cache synchronously on write | Infrequently written, frequently read data |
| **Cache-Aside** | Application checks cache first; on miss, reads from storage and populates cache | General query workloads |

**Event-Driven Cache Invalidation:**

```python
# Cache invalidation triggered by Kafka events
from thunders.cache import RedisCache, CacheInvalidator

cache = RedisCache(
    cluster_nodes=["redis-0:6379", "redis-1:6379", "redis-2:6379"],
    default_ttl=300,  # 5 minutes
)

invalidator = CacheInvalidator(
    cache=cache,
    consumer_group="cache-invalidator",
    topics=["thunders.processing.batch_completed", "thunders.storage.segment_created"]
)

# Event handler: invalidate cache when new data is available
@invalidator.on_event("thunders.processing.batch_completed")
def handle_batch_completed(event):
    datasets = event.value["output_datasets"]
    for dataset in datasets:
        # Invalidate all cache keys for this dataset
        cache.delete_pattern(f"query:{dataset}:*")
        cache.delete_pattern(f"dashboard:{dataset}:*")
        logger.info(f"Invalidated cache for dataset: {dataset}")
```

### Distributed Cache Configuration

```yaml
# Redis cluster configuration for distributed caching
redis:
  cluster:
    nodes: 6                          # 3 masters + 3 replicas
    replication: true
    master_resources:
      cpu: "4"
      memory: "32Gi"
    replica_resources:
      cpu: "2"
      memory: "16Gi"
    persistence:
      enabled: true
      storageClass: ssd
      size: 50Gi

  cache_config:
    maxmemory_policy: "allkeys-lru"
    maxmemory: "28gb"                 # Leave headroom for Redis overhead
    timeout: 300                      # Client timeout in seconds
    tcp_keepalive: 60
    save: ""                          # Disable RDB snapshots (cache only)
    appendonly: "no"                  # Disable AOF (cache data is reproducible)

  key_naming:
    pattern: "{cache_type}:{dataset}:{hash}"
    examples:
      - "query:user_events:sha256(a1b2c3)"
      - "dashboard:revenue_summary:daily"
      - "feature:user_features:user-12345"
```

---

## Network Topology and Communication

### Physical Network Topology

Thunders is deployed across multiple availability zones (AZs) within a cloud region, with cross-region disaster recovery:

```
                    ┌─────────────────────────────┐
                    │     Internet / Clients        │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │     Load Balancer (ALB)        │
                    │     SSL Termination, WAF       │
                    └──────────────┬───────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          │                        │                        │
   ┌──────▼──────┐         ┌──────▼──────┐         ┌──────▼──────┐
   │  AZ-a       │         │  AZ-b       │         │  AZ-c       │
   │             │         │             │         │             │
   │  Gateway×2  │         │  Gateway×2  │         │  Gateway×2  │
   │  Kafka×2    │◄────────►Kafka×2    ◄────────►  │  Kafka×2    │
   │  Flink×3    │         │  Flink×3    │         │  Flink×3    │
   │  Druid×2    │         │  Druid×2    │         │  Druid×2    │
   │  CH×2       │         │  CH×2       │         │  CH×2       │
   │  Redis×2    │         │  Redis×2    │         │  Redis×2    │
   └─────────────┘         └─────────────┘         └─────────────┘
          │                        │                        │
          └────────────────────────┼────────────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │   Cross-AZ Replication         │
                    │   Kafka ISR, CH ReplicatedMT   │
                    │   Druid segment replication    │
                    └───────────────────────────────┘
```

### Network Communication Patterns

| Pattern | Protocol | Source → Destination | Frequency | Bandwidth |
|---|---|---|---|---|
| Client ingestion | HTTP/2, gRPC | External → Gateway | Continuous | Up to 10 Gbps |
| Event streaming | Kafka (TCP) | Gateway → Kafka → Flink | Continuous | Up to 50 Gbps |
| State checkpoint | S3 API (HTTP) | Flink → S3/MinIO | Every 30s | Burst: 5 Gbps |
| Query execution | HTTP, JDBC | Gateway → Druid/CH | On-demand | Up to 5 Gbps |
| Cross-AZ replication | Kafka ISR (TCP) | Broker ↔ Broker | Continuous | Up to 20 Gbps |
| Cache lookup | Redis protocol | Analytics → Redis | Per-query | Up to 2 Gbps |

### Network Policies and Security

All inter-service communication is secured and segmented using Kubernetes Network Policies and Istio mTLS:

```yaml
# Network policy: Flink can only talk to Kafka on port 9092
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: flink-to-kafka
  namespace: thunders
spec:
  podSelector:
    matchLabels:
      app: flink-taskmanager
  policyTypes:
    - Egress
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: kafka
      ports:
        - port: 9092
          protocol: TCP
    - to:     # Also allow DNS resolution
        - namespaceSelector:
            matchLabels:
              name: kube-system
      ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
```

### Istio mTLS Configuration

```yaml
# Istio PeerAuthentication: enforce mTLS for all thunders services
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: thunders-mtls
  namespace: thunders
spec:
  mtls:
    mode: STRICT    # All inter-service communication must use mTLS
```

### Bandwidth and Latency Considerations

Cross-AZ traffic incurs both cost and latency overhead. Thunders optimizes for this by:

1. **Data locality**: Flink task managers are scheduled on the same AZ as the Kafka partitions they consume
2. **Zone-aware replication**: Kafka's `broker.rack` configuration aligns partitions with AZs
3. **Read-local preference**: ClickHouse and Druid prefer reading from local replicas before crossing AZ boundaries
4. **Cache locality**: Redis cache nodes are distributed across AZs with client-side AZ-aware routing

```yaml
# Kafka zone-aware configuration
kafka:
  broker_rack_awareness: true
  rack_config:
    broker-0: "az-a"
    broker-1: "az-a"
    broker-2: "az-b"
    broker-3: "az-b"
    broker-4: "az-c"
    broker-5: "az-c"

  # Prefer same-AZ replica for consumer reads
  consumer:
    rack_id: "${POD_AZ}"    # Set from Kubernetes downward API
    replica_selector: org.apache.kafka.clients.consumer.RangeAssignor
```

---

## Related Documentation

- [System Architecture](system_architecture.md) — Comprehensive architecture and technology stack
- [Data Flow](data_flow.md) — End-to-end data movement and transformation
- [Microservices Architecture](microservices.md) — Service decomposition and communication
- [Distributed Architecture](distributed_architecture.md) — Cluster deployment and node configuration
- [Scalability Design](scalability_design.md) — Scaling strategies and capacity planning
