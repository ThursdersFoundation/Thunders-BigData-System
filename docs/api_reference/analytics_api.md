# Analytics API Reference

The Analytics API provides endpoints for querying, aggregating, and exporting data stored in the Thunders-BigData-System platform. It supports SQL queries, time-series analysis, aggregation pipelines, and real-time streaming analytics via WebSocket connections.

**Base URL**: `https://api.thunders.example.com/api/v1`

---

## Table of Contents

1. [Query Endpoints](#query-endpoints)
2. [Time-Series Data API](#time-series-data-api)
3. [Aggregation Endpoints](#aggregation-endpoints)
4. [SQL Query API](#sql-query-api)
5. [Export API](#export-api)
6. [WebSocket Real-Time Analytics](#websocket-real-time-analytics)
7. [Examples with curl and Python](#examples-with-curl-and-python)

---

## Query Endpoints

### POST /api/v1/analytics/query

Execute an analytics query against one or more datasets. The query language supports SQL-92 compatible syntax with extensions for time-series and geospatial operations.

**Request Body**:

```json
{
  "sql": "SELECT event_type, COUNT(*) AS event_count, AVG(amount) AS avg_amount FROM user_events WHERE timestamp >= '2024-01-01' AND timestamp < '2024-02-01' GROUP BY event_type ORDER BY event_count DESC LIMIT 10",
  "options": {
    "timeout_ms": 30000,
    "max_rows": 10000,
    "format": "json",
    "include_metadata": true,
    "cache_ttl_seconds": 300,
    "dry_run": false
  }
}
```

**Query Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timeout_ms` | integer | `30000` | Query timeout in milliseconds (max: 300000) |
| `max_rows` | integer | `10000` | Maximum rows to return (max: 100000) |
| `format` | string | `json` | Response format: `json`, `arrow`, `csv` |
| `include_metadata` | boolean | `false` | Include execution metadata in response |
| `cache_ttl_seconds` | integer | `0` | Cache results for N seconds (0 = no cache) |
| `dry_run` | boolean | `false` | Validate query without executing |

**Response** (200 OK):

```json
{
  "results": [
    {"event_type": "page_view", "event_count": 1245678, "avg_amount": null},
    {"event_type": "click", "event_count": 456789, "avg_amount": null},
    {"event_type": "purchase", "event_count": 12345, "avg_amount": 45.67},
    {"event_type": "return", "event_count": 987, "avg_amount": 32.15}
  ],
  "metadata": {
    "query_id": "q-i9j0k1l2",
    "rows_returned": 4,
    "rows_scanned": 54321000,
    "bytes_scanned": 2147483648,
    "execution_time_ms": 342,
    "cache_hit": false,
    "datasources": ["thunders.user_events"],
    "partition_pruned": 45,
    "partition_total": 365
  }
}
```

### GET /api/v1/analytics/query/{query_id}

Retrieve results of a previously executed query.

**Response** (200 OK):

Same as the POST response above, with an additional `expires_at` field indicating when the cached results will be purged.

### POST /api/v1/analytics/query/async

Submit a long-running query for asynchronous execution. Use this endpoint for queries expected to run longer than 30 seconds.

**Request Body**: Same as the synchronous query endpoint.

**Response** (202 Accepted):

```json
{
  "query_id": "q-m3n4o5p6",
  "status": "QUEUED",
  "submitted_at": "2024-01-15T10:30:00.000Z",
  "estimated_duration_seconds": 120,
  "links": {
    "status": "/api/v1/analytics/query/async/q-m3n4o5p6",
    "cancel": "/api/v1/analytics/query/async/q-m3n4o5p6/cancel"
  }
}
```

**Poll for Status**:

```
GET /api/v1/analytics/query/async/{query_id}
```

```json
{
  "query_id": "q-m3n4o5p6",
  "status": "COMPLETED",
  "submitted_at": "2024-01-15T10:30:00.000Z",
  "completed_at": "2024-01-15T10:32:30.000Z",
  "execution_time_ms": 150000,
  "rows_produced": 1000000,
  "bytes_scanned": 53687091200,
  "links": {
    "results": "/api/v1/analytics/query/async/q-m3n4o5p6/results",
    "export": "/api/v1/analytics/export?query_id=q-m3n4o5p6"
  }
}
```

---

## Time-Series Data API

The time-series API provides specialized endpoints for querying temporal data with automatic time bucketing, interpolation, and downsampling.

### POST /api/v1/analytics/timeseries

Query time-series data with automatic aggregation by time intervals.

**Request Body**:

```json
{
  "dataset": "iot_metrics",
  "measure": "value",
  "dimensions": ["device_id"],
  "filters": {
    "device_id": ["sensor-001", "sensor-002"],
    "metric_name": "temperature"
  },
  "time_range": {
    "start": "2024-01-15T00:00:00Z",
    "end": "2024-01-15T23:59:59Z"
  },
  "granularity": "1h",
  "aggregations": ["avg", "min", "max", "count"],
  "fill": "null",
  "timezone": "UTC"
}
```

**Request Schema**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dataset` | string | Yes | Source dataset |
| `measure` | string | Yes | Numeric field to aggregate |
| `dimensions` | array | No | Group-by dimensions |
| `filters` | object | No | Dimension filters (key: field, value: array of allowed values) |
| `time_range.start` | string | Yes | Start timestamp (ISO 8601) |
| `time_range.end` | string | Yes | End timestamp (ISO 8601) |
| `granularity` | string | Yes | Time bucket: `1m`, `5m`, `15m`, `1h`, `6h`, `1d`, `1w`, `1M` |
| `aggregations` | array | No | Aggregation functions: `avg`, `sum`, `min`, `max`, `count`, `stddev`, `p50`, `p95`, `p99` |
| `fill` | string | No | Missing value fill strategy: `null` (default), `zero`, `previous`, `linear` |
| `timezone` | string | No | Timezone for bucket alignment (default: `UTC`) |

**Response** (200 OK):

```json
{
  "dataset": "iot_metrics",
  "granularity": "1h",
  "time_range": {
    "start": "2024-01-15T00:00:00Z",
    "end": "2024-01-15T23:59:59Z"
  },
  "series": [
    {
      "dimensions": {"device_id": "sensor-001"},
      "data_points": [
        {"time": "2024-01-15T00:00:00Z", "avg": 22.1, "min": 21.5, "max": 22.8, "count": 60},
        {"time": "2024-01-15T01:00:00Z", "avg": 22.3, "min": 21.8, "max": 23.1, "count": 60},
        {"time": "2024-01-15T02:00:00Z", "avg": null, "min": null, "max": null, "count": 0},
        {"time": "2024-01-15T03:00:00Z", "avg": 21.9, "min": 21.4, "max": 22.5, "count": 60}
      ]
    },
    {
      "dimensions": {"device_id": "sensor-002"},
      "data_points": [
        {"time": "2024-01-15T00:00:00Z", "avg": 24.5, "min": 24.0, "max": 25.2, "count": 60},
        {"time": "2024-01-15T01:00:00Z", "avg": 24.7, "min": 24.2, "max": 25.5, "count": 60}
      ]
    }
  ],
  "metadata": {
    "query_id": "ts-q1r2s3t4",
    "execution_time_ms": 85,
    "data_points_returned": 6,
    "data_points_scanned": 86400
  }
}
```

### POST /api/v1/analytics/timeseries/compare

Compare time-series data across different time periods for trend analysis.

**Request Body**:

```json
{
  "dataset": "sales",
  "measure": "revenue",
  "dimensions": ["region"],
  "time_ranges": [
    {"label": "current_period", "start": "2024-01-01T00:00:00Z", "end": "2024-01-31T23:59:59Z"},
    {"label": "previous_period", "start": "2023-12-01T00:00:00Z", "end": "2023-12-31T23:59:59Z"},
    {"label": "same_period_last_year", "start": "2023-01-01T00:00:00Z", "end": "2023-01-31T23:59:59Z"}
  ],
  "granularity": "1d",
  "aggregations": ["sum"]
}
```

**Response** (200 OK):

```json
{
  "comparison": [
    {
      "dimensions": {"region": "NA"},
      "periods": {
        "current_period": [
          {"time": "2024-01-01", "sum": 150000},
          {"time": "2024-01-02", "sum": 155000}
        ],
        "previous_period": [
          {"time": "2023-12-01", "sum": 140000},
          {"time": "2023-12-02", "sum": 142000}
        ],
        "same_period_last_year": [
          {"time": "2023-01-01", "sum": 120000},
          {"time": "2023-01-02", "sum": 125000}
        ]
      },
      "summary": {
        "current_period_total": 4500000,
        "previous_period_total": 4200000,
        "same_period_last_year_total": 3600000,
        "period_over_period_change": 0.0714,
        "year_over_year_change": 0.25
      }
    }
  ]
}
```

---

## Aggregation Endpoints

### POST /api/v1/analytics/aggregate

Execute a structured aggregation query without writing SQL. This endpoint provides a declarative interface for common aggregation patterns.

**Request Body**:

```json
{
  "dataset": "user_events",
  "dimensions": ["event_type", "device_type"],
  "measures": [
    {"field": "amount", "function": "sum", "alias": "total_revenue"},
    {"field": "amount", "function": "avg", "alias": "avg_order_value"},
    {"field": "user_id", "function": "count_distinct", "alias": "unique_users"}
  ],
  "filters": [
    {"field": "timestamp", "operator": ">=", "value": "2024-01-01T00:00:00Z"},
    {"field": "timestamp", "operator": "<", "value": "2024-02-01T00:00:00Z"},
    {"field": "event_type", "operator": "in", "value": ["purchase", "refund"]}
  ],
  "sort": [{"field": "total_revenue", "order": "desc"}],
  "limit": 50,
  "having": [
    {"field": "total_revenue", "operator": ">", "value": 10000}
  ]
}
```

**Supported Filter Operators**:

| Operator | Value Type | Description |
|----------|-----------|-------------|
| `=` | scalar | Equal to |
| `!=` | scalar | Not equal to |
| `>` | scalar | Greater than |
| `>=` | scalar | Greater than or equal to |
| `<` | scalar | Less than |
| `<=` | scalar | Less than or equal to |
| `in` | array | Value in set |
| `not_in` | array | Value not in set |
| `like` | string | Pattern match (SQL LIKE) |
| `is_null` | boolean | Null check |
| `between` | array[2] | Range (inclusive) |

**Supported Aggregate Functions**:

| Function | Description |
|----------|-------------|
| `sum` | Sum of values |
| `avg` | Average of values |
| `min` | Minimum value |
| `max` | Maximum value |
| `count` | Count of non-null values |
| `count_distinct` | Count of distinct values |
| `stddev` | Standard deviation |
| `variance` | Variance |
| `median` | Median value |
| `percentile` | Arbitrary percentile (requires `percentile_value` parameter) |

**Response** (200 OK):

```json
{
  "results": [
    {
      "event_type": "purchase",
      "device_type": "mobile",
      "total_revenue": 234567.89,
      "avg_order_value": 45.67,
      "unique_users": 8432
    },
    {
      "event_type": "purchase",
      "device_type": "desktop",
      "total_revenue": 189432.12,
      "avg_order_value": 62.34,
      "unique_users": 5123
    }
  ],
  "metadata": {
    "query_id": "agg-u5v6w7x8",
    "rows_returned": 2,
    "rows_scanned": 54321000,
    "execution_time_ms": 215
  }
}
```

### POST /api/v1/analytics/pivot

Generate pivot table results for cross-tabulation analysis.

**Request Body**:

```json
{
  "dataset": "sales",
  "row_dimensions": ["region"],
  "column_dimensions": ["category"],
  "measure": {"field": "revenue", "function": "sum"},
  "filters": [
    {"field": "year", "operator": "=", "value": 2024}
  ]
}
```

**Response** (200 OK):

```json
{
  "pivot": {
    "rows": ["NA", "EU", "APAC"],
    "columns": ["Electronics", "Clothing", "Home"],
    "values": [
      [1500000, 800000, 600000],
      [1200000, 650000, 450000],
      [900000, 400000, 350000]
    ],
    "row_totals": [2900000, 2300000, 1650000],
    "column_totals": [3600000, 1850000, 1400000],
    "grand_total": 6850000
  }
}
```

---

## SQL Query API

### POST /api/v1/analytics/sql

Execute raw SQL queries against the Thunders query engine. This endpoint provides full SQL access for advanced users and BI tool integration.

**Request Body**:

```json
{
  "sql": "SELECT region, category, SUM(revenue) AS total_revenue FROM sales WHERE year = 2024 GROUP BY ROLLUP(region, category) ORDER BY total_revenue DESC",
  "parameters": [],
  "options": {
    "timeout_ms": 60000,
    "max_rows": 50000,
    "format": "json",
    "include_plan": false,
    "include_metadata": true
  }
}
```

### Parameterized Queries

Use parameterized queries to prevent SQL injection:

```json
{
  "sql": "SELECT * FROM user_events WHERE user_id = ? AND timestamp BETWEEN ? AND ? LIMIT ?",
  "parameters": [
    {"type": "STRING", "value": "u-12345"},
    {"type": "TIMESTAMP", "value": "2024-01-01T00:00:00Z"},
    {"type": "TIMESTAMP", "value": "2024-02-01T00:00:00Z"},
    {"type": "INTEGER", "value": 100}
  ],
  "options": {
    "timeout_ms": 10000,
    "format": "json"
  }
}
```

### Supported SQL Extensions

The Thunders query engine extends standard SQL with the following functions:

| Function | Description | Example |
|----------|-------------|---------|
| `TIME_BUCKET(granularity, timestamp)` | Truncate timestamp to bucket boundary | `TIME_BUCKET('1h', timestamp)` |
| `APPROX_COUNT_DISTINCT(field)` | HyperLogLog approximate distinct count | `APPROX_COUNT_DISTINCT(user_id)` |
| `APPROX_PERCENTILE(field, percentile)` | Approximate percentile calculation | `APPROX_PERCENTILE(amount, 0.95)` |
| `EVENT_COUNT(window, condition)` | Count events matching condition in window | `EVENT_COUNT('5m', event_type='error')` |
| `FIRST_VALUE(field) OVER (...)` | First value in a window | `FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts)` |
| `LAST_VALUE(field) OVER (...)` | Last value in a window | `LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts)` |
| `GEO_DISTANCE(lat1, lon1, lat2, lon2)` | Haversine distance in km | `GEO_DISTANCE(lat, lon, 40.7128, -74.0060)` |
| `JSON_EXTRACT(json_field, path)` | Extract value from JSON string | `JSON_EXTRACT(properties, '$.page')` |
| `ARRAY_AGG(field)` | Aggregate values into an array | `ARRAY_AGG(DISTINCT event_type)` |

### Query Plan Analysis

Set `include_plan: true` to get the query execution plan without running the query:

```json
{
  "sql": "SELECT * FROM user_events WHERE user_id = 'u-12345'",
  "options": {
    "dry_run": true,
    "include_plan": true
  }
}
```

**Response**:

```json
{
  "plan": {
    "logical_plan": "Scan(user_events, [user_id=u-12345]) -> Project[*]",
    "physical_plan": "IcebergScan(user_events, snapshot=1234567890, partitions=[p5,p6]) -> Project[*] -> SortMergeJoin -> HashAggregate",
    "estimated_cost": {
      "cpu": 0.5,
      "memory": "128MB",
      "bytes_scanned": 536870912
    },
    "optimizations": [
      "partition_pruning: pruned 359 of 365 partitions",
      "predicate_pushdown: moved filter to scan",
      "column_pruning: selected 5 of 24 columns"
    ]
  }
}
```

---

## Export API

The Export API enables bulk data extraction in various formats for integration with external tools, data warehouses, and ML pipelines.

### POST /api/v1/analytics/export

Export query results or dataset contents to a file.

**Request Body**:

```json
{
  "source": {
    "type": "query",
    "sql": "SELECT * FROM user_events WHERE timestamp >= '2024-01-01' AND timestamp < '2024-02-01'"
  },
  "format": "parquet",
  "options": {
    "compression": "snappy",
    "partition_by": ["event_type"],
    "max_file_size_bytes": 1073741824,
    "null_value": "",
    "include_header": true
  },
  "destination": {
    "type": "presigned_url"
  }
}
```

**Export Formats**:

| Format | Compression | Description |
|--------|-------------|-------------|
| `csv` | `gzip`, `none` | Comma-separated values with optional header |
| `json` | `gzip`, `none` | Newline-delimited JSON (NDJSON) |
| `parquet` | `snappy`, `gzip`, `zstd`, `none` | Apache Parquet columnar format |
| `avro` | `snappy`, `deflate`, `none` | Apache Avro binary format |
| `orc` | `snappy`, `zlib`, `none` | Apache ORC columnar format |

**Export Source Types**:

| Source Type | Description |
|-------------|-------------|
| `query` | Export results of a SQL query |
| `dataset` | Export entire dataset contents |
| `query_id` | Export results of a previously executed query |

**Destination Types**:

| Destination | Description |
|-------------|-------------|
| `presigned_url` | Generate a presigned URL for download (default) |
| `s3` | Write directly to an S3 bucket |
| `gcs` | Write directly to Google Cloud Storage |
| `azure_blob` | Write directly to Azure Blob Storage |

**Response** (202 Accepted):

```json
{
  "export_id": "exp-y1z2a3b4",
  "status": "PROCESSING",
  "format": "parquet",
  "estimated_size_bytes": 5368709120,
  "estimated_duration_seconds": 180,
  "created_at": "2024-01-15T10:30:00.000Z",
  "links": {
    "status": "/api/v1/analytics/export/exp-y1z2a3b4",
    "cancel": "/api/v1/analytics/export/exp-y1z2a3b4/cancel"
  }
}
```

### Check Export Status

```
GET /api/v1/analytics/export/{export_id}
```

**Response** (when completed):

```json
{
  "export_id": "exp-y1z2a3b4",
  "status": "COMPLETED",
  "format": "parquet",
  "files": [
    {
      "name": "user_events_part1.parquet",
      "size_bytes": 1073741824,
      "rows": 5000000,
      "download_url": "https://s3.amazonaws.com/thunders-exports/exp-y1z2a3b4/user_events_part1.parquet?X-Amz-Expires=3600&...",
      "expires_at": "2024-01-15T11:30:00Z"
    },
    {
      "name": "user_events_part2.parquet",
      "size_bytes": 536870912,
      "rows": 2500000,
      "download_url": "https://s3.amazonaws.com/thunders-exports/exp-y1z2a3b4/user_events_part2.parquet?X-Amz-Expires=3600&...",
      "expires_at": "2024-01-15T11:30:00Z"
    }
  ],
  "total_rows": 7500000,
  "total_size_bytes": 1610612736,
  "completed_at": "2024-01-15T10:33:00.000Z",
  "execution_time_ms": 180000
}
```

### Export to Cloud Storage

```json
{
  "source": {"type": "dataset", "dataset": "user_events"},
  "format": "parquet",
  "options": {"compression": "zstd", "partition_by": ["event_type"]},
  "destination": {
    "type": "s3",
    "bucket": "my-data-warehouse",
    "prefix": "imports/user_events/",
    "region": "us-east-1",
    "credentials_secret_ref": "secret/s3-credentials"
  }
}
```

---

## WebSocket Real-Time Analytics

The WebSocket API provides real-time streaming of analytics results as new data arrives. This is ideal for dashboards, monitoring, and alerting applications.

### Connection

```
ws://api.thunders.example.com/api/v1/analytics/stream?token=<bearer_token>
```

### Subscribe to a Real-Time Query

After establishing a WebSocket connection, send a subscription message:

```json
{
  "action": "subscribe",
  "subscription_id": "sub-001",
  "query": {
    "dataset": "iot_metrics",
    "dimensions": ["device_id"],
    "measures": [{"field": "value", "function": "avg"}],
    "granularity": "1m",
    "window": "5m"
  }
}
```

### Receive Real-Time Updates

```json
{
  "subscription_id": "sub-001",
  "type": "data",
  "data": {
    "device_id": "sensor-001",
    "time": "2024-01-15T10:30:00Z",
    "avg_value": 23.4,
    "record_count": 60
  },
  "timestamp": "2024-01-15T10:30:00.500Z"
}
```

### Subscribe to Alerts

```json
{
  "action": "subscribe",
  "subscription_id": "sub-alert-001",
  "alert": {
    "dataset": "iot_metrics",
    "condition": "value > 100",
    "throttle_seconds": 60,
    "dimensions": ["device_id"]
  }
}
```

**Alert Event**:

```json
{
  "subscription_id": "sub-alert-001",
  "type": "alert",
  "data": {
    "device_id": "sensor-042",
    "metric_name": "temperature",
    "value": 105.3,
    "threshold": 100,
    "condition": "value > 100",
    "triggered_at": "2024-01-15T10:30:00Z"
  }
}
```

### Unsubscribe

```json
{
  "action": "unsubscribe",
  "subscription_id": "sub-001"
}
```

### WebSocket Error Handling

```json
{
  "subscription_id": "sub-001",
  "type": "error",
  "error": {
    "code": "QUERY_TIMEOUT",
    "message": "Real-time query exceeded maximum execution time of 60 seconds"
  }
}
```

---

## Examples with curl and Python

### cURL Examples

**Basic query**:

```bash
curl -X POST "https://api.thunders.example.com/api/v1/analytics/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) AS total_events FROM user_events WHERE timestamp >= '\''2024-01-01'\''",
    "options": {"timeout_ms": 30000}
  }'
```

**Time-series query**:

```bash
curl -X POST "https://api.thunders.example.com/api/v1/analytics/timeseries" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "iot_metrics",
    "measure": "value",
    "dimensions": ["device_id"],
    "filters": {"metric_name": ["temperature"]},
    "time_range": {"start": "2024-01-15T00:00:00Z", "end": "2024-01-15T23:59:59Z"},
    "granularity": "1h",
    "aggregations": ["avg", "min", "max"]
  }'
```

**Export to Parquet**:

```bash
curl -X POST "https://api.thunders.example.com/api/v1/analytics/export" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source": {"type": "query", "sql": "SELECT * FROM user_events WHERE timestamp >= '\''2024-01-01'\''"},
    "format": "parquet",
    "options": {"compression": "snappy"},
    "destination": {"type": "presigned_url"}
  }'
```

### Python SDK Examples

**Execute a query and iterate results**:

```python
from thunders import ThundersClient

client = ThundersClient(base_url="https://api.thunders.example.com", api_key="tk-abc123")

# Execute a SQL query
results = client.analytics.query(
    sql="SELECT event_type, COUNT(*) AS cnt FROM user_events GROUP BY event_type",
    timeout_ms=30000,
    include_metadata=True
)

for row in results:
    print(f"{row['event_type']}: {row['cnt']}")

print(f"Query took {results.metadata.execution_time_ms}ms")
print(f"Scanned {results.metadata.bytes_scanned} bytes")
```

**Time-series analysis**:

```python
from datetime import datetime, timedelta

# Query time-series data
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=7)

ts_result = client.analytics.timeseries(
    dataset="iot_metrics",
    measure="value",
    dimensions=["device_id"],
    filters={"metric_name": ["temperature"]},
    time_range={"start": start_time.isoformat(), "end": end_time.isoformat()},
    granularity="1h",
    aggregations=["avg", "min", "max", "p95"],
    fill="linear"
)

for series in ts_result.series:
    device_id = series.dimensions["device_id"]
    for point in series.data_points:
        print(f"{device_id} @ {point.time}: avg={point.avg}, p95={point.p95}")
```

**Async long-running query**:

```python
import time

# Submit async query
job = client.analytics.query_async(
    sql="SELECT * FROM large_table WHERE complex_condition = true",
    timeout_ms=300000
)

print(f"Query submitted: {job.query_id}")

# Poll for completion
while True:
    status = client.analytics.get_query_status(job.query_id)
    if status.status == "COMPLETED":
        results = client.analytics.get_query_results(job.query_id)
        print(f"Got {len(results)} rows")
        break
    elif status.status == "FAILED":
        print(f"Query failed: {status.error_message}")
        break
    else:
        print(f"Status: {status.status}, waiting...")
        time.sleep(5)
```

**Export to file**:

```python
# Export dataset to Parquet
export = client.analytics.export(
    source={"type": "dataset", "dataset": "user_events"},
    format="parquet",
    options={"compression": "zstd", "partition_by": ["event_type"]},
    destination={"type": "presigned_url"}
)

# Wait for completion and download
while export.status != "COMPLETED":
    export = client.analytics.get_export_status(export.export_id)
    time.sleep(2)

for file in export.files:
    client.analytics.download_file(file.download_url, f"./exports/{file.name}")
    print(f"Downloaded {file.name} ({file.size_bytes} bytes, {file.rows} rows)")
```

**WebSocket real-time stream**:

```python
import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    if data["type"] == "data":
        print(f"Real-time update: {data['data']}")
    elif data["type"] == "alert":
        print(f"ALERT: {data['data']}")

def on_open(ws):
    # Subscribe to real-time metrics
    ws.send(json.dumps({
        "action": "subscribe",
        "subscription_id": "sub-001",
        "query": {
            "dataset": "iot_metrics",
            "dimensions": ["device_id"],
            "measures": [{"field": "value", "function": "avg"}],
            "granularity": "1m",
            "window": "5m"
        }
    }))

ws = websocket.WebSocketApp(
    "wss://api.thunders.example.com/api/v1/analytics/stream?token=<token>",
    on_open=on_open,
    on_message=on_message
)
ws.run_forever()
```

---

## See Also

- [Ingestion API Reference](ingestion_api.md) - Data ingestion endpoints
- [ML API Reference](ml_api.md) - Machine learning pipeline API
- [REST API Documentation](../api/rest_api.md) - General REST API overview
- [Analytics Tutorial](../tutorials/analytics.md) - Step-by-step analytics guide
