# Ingestion API Reference

The Ingestion API provides endpoints for ingesting data into the Thunders-BigData-System platform. It supports both batch and streaming ingestion modes, file uploads, and Kafka-based streaming configurations. All endpoints require authentication and are subject to rate limiting.

**Base URL**: `https://api.thunders.example.com/api/v1`

---

## Table of Contents

1. [Authentication and Authorization](#authentication-and-authorization)
2. [Batch Ingestion](#batch-ingestion)
3. [Stream Ingestion](#stream-ingestion)
4. [Kafka Ingestion Configuration](#kafka-ingestion-configuration)
5. [File Upload API](#file-upload-api)
6. [Rate Limiting](#rate-limiting)
7. [Error Codes and Handling](#error-codes-and-handling)

---

## Authentication and Authorization

All Ingestion API requests require a valid Bearer token in the `Authorization` header. The API supports OAuth2 client credentials and authorization code flows.

### Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `Authorization` | Yes | `Bearer <token>` — OAuth2 access token |
| `Content-Type` | Yes | `application/json` for JSON payloads |
| `X-Request-ID` | Recommended | Client-provided UUID for request tracing |
| `X-Tenant-ID` | Yes (multi-tenant) | Tenant identifier for data isolation |
| `X-Idempotency-Key` | Recommended | Unique key for safe retries (24-hour window) |

### Obtaining a Token

```bash
# OAuth2 client credentials flow
curl -X POST https://auth.thunders.example.com/oauth2/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=thunders-ingestion-client" \
  -d "client_secret=<your-client-secret>" \
  -d "scope=ingest:write ingest:read"
```

Response:

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "scope": "ingest:write ingest:read"
}
```

### Required Permissions

| Endpoint | Required Scope |
|----------|---------------|
| `POST /api/v1/ingest/batch` | `ingest:write` |
| `POST /api/v1/ingest/stream` | `ingest:write` |
| `POST /api/v1/ingest/upload` | `ingest:write` |
| `GET /api/v1/ingest/jobs/{job_id}` | `ingest:read` |
| `POST /api/v1/ingest/kafka/config` | `ingest:admin` |

---

## Batch Ingestion

The batch ingestion endpoint accepts arrays of records for processing. It validates records against the dataset schema, applies deduplication logic, and routes valid records to the processing pipeline.

### POST /api/v1/ingest/batch

Submit a batch of records to a dataset for processing.

**Request Body**:

```json
{
  "dataset": "user_events",
  "schema_id": "user_events_v3",
  "records": [
    {
      "user_id": "u-12345",
      "event_type": "page_view",
      "timestamp": "2024-01-15T10:30:00.000Z",
      "properties": {
        "page": "/products/widget-a",
        "referrer": "google.com",
        "device": "mobile"
      }
    },
    {
      "user_id": "u-67890",
      "event_type": "purchase",
      "timestamp": "2024-01-15T10:31:00.000Z",
      "properties": {
        "product_id": "p-456",
        "amount": 29.99,
        "currency": "USD"
      }
    }
  ],
  "options": {
    "on_error": "continue",
    "deduplication": true,
    "dedup_key": ["user_id", "event_type", "timestamp"],
    "batch_id": "batch-20240115-001"
  }
}
```

**Request Schema**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dataset` | string | Yes | Target dataset identifier |
| `schema_id` | string | No | Schema version for validation (uses latest if omitted) |
| `records` | array | Yes | Array of record objects (max 10,000 per request) |
| `options.on_error` | string | No | Error handling: `continue` (default) or `abort` |
| `options.deduplication` | boolean | No | Enable deduplication (default: `true`) |
| `options.dedup_key` | array | No | Fields to use for deduplication |
| `options.batch_id` | string | No | Client-assigned batch identifier for tracking |

**Response** (200 OK):

```json
{
  "ingestion_id": "ing-a1b2c3d4",
  "dataset": "user_events",
  "accepted_count": 2,
  "rejected_count": 0,
  "rejected_records": [],
  "deduplicated_count": 0,
  "processed_at": "2024-01-15T10:30:01.234Z",
  "batch_id": "batch-20240115-001",
  "links": {
    "status": "/api/v1/ingest/jobs/ing-a1b2c3d4"
  }
}
```

**Partial Acceptance Response** (207 Multi-Status):

```json
{
  "ingestion_id": "ing-e5f6g7h8",
  "dataset": "user_events",
  "accepted_count": 3,
  "rejected_count": 1,
  "rejected_records": [
    {
      "index": 3,
      "record": { "user_id": null, "event_type": "click" },
      "errors": [
        {
          "field": "user_id",
          "code": "NULL_NOT_ALLOWED",
          "message": "Field 'user_id' is required and cannot be null"
        }
      ]
    }
  ],
  "deduplicated_count": 0,
  "processed_at": "2024-01-15T10:30:01.234Z"
}
```

**cURL Example**:

```bash
curl -X POST "https://api.thunders.example.com/api/v1/ingest/batch" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-001" \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d '{
    "dataset": "user_events",
    "schema_id": "user_events_v3",
    "records": [
      {"user_id": "u-001", "event_type": "click", "timestamp": "2024-01-15T10:00:00Z", "properties": {}}
    ]
  }'
```

**Python SDK Example**:

```python
from thunders import ThundersClient

client = ThundersClient(base_url="https://api.thunders.example.com", api_key="tk-abc123")

response = client.ingest_batch(
    dataset="user_events",
    schema_id="user_events_v3",
    records=[
        {"user_id": "u-001", "event_type": "page_view", "timestamp": "2024-01-15T10:00:00Z"},
        {"user_id": "u-002", "event_type": "purchase", "timestamp": "2024-01-15T10:01:00Z"},
    ],
    options={
        "on_error": "continue",
        "deduplication": True,
        "dedup_key": ["user_id", "event_type", "timestamp"]
    }
)

print(f"Accepted: {response.accepted_count}, Rejected: {response.rejected_count}")
print(f"Ingestion ID: {response.ingestion_id}")
```

---

## Stream Ingestion

The stream ingestion endpoint establishes a connection for continuous data flow. It is designed for real-time event streaming with low-latency acknowledgment.

### POST /api/v1/ingest/stream

Submit a stream of records to a dataset. Each request can contain one or more records meant for immediate processing in the streaming pipeline.

**Request Body**:

```json
{
  "dataset": "iot_metrics",
  "schema_id": "iot_metrics_v2",
  "records": [
    {
      "device_id": "sensor-001",
      "metric_name": "temperature",
      "value": 23.7,
      "timestamp": "2024-01-15T10:30:00.000Z",
      "tags": {
        "location": "warehouse-a",
        "floor": "3"
      }
    }
  ],
  "stream_options": {
    "partition_key": "device_id",
    "guarantee": "at_least_once",
    "compression": "none",
    "batch": false
  }
}
```

**Stream-Specific Options**:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `stream_options.partition_key` | string | Auto | Field to use for Kafka partition routing |
| `stream_options.guarantee` | string | `at_least_once` | Delivery guarantee: `at_least_once` or `exactly_once` |
| `stream_options.compression` | string | `none` | Payload compression: `none`, `gzip`, `lz4`, `zstd` |
| `stream_options.batch` | boolean | `false` | Buffer records for micro-batching |

**Response** (200 OK):

```json
{
  "ingestion_id": "ing-s1t2u3v4",
  "dataset": "iot_metrics",
  "accepted_count": 1,
  "rejected_count": 0,
  "partition": 5,
  "offset": 284729,
  "processed_at": "2024-01-15T10:30:00.456Z"
}
```

### Server-Sent Events (SSE) Endpoint

For receiving real-time ingestion status updates:

```
GET /api/v1/ingest/stream/{dataset}/status
```

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `last_event_id` | string | - | Resume from last received event ID |
| `include_records` | boolean | `false` | Include record details in events |

**Event Format**:

```
event: ingested
id: evt-001
data: {"ingestion_id":"ing-s1t2u3v4","dataset":"iot_metrics","count":1,"timestamp":"2024-01-15T10:30:00.456Z"}

event: rejected
id: evt-002
data: {"ingestion_id":"ing-w5x6y7z8","dataset":"iot_metrics","count":1,"reason":"SCHEMA_VALIDATION_FAILED"}
```

---

## Kafka Ingestion Configuration

Thunders-BigData-System supports direct Kafka topic ingestion for high-throughput scenarios. Configure a Kafka source to have records automatically ingested from an existing Kafka topic.

### POST /api/v1/ingest/kafka/config

Create or update a Kafka ingestion configuration.

**Request Body**:

```json
{
  "name": "iot-sensor-ingestion",
  "dataset": "iot_metrics",
  "schema_id": "iot_metrics_v2",
  "source": {
    "bootstrap_servers": "kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092",
    "topic": "iot-sensor-raw",
    "consumer_group_id": "thunders-iot-consumer",
    "auto_offset_reset": "earliest",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "credentials_secret_ref": "secret/kafka-credentials",
    "deserialization_format": "avro",
    "schema_registry_url": "https://schema-registry.thunders.example.com:8081"
  },
  "processing": {
    "batch_size": 500,
    "batch_interval_ms": 5000,
    "max_retries": 3,
    "retry_delay_ms": 1000,
    "dead_letter_topic": "iot-sensor-dlq",
    "on_error": "continue"
  },
  "transform": {
    "enabled": true,
    "type": "jsonpath",
    "mappings": [
      {"source": "$.device_id", "target": "device_id"},
      {"source": "$.reading.value", "target": "value", "cast": "double"},
      {"source": "$.ts", "target": "timestamp", "cast": "timestamp"}
    ]
  }
}
```

**Response** (201 Created):

```json
{
  "name": "iot-sensor-ingestion",
  "dataset": "iot_metrics",
  "status": "ACTIVE",
  "created_at": "2024-01-15T10:30:00.000Z",
  "consumer_group_id": "thunders-iot-consumer",
  "current_lag": 0,
  "messages_processed": 0,
  "links": {
    "self": "/api/v1/ingest/kafka/config/iot-sensor-ingestion",
    "status": "/api/v1/ingest/kafka/config/iot-sensor-ingestion/status",
    "pause": "/api/v1/ingest/kafka/config/iot-sensor-ingestion/pause",
    "resume": "/api/v1/ingest/kafka/config/iot-sensor-ingestion/resume"
  }
}
```

### GET /api/v1/ingest/kafka/config/{name}/status

Retrieve the current status and metrics of a Kafka ingestion configuration.

**Response** (200 OK):

```json
{
  "name": "iot-sensor-ingestion",
  "status": "ACTIVE",
  "consumer_group_id": "thunders-iot-consumer",
  "lag": {
    "current": 1523,
    "max": 100000,
    "partitions": {
      "0": {"current_offset": 98432, "end_offset": 99955, "lag": 1523},
      "1": {"current_offset": 98200, "end_offset": 98200, "lag": 0},
      "2": {"current_offset": 98765, "end_offset": 98765, "lag": 0}
    }
  },
  "throughput": {
    "records_per_second": 1250,
    "bytes_per_second": 524288
  },
  "errors": {
    "total": 12,
    "dead_letter_count": 3,
    "last_error": "2024-01-15T09:45:00Z",
    "last_error_message": "Avro deserialization failed for record at offset 98430"
  },
  "last_processed_offset": 98765,
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### Manage Kafka Ingestion

```bash
# Pause ingestion
POST /api/v1/ingest/kafka/config/{name}/pause

# Resume ingestion
POST /api/v1/ingest/kafka/config/{name}/resume

# Reset offsets (seek to beginning)
POST /api/v1/ingest/kafka/config/{name}/reset
{
  "offset": "earliest"  # or "latest" or specific offset number
}

# Delete ingestion configuration
DELETE /api/v1/ingest/kafka/config/{name}
```

---

## File Upload API

The file upload endpoint supports batch ingestion of data from files in CSV, JSON, Parquet, Avro, and ORC formats.

### POST /api/v1/ingest/upload

Upload a file for batch ingestion into a dataset.

**Request** (multipart/form-data):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | file | Yes | Data file (max 5 GB) |
| `dataset` | string | Yes | Target dataset identifier |
| `format` | string | Yes | File format: `csv`, `json`, `parquet`, `avro`, `orc` |
| `schema_id` | string | No | Schema for validation |
| `delimiter` | string | No | CSV delimiter (default: `,`) |
| `header` | boolean | No | CSV has header row (default: `true`) |
| `compression` | string | No | File compression: `none`, `gzip`, `snappy`, `zstd` |
| `on_error` | string | No | Error handling: `continue` or `abort` |
| `mode` | string | No | Write mode: `append` (default) or `overwrite` |

**cURL Example**:

```bash
curl -X POST "https://api.thunders.example.com/api/v1/ingest/upload" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Tenant-ID: tenant-001" \
  -F "file=@events_2024_01_15.csv" \
  -F "dataset=user_events" \
  -F "format=csv" \
  -F "schema_id=user_events_v3" \
  -F "delimiter=," \
  -F "header=true" \
  -F "compression=gzip"
```

**Response** (202 Accepted):

```json
{
  "job_id": "job-e5f6g7h8",
  "status": "QUEUED",
  "dataset": "user_events",
  "file_name": "events_2024_01_15.csv",
  "file_size_bytes": 52428800,
  "format": "csv",
  "estimated_duration_seconds": 300,
  "created_at": "2024-01-15T10:30:00.000Z",
  "links": {
    "status": "/api/v1/ingest/jobs/job-e5f6g7h8",
    "cancel": "/api/v1/ingest/jobs/job-e5f6g7h8/cancel"
  }
}
```

### Large File Upload (Multipart)

For files larger than 100 MB, use the multipart upload API:

```
# Step 1: Initiate upload
POST /api/v1/ingest/upload/initiate
{
  "dataset": "user_events",
  "file_name": "large_events.parquet",
  "file_size_bytes": 5368709120,
  "format": "parquet"
}

# Response:
{
  "upload_id": "upl-i9j0k1l2",
  "chunk_size": 104857600,
  "total_chunks": 51,
  "presigned_urls": [
    {"chunk": 1, "url": "https://s3.amazonaws.com/thunders-upload/..."},
    {"chunk": 2, "url": "https://s3.amazonaws.com/thunders-upload/..."}
  ]
}

# Step 2: Upload chunks
PUT <presigned_url_for_chunk_1>
Content-Type: application/octet-stream
<chunk_data>

# Step 3: Complete upload
POST /api/v1/ingest/upload/complete
{
  "upload_id": "upl-i9j0k1l2",
  "dataset": "user_events",
  "format": "parquet"
}
```

### Check Upload Job Status

```
GET /api/v1/ingest/jobs/{job_id}
```

**Response**:

```json
{
  "job_id": "job-e5f6g7h8",
  "status": "RUNNING",
  "progress": {
    "records_processed": 450000,
    "records_total": 1000000,
    "percentage": 45,
    "bytes_processed": 236283371,
    "bytes_total": 52428800,
    "processing_speed_records_per_second": 15000
  },
  "errors": {
    "count": 5,
    "sample": [
      {"line": 1234, "message": "Invalid timestamp format"}
    ]
  },
  "started_at": "2024-01-15T10:30:10Z",
  "estimated_completion_at": "2024-01-15T10:34:10Z",
  "links": {
    "cancel": "/api/v1/ingest/jobs/job-e5f6g7h8/cancel"
  }
}
```

---

## Rate Limiting

The Ingestion API enforces rate limits to ensure fair resource usage and platform stability.

### Rate Limit Tiers

| Tier | Requests/Minute | Records/Minute | Burst Limit |
|------|----------------|----------------|-------------|
| Standard | 600 | 1,000,000 | 100 requests |
| Premium | 3,000 | 10,000,000 | 500 requests |
| Enterprise | 12,000 | 100,000,000 | 2,000 requests |

### Rate Limit Headers

Every API response includes rate limit information in the HTTP headers:

```
X-RateLimit-Limit: 600
X-RateLimit-Remaining: 587
X-RateLimit-Reset: 1705312800
X-RateLimit-Resource: ingestion
Retry-After: 13
```

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Maximum requests allowed per minute |
| `X-RateLimit-Remaining` | Requests remaining in the current window |
| `X-RateLimit-Reset` | Unix timestamp when the rate limit window resets |
| `X-RateLimit-Resource` | The rate-limited resource type |
| `Retry-After` | Seconds until the client should retry (only present on 429 responses) |

### Rate Limit Exceeded Response

```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded for resource 'ingestion'. Limit: 600 requests/minute. Retry after 13 seconds.",
    "request_id": "req-m3n4o5p6",
    "timestamp": "2024-01-15T10:30:00.000Z"
  }
}
```

### Best Practices for Rate Limiting

1. **Exponential backoff**: When receiving 429 responses, implement exponential backoff starting at 1 second, doubling up to 60 seconds.
2. **Batch records**: Use the batch endpoint with the maximum 10,000 records per request instead of single-record calls.
3. **Distribute load**: Spread ingestion across multiple API keys if approaching limits.
4. **Monitor headers**: Track `X-RateLimit-Remaining` to proactively throttle before hitting limits.

---

## Error Codes and Handling

The Ingestion API uses standard HTTP status codes and a consistent error response format.

### Error Response Format

```json
{
  "error": {
    "code": "SCHEMA_VALIDATION_FAILED",
    "message": "Schema validation failed for dataset 'user_events'",
    "details": [
      {
        "field": "records[0].timestamp",
        "issue": "Invalid ISO 8601 timestamp",
        "value": "not-a-date"
      }
    ],
    "request_id": "req-7f3a2b1c",
    "timestamp": "2024-01-15T10:30:00.000Z"
  }
}
```

### Ingestion-Specific Error Codes

| Error Code | HTTP Status | Description | Resolution |
|------------|-------------|-------------|------------|
| `DATASET_NOT_FOUND` | 404 | The specified dataset does not exist | Create the dataset first via `POST /api/v1/datasets` |
| `SCHEMA_NOT_FOUND` | 404 | The specified schema_id does not exist | Register the schema first or omit schema_id to use the latest |
| `SCHEMA_VALIDATION_FAILED` | 422 | One or more records failed schema validation | Fix the invalid records and resubmit |
| `SCHEMA_INCOMPATIBLE` | 409 | Schema is incompatible with the dataset's current schema | Use a backward-compatible schema or create a new version |
| `RECORD_TOO_LARGE` | 413 | A single record exceeds the 1 MB size limit | Split large records or reduce field sizes |
| `BATCH_TOO_LARGE` | 413 | The batch exceeds 10,000 records or 100 MB total | Split into smaller batches |
| `DUPLICATE_BATCH_ID` | 409 | A batch with the same batch_id was already processed | Use a unique batch_id or omit it |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests in the current time window | Implement backoff and retry after the indicated time |
| `INGESTION_PAUSED` | 503 | Ingestion is temporarily paused for the dataset | Check dataset status and retry later |
| `STORAGE_QUOTA_EXCEEDED` | 507 | Dataset storage quota has been reached | Increase the dataset quota or archive old data |
| `KAFKA_UNAVAILABLE` | 503 | Cannot connect to the Kafka cluster | Check Kafka cluster health and retry |
| `DESERIALIZATION_ERROR` | 422 | Failed to deserialize the provided data | Verify the data format matches the specified format |
| `UNSUPPORTED_FORMAT` | 400 | The specified file format is not supported | Use one of: csv, json, parquet, avro, orc |
| `AUTHENTICATION_FAILED` | 401 | Invalid or expired authentication token | Obtain a new token and retry |
| `AUTHORIZATION_DENIED` | 403 | Insufficient permissions for the requested operation | Contact your administrator for the required scope |
| `INTERNAL_ERROR` | 500 | An unexpected server error occurred | Retry with exponential backoff; contact support if persistent |

### Retry Strategy

For transient errors (429, 503, 500), implement the following retry strategy:

```python
import time
import requests

def ingest_with_retry(url, data, max_retries=5, token=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    for attempt in range(max_retries):
        response = requests.post(url, json=data, headers=headers)

        if response.status_code == 200:
            return response.json()

        if response.status_code in (429, 503, 500):
            retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
            time.sleep(min(retry_after, 60))
            continue

        # Non-retryable error
        response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded")
```

---

## See Also

- [Analytics API Reference](analytics_api.md) - Query and analyze ingested data
- [ML API Reference](ml_api.md) - Machine learning pipeline API
- [REST API Documentation](../api/rest_api.md) - General REST API overview
- [Data Ingestion Tutorial](../tutorials/data_ingestion.md) - Step-by-step ingestion guide
