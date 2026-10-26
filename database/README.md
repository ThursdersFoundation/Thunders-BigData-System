# Database

Datastore definitions for the Thunders BigData System.

## Datastore Overview

| Store | Purpose | Files | Connection String |
| --- | --- | --- | --- |
| PostgreSQL 16 | Core relational catalog: users, datasets, pipelines, runs, audit logs. | `postgres/schema.sql`, `postgres/indexes.sql`, `postgres/seed.sql` | `postgresql://thunders:thunders@localhost:5432/thunders` |
| MongoDB 7 | Document store: sensor events, report snapshots, pipeline events. | `mongodb/collections.json`, `mongodb/indexes.js` | `mongodb://localhost:27017/thunders` |
| Redis 7 | Cache, rate limiting, distributed locks. | `redis/cache.conf` | `redis://localhost:6379/0` |
| ClickHouse 24 | Time-series analytics over sensor and transaction events. | `clickhouse/schema.sql`, `clickhouse/tables.sql` | `clickhouse://localhost:8123/thunders` |

## Initializing PostgreSQL (via docker compose)

Assumes the compose project exposes a `postgres` service (see the root
`docker-compose.yml`) with database/user `thunders`.

```bash
# 1. Core schema (idempotent)
docker compose exec -T postgres psql -U thunders -d thunders < database/postgres/schema.sql

# 2. Secondary indexes
docker compose exec -T postgres psql -U thunders -d thunders < database/postgres/indexes.sql

# 3. Demo seed data (transactional, safe to re-run)
docker compose exec -T postgres psql -U thunders -d thunders < database/postgres/seed.sql

# 4. Migrations (in numeric order)
docker compose exec -T postgres psql -U thunders -d thunders -1 -f - < database/migrations/001_initial.sql
docker compose exec -T postgres psql -U thunders -d thunders -1 -f - < database/migrations/002_users.sql
docker compose exec -T postgres psql -U thunders -d thunders -1 -f - < database/migrations/003_logs.sql
```

Or from inside the container shell:

```bash
docker compose exec postgres bash
psql -U thunders -d thunders -f /docker-entrypoint-initdb.d/schema.sql   # path depends on volume mounts
```

### Other stores

```bash
# MongoDB indexes (mongosh)
mongosh "mongodb://localhost:27017" --file database/mongodb/indexes.js

# ClickHouse schema
clickhouse-client --host localhost --multiquery < database/clickhouse/schema.sql
clickhouse-client --host localhost --multiquery < database/clickhouse/tables.sql

# Redis with the project cache config
redis-server database/redis/cache.conf
```

## Migration Policy

- Migrations live in `database/migrations/` and are named
  `NNN_description.sql` (zero-padded, strictly increasing).
- Apply them in numeric order, once. Each file is transactional
  (`BEGIN; ... COMMIT;`) and idempotent where feasible.
- **Never edit an applied migration.** Fix forward by adding a new numbered
  file; the baseline DDL intentionally lives in `postgres/schema.sql` for
  scaffold simplicity (see `migrations/001_initial.sql`).
- Track applied migrations in your deployment tooling (Flyway/Liquibase/
  simple `_migrations` table) before enabling CI schema checks. TODO.

## Conventions

- SQL keywords UPPERCASE, identifiers lowercase snake_case, schema
  (`thunders.`) qualified in every statement.
- Deterministic UUIDs in seed data so re-runs stay idempotent via
  `ON CONFLICT DO NOTHING`.
- No credentials are stored in this directory; demo auth uses the
  placeholder hash `$2b$12$REPLACE_ME` documented in `postgres/seed.sql`.
