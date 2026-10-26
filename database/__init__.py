"""Database package for the Thunders BigData System.

Contains datastore definitions and operational scripts:

- postgres: core relational schema, indexes and demo seed data.
- mongodb: collection layout and mongosh index scripts.
- redis: cache configuration with key naming conventions.
- clickhouse: time-series tables for sensor and transaction events.
- migrations: numbered, append-only SQL migrations (PostgreSQL).
"""

__all__: list[str] = []
