# Thunders API

FastAPI service exposing REST endpoints for the Thunders BigData System.

## Run locally

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

The OpenAPI spec is available at http://localhost:8000/docs.

## Structure

- `app/` - application factory, routers, deps
- `controllers/` - request handlers
- `routes/` - route definitions
- `middleware/` - request/response middleware
- `services/` - business logic
- `repositories/` - data access
- `models/` - ORM models
- `schemas/` - Pydantic schemas
- `config/` - configuration loaders
