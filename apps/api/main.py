"""FastAPI application entrypoint for Thunders BigData System."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import auth, datasets, pipelines, ai
from shared.middleware.logger import LoggingMiddleware

app = FastAPI(
    title="Thunders BigData System API",
    version="0.1.0",
    description="REST API for the Thunders BigData System platform.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(LoggingMiddleware)

app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(datasets.router, prefix="/datasets", tags=["datasets"])
app.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
app.include_router(ai.router, prefix="/ai", tags=["ai"])


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
