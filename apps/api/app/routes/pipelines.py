"""Pipeline routes."""

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def list_pipelines() -> list[dict]:
    return []


@router.post("/{pipeline_id}/run")
async def run_pipeline(pipeline_id: str) -> dict:
    return {"run_id": "TODO", "pipeline_id": pipeline_id, "status": "queued"}
