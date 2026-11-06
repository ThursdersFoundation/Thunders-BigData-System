"""Dataset routes."""

from fastapi import APIRouter

router = APIRouter()


@router.get("")
async def list_datasets() -> list[dict]:
    return []


@router.post("")
async def create_dataset(payload: dict) -> dict:
    return {"id": "TODO", **payload}


@router.get("/{dataset_id}")
async def get_dataset(dataset_id: str) -> dict:
    return {"id": dataset_id}
