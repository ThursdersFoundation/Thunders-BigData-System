"""AI routes."""

from fastapi import APIRouter

router = APIRouter()


@router.post("/predict")
async def predict(payload: dict) -> dict:
    return {"prediction": "TODO"}


@router.post("/chat")
async def chat(payload: dict) -> dict:
    return {"reply": "TODO"}


@router.get("/models")
async def list_models() -> list[dict]:
    return []
