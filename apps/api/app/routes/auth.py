"""Authentication routes."""

from fastapi import APIRouter

router = APIRouter()


@router.post("/login")
async def login(credentials: dict) -> dict:
    # TODO: validate credentials and issue JWT
    return {"access_token": "TODO", "token_type": "bearer"}


@router.post("/register")
async def register(payload: dict) -> dict:
    # TODO: create user
    return {"id": "TODO"}


@router.get("/me")
async def me() -> dict:
    # TODO: read from current user
    return {"user": "TODO"}
