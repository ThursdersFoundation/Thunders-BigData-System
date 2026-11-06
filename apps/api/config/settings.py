"""API-specific settings."""

from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000"]

    class Config:
        env_prefix = "API_"


settings = APISettings()
