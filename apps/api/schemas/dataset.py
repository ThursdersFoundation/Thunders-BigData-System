"""Pydantic schemas for datasets."""

from pydantic import BaseModel, Field
from typing import Literal


class DatasetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    layer: Literal["bronze", "silver", "gold"]


class DatasetOut(BaseModel):
    id: str
    name: str
    layer: str
