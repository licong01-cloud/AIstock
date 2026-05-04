from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ValidationPage(BaseModel):
    items: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 20
    has_more: bool = False


class ValidationResponse(BaseModel):
    status: str = "success"
    data: Any
