from dataclasses import dataclass
from typing import Any


@dataclass
class FieldSchema:
    dataset: str
    name: str
    cn_name: str
    dtype: str
    unit: str
    scale: float | None = None
    extras: dict[str, Any] | None = None


@dataclass
class SourceMapping:
    dataset: str
    field: str
    source_system: str
    source_table: str | None
    source_field: str
    source_dtype: str
    source_unit: str
    factor: float


@dataclass
class DatasetSchema:
    name: str
    fields: list[FieldSchema]
    sources: list[SourceMapping]
