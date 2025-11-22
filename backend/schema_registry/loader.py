"""从 CSV 文件加载数据集 schema 的最小骨架实现。

后续会根据 docs/data_schema_fields.csv 和 data_schema_source_mapping.csv
完善加载逻辑与错误校验。当前仅保留接口与结构。"""

from pathlib import Path
from typing import Dict
import csv

from .models import DatasetSchema, FieldSchema, SourceMapping


class SchemaRegistry:
    def __init__(self) -> None:
        self._datasets: Dict[str, DatasetSchema] = {}

    def get_dataset(self, name: str) -> DatasetSchema | None:
        return self._datasets.get(name)

    def list_datasets(self) -> list[str]:
        return sorted(self._datasets.keys())


def load_from_csv(root: Path) -> SchemaRegistry:
    """从给定根目录下的 CSV 文件加载 schema。

    root 通常为项目根目录的 docs/ 目录。
    当前实现仅返回空的 registry，后续再逐步完善。
    """

    registry = SchemaRegistry()

    fields_path = root / "data_schema_fields.csv"
    mapping_path = root / "data_schema_source_mapping.csv"

    datasets: Dict[str, DatasetSchema] = {}
    field_rows: Dict[str, list[FieldSchema]] = {}
    source_rows: Dict[str, list[SourceMapping]] = {}

    if fields_path.exists():
        with fields_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dataset = (row.get("数据集") or "").strip()
                if not dataset:
                    continue
                name = (row.get("规范字段名") or "").strip()
                cn_name = (row.get("中文字段名") or "").strip()
                dtype = (row.get("数据类型") or "").strip()
                unit = (row.get("单位") or "").strip()
                scale_raw = row.get("小数位数")
                scale = None
                if scale_raw not in (None, ""):
                    try:
                        scale = float(scale_raw)
                    except Exception:
                        scale = None
                extras: Dict[str, object] = {}
                default_fmt = row.get("默认格式")
                if default_fmt not in (None, ""):
                    extras["default_format"] = default_fmt
                desc = row.get("字段说明")
                if desc not in (None, ""):
                    extras["description"] = desc
                field = FieldSchema(
                    dataset=dataset,
                    name=name,
                    cn_name=cn_name,
                    dtype=dtype,
                    unit=unit,
                    scale=scale,
                    extras=extras or None,
                )
                field_rows.setdefault(dataset, []).append(field)

    if mapping_path.exists():
        with mapping_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dataset = (row.get("数据集") or "").strip()
                if not dataset:
                    continue
                field_name = (row.get("规范字段名") or "").strip()
                source_system = (row.get("来源系统") or "").strip()
                source_table = (row.get("来源接口") or "").strip() or None
                source_field = (row.get("来源字段名") or "").strip()
                source_dtype = (row.get("来源数据类型") or "").strip()
                source_unit = (row.get("来源单位") or "").strip()
                factor_raw = row.get("转换系数")
                factor = 1.0
                if factor_raw not in (None, ""):
                    try:
                        factor = float(factor_raw)
                    except Exception:
                        factor = 1.0
                mapping = SourceMapping(
                    dataset=dataset,
                    field=field_name,
                    source_system=source_system,
                    source_table=source_table,
                    source_field=source_field,
                    source_dtype=source_dtype,
                    source_unit=source_unit,
                    factor=factor,
                )
                source_rows.setdefault(dataset, []).append(mapping)

    all_dataset_names = set(field_rows.keys()) | set(source_rows.keys())
    for name in all_dataset_names:
        ds = DatasetSchema(
            name=name,
            fields=field_rows.get(name, []),
            sources=source_rows.get(name, []),
        )
        datasets[name] = ds

    registry._datasets = datasets
    return registry
