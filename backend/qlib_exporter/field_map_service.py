from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .config import ensure_snapshot_root
from .field_map import (
    FieldMapRow,
    attach_column_comments_to_h5,
    build_field_map_rows_for_snapshot,
    write_field_map_csv,
    _infer_dtype_hints,
)


def _project_root() -> Path:
    # backend/qlib_exporter/field_map_service.py -> AIstock/
    return Path(__file__).resolve().parents[2]


def _read_h5_columns(h5_path: Path) -> List[str]:
    # 只取列名，不加载全量数据
    if not h5_path.exists():
        return []
    try:
        df_head = pd.read_hdf(str(h5_path), key="data", start=0, stop=1)
        return list(df_head.columns)
    except Exception:
        # 某些 HDF5 格式下 start/stop 不支持，fallback
        df = pd.read_hdf(str(h5_path), key="data")
        return list(df.columns)


def _read_h5_columns_and_dtypes(h5_path: Path) -> tuple[List[str], Dict[str, str]]:
    """读取 H5 文件的列名和数据类型。"""
    if not h5_path.exists():
        return [], {}
    try:
        df_head = pd.read_hdf(str(h5_path), key="data", start=0, stop=5)
        columns = list(df_head.columns)
        dtypes = _infer_dtype_hints(df_head)
        return columns, dtypes
    except Exception:
        df = pd.read_hdf(str(h5_path), key="data")
        columns = list(df.columns)
        dtypes = _infer_dtype_hints(df)
        return columns, dtypes


def export_field_map_for_snapshot(
    *,
    snapshot_id: str,
    out_csv: Optional[Path] = None,
    write_to_h5: bool = True,
) -> Dict[str, object]:
    snap_root = ensure_snapshot_root()
    snap_dir = snap_root / snapshot_id
    if not snap_dir.exists():
        raise FileNotFoundError(f"snapshot not found: {snap_dir}")

    daily_basic_h5 = snap_dir / "daily_basic.h5"
    moneyflow_h5 = snap_dir / "moneyflow.h5"
    bak_basic_h5 = snap_dir / "bak_basic.h5"
    margin_detail_h5 = snap_dir / "margin_detail.h5"
    cyq_perf_h5 = snap_dir / "cyq_perf.h5"
    sector_data_h5 = snap_dir / "sector_data.h5"

    daily_basic_cols, daily_basic_dtypes = _read_h5_columns_and_dtypes(daily_basic_h5) if daily_basic_h5.exists() else (None, None)
    moneyflow_cols, moneyflow_dtypes = _read_h5_columns_and_dtypes(moneyflow_h5) if moneyflow_h5.exists() else (None, None)
    bak_basic_cols, bak_basic_dtypes = _read_h5_columns_and_dtypes(bak_basic_h5) if bak_basic_h5.exists() else (None, None)
    margin_detail_cols, margin_detail_dtypes = _read_h5_columns_and_dtypes(margin_detail_h5) if margin_detail_h5.exists() else (None, None)
    cyq_perf_cols, cyq_perf_dtypes = _read_h5_columns_and_dtypes(cyq_perf_h5) if cyq_perf_h5.exists() else (None, None)
    sector_data_cols, sector_data_dtypes = _read_h5_columns_and_dtypes(sector_data_h5) if sector_data_h5.exists() else (None, None)

    rows: List[FieldMapRow] = build_field_map_rows_for_snapshot(
        daily_basic_columns=daily_basic_cols,
        moneyflow_columns=moneyflow_cols,
        bak_basic_columns=bak_basic_cols,
        margin_detail_columns=margin_detail_cols,
        cyq_perf_columns=cyq_perf_cols,
        sector_data_columns=sector_data_cols,
        daily_basic_dtypes=daily_basic_dtypes,
        moneyflow_dtypes=moneyflow_dtypes,
        bak_basic_dtypes=bak_basic_dtypes,
        margin_detail_dtypes=margin_detail_dtypes,
        cyq_perf_dtypes=cyq_perf_dtypes,
        sector_data_dtypes=sector_data_dtypes,
    )

    if out_csv is None:
        out_csv = snap_dir / "metadata" / "aistock_field_map.csv"

    write_field_map_csv(rows, out_csv)

    # per-file mapping
    col2cn: Dict[str, str] = {r.name: r.meaning_cn for r in rows}

    written_h5: Dict[str, int] = {}
    if write_to_h5:
        if daily_basic_h5.exists() and daily_basic_cols is not None:
            attach_column_comments_to_h5(
                daily_basic_h5,
                {c: col2cn.get(c, "") for c in daily_basic_cols},
            )
            written_h5[str(daily_basic_h5)] = len(daily_basic_cols)
        if moneyflow_h5.exists() and moneyflow_cols is not None:
            attach_column_comments_to_h5(
                moneyflow_h5,
                {c: col2cn.get(c, "") for c in moneyflow_cols},
            )
            written_h5[str(moneyflow_h5)] = len(moneyflow_cols)
        if bak_basic_h5.exists() and bak_basic_cols is not None:
            attach_column_comments_to_h5(
                bak_basic_h5,
                {c: col2cn.get(c, "") for c in bak_basic_cols},
            )
            written_h5[str(bak_basic_h5)] = len(bak_basic_cols)
        if margin_detail_h5.exists() and margin_detail_cols is not None:
            attach_column_comments_to_h5(
                margin_detail_h5,
                {c: col2cn.get(c, "") for c in margin_detail_cols},
            )
            written_h5[str(margin_detail_h5)] = len(margin_detail_cols)
        if cyq_perf_h5.exists() and cyq_perf_cols is not None:
            attach_column_comments_to_h5(
                cyq_perf_h5,
                {c: col2cn.get(c, "") for c in cyq_perf_cols},
            )
            written_h5[str(cyq_perf_h5)] = len(cyq_perf_cols)
        if sector_data_h5.exists() and sector_data_cols is not None:
            attach_column_comments_to_h5(
                sector_data_h5,
                {c: col2cn.get(c, "") for c in sector_data_cols},
            )
            written_h5[str(sector_data_h5)] = len(sector_data_cols)

    # 生成统一的 static_factors_schema.csv（供 RD-Agent 使用）
    def _generate_static_factors_schema(rows: List[FieldMapRow], schema_path: Path) -> None:
        """生成 RD-Agent 兼容的统一 Schema 文件"""
        import csv
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        with open(schema_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['name', 'meaning_cn', 'unit', 'source_table', 'comment', 'dtype'])
            for row in rows:
                writer.writerow([
                    row.name,
                    row.meaning_cn,
                    row.unit,
                    row.source_table,
                    row.comment,
                    row.dtype_hint,  # 使用 pandas dtype
                ])
    
    schema_dir = snap_dir / "metadata" / "schemas"
    schema_csv_path = schema_dir / "static_factors_schema.csv"
    _generate_static_factors_schema(rows, schema_csv_path)

    return {
        "snapshot_id": snapshot_id,
        "csv_path": str(out_csv),
        "schema_path": str(schema_csv_path),
        "rows": len(rows),
        "written_h5": written_h5,
        "has_daily_basic": daily_basic_h5.exists(),
        "has_moneyflow": moneyflow_h5.exists(),
        "has_bak_basic": bak_basic_h5.exists(),
        "has_margin_detail": margin_detail_h5.exists(),
        "has_cyq_perf": cyq_perf_h5.exists(),
        "has_sector_data": sector_data_h5.exists(),
    }
