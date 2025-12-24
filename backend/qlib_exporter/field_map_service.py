from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .config import ensure_snapshot_root
from .field_map import (
    FieldMapRow,
    attach_column_comments_to_h5,
    build_field_map_rows_for_snapshot,
    write_field_map_csv,
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

    daily_basic_cols = _read_h5_columns(daily_basic_h5) if daily_basic_h5.exists() else None
    moneyflow_cols = _read_h5_columns(moneyflow_h5) if moneyflow_h5.exists() else None

    rows: List[FieldMapRow] = build_field_map_rows_for_snapshot(
        daily_basic_columns=daily_basic_cols,
        moneyflow_columns=moneyflow_cols,
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
                {c: col2cn.get(c, c) for c in daily_basic_cols},
            )
            written_h5[str(daily_basic_h5)] = len(daily_basic_cols)
        if moneyflow_h5.exists() and moneyflow_cols is not None:
            attach_column_comments_to_h5(
                moneyflow_h5,
                {c: col2cn.get(c, c) for c in moneyflow_cols},
            )
            written_h5[str(moneyflow_h5)] = len(moneyflow_cols)

    return {
        "snapshot_id": snapshot_id,
        "csv_path": str(out_csv),
        "rows": len(rows),
        "written_h5": written_h5,
        "has_daily_basic": daily_basic_h5.exists(),
        "has_moneyflow": moneyflow_h5.exists(),
    }
