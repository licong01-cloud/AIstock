from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ..db.pg_pool import get_conn
from .rdagent_registry_service import RDRegistryReader, RDWorkspace
from .rdagent_store_service import resolve_strategy_store_root
from .rdagent_signals_service import persist_rdagent_signals, read_signals_parquet


@dataclass(frozen=True)
class ImportResult:
    strategy_id: str
    strategy_version_id: str
    artifact_root_path: str
    inferred_strategy_kind: str
    inferred_output_mode: str


def _infer_output_mode(df: pd.DataFrame) -> str:
    cols = {c.lower() for c in df.columns}
    if "target_weight" in cols or "weight" in cols:
        return "target_weight"
    return "topk"


def _infer_strategy_kind(df: pd.DataFrame) -> str:
    # heuristic: if per date multiple symbols => portfolio; else single_symbol
    for date_col in ("trade_date", "datetime", "date", "time"):
        if date_col in df.columns:
            g = df.groupby(date_col)["symbol"].nunique(dropna=True)
            if len(g) > 0 and g.max() > 1:
                return "portfolio"
            return "single_symbol"
    return "portfolio"


def _load_signals_preview(signals_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(signals_path)
    # keep only small preview for inference
    if len(df) > 5000:
        df = df.head(5000)
    return df


def _ensure_source_row(cur) -> int:
    cur.execute(
        """
        INSERT INTO trading.strategy_source (source_type, name, description)
        VALUES ('rdagent', 'RD-Agent', 'RD-Agent research outputs')
        ON CONFLICT (source_type) DO UPDATE SET name=EXCLUDED.name
        RETURNING source_id
        """
    )
    row = cur.fetchone()
    return int(row[0])


def _candidate_key(task_run_id: str, loop_id: int, workspace_id: str) -> str:
    return f"task_run:{task_run_id}/loop:{loop_id}/workspace:{workspace_id}"


def import_best_workspace(
    registry_db_path: str,
    task_run_id: str,
    loop_id: int,
    workspace_id: str,
    strategy_name: Optional[str] = None,
    strategy_kind: Optional[str] = None,
    output_mode: Optional[str] = None,
    enabled: bool = True,
) -> ImportResult:
    disable_backtest = (os.environ.get("AISTOCK_DISABLE_BACKTEST_IMPORT") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    env_name = (os.environ.get("AISTOCK_ENV") or "").strip().lower()
    if disable_backtest or env_name in {"prod", "production"}:
        raise RuntimeError(
            "生产/实盘环境已禁用从 RD-Agent workspace 导入 signals.parquet（回测/离线产物）。"
        )

    reader = RDRegistryReader(registry_db_path)
    ws: RDWorkspace = reader.get_workspace(workspace_id)

    raw_workspace_path = ws.workspace_path
    if os.name == "nt" and raw_workspace_path.startswith("/mnt/") and len(raw_workspace_path) > 6:
        # WSL style: /mnt/f/Dev/...  ->  F:/Dev/...
        drive = raw_workspace_path[5]
        if raw_workspace_path[6:7] == "/":
            rest = raw_workspace_path[7:]
            raw_workspace_path = f"{drive.upper()}:/{rest}"

    workspace_path = Path(raw_workspace_path)
    if not workspace_path.exists():
        raise FileNotFoundError(f"workspace_path not found: {workspace_path}")

    signals_parquet_rel, _ = reader.find_signal_files(workspace_id)
    if signals_parquet_rel is None:
        # best-effort: check workspace root
        if (workspace_path / "signals.parquet").exists():
            signals_parquet_rel = "signals.parquet"

    if signals_parquet_rel is None:
        raise FileNotFoundError("signals.parquet not found for workspace")

    signals_abs = workspace_path / signals_parquet_rel
    df_preview = _load_signals_preview(signals_abs)

    inferred_output_mode = output_mode or _infer_output_mode(df_preview)
    inferred_strategy_kind = strategy_kind or _infer_strategy_kind(df_preview)

    src_key = _candidate_key(task_run_id, loop_id, workspace_id)
    version_tag = f"{task_run_id}_{loop_id}_{workspace_id}"

    strategy_id = str(uuid.uuid4())
    strategy_version_id = str(uuid.uuid4())

    store_root = Path(resolve_strategy_store_root())
    artifact_root = store_root / "sources" / "rdagent" / src_key.replace(":", "=").replace("/", "__") / "versions" / version_tag / "artifacts"
    artifact_root.mkdir(parents=True, exist_ok=True)

    # Copy minimal files
    (artifact_root / "signals.parquet").write_bytes(signals_abs.read_bytes())
    if ws.manifest_path:
        src_manifest = workspace_path / ws.manifest_path
        if src_manifest.exists():
            (artifact_root / "manifest.json").write_bytes(src_manifest.read_bytes())
    if ws.summary_path:
        src_summary = workspace_path / ws.summary_path
        if src_summary.exists():
            (artifact_root / "summary.json").write_bytes(src_summary.read_bytes())

    manifest_json: Optional[Dict[str, Any]] = None
    manifest_file = artifact_root / "manifest.json"
    if manifest_file.exists():
        try:
            manifest_json = json.loads(manifest_file.read_text(encoding="utf-8"))
        except Exception:
            manifest_json = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            source_id = _ensure_source_row(cur)

            cur.execute(
                """
                INSERT INTO trading.strategy (
                    strategy_id, source_id, source_strategy_key, strategy_name,
                    strategy_kind, output_mode, universe_spec, enabled
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    strategy_id,
                    source_id,
                    src_key,
                    strategy_name or src_key,
                    inferred_strategy_kind,
                    inferred_output_mode,
                    None,
                    enabled,
                ),
            )

            cur.execute(
                """
                INSERT INTO trading.strategy_version (
                    strategy_version_id, strategy_id, version_tag,
                    manifest_json, artifact_root_path, import_status
                )
                VALUES (%s,%s,%s,%s,%s,'imported')
                """,
                (
                    strategy_version_id,
                    strategy_id,
                    version_tag,
                    manifest_json,
                    str(artifact_root),
                ),
            )

            conn.commit()

    # Signals ETL: read the copied parquet and upsert into DB
    copied_signals = artifact_root / "signals.parquet"
    if copied_signals.exists():
        df_signals = read_signals_parquet(str(copied_signals))
        persist_rdagent_signals(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            output_mode=inferred_output_mode,
            df=df_signals,
        )

    return ImportResult(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        artifact_root_path=str(artifact_root),
        inferred_strategy_kind=inferred_strategy_kind,
        inferred_output_mode=inferred_output_mode,
    )
