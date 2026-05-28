"""HMM 模型训练管理的核心业务逻辑。

提供超参版本 CRUD、训练任务管理、快照管理等功能。
通过 model_type 字段区分不同模型类型，当前首先实现 sector_hmm。
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import calendar
import hashlib
import traceback
from datetime import date, datetime, timedelta, timezone
from dataclasses import asdict, fields as dc_fields
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras

from ..db.pg_pool import get_conn
from ..quant_models.hmm.sector_hmm import SectorHMMConfig

logger = logging.getLogger(__name__)

# Mapping from model_type to its default config dataclass
_DEFAULT_CONFIG_MAP: Dict[str, type] = {
    "sector_hmm": SectorHMMConfig,
}

DEFAULT_ROLLING_TRAIN_YEARS = 3.0
DEFAULT_VALIDATION_WINDOW_MONTHS = 3
MIN_RECOMMENDED_VALIDATION_TRADING_DAYS = 60
MIN_RECOMMENDED_TRAIN_TRADING_DAYS = 500
HMM_DAILY_COEFFICIENT_MODE = "daily_asof_prediction_v1"


def _coerce_date(value: Any, *, field_name: str = "date") -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise ValueError(f"{field_name} must be a date or ISO date string")


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _add_months(day: date, months: int) -> date:
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    return day.replace(day=min(day.day, _last_day_of_month(year, month)), year=year, month=month)


class HMMTrainingService:
    """HMM 模型训练管理的核心业务逻辑。"""

    def __init__(self) -> None:
        raw_dir = os.getenv("HMM_MODELS_DIR", "")
        if not raw_dir:
            # Default: AIstock/data/hmm_models/ relative to project root
            raw_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data", "hmm_models",
            )
        self.models_dir: str = os.path.abspath(raw_dir)

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------

    @staticmethod
    def _fill_default_config(config_json: Dict[str, Any], model_type: str = "sector_hmm") -> Dict[str, Any]:
        """用对应 model_type 的默认值填充缺失字段，忽略未知字段。"""
        config_cls = _DEFAULT_CONFIG_MAP.get(model_type, SectorHMMConfig)
        defaults = asdict(config_cls())
        known_keys = {f.name for f in dc_fields(config_cls)}
        result: Dict[str, Any] = {}
        for key in known_keys:
            if key in config_json:
                result[key] = config_json[key]
            else:
                result[key] = defaults[key]
        return result

    def _build_model_path(self, config_id: str, snapshot_date: str) -> str:
        """生成模型文件路径：{models_dir}/{config_id}/{snapshot_date}/models.json"""
        return os.path.join(
            self.models_dir, config_id, snapshot_date, "models.json"
        )

    # ------------------------------------------------------------------
    # 超参版本 CRUD
    # ------------------------------------------------------------------

    def create_config(
        self,
        model_type: str,
        display_name: str,
        config_json: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建超参版本，缺失字段用对应 model_type 的默认值填充。

        Returns
        -------
        dict
            完整的配置记录（含 config_id、created_at 等）。
        """
        filled = self._fill_default_config(config_json, model_type)

        sql = """
            INSERT INTO model_train_configs
                (model_type, display_name, config_json)
            VALUES (%s, %s, %s)
            RETURNING config_id, model_type, display_name, config_json,
                      cron_expression, cron_enabled, created_at
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    cur.execute(sql, (model_type, display_name, json.dumps(filled)))
                except psycopg2.IntegrityError:
                    conn.rollback()
                    raise ValueError(
                        f"配置名称 '{display_name}' 在 {model_type} 下已存在"
                    )
                row = cur.fetchone()
        return dict(row)

    def list_configs(self, model_type: str = "sector_hmm") -> List[Dict[str, Any]]:
        """列出指定 model_type 的超参版本，附带 snapshot_count，按 created_at 降序。"""
        sql = """
            SELECT c.config_id, c.model_type, c.display_name, c.config_json,
                   c.cron_expression, c.cron_enabled, c.created_at,
                   COALESCE(s.cnt, 0) AS snapshot_count
            FROM model_train_configs c
            LEFT JOIN (
                SELECT config_id, COUNT(*) AS cnt
                FROM model_train_snapshots
                GROUP BY config_id
            ) s ON s.config_id = c.config_id
            WHERE c.model_type = %s
            ORDER BY c.created_at DESC
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (model_type,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]

    def get_config(self, config_id: str) -> Optional[Dict[str, Any]]:
        """Fetch one HMM config by id regardless of UI-visible model_type."""
        sql = """
            SELECT c.config_id, c.model_type, c.display_name, c.config_json,
                   c.cron_expression, c.cron_enabled, c.created_at,
                   COALESCE(s.cnt, 0) AS snapshot_count
            FROM model_train_configs c
            LEFT JOIN (
                SELECT config_id, COUNT(*) AS cnt
                FROM model_train_snapshots
                GROUP BY config_id
            ) s ON s.config_id = c.config_id
            WHERE c.config_id = %s
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (config_id,))
                row = cur.fetchone()
        return dict(row) if row else None

    @staticmethod
    def _snapshot_display_name(row: Dict[str, Any]) -> str:
        metrics = row.get("metrics_json") or {}
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except json.JSONDecodeError:
                metrics = {}
        if isinstance(metrics, dict):
            for key in ("snapshot_display_name", "display_name"):
                value = metrics.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        config_name = str(row.get("config_display_name") or row.get("config_id") or "").strip()
        trained_at = row.get("trained_at")
        if isinstance(trained_at, datetime):
            trained_date = trained_at.date().isoformat()
        elif hasattr(trained_at, "isoformat"):
            trained_date = trained_at.isoformat()
        else:
            trained_date = str(trained_at or "")[:10]
        if config_name and trained_date:
            return f"{config_name}__snapshot_{trained_date}"
        return str(row.get("snapshot_id") or "").strip()

    @classmethod
    def _attach_snapshot_display_name(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        out["display_name"] = cls._snapshot_display_name(out)
        out["coefficient_artifacts"] = cls._list_snapshot_coefficient_artifacts(out.get("model_path"))
        return out

    @staticmethod
    def _list_snapshot_coefficient_artifacts(model_path: Any) -> List[Dict[str, Any]]:
        """Return explicit HMM coefficient coverage for UI preflight.

        The selection runtime still validates the selected artifact and exact
        trade date. This metadata is only used to prevent users from selecting a
        snapshot/preset that cannot possibly cover the requested date.
        """

        if not model_path:
            return []
        model_dir = os.path.dirname(os.path.abspath(str(model_path)))
        if not os.path.isdir(model_dir):
            return []

        artifacts: List[Dict[str, Any]] = []
        for filename in sorted(os.listdir(model_dir)):
            if not filename.startswith("coefficients_") or not filename.endswith(".json"):
                continue
            path = os.path.join(model_dir, filename)
            stem = filename[:-5]
            parts = stem.split("_")
            if len(parts) < 4:
                continue
            preset = "_".join(parts[1:-2])
            start_date = parts[-2]
            end_date = parts[-1]
            artifact: Dict[str, Any] = {
                "filename": filename,
                "path": path,
                "preset": preset,
                "start_date": start_date,
                "end_date": end_date,
                "covered_trade_dates": [],
                "date_count": 0,
            }
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                artifact["artifact_sha256"] = HMMTrainingService._file_sha256(path)
                daily = payload.get("daily_coefficients") if isinstance(payload, dict) else None
                if isinstance(daily, dict):
                    covered = sorted(str(item) for item in daily.keys())
                    artifact["covered_trade_dates"] = covered
                    artifact["date_count"] = len(covered)
                    if covered:
                        artifact["start_date"] = covered[0]
                        artifact["end_date"] = covered[-1]
                if isinstance(payload, dict):
                    for key in (
                        "generation_mode",
                        "as_of_trade_date",
                        "effective_trade_date",
                        "generated_at",
                        "snapshot_id",
                        "config_id",
                        "input_data_max_dates",
                    ):
                        if key in payload:
                            artifact[key] = payload[key]
            except Exception as exc:  # pragma: no cover - diagnostic metadata only
                artifact["parse_error"] = str(exc)
            artifacts.append(artifact)
        return artifacts

    def delete_config(self, config_id: str) -> None:
        """删除超参版本。有关联快照时拒绝（抛出 ValueError）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM model_train_snapshots WHERE config_id = %s",
                    (config_id,),
                )
                count = cur.fetchone()[0]
                if count > 0:
                    raise ValueError(
                        f"无法删除：配置 {config_id} 下仍有 {count} 个关联快照"
                    )
                cur.execute(
                    "DELETE FROM model_train_configs WHERE config_id = %s",
                    (config_id,),
                )

    # ------------------------------------------------------------------
    # 训练任务管理 (Task 4.1)
    # ------------------------------------------------------------------

    def trigger_training(self, config_id: str) -> Dict[str, Any]:
        """触发训练：检查并发限制，创建 job，返回 job_id。

        如果该 config_id 下已有 pending 或 running 的 job，则拒绝（ValueError）。
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM model_train_jobs
                       WHERE config_id = %s AND status IN ('pending', 'running')""",
                    (config_id,),
                )
                active_count = cur.fetchone()[0]
                if active_count > 0:
                    raise ValueError(
                        f"配置 {config_id} 下已有活跃训练任务（pending/running），"
                        "请等待完成后再触发"
                    )

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """INSERT INTO model_train_jobs (config_id, status)
                       VALUES (%s, 'pending')
                       RETURNING job_id, config_id, snapshot_id, status,
                                 started_at, completed_at, error_message""",
                    (config_id,),
                )
                row = cur.fetchone()
        return dict(row)

    def preview_rolling_training(
        self,
        config_id: str,
        *,
        as_of_date: date | None = None,
        train_window_years: float = DEFAULT_ROLLING_TRAIN_YEARS,
        validation_window_months: int = DEFAULT_VALIDATION_WINDOW_MONTHS,
    ) -> Dict[str, Any]:
        """Build a manual rolling-training plan without mutating DB state."""

        config_row = self._get_config(config_id)
        latest_completed, data_max_dates = self._latest_completed_hmm_data_date(as_of_date=as_of_date)
        lookback_days = int(train_window_years * 365) + 140
        trading_days = self._list_trading_days(
            latest_completed - timedelta(days=lookback_days),
            latest_completed,
        )
        plan = self.build_rolling_training_plan(
            trading_days=trading_days,
            latest_completed_trade_date=latest_completed,
            train_window_years=train_window_years,
            validation_window_months=validation_window_months,
        )
        base_config = config_row["config_json"]
        if isinstance(base_config, str):
            base_config = json.loads(base_config)
        rolling_config = self._rolling_config_from_plan(base_config, plan)
        return {
            "config_id": config_id,
            "model_type": config_row["model_type"],
            "display_name": config_row["display_name"],
            "recommended_validation_window_months": DEFAULT_VALIDATION_WINDOW_MONTHS,
            "best_validation_policy": "latest_3_calendar_months",
            "data_max_dates": {key: value.isoformat() if value else None for key, value in data_max_dates.items()},
            **plan,
            "rolling_config_json": rolling_config,
            "manual_confirm_required": True,
            "confirm_boolean_required": True,
        }

    def trigger_rolling_training(
        self,
        config_id: str,
        *,
        confirm_retrain: bool = False,
        as_of_date: date | None = None,
        train_window_years: float = DEFAULT_ROLLING_TRAIN_YEARS,
        validation_window_months: int = DEFAULT_VALIDATION_WINDOW_MONTHS,
    ) -> Dict[str, Any]:
        """Persist a rolling plan on the config and create a pending job."""

        if not confirm_retrain:
            raise ValueError("rolling HMM training requires explicit boolean confirmation")
        preview = self.preview_rolling_training(
            config_id,
            as_of_date=as_of_date,
            train_window_years=train_window_years,
            validation_window_months=validation_window_months,
        )
        rolling_config = preview["rolling_config_json"]
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT COUNT(*) FROM model_train_jobs
                       WHERE config_id = %s AND status IN ('pending', 'running')""",
                    (config_id,),
                )
                active_count = cur.fetchone()[0]
                if active_count > 0:
                    raise ValueError(
                        f"配置 {config_id} 下已有活跃训练任务（pending/running），"
                        "请等待完成后再触发"
                    )
                cur.execute(
                    """
                    UPDATE model_train_configs
                    SET config_json = %s
                    WHERE config_id = %s
                    """,
                    (json.dumps(rolling_config, ensure_ascii=False), config_id),
                )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """INSERT INTO model_train_jobs (config_id, status)
                       VALUES (%s, 'pending')
                       RETURNING job_id, config_id, snapshot_id, status,
                                 started_at, completed_at, error_message""",
                    (config_id,),
                )
                row = cur.fetchone()
        job = dict(row)
        job["rolling_training_preview"] = {
            key: value
            for key, value in preview.items()
            if key != "rolling_config_json"
        }
        return job

    def _get_config(self, config_id: str) -> Dict[str, Any]:
        sql = """
            SELECT config_id, model_type, display_name, config_json,
                   cron_expression, cron_enabled, created_at
            FROM model_train_configs
            WHERE config_id = %s
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (config_id,))
                row = cur.fetchone()
        if row is None:
            raise ValueError(f"配置 {config_id} 不存在")
        return dict(row)

    def _latest_completed_hmm_data_date(
        self,
        *,
        as_of_date: date | None,
    ) -> tuple[date, Dict[str, date | None]]:
        cutoff = as_of_date or date.today()
        queries = {
            "sector_data": "SELECT MAX(trade_date) FROM market.sector_data WHERE trade_date <= %s",
            "sw_daily": "SELECT MAX(trade_date) FROM market.sw_daily WHERE trade_date <= %s",
            "index_daily_000300": (
                "SELECT MAX(trade_date) FROM market.index_daily "
                "WHERE ts_code = '000300.SH' AND trade_date <= %s"
            ),
        }
        max_dates: Dict[str, date | None] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                for name, sql in queries.items():
                    cur.execute(sql, (cutoff,))
                    value = cur.fetchone()[0]
                    max_dates[name] = _coerce_date(value, field_name=name) if value else None
        missing = [name for name, value in max_dates.items() if value is None]
        if missing:
            raise RuntimeError(
                "HMM rolling training requires completed sector/index data before planning; "
                f"missing max trade_date for: {', '.join(missing)}"
            )
        return min(value for value in max_dates.values() if value is not None), max_dates

    def _list_trading_days(self, start_date: date, end_date: date) -> List[date]:
        sql = """
            SELECT cal_date
            FROM market.trading_calendar
            WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
            ORDER BY cal_date
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_date, end_date))
                rows = cur.fetchall()
        trading_days = [_coerce_date(row[0], field_name="cal_date") for row in rows]
        if not trading_days:
            raise RuntimeError(
                f"market.trading_calendar has no trading days between {start_date} and {end_date}"
            )
        return trading_days

    def _require_trading_day(self, trade_date: date, *, field_name: str) -> None:
        self._list_trading_days(trade_date, trade_date)

    def _next_trading_day(self, trade_date: date) -> date:
        search_end = trade_date + timedelta(days=31)
        days = self._list_trading_days(trade_date + timedelta(days=1), search_end)
        return days[0]

    @staticmethod
    def _safe_preset_key(signal_preset: str) -> str:
        text = str(signal_preset or "").strip()
        if not text:
            raise ValueError("signal_preset is required")
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
        if any(char not in allowed for char in text):
            raise ValueError(
                "signal_preset may only contain letters, digits, underscores, and hyphens"
            )
        return text

    @staticmethod
    def _file_sha256(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _normalise_config_json(config_json: Any) -> Dict[str, Any]:
        if isinstance(config_json, dict):
            return dict(config_json)
        if isinstance(config_json, str):
            loaded = json.loads(config_json)
            if not isinstance(loaded, dict):
                raise ValueError("HMM config_json must decode to a JSON object")
            return loaded
        raise ValueError("HMM config_json must be a JSON object")

    @staticmethod
    def _default_signal_presets() -> Dict[str, Dict[str, float]]:
        return {
            "preset_A": {"trending": 1.05, "neutral": 1.00, "fading": 0.96},
            "preset_B": {"trending": 1.10, "neutral": 1.00, "fading": 0.92},
        }


    @staticmethod
    def _signal_preset_metadata_keys() -> set[str]:
        return {
            "label",
            "name",
            "display_name",
            "description",
            "comment",
            "comments",
            "note",
            "notes",
            "metadata",
            "version",
        }

    @classmethod
    def _coefficient_mapping_from_preset(
        cls,
        preset_coeffs: Any,
        *,
        signal_preset: str,
    ) -> Dict[str, Any] | None:
        if not isinstance(preset_coeffs, dict):
            return None
        if "coefficients" in preset_coeffs:
            nested = preset_coeffs.get("coefficients")
            if isinstance(nested, dict):
                if "1" in nested and isinstance(nested["1"], dict):
                    return nested["1"]
                for value in nested.values():
                    if isinstance(value, dict):
                        return value
                return nested
            return None

        metadata_keys = cls._signal_preset_metadata_keys()
        scalar_coeffs = {
            str(key): value
            for key, value in preset_coeffs.items()
            if str(key) not in metadata_keys and not isinstance(value, dict)
        }
        if scalar_coeffs:
            return scalar_coeffs

        nested_candidates = {
            str(key): value
            for key, value in preset_coeffs.items()
            if str(key) not in metadata_keys and isinstance(value, dict)
        }
        if "1" in nested_candidates:
            return nested_candidates["1"]
        if nested_candidates:
            return next(iter(nested_candidates.values()))
        return None

    @classmethod
    def _extract_signal_preset_coefficients(
        cls,
        config_json: Any,
        signal_preset: str,
    ) -> Dict[str, float]:
        cfg = cls._normalise_config_json(config_json)
        presets = cfg.get("signal_presets")
        if not isinstance(presets, dict) or not presets:
            presets = cls._default_signal_presets()
        if signal_preset not in presets:
            raise ValueError(f"HMM signal_preset does not exist in config: {signal_preset}")
        preset_coeffs = presets[signal_preset]
        preset_coeffs = cls._coefficient_mapping_from_preset(
            preset_coeffs,
            signal_preset=signal_preset,
        )
        if not isinstance(preset_coeffs, dict) or not preset_coeffs:
            raise ValueError(f"HMM signal_preset has no coefficients: {signal_preset}")
        result: Dict[str, float] = {}
        for label, raw_value in preset_coeffs.items():
            try:
                value = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"HMM coefficient for {signal_preset}.{label} must be numeric"
                ) from exc
            if not value > 0:
                raise ValueError(
                    f"HMM coefficient for {signal_preset}.{label} must be positive"
                )
            result[str(label)] = value
        return result

    def _resolve_daily_coefficient_plan(
        self,
        *,
        snapshot_id: str,
        signal_preset: str,
        as_of_date: date | None = None,
        effective_trade_date: date | None = None,
    ) -> Dict[str, Any]:
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ValueError(f"HMM snapshot {snapshot_id} does not exist")
        snapshot_status = str(snapshot.get("status") or "").strip().lower()
        if snapshot_status not in {"completed", "success", "succeeded", "ready"}:
            raise RuntimeError(
                f"HMM snapshot {snapshot_id} is not completed: {snapshot.get('status')}"
            )

        model_path = os.path.abspath(str(snapshot.get("model_path") or ""))
        if not model_path or not os.path.isfile(model_path):
            raise RuntimeError(
                f"HMM model artifact is missing for snapshot {snapshot_id}: {snapshot.get('model_path')}"
            )

        preset_key = self._safe_preset_key(signal_preset)
        config_row = self._get_config(str(snapshot["config_id"]))
        preset_coeffs = self._extract_signal_preset_coefficients(
            config_row["config_json"],
            preset_key,
        )

        requested_as_of = _coerce_date(as_of_date, field_name="as_of_date") if as_of_date else None
        latest_completed, data_max_dates = self._latest_completed_hmm_data_date(
            as_of_date=requested_as_of,
        )
        if requested_as_of is not None:
            self._require_trading_day(requested_as_of, field_name="as_of_date")
            if latest_completed < requested_as_of:
                raise RuntimeError(
                    "HMM daily coefficient generation requires completed data for as_of_date; "
                    f"requested {requested_as_of}, latest common data date is {latest_completed}"
                )
            as_of = requested_as_of
        else:
            as_of = latest_completed
            self._require_trading_day(as_of, field_name="as_of_date")

        if effective_trade_date is None:
            effective = self._next_trading_day(as_of)
        else:
            effective = _coerce_date(
                effective_trade_date,
                field_name="effective_trade_date",
            )
            self._require_trading_day(effective, field_name="effective_trade_date")
        if effective <= as_of:
            raise ValueError(
                "effective_trade_date must be later than as_of_trade_date for daily HMM prediction"
            )

        model_dir = os.path.dirname(model_path)
        output_filename = f"coefficients_{preset_key}_{effective.isoformat()}_{effective.isoformat()}.json"
        output_path = os.path.join(model_dir, output_filename)
        existing = os.path.exists(output_path)
        existing_status: Dict[str, Any] | None = None
        if existing:
            existing_status = self._validate_existing_daily_artifact(
                output_path=output_path,
                snapshot_id=snapshot_id,
                config_id=str(snapshot["config_id"]),
                signal_preset=preset_key,
                as_of_trade_date=as_of,
                effective_trade_date=effective,
            )

        return {
            "snapshot_id": snapshot_id,
            "snapshot_display_name": snapshot.get("display_name"),
            "config_id": str(snapshot["config_id"]),
            "config_display_name": config_row.get("display_name"),
            "model_path": model_path,
            "model_dir": model_dir,
            "signal_preset": preset_key,
            "preset_coeffs": preset_coeffs,
            "as_of_trade_date": as_of.isoformat(),
            "effective_trade_date": effective.isoformat(),
            "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
            "input_data_max_dates": {
                key: value.isoformat() if value else None
                for key, value in data_max_dates.items()
            },
            "config_json": config_row.get("config_json") or {},
            "output_filename": output_filename,
            "output_path": output_path,
            "existing_artifact": existing,
            "existing_artifact_status": existing_status,
            "requires_wsl": True,
            "confirm_boolean_required": True,
        }

    def _validate_existing_daily_artifact(
        self,
        *,
        output_path: str,
        snapshot_id: str,
        config_id: str,
        signal_preset: str,
        as_of_trade_date: date,
        effective_trade_date: date,
    ) -> Dict[str, Any]:
        try:
            with open(output_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception as exc:
            raise RuntimeError(
                f"HMM daily coefficient artifact exists but cannot be read: {output_path}"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"HMM daily coefficient artifact is not a JSON object: {output_path}")
        expected = {
            "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
            "snapshot_id": snapshot_id,
            "config_id": config_id,
            "preset_key": signal_preset,
            "as_of_trade_date": as_of_trade_date.isoformat(),
            "effective_trade_date": effective_trade_date.isoformat(),
        }
        mismatches = {
            key: {"expected": value, "actual": payload.get(key)}
            for key, value in expected.items()
            if str(payload.get(key) or "") != value
        }
        daily = payload.get("daily_coefficients")
        if not isinstance(daily, dict) or effective_trade_date.isoformat() not in daily:
            mismatches["daily_coefficients"] = {
                "expected": effective_trade_date.isoformat(),
                "actual": sorted(daily.keys()) if isinstance(daily, dict) else type(daily).__name__,
            }
        if mismatches:
            raise RuntimeError(
                "HMM daily coefficient artifact already exists with different metadata; "
                f"refusing to overwrite {output_path}: {json.dumps(mismatches, ensure_ascii=False)}"
            )
        return {
            "status": "EXISTS",
            "path": output_path,
            "artifact_sha256": self._file_sha256(output_path),
            "date_count": len(daily),
        }

    def preview_daily_coefficients(
        self,
        snapshot_id: str,
        *,
        signal_preset: str,
        as_of_date: date | None = None,
        effective_trade_date: date | None = None,
    ) -> Dict[str, Any]:
        return self._resolve_daily_coefficient_plan(
            snapshot_id=snapshot_id,
            signal_preset=signal_preset,
            as_of_date=as_of_date,
            effective_trade_date=effective_trade_date,
        )

    def generate_daily_coefficients(
        self,
        snapshot_id: str,
        *,
        signal_preset: str,
        confirm_generate: bool = False,
        as_of_date: date | None = None,
        effective_trade_date: date | None = None,
    ) -> Dict[str, Any]:
        if not confirm_generate:
            raise ValueError(
                "HMM daily coefficient generation requires explicit boolean confirmation"
            )
        plan = self._resolve_daily_coefficient_plan(
            snapshot_id=snapshot_id,
            signal_preset=signal_preset,
            as_of_date=as_of_date,
            effective_trade_date=effective_trade_date,
        )
        return self._execute_daily_coefficient_plan(plan)

    def _execute_daily_coefficient_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Run the blocking WSL coefficient generation for an already validated plan."""
        if plan["existing_artifact"]:
            return {
                **plan,
                "status": "EXISTS",
                "created": False,
                "artifact_sha256": plan["existing_artifact_status"]["artifact_sha256"],
            }

        wsl_model_path = self._to_wsl_path(plan["model_path"])
        wsl_output_path = self._to_wsl_path(plan["output_path"])
        wsl_script = self._to_wsl_path(
            os.path.abspath(os.path.join(
                os.path.dirname(__file__), "..", "..", "scripts",
                "precompute_hmm_coefficients.py",
            ))
        )
        stdin_params = {
            "model_path": wsl_model_path,
            "test_start": plan["as_of_trade_date"],
            "backtest_end": plan["as_of_trade_date"],
            "output_trade_date": plan["effective_trade_date"],
            "as_of_trade_date": plan["as_of_trade_date"],
            "generation_mode": HMM_DAILY_COEFFICIENT_MODE,
            "snapshot_id": plan["snapshot_id"],
            "config_id": plan["config_id"],
            "input_data_max_dates": plan["input_data_max_dates"],
            "config_json": plan.get("config_json") or {},
            "preset_coeffs": plan["preset_coeffs"],
            "preset_key": plan["signal_preset"],
            "db_host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
            "db_port": int(os.getenv("TDX_DB_PORT", "5432")),
            "db_name": os.getenv("TDX_DB_NAME", "aistock"),
            "db_user": os.getenv("TDX_DB_USER", "postgres"),
            "db_password": os.getenv("TDX_DB_PASSWORD", ""),
        }
        wsl_cmd = (
            "source ~/miniconda3/etc/profile.d/conda.sh && "
            "conda activate rdagent-gpu && "
            f"python {wsl_script} --output-path '{wsl_output_path}'"
        )
        proc = subprocess.run(
            ["wsl", "bash", "-c", wsl_cmd],
            input=json.dumps(stdin_params, ensure_ascii=False),
            capture_output=True,
            text=True,
            timeout=300,
            encoding="utf-8",
            errors="replace",
        )
        if proc.stderr:
            for line in proc.stderr.strip().splitlines():
                logger.info("[HMM-daily-coefficients] %s", line)
        if proc.returncode != 0:
            raise RuntimeError(
                "HMM daily coefficient generation failed: "
                f"returncode={proc.returncode}; stderr_tail={proc.stderr[-4000:]}"
            )
        if not os.path.isfile(plan["output_path"]):
            raise RuntimeError(
                f"HMM daily coefficient generation did not create output file: {plan['output_path']}"
            )
        artifact_status = self._validate_existing_daily_artifact(
            output_path=plan["output_path"],
            snapshot_id=plan["snapshot_id"],
            config_id=plan["config_id"],
            signal_preset=plan["signal_preset"],
            as_of_trade_date=_coerce_date(plan["as_of_trade_date"], field_name="as_of_trade_date"),
            effective_trade_date=_coerce_date(plan["effective_trade_date"], field_name="effective_trade_date"),
        )
        return {
            **plan,
            "status": "CREATED",
            "created": True,
            "artifact_sha256": artifact_status["artifact_sha256"],
        }

    @staticmethod
    def _normalise_daily_job_row(row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        for key in ("as_of_trade_date", "effective_trade_date"):
            value = out.get(key)
            if isinstance(value, (date, datetime)):
                out[key] = value.isoformat()[:10]
        for key in ("requested_at", "started_at", "completed_at"):
            value = out.get(key)
            if hasattr(value, "isoformat"):
                out[key] = value.isoformat()
        for key in ("input_data_max_dates", "plan_json", "result_json", "error_context"):
            value = out.get(key)
            if isinstance(value, str):
                try:
                    out[key] = json.loads(value)
                except json.JSONDecodeError:
                    pass
        return out

    def _insert_daily_coefficient_job(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        sql = """
            INSERT INTO model_train_daily_coefficient_jobs
                (snapshot_id, config_id, signal_preset, as_of_trade_date,
                 effective_trade_date, generation_mode, status,
                 input_data_max_dates, output_path, plan_json)
            VALUES
                (%s, %s, %s, %s, %s, %s, 'PENDING', %s, %s, %s)
            RETURNING job_id, snapshot_id, config_id, signal_preset,
                      as_of_trade_date, effective_trade_date, generation_mode,
                      status, result_status, requested_at, started_at, completed_at,
                      input_data_max_dates, output_path, artifact_sha256,
                      plan_json, result_json, error_message, error_context
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    sql,
                    (
                        plan["snapshot_id"],
                        plan["config_id"],
                        plan["signal_preset"],
                        plan["as_of_trade_date"],
                        plan["effective_trade_date"],
                        plan["generation_mode"],
                        psycopg2.extras.Json(plan["input_data_max_dates"]),
                        plan["output_path"],
                        psycopg2.extras.Json(plan),
                    ),
                )
                row = cur.fetchone()
        return self._normalise_daily_job_row(dict(row))

    def start_daily_coefficients_job(
        self,
        snapshot_id: str,
        *,
        signal_preset: str,
        confirm_generate: bool = False,
        as_of_date: date | None = None,
        effective_trade_date: date | None = None,
    ) -> Dict[str, Any]:
        """Create a durable background job and return immediately.

        The WSL generation itself runs in ``run_daily_coefficients_job`` so UI
        calls do not depend on a long-lived Next.js dev proxy connection.
        """
        if not confirm_generate:
            raise ValueError(
                "HMM daily coefficient generation requires explicit boolean confirmation"
            )
        plan = self._resolve_daily_coefficient_plan(
            snapshot_id=snapshot_id,
            signal_preset=signal_preset,
            as_of_date=as_of_date,
            effective_trade_date=effective_trade_date,
        )
        return self._insert_daily_coefficient_job(plan)

    def get_daily_coefficient_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT job_id, snapshot_id, config_id, signal_preset,
                   as_of_trade_date, effective_trade_date, generation_mode,
                   status, result_status, requested_at, started_at, completed_at,
                   input_data_max_dates, output_path, artifact_sha256,
                   plan_json, result_json, error_message, error_context
            FROM model_train_daily_coefficient_jobs
            WHERE job_id = %s
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (job_id,))
                row = cur.fetchone()
        return self._normalise_daily_job_row(dict(row)) if row else None

    def list_daily_coefficient_jobs(
        self,
        *,
        snapshot_id: str | None = None,
        config_id: str | None = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        if not snapshot_id and not config_id:
            raise ValueError("snapshot_id or config_id is required when listing daily coefficient jobs")
        limit = max(1, min(int(limit), 200))
        where: List[str] = []
        params: List[Any] = []
        if snapshot_id:
            where.append("snapshot_id = %s")
            params.append(snapshot_id)
        if config_id:
            where.append("config_id = %s")
            params.append(config_id)
        params.append(limit)
        sql = f"""
            SELECT job_id, snapshot_id, config_id, signal_preset,
                   as_of_trade_date, effective_trade_date, generation_mode,
                   status, result_status, requested_at, started_at, completed_at,
                   input_data_max_dates, output_path, artifact_sha256,
                   plan_json, result_json, error_message, error_context
            FROM model_train_daily_coefficient_jobs
            WHERE {' AND '.join(where)}
            ORDER BY requested_at DESC
            LIMIT %s
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [self._normalise_daily_job_row(dict(row)) for row in rows]

    def _mark_daily_coefficient_job_running(self, job_id: str) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE model_train_daily_coefficient_jobs
                    SET status = 'RUNNING', started_at = COALESCE(started_at, NOW()),
                        error_message = NULL, error_context = NULL
                    WHERE job_id = %s AND status = 'PENDING'
                    """,
                    (job_id,),
                )

    def _complete_daily_coefficient_job(self, job_id: str, result: Dict[str, Any]) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE model_train_daily_coefficient_jobs
                    SET status = 'COMPLETED',
                        result_status = %s,
                        completed_at = NOW(),
                        output_path = %s,
                        artifact_sha256 = %s,
                        result_json = %s,
                        error_message = NULL,
                        error_context = NULL
                    WHERE job_id = %s
                    """,
                    (
                        str(result.get("status") or ""),
                        str(result.get("output_path") or ""),
                        result.get("artifact_sha256"),
                        psycopg2.extras.Json(result),
                        job_id,
                    ),
                )

    def _fail_daily_coefficient_job(
        self,
        job_id: str,
        message: str,
        context: Dict[str, Any] | None = None,
    ) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE model_train_daily_coefficient_jobs
                    SET status = 'FAILED',
                        completed_at = NOW(),
                        error_message = %s,
                        error_context = %s
                    WHERE job_id = %s
                    """,
                    (message, psycopg2.extras.Json(context or {}), job_id),
                )

    def run_daily_coefficients_job(self, job_id: str) -> None:
        """Background executor for daily HMM coefficient jobs."""
        job = self.get_daily_coefficient_job(job_id)
        if job is None:
            logger.error("HMM daily coefficient job does not exist: %s", job_id)
            return
        if str(job.get("status") or "").upper() in {"COMPLETED", "FAILED"}:
            logger.info("HMM daily coefficient job already terminal: %s", job_id)
            return

        self._mark_daily_coefficient_job_running(job_id)
        try:
            plan = self._resolve_daily_coefficient_plan(
                snapshot_id=str(job["snapshot_id"]),
                signal_preset=str(job["signal_preset"]),
                as_of_date=_coerce_date(job["as_of_trade_date"], field_name="as_of_trade_date"),
                effective_trade_date=_coerce_date(
                    job["effective_trade_date"],
                    field_name="effective_trade_date",
                ),
            )
            result = self._execute_daily_coefficient_plan(plan)
            self._complete_daily_coefficient_job(job_id, result)
        except Exception as exc:  # fail-fast and persist full diagnostic context
            logger.exception("HMM daily coefficient job failed: %s", job_id)
            self._fail_daily_coefficient_job(
                job_id,
                str(exc),
                {
                    "exception_type": exc.__class__.__name__,
                    "traceback_tail": traceback.format_exc()[-8000:],
                },
            )

    @staticmethod
    def build_rolling_training_plan(
        *,
        trading_days: Iterable[date | datetime | str],
        latest_completed_trade_date: date | datetime | str,
        train_window_years: float = DEFAULT_ROLLING_TRAIN_YEARS,
        validation_window_months: int = DEFAULT_VALIDATION_WINDOW_MONTHS,
    ) -> Dict[str, Any]:
        """Compute the manual rolling HMM train/validation split.

        The preferred validation window is the latest three calendar months.
        One- or two-month windows remain available for diagnostics but produce
        warnings because they are usually too noisy for HMM state validation.
        """

        if train_window_years <= 0:
            raise ValueError("train_window_years must be positive")
        if validation_window_months not in (1, 2, 3):
            raise ValueError("validation_window_months must be one of 1, 2, or 3")

        latest_requested = _coerce_date(latest_completed_trade_date, field_name="latest_completed_trade_date")
        unique_days = sorted({_coerce_date(day, field_name="trading_day") for day in trading_days})
        usable_days = [day for day in unique_days if day <= latest_requested]
        if not usable_days:
            raise ValueError("trading_days must contain at least one date <= latest_completed_trade_date")

        latest_completed = usable_days[-1]
        warnings: List[str] = []
        if latest_completed != latest_requested:
            warnings.append(
                "latest_completed_trade_date was not in trading_days; adjusted to previous trading day"
            )

        validation_anchor = _add_months(latest_completed, -validation_window_months) + timedelta(days=1)
        validation_days = [day for day in usable_days if validation_anchor <= day <= latest_completed]
        if not validation_days:
            raise ValueError("computed validation window contains no trading days")
        validation_start = validation_days[0]
        validation_end = validation_days[-1]

        train_candidates = [day for day in usable_days if day < validation_start]
        if not train_candidates:
            raise ValueError("computed training window has no trading day before validation_start")
        train_end = train_candidates[-1]

        whole_months = int(round(train_window_years * 12))
        if abs(train_window_years * 12 - whole_months) < 1e-9:
            train_anchor = _add_months(train_end, -whole_months)
        else:
            train_anchor = train_end - timedelta(days=int(train_window_years * 365))
        train_days = [day for day in usable_days if train_anchor <= day <= train_end]
        if not train_days:
            raise ValueError("computed training window contains no trading days")
        train_start = train_days[0]

        if validation_window_months < DEFAULT_VALIDATION_WINDOW_MONTHS:
            warnings.append(
                "validation_window_months below 3 is supported only for diagnostics; "
                "3 calendar months is the recommended default"
            )
        if len(validation_days) < MIN_RECOMMENDED_VALIDATION_TRADING_DAYS:
            warnings.append(
                f"validation window has only {len(validation_days)} trading days; "
                f"{MIN_RECOMMENDED_VALIDATION_TRADING_DAYS}+ is recommended"
            )
        if len(train_days) < MIN_RECOMMENDED_TRAIN_TRADING_DAYS:
            warnings.append(
                f"training window has only {len(train_days)} trading days; "
                f"{MIN_RECOMMENDED_TRAIN_TRADING_DAYS}+ is recommended"
            )

        return {
            "latest_completed_trade_date": latest_completed.isoformat(),
            "train_window_years": train_window_years,
            "validation_window_months": validation_window_months,
            "train_start": train_start.isoformat(),
            "train_end": train_end.isoformat(),
            "validation_start": validation_start.isoformat(),
            "validation_end": validation_end.isoformat(),
            "coefficient_start": validation_start.isoformat(),
            "coefficient_end": validation_end.isoformat(),
            "train_trading_days": len(train_days),
            "validation_trading_days": len(validation_days),
            "recommended_validation_window_months": DEFAULT_VALIDATION_WINDOW_MONTHS,
            "best_validation_policy": "latest_3_calendar_months",
            "warnings": warnings,
        }

    @staticmethod
    def _rolling_config_from_plan(base_config: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        config = dict(base_config or {})
        config.update(
            {
                "train_start": plan["train_start"],
                "train_end": plan["train_end"],
                "val_start": plan["validation_start"],
                "val_end": plan["validation_end"],
                "coefficient_start": plan["coefficient_start"],
                "coefficient_end": plan["coefficient_end"],
            }
        )
        rolling_meta = dict(config.get("rolling_training") or {})
        rolling_meta.update(
            {
                "enabled": True,
                "executor": "wsl_rdagent_gpu",
                "train_window_years": plan["train_window_years"],
                "validation_window_months": plan["validation_window_months"],
                "best_validation_policy": plan["best_validation_policy"],
                "latest_completed_trade_date": plan["latest_completed_trade_date"],
                "train_trading_days": plan["train_trading_days"],
                "validation_trading_days": plan["validation_trading_days"],
                "warnings": list(plan.get("warnings") or []),
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "precompute_coefficients_required": True,
            }
        )
        config["rolling_training"] = rolling_meta
        return config

    def run_training(self, job_id: str, config_id: str) -> None:
        """后台执行训练：WSL subprocess → 更新状态 → 创建快照。

        1. 更新 job status='running' + started_at
        2. 查询 config_json，构建 model_path
        3. 通过 subprocess.run 执行 wsl python hmm_train_script.py
        4. 成功时创建 snapshot 记录，失败时记录 error_message
        """
        # Step 1: Mark job as running
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE model_train_jobs
                       SET status = 'running', started_at = NOW()
                       WHERE job_id = %s""",
                    (job_id,),
                )

        # Step 2: Fetch config_json
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT config_json FROM model_train_configs WHERE config_id = %s",
                    (config_id,),
                )
                row = cur.fetchone()
                if row is None:
                    self._fail_job(job_id, f"配置 {config_id} 不存在")
                    return
                config_json = row["config_json"]

        # Step 3: Build output path and ensure directory exists
        snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        model_path = self._build_model_path(config_id, snapshot_date)
        os.makedirs(os.path.dirname(model_path), exist_ok=True)

        # Step 4: Convert Windows path to WSL /mnt/... format
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "hmm_train_script.py")
        )
        wsl_script_path = self._to_wsl_path(script_path)
        wsl_output_path = self._to_wsl_path(os.path.abspath(model_path))

        config_json_str = json.dumps(
            config_json if isinstance(config_json, dict) else json.loads(config_json)
        )

        # 将 config_json 写入临时文件（避免 shell 转义问题）
        config_tmp_path = os.path.join(os.path.dirname(model_path), "_config_tmp.json")
        os.makedirs(os.path.dirname(config_tmp_path), exist_ok=True)
        with open(config_tmp_path, "w", encoding="utf-8") as f:
            f.write(config_json_str)
        wsl_config_tmp = self._to_wsl_path(os.path.abspath(config_tmp_path))

        db_password = os.environ["TDX_DB_PASSWORD"]
        wsl_cmd = (
            "source ~/miniconda3/etc/profile.d/conda.sh && "
            "conda activate rdagent-gpu && "
            f"python {wsl_script_path} "
            f"--config-file '{wsl_config_tmp}' "
            f"--output-path '{wsl_output_path}' "
            f"--db-password '{db_password}'"
        )

        try:
            result = subprocess.run(
                ["wsl", "bash", "-c", wsl_cmd],
                capture_output=True,
                text=True,
                timeout=1800,  # 30 minutes
                encoding="utf-8",
                errors="replace",
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"
                self._fail_job(job_id, error_msg)
                return

            # Parse stdout for summary + metrics
            stdout_text = result.stdout.strip()
            sector_count = 0
            metrics_json = None
            try:
                summary = json.loads(stdout_text.split("\n")[-1])
                sector_count = summary.get("sector_count", 0)
                if "metrics" in summary:
                    metrics_json = json.dumps(summary["metrics"])
            except (json.JSONDecodeError, IndexError):
                logger.warning("无法解析训练脚本输出: %s", stdout_text)

            # Step 5: 预计算 HMM 系数文件。Paper/Selection runtime 依赖这些
            # artifact，因此预计算失败时训练 job 必须失败，不能产生 ready 快照。
            self._precompute_coefficients_for_snapshot(model_path, config_json)

            # Step 6: Mark job completed and create snapshot
            with get_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Create snapshot (with metrics if available)
                    cur.execute(
                        """INSERT INTO model_train_snapshots
                               (config_id, model_path, sector_count, status, metrics_json)
                           VALUES (%s, %s, %s, 'completed', %s)
                           RETURNING snapshot_id""",
                        (config_id, model_path, sector_count, metrics_json),
                    )
                    snapshot_row = cur.fetchone()
                    snapshot_id = snapshot_row["snapshot_id"]

                    # Update job as completed with snapshot reference
                    cur.execute(
                        """UPDATE model_train_jobs
                           SET status = 'completed',
                               completed_at = NOW(),
                               snapshot_id = %s
                           WHERE job_id = %s""",
                        (snapshot_id, job_id),
                    )

            logger.info(
                "训练完成: job=%s, config=%s, snapshot=%s, sectors=%d",
                job_id, config_id, snapshot_id, sector_count,
            )

        except subprocess.TimeoutExpired:
            self._fail_job(job_id, "训练超时（超过 30 分钟）")
        except Exception as exc:
            self._fail_job(job_id, str(exc))
        finally:
            if os.path.exists(config_tmp_path):
                os.remove(config_tmp_path)

    def list_jobs(self, config_id: str) -> List[Dict[str, Any]]:
        """列出某配置的训练任务，按创建时间降序。"""
        sql = """
            SELECT job_id, config_id, snapshot_id, status,
                   started_at, completed_at, error_message
            FROM model_train_jobs
            WHERE config_id = %s
            ORDER BY started_at DESC NULLS LAST
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (config_id,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # 快照管理 (Task 4.2)
    # ------------------------------------------------------------------

    def list_snapshots(self, config_id: str) -> List[Dict[str, Any]]:
        """列出某配置的所有快照，按 trained_at 降序。"""
        sql = """
            SELECT s.snapshot_id, s.config_id, s.trained_at, s.model_path,
                   s.sector_count, s.status, s.metrics_json,
                   c.display_name AS config_display_name
            FROM model_train_snapshots s
            JOIN model_train_configs c ON c.config_id = s.config_id
            WHERE s.config_id = %s
            ORDER BY s.trained_at DESC
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (config_id,))
                rows = cur.fetchall()
        return [self._attach_snapshot_display_name(dict(r)) for r in rows]

    def get_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """获取快照详情，含 metrics_json。"""
        sql = """
            SELECT s.snapshot_id, s.config_id, s.trained_at, s.model_path,
                   s.sector_count, s.status, s.metrics_json,
                   c.display_name AS config_display_name
            FROM model_train_snapshots s
            JOIN model_train_configs c ON c.config_id = s.config_id
            WHERE s.snapshot_id = %s
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (snapshot_id,))
                row = cur.fetchone()
        return self._attach_snapshot_display_name(dict(row)) if row else None

    def delete_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """删除快照：DB 记录 + 模型文件 + 空目录。

        Returns
        -------
        dict
            {"deleted": True, "file_missing": bool}
        """
        # Fetch model_path before deleting
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ValueError(f"快照 {snapshot_id} 不存在")

        model_path = snapshot["model_path"]
        file_missing = False

        # Delete DB record first (also remove referencing jobs' snapshot_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Clear snapshot_id references in jobs
                cur.execute(
                    "UPDATE model_train_jobs SET snapshot_id = NULL WHERE snapshot_id = %s",
                    (snapshot_id,),
                )
                cur.execute(
                    "DELETE FROM model_train_snapshots WHERE snapshot_id = %s",
                    (snapshot_id,),
                )

        # Delete model file from filesystem
        if os.path.exists(model_path):
            try:
                os.remove(model_path)
                # Clean up empty parent directories
                parent = os.path.dirname(model_path)
                while parent and parent != self.models_dir:
                    try:
                        os.rmdir(parent)  # only removes if empty
                    except OSError:
                        break
                    parent = os.path.dirname(parent)
            except OSError as exc:
                logger.error("删除模型文件失败 %s: %s", model_path, exc)
                raise
        else:
            file_missing = True
            logger.warning("模型文件不存在: %s", model_path)

        return {"deleted": True, "file_missing": file_missing}

    def resolve_model_path(self, snapshot_id: str) -> Optional[str]:
        """根据 snapshot_id 查询 model_path。"""
        sql = "SELECT model_path FROM model_train_snapshots WHERE snapshot_id = %s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (snapshot_id,))
                row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # 滚动训练调度 (Task 8.1)
    # ------------------------------------------------------------------

    def update_cron(
        self, config_id: str, cron_expr: Optional[str], enabled: bool
    ) -> Dict[str, Any]:
        """更新滚动训练计划的 cron 表达式和启用状态。

        Returns
        -------
        dict
            更新后的配置记录。
        """
        sql = """
            UPDATE model_train_configs
            SET cron_expression = %s, cron_enabled = %s
            WHERE config_id = %s
            RETURNING config_id, model_type, display_name, config_json,
                      cron_expression, cron_enabled, created_at
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (cron_expr, enabled, config_id))
                row = cur.fetchone()
        if row is None:
            raise ValueError(f"配置 {config_id} 不存在")
        return dict(row)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _fail_job(self, job_id: str, error_message: str) -> None:
        """将 job 标记为 failed 并记录错误信息。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE model_train_jobs
                       SET status = 'failed',
                           completed_at = NOW(),
                           error_message = %s
                       WHERE job_id = %s""",
                    (error_message, job_id),
                )
        logger.error("训练失败: job=%s, error=%s", job_id, error_message)

    def _precompute_coefficients_for_snapshot(
        self, model_path: str, config_json: Any,
    ) -> None:
        """训练完成后，自动为所有 preset 预计算系数文件.

        文件存放在 model_path 同级目录:
            coefficients_{preset}_{test_start}_{backtest_end}.json

        使用 WSL 子进程执行 precompute_hmm_coefficients.py --output-path。
        """
        cfg = config_json if isinstance(config_json, dict) else json.loads(config_json)

        # 获取 signal_presets（可能在 config_json 中，也可能用默认值）
        signal_presets = cfg.get("signal_presets", {})
        if not signal_presets:
            signal_presets = {
                "preset_A": {"trending": 1.05, "neutral": 1.00, "fading": 0.96},
                "preset_B": {"trending": 1.10, "neutral": 1.00, "fading": 0.92},
            }

        rolling_meta = cfg.get("rolling_training") if isinstance(cfg.get("rolling_training"), dict) else {}
        strict_range = bool(rolling_meta.get("precompute_coefficients_required"))
        test_start = cfg.get("coefficient_start") or cfg.get("val_start")
        backtest_end = cfg.get("coefficient_end") or cfg.get("val_end")
        if not test_start or not backtest_end:
            if strict_range:
                raise RuntimeError(
                    "rolling HMM training requires explicit coefficient_start/coefficient_end "
                    "or val_start/val_end for coefficient precompute"
                )
            test_start = "2024-07-01"
            test_end = cfg.get("test_end", "2026-04-28")
            backtest_end = (
                datetime.strptime(str(test_end), "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")
        test_start = _coerce_date(test_start, field_name="coefficient_start").isoformat()
        backtest_end = _coerce_date(backtest_end, field_name="coefficient_end").isoformat()
        if test_start > backtest_end:
            raise RuntimeError(
                f"invalid HMM coefficient precompute range: {test_start} > {backtest_end}"
            )

        model_dir = os.path.dirname(os.path.abspath(model_path))
        wsl_model_path = self._to_wsl_path(os.path.abspath(model_path))
        wsl_script = self._to_wsl_path(
            os.path.abspath(os.path.join(
                os.path.dirname(__file__), "..", "..", "scripts",
                "precompute_hmm_coefficients.py",
            ))
        )

        # DB 连接参数
        db_password = os.environ["TDX_DB_PASSWORD"]

        generated_count = 0
        for preset_key, preset_coeffs in signal_presets.items():
            actual_coeffs = self._extract_signal_preset_coefficients(
                {"signal_presets": {preset_key: preset_coeffs}},
                str(preset_key),
            )

            coeff_filename = f"coefficients_{preset_key}_{test_start}_{backtest_end}.json"
            coeff_path = os.path.join(model_dir, coeff_filename)
            wsl_coeff_path = self._to_wsl_path(coeff_path)

            stdin_params = {
                "model_path": wsl_model_path,
                "test_start": test_start,
                "backtest_end": backtest_end,
                "preset_coeffs": actual_coeffs,
                "preset_key": preset_key,
                "db_host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
                "db_port": int(os.getenv("TDX_DB_PORT", "5432")),
                "db_name": os.getenv("TDX_DB_NAME", "aistock"),
                "db_user": os.getenv("TDX_DB_USER", "postgres"),
                "db_password": db_password,
            }

            wsl_cmd = (
                "source ~/miniconda3/etc/profile.d/conda.sh && "
                "conda activate rdagent-gpu && "
                f"python {wsl_script} --output-path '{wsl_coeff_path}'"
            )

            logger.info(f"预计算系数: preset={preset_key}, output={coeff_filename}")
            proc = subprocess.run(
                ["wsl", "bash", "-c", wsl_cmd],
                input=json.dumps(stdin_params, ensure_ascii=False),
                capture_output=True, text=True, timeout=300,
                encoding="utf-8", errors="replace",
            )

            if proc.stderr:
                for line in proc.stderr.strip().splitlines():
                    logger.info(f"[HMM-precompute] {line}")

            if proc.returncode != 0:
                logger.error(f"预计算失败 preset={preset_key}: {proc.stderr}")
                raise RuntimeError(f"HMM 系数预计算失败 ({preset_key}): {proc.stderr}")

            logger.info(f"预计算完成: {coeff_filename}")
            generated_count += 1

        if generated_count == 0:
            raise RuntimeError("HMM coefficient precompute produced no artifacts")

    @staticmethod
    def _to_wsl_path(win_path: str) -> str:
        """Convert a Windows absolute path to WSL /mnt/... format.

        Example: C:\\Users\\foo\\bar → /mnt/c/Users/foo/bar
        """
        path = win_path.replace("\\", "/")
        if len(path) >= 2 and path[1] == ":":
            drive = path[0].lower()
            return f"/mnt/{drive}{path[2:]}"
        return path
