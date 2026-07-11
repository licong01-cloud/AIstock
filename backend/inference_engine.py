"""Inference Layer for AIstock.

This module implements the core inference logic required to load RD-Agent 
evolved models and produce trading signals using the Data Service Layer.
Strictly follows Section 7.10 of Phase3_Detail_Design_RD-Agent_AIstock_v1.md
and the requirements in 模型权重文件定位方案_v2.md.
"""

import inspect
import json
import logging
import math
import pickle
import os
import tempfile
import shutil
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .db.pg_pool import get_conn
from .data_service.api import get_history_window
from .data_service.cache import get_selection_data_cache
from .data_service.preprocessor import (
    compute_precomputed_factors,
    validate_precomputed_factors,
    get_required_data_window,
    check_data_window_sufficient,
)
from .data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP
from .services.factor_validator import FactorValidator
from .services.strategy_package.workspace_policy import (
    ensure_not_forbidden_worker_workspace_path,
    is_under_allowed_artifact_root,
)

logger = logging.getLogger("aistock.inference")
LAST_STRICT_FEATURE_FILTER: dict[str, Any] | None = None

def _strict_inference_enabled() -> bool:
    return str(os.environ.get("AISTOCK_STRICT_INFERENCE", "")).strip().lower() in {"1", "true", "yes", "on"}


def _validate_qe_runtime_workspace_path(workspace_path: str) -> Path:
    path = Path(workspace_path)
    ensure_not_forbidden_worker_workspace_path(path, purpose="InferenceEngine QE experiment workspace_path")
    if not is_under_allowed_artifact_root(path, extra_roots=[Path.cwd() / "rdagent_assets" / "strategy_package_runtime"]):
        raise ValueError(
            "QE experiment inference workspace_path must be an AIstock-owned runtime cache; "
            "direct QE/RD-Agent worker workspace paths are forbidden"
        )
    return path


def _drop_invalid_feature_rows_for_strict(X: pd.DataFrame) -> pd.DataFrame:
    """Drop unscorable rows in strict mode instead of filling missing features."""

    global LAST_STRICT_FEATURE_FILTER
    if not _strict_inference_enabled():
        LAST_STRICT_FEATURE_FILTER = None
        return X

    numeric = X.copy()
    for col in numeric.columns:
        if numeric[col].dtype == "object":
            numeric[col] = pd.to_numeric(numeric[col], errors="coerce")
    values = numeric.to_numpy(dtype="float64", copy=False)
    invalid_mask = pd.isna(values) | ~np.isfinite(values)
    if not invalid_mask.any():
        LAST_STRICT_FEATURE_FILTER = {
            "enabled": True,
            "input_rows": int(len(numeric)),
            "kept_rows": int(len(numeric)),
            "dropped_rows": 0,
            "invalid_cell_count": 0,
            "invalid_columns": [],
        }
        return numeric

    invalid_rows = invalid_mask.any(axis=1)
    invalid_columns = sorted({str(numeric.columns[col_idx]) for _, col_idx in zip(*np.where(invalid_mask))})
    invalid_column_counts = invalid_mask.sum(axis=0)
    invalid_column_details = [
        {"column": str(numeric.columns[idx]), "invalid_count": int(count)}
        for idx, count in sorted(
            enumerate(invalid_column_counts),
            key=lambda item: int(item[1]),
            reverse=True,
        )
        if int(count) > 0
    ]
    dropped_rows = int(invalid_rows.sum())
    kept_rows = int((~invalid_rows).sum())
    LAST_STRICT_FEATURE_FILTER = {
        "enabled": True,
        "input_rows": int(len(numeric)),
        "kept_rows": kept_rows,
        "dropped_rows": dropped_rows,
        "invalid_cell_count": int(invalid_mask.sum()),
        "invalid_columns": invalid_columns[:200],
        "invalid_column_details": invalid_column_details[:200],
    }
    if kept_rows <= 0:
        raise ValueError(
            "strict StrategyPackage inference found no fully-scored instruments; "
            "refusing to fill missing features with defaults",
            LAST_STRICT_FEATURE_FILTER,
        )
    logger.warning(
        "strict StrategyPackage inference dropped %s unscorable rows with missing/non-finite features; "
        "kept %s rows and did not fill defaults",
        dropped_rows,
        kept_rows,
    )
    return numeric.loc[~invalid_rows]


def _inference_natural_days_needed(required_window: int) -> int:
    """Convert required trading-day lookback to natural days for DB loading.

    The default keeps the historical behavior. StrategyPackage strict WSL
    inference can opt in to a wider audited window through environment variables
    so long-window factors do not fail only because holidays reduced the loaded
    trading-day count. This is not a fallback: strict mode still fails if the
    wider window cannot provide enough rows.
    """

    multiplier_raw = os.environ.get("AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER", "1.5")
    buffer_raw = os.environ.get("AISTOCK_INFERENCE_NATURAL_DAY_BUFFER", "10")
    try:
        multiplier = float(multiplier_raw)
        buffer_days = int(buffer_raw)
    except ValueError as exc:
        raise ValueError(
            "invalid inference data-window configuration: "
            f"AISTOCK_INFERENCE_NATURAL_DAY_MULTIPLIER={multiplier_raw!r}, "
            f"AISTOCK_INFERENCE_NATURAL_DAY_BUFFER={buffer_raw!r}"
        ) from exc
    if multiplier <= 0 or buffer_days < 0:
        raise ValueError(
            "invalid inference data-window configuration: multiplier must be positive "
            "and buffer must be non-negative"
        )
    return int(math.ceil(required_window * multiplier)) + buffer_days


def _build_score_frame_for_scored_features(scored_features: pd.DataFrame, scores: Any) -> pd.DataFrame:
    """Build a score frame aligned to the rows that were actually scored."""

    score_values = np.asarray(scores)
    if score_values.ndim > 1:
        score_values = score_values[:, -1]
    if len(score_values) != len(scored_features):
        raise ValueError(
            "model score length mismatch after strict feature filtering; "
            "refusing to pad, truncate, or align scores to unscorable instruments: "
            f"scores={len(score_values)}, scored_rows={len(scored_features)}"
        )
    df_scores = pd.DataFrame(index=scored_features.index)
    df_scores["score"] = score_values
    return df_scores


def _safe_get_datetime_level(df_or_index) -> pd.Index:
    """安全地从DataFrame或Index中获取datetime层级
    
    处理以下情况：
    1. MultiIndex且有'datetime' name -> 返回该level
    2. MultiIndex但没有'datetime' name -> 返回第0个level（假设是日期）
    3. 单列索引且name为'datetime' -> 返回index本身
    4. 其他情况 -> 返回index（尝试转换）
    """
    idx = df_or_index.index if hasattr(df_or_index, 'index') else df_or_index
    
    if isinstance(idx, pd.MultiIndex):
        if "datetime" in idx.names:
            return idx.get_level_values("datetime")
        else:
            # 返回第一个level，通常是日期
            return idx.get_level_values(0)
    else:
        # 单列索引
        if idx.name == "datetime" or idx.name is None:
            return idx
        else:
            # 尝试返回index本身
            return idx


# ============================================================
# 可复用的模块级函数（从 _run_inference_impl 中提取）
# ============================================================

def load_model_from_pkl(model_file: Path) -> Tuple[Any, str, Any, int]:
    """加载模型 pkl 文件，检测类型，返回 (model, model_kind, inner_model, num_features).

    model_kind: "lgb" | "pytorch" | "qlib_generic"
    """
    import sys

    # 将模型所在目录添加到 sys.path，以便 pickle 能找到自定义类
    model_dir = str(Path(model_file).parent)
    if model_dir not in sys.path:
        sys.path.insert(0, model_dir)

    with open(model_file, "rb") as f:
        model = pickle.load(f)

    # PyTorch 设备处理
    try:
        import torch

        device = "cuda" if torch.cuda.is_available() else "cpu"

        if hasattr(model, "dnn_model") and hasattr(model.dnn_model, "parameters"):
            try:
                param = next(model.dnn_model.parameters())
                if param.device.type != device:
                    if device == "cuda":
                        model.dnn_model = model.dnn_model.cuda()
                    else:
                        model.dnn_model = model.dnn_model.cpu()
            except StopIteration:
                pass

        # eval 模式
        if hasattr(model, "dnn_model") and isinstance(model.dnn_model, torch.nn.Module):
            model.dnn_model.eval()
        elif isinstance(model, torch.nn.Module):
            model.eval()
    except ModuleNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"PyTorch 模型初始化时出错: {e}")

    # 确定性类型判断
    model_kind = "unknown"
    inner_model = None
    num_features_expected = 0

    if hasattr(model, "model") and model.model is not None:
        inner_model = model.model
        model_kind = "lgb"
        val = getattr(inner_model, "num_feature", 0)
        if callable(val):
            num_features_expected = val()
        else:
            num_features_expected = int(val) if val else 0
        if num_features_expected == 0:
            num_features_expected = getattr(inner_model, "n_features_", 0)
        logger.info(f"检测到LGB模型 (inner: {type(inner_model).__name__}), 特征数={num_features_expected}")
    elif hasattr(model, "dnn_model") and model.dnn_model is not None:
        inner_model = model.dnn_model
        model_kind = "pytorch"
        try:
            first_layer = next(model.dnn_model.parameters(), None)
            if first_layer is not None:
                num_features_expected = first_layer.shape[-1]
        except Exception:
            pass
        logger.info(f"检测到PyTorch模型 (inner: {type(inner_model).__name__}), 特征数={num_features_expected}")
    elif hasattr(model, "predict") and callable(model.predict):
        model_kind = "qlib_generic"
        logger.info(f"检测到通用Qlib模型: {type(model).__name__}")
    else:
        raise ValueError(
            f"无法识别的模型类型: {type(model).__name__}。"
            f"期望 LGBModel(有model属性) 或 GeneralPTNN(有dnn_model属性) 或其他Qlib Model子类(有predict方法)。"
            f"模型属性: {[a for a in dir(model) if not a.startswith('_')]}"
        )

    return model, model_kind, inner_model, num_features_expected


def assemble_features(
    alpha_subset: pd.DataFrame,
    df_factors: pd.DataFrame,
    factor_order: List[str],
    alpha158_feats: List[str],
    dynamic_feats: List[str],
) -> pd.DataFrame:
    """按 factor_order 顺序组装 alpha158 + dynamic 因子 → 单一 DataFrame."""
    final_cols_data: Dict[str, pd.Series] = {}

    # 索引对齐
    if len(alpha158_feats) > 0 and not df_factors.empty and not alpha_subset.index.equals(df_factors.index):
        common_index = alpha_subset.index.intersection(df_factors.index)
        if len(common_index) == 0:
            raise ValueError(
                f"Alpha158因子和SOTA因子没有共同的索引。\n"
                f"Alpha158索引范围: {alpha_subset.index.min()} 到 {alpha_subset.index.max()}\n"
                f"SOTA因子索引范围: {df_factors.index.min()} 到 {df_factors.index.max()}"
            )
        alpha_subset = alpha_subset.loc[common_index]
        df_factors = df_factors.loc[common_index]

    for feat_name in factor_order:
        if feat_name in alpha158_feats:
            if feat_name not in alpha_subset.columns:
                raise ValueError(f"Alpha158 因子 {feat_name} 计算失败，无法找到该列。")
            col_data = alpha_subset[feat_name]
            if not isinstance(col_data, pd.Series):
                raise ValueError(f"Alpha158 因子 {feat_name} 数据类型错误: {type(col_data)}")
            final_cols_data[feat_name] = col_data
        elif feat_name in dynamic_feats:
            if isinstance(df_factors.columns, pd.MultiIndex):
                col_key = ("feature", feat_name)
                if col_key not in df_factors.columns:
                    raise ValueError(f"SOTA 动态因子 {feat_name} 无法找到（MultiIndex列）。")
                col_data = df_factors[col_key]
            else:
                if feat_name not in df_factors.columns:
                    raise ValueError(f"SOTA 动态因子 {feat_name} 无法找到。可用: {list(df_factors.columns)}")
                col_data = df_factors[feat_name]
            if not isinstance(col_data, pd.Series):
                raise ValueError(f"SOTA 动态因子 {feat_name} 数据类型错误: {type(col_data)}")
            final_cols_data[feat_name] = col_data
        else:
            raise ValueError(f"因子 {feat_name} 既不在alpha158_factors中，也不在dynamic_factors中。")

    final_index = alpha_subset.index if len(alpha158_feats) > 0 else df_factors.index
    df_combined = pd.DataFrame(final_cols_data, index=final_index)

    # 确保列顺序严格匹配
    available_cols = [c for c in factor_order if c in df_combined.columns]
    df_combined = df_combined[available_cols]

    if set(df_combined.columns.tolist()) != set(factor_order):
        missing = [c for c in factor_order if c not in df_combined.columns]
        if missing:
            logger.error(f"assemble_features: 缺失列 {missing}")

    return df_combined


def predict_scores(
    model: Any,
    inner_model: Any,
    model_kind: str,
    X: pd.DataFrame,
) -> np.ndarray:
    """模型预测三分支: lgb / pytorch / qlib_generic → 返回 1-D scores 数组."""
    # 确保数值类型
    X = X.copy()
    for col in X.columns:
        if X[col].dtype == "object":
            X[col] = pd.to_numeric(X[col], errors="coerce")
    if _strict_inference_enabled():
        values = X.to_numpy(dtype="float64", copy=False)
        invalid_mask = pd.isna(values) | ~np.isfinite(values)
        if invalid_mask.any():
            invalid_columns = sorted(
                {
                    str(X.columns[col_idx])
                    for _, col_idx in zip(*np.where(invalid_mask))
                }
            )
            raise ValueError(
                "strict StrategyPackage inference found missing or non-finite model features; "
                "refusing to fill with defaults",
                {"invalid_columns": invalid_columns[:50], "invalid_cell_count": int(invalid_mask.sum())},
            )
    else:
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)

    if model_kind == "lgb":
        scores = inner_model.predict(X.values)
    elif model_kind == "pytorch":
        import torch

        inner_model.eval()
        with torch.no_grad():
            x_values = X.values.astype("float32")
            x_tensor = torch.tensor(x_values, dtype=torch.float32)

            model_type_name = type(inner_model).__name__
            is_seq = any(kw in model_type_name for kw in ["GRU", "LSTM", "RNN", "Sequence", "Recurrent"])
            if is_seq:
                x_tensor = x_tensor.unsqueeze(1)

            if torch.cuda.is_available():
                x_tensor = x_tensor.cuda()
            elif hasattr(model, "device"):
                x_tensor = x_tensor.to(model.device)

            output = inner_model(x_tensor)
            scores = output.cpu().numpy()

            if len(scores.shape) == 3:
                scores = scores[:, -1, :]
            if len(scores.shape) == 2 and scores.shape[1] > 1:
                scores = scores[:, 0]
    elif model_kind == "qlib_generic":
        scores = model.predict(X)
    else:
        raise RuntimeError(f"无法预测: 未知的模型类型 model_kind={model_kind}")

    if hasattr(scores, "values"):
        scores = scores.values
    if hasattr(scores, "flatten"):
        scores = scores.flatten()

    return scores


def _apply_saved_qe_infer_processors(
    X: pd.DataFrame,
    *,
    task_dir: Path,
    primary_assets: dict[str, Any],
) -> pd.DataFrame:
    """Apply fitted Qlib infer processors packaged with the QE model."""
    relpath = primary_assets.get("dataset_processor_relpath") or primary_assets.get("dataset_relpath")
    if not relpath:
        return X
    processor_path = task_dir / str(relpath)
    if not processor_path.exists() or not processor_path.is_file():
        if _strict_inference_enabled():
            raise ValueError(f"strict inference dataset processor artifact is missing: {processor_path}")
        return X

    import sys

    search_paths = [
        task_dir,
        task_dir / "model",
        Path(__file__).resolve().parent / "services" / "quantevolver",
    ]
    for path in search_paths:
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    with open(processor_path, "rb") as f:
        dataset = pickle.load(f)
    handler = getattr(dataset, "handler", None)
    processors = list(getattr(handler, "infer_processors", []) or [])
    if not processors:
        return X

    original_columns = list(X.columns)
    processed = X.copy()
    if not isinstance(processed.columns, pd.MultiIndex):
        processed.columns = pd.MultiIndex.from_product([["feature"], original_columns])
    for processor in processors:
        processed = processor(processed)

    if isinstance(processed.columns, pd.MultiIndex) and "feature" in processed.columns.get_level_values(0):
        processed = processed["feature"]
    processed = processed[original_columns]
    logger.info(
        "applied saved QE infer processors: artifact=%s processors=%s",
        processor_path,
        [type(processor).__name__ for processor in processors],
    )
    return processed


def save_signals_to_db(
    task_run_id: str,
    loop_id: int,
    trade_date: datetime,
    df_scores: pd.DataFrame,
) -> None:
    """将选股结果存入 trading.rdagent_signal（从 InferenceEngine._save_signals_to_db 提取）."""
    strategy_id_loop = str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_loop:{task_run_id}:{loop_id}"))

    source_sql = """
        INSERT INTO trading.strategy_source (source_type, name, description)
        VALUES ('rdagent', 'RD-Agent', 'RD-Agent generated strategies')
        ON CONFLICT (source_type) DO NOTHING
    """
    strategy_sql = """
        INSERT INTO trading.strategy (strategy_id, source_id, source_strategy_key, strategy_name, strategy_kind, output_mode, created_at)
        VALUES (%s, (SELECT source_id FROM trading.strategy_source WHERE source_type = 'rdagent'), %s, %s, 'portfolio', 'topk', NOW())
        ON CONFLICT (strategy_id) DO NOTHING
    """
    v_sql = """
        INSERT INTO trading.strategy_version (strategy_version_id, strategy_id, version_tag, artifact_root_path, import_status, created_at)
        VALUES (%s, %s, 'replay', %s, 'imported', NOW())
        ON CONFLICT (strategy_id, version_tag) DO UPDATE SET artifact_root_path = EXCLUDED.artifact_root_path
    """

    td = trade_date.date() if isinstance(trade_date, datetime) else trade_date

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(source_sql)

                strategy_name = f"RDAgent_{task_run_id}_Loop{loop_id}"
                source_strategy_key = f"{task_run_id}:{loop_id}"
                cur.execute(strategy_sql, (strategy_id_loop, source_strategy_key, strategy_name))

                sv_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_version:{task_run_id}:{loop_id}:replay"))
                cur.execute(v_sql, (sv_id, strategy_id_loop, f"rdagent_tasks/{task_run_id}"))

                cur.execute(
                    "SELECT strategy_version_id FROM trading.strategy_version WHERE strategy_id = %s AND version_tag = 'replay'",
                    (strategy_id_loop,),
                )
                row = cur.fetchone()
                if not row:
                    logger.error(f"无法定位 strategy_version_id: {strategy_id_loop}")
                    return
                sv_id = row[0]

                df_sorted = df_scores.sort_values(by="score", ascending=False)
                records = []
                for rank, (idx, r) in enumerate(df_sorted.iterrows(), 1):
                    instrument = idx[1] if isinstance(idx, tuple) else idx
                    records.append((strategy_id_loop, sv_id, td, instrument, float(r["score"]), rank, "topk"))

                from psycopg2.extras import execute_values
                execute_values(cur, """
                    INSERT INTO trading.rdagent_signal (strategy_id, strategy_version_id, trade_date, symbol, score, rank, output_mode)
                    VALUES %s
                    ON CONFLICT (strategy_version_id, trade_date, symbol) DO UPDATE SET score = EXCLUDED.score, rank = EXCLUDED.rank
                """, records)
            conn.commit()
        logger.info(f"成功保存 {len(records)} 条信号到数据库")
    except Exception as e:
        logger.error(f"保存信号到数据库失败: {e}")


class InferenceEngine:
    """Core engine to handle model loading and prediction."""

    def __init__(self, factor_validator: Optional[FactorValidator] = None):
        self.initialized = False
        self.validator = factor_validator or FactorValidator()
        self.assets_root = (Path(__file__).resolve().parents[1] / "rdagent_assets" / "rdagent_tasks").resolve()

    def _get_latest_available_trade_date(self, target_date: datetime) -> datetime:
        """从数据库查询小于等于 target_date 的最近一个交易日"""
        sql = "SELECT cal_date FROM market.trading_calendar WHERE cal_date <= %s AND is_trading = TRUE ORDER BY cal_date DESC LIMIT 1"
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (target_date.date(),))
                    row = cur.fetchone()
                    if row:
                        return datetime.combine(row[0], datetime.min.time())
        except Exception as e:
            if _strict_inference_enabled():
                raise ValueError(f"strict inference requires trading calendar query success: {e}") from e
            logger.warning(f"查询交易日历失败，使用原日期: {e}")
        if _strict_inference_enabled():
            raise ValueError(f"strict inference found no trading calendar date <= {target_date.date()}")
        return target_date

    def _resolve_inference_start_date(
        self,
        actual_date: datetime,
        required_window: int,
        buffer_days: int = 5,
    ) -> tuple[datetime, str]:
        """按交易日历反推推理读取起点，避免节假日压缩交易日窗口。"""

        required_trading_days = required_window + buffer_days
        if required_trading_days <= 0:
            raise ValueError("required_window + buffer_days must be positive")

        sql = """
            SELECT cal_date
            FROM market.trading_calendar
            WHERE cal_date <= %s AND is_trading = TRUE
            ORDER BY cal_date DESC
            OFFSET %s LIMIT 1
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (actual_date.date(), required_trading_days - 1))
                    row = cur.fetchone()
                    if row:
                        return datetime.combine(row[0], datetime.min.time()), "trading_calendar"
        except Exception as e:
            if _strict_inference_enabled():
                raise ValueError(
                    "strict inference requires trading-calendar start-date resolution: "
                    f"actual_date={actual_date.date()}, required_trading_days={required_trading_days}, error={e}"
                ) from e
            logger.warning(f"按交易日历反推推理起点失败，将使用自然日估算: {e}")

        if _strict_inference_enabled():
            raise ValueError(
                "strict inference found insufficient trading-calendar history: "
                f"actual_date={actual_date.date()}, required_trading_days={required_trading_days}"
            )

        natural_days_needed = _inference_natural_days_needed(required_window)
        return actual_date - timedelta(days=natural_days_needed), "natural_days_fallback"

    def _validate_data_freshness(self, df_history: pd.DataFrame, requested_date: datetime, actual_date: datetime) -> None:
        """验证数据时效性，确保数据不滞后
        
        Args:
            df_history: 历史数据DataFrame
            requested_date: 用户请求的日期
            actual_date: 归一化后的实际数据日期
        
        Raises:
            ValueError: 当数据滞后超过允许范围时
        """
        if df_history.empty:
            raise ValueError("历史数据为空，无法进行选股。请检查数据源是否正常。")
        
        # 获取数据中的最新日期
        try:
            # 处理不同的index结构
            if isinstance(df_history.index, pd.MultiIndex):
                # MultiIndex情况，获取datetime level
                if "datetime" in df_history.index.names:
                    latest_date = df_history.index.get_level_values("datetime").max()
                else:
                    # 尝试第一个level
                    latest_date = df_history.index.get_level_values(0).max()
            elif hasattr(df_history.index, 'name'):
                # 单列索引情况
                if df_history.index.name == "datetime":
                    latest_date = df_history.index.max()
                else:
                    # 尝试从列中获取日期
                    if "datetime" in df_history.columns:
                        latest_date = pd.to_datetime(df_history["datetime"]).max()
                    else:
                        latest_date = df_history.index.max()
            else:
                # 最后的fallback
                latest_date = df_history.index.max()
                
            if not isinstance(latest_date, datetime):
                latest_date = pd.to_datetime(latest_date).to_pydatetime()
                
        except Exception as e:
            raise ValueError(f"无法从历史数据中提取日期信息: {e}")
        
        # 计算滞后天数（相对于归一化后的实际日期）
        lag_days = (actual_date.date() - latest_date.date()).days
        
        # 允许的最大滞后天数（默认3天）
        max_lag_days = int(os.environ.get("AISTOCK_MAX_DATA_LAG_DAYS", "3"))
        
        if lag_days > max_lag_days:
            raise ValueError(
                f"数据滞后严重，无法进行选股！\n"
                f"  - 用户请求日期: {requested_date.date()}\n"
                f"  - 归一化交易日: {actual_date.date()}\n"
                f"  - 数据最新日期: {latest_date.date()}\n"
                f"  - 数据滞后天数: {lag_days} 天（允许最大滞后 {max_lag_days} 天）\n"
                f"请更新数据后再进行选股。"
            )
        
        if lag_days > 0:
            logger.warning(
                f"数据存在滞后: 请求日期={requested_date.date()}, "
                f"归一化交易日={actual_date.date()}, "
                f"数据最新日期={latest_date.date()}, "
                f"滞后={lag_days}天（允许范围内）"
            )
        else:
            logger.info(
                f"数据时效性检查通过: 请求日期={requested_date.date()}, "
                f"归一化交易日={actual_date.date()}, "
                f"数据最新日期={latest_date.date()}"
            )

    def _get_default_universe_excluding_st(self, trade_date: datetime | None = None) -> list[str]:
        """Use the platform ST PIT universe for live/latest-data inference."""
        effective_date = (trade_date or datetime.now()).date()
        try:
            from .services.stock_universe_pit_service import StockUniversePitService

            universe = StockUniversePitService().get_eligible_codes(trade_date=effective_date, ensure=True)
            stock_count = len(universe)
            logger.info(
                "stock universe resolved from platform ST PIT: "
                f"trade_date={effective_date}, count={stock_count}"
            )
            if stock_count <= 0 and _strict_inference_enabled():
                raise ValueError(f"strict inference ST PIT universe is empty for {effective_date}")
            return universe
        except Exception as e:
            if _strict_inference_enabled():
                raise ValueError(f"strict inference ST PIT universe query failed: {e}") from e
            logger.error(f"ST PIT universe query failed, falling back to legacy active SH/SZ universe: {e}")

        sql = """
            SELECT ts_code FROM market.stock_basic
            WHERE (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
              AND list_status = 'L'
            ORDER BY ts_code
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall() or []
        return [str(r[0]) for r in rows if r and r[0]]
    def _compute_alpha158_last_day_only(self, df_history: pd.DataFrame, col_list: List[str]) -> pd.DataFrame:
        """优化版本：只计算最后一天的 Alpha158 因子值，大幅降低内存占用
        
        内存优化策略：
        1. 只返回最后一天的因子值（选股只需要最新数据）
        2. 对于滚动窗口计算，只计算最后一个窗口
        3. 及时释放中间结果
        4. 避免重复计算
        
        预期效果：内存占用从 50GB+ 降低到 < 2GB
        """
        import gc
        
        df = df_history
        if not isinstance(df.index, pd.MultiIndex) or "instrument" not in df.index.names or "datetime" not in df.index.names:
            raise ValueError("df_history index 必须为 MultiIndex(datetime, instrument)")
        
        # 获取最后一天的日期
        last_date = _safe_get_datetime_level(df).max()

        def _pick_col(candidates: List[str]) -> str:
            for c in candidates:
                if c in df.columns:
                    return c
            raise KeyError(f"Missing columns: {candidates}")

        try:
            close_col = _pick_col(["close"])
            open_col = _pick_col(["open"])
            high_col = _pick_col(["high"])
            low_col = _pick_col(["low"])
            vol_col = _pick_col(["volume", "vol"])
        except Exception as e:
            logger.error(f"缺少必需 OHLCV 列: {e}")
            raise ValueError(f"缺少必需 OHLCV 列: {e}")

        close = df[close_col]
        vol = df[vol_col]
        eps = 1e-12

        g_close = close.groupby(level="instrument")
        g_vol = vol.groupby(level="instrument")
        ret = g_close.pct_change()

        def _rolling_reg_r2_and_resi_last(y: pd.Series, win: int) -> Tuple[pd.Series, pd.Series]:
            """优化版本：只计算每只股票最后一个窗口的回归结果"""
            def _calc_last_window(group):
                arr = group.values
                if len(arr) < win:
                    return pd.Series([np.nan, np.nan], index=['r2', 'resi'])
                
                # 只取最后 win 个数据点
                yv = arr[-win:]
                x = np.arange(win)
                
                try:
                    slope, intercept = np.polyfit(x, yv, 1)
                    y_hat = slope * x + intercept
                    resid = yv[-1] - y_hat[-1]
                    sst = np.sum((yv - np.mean(yv))**2)
                    sse = np.sum((yv - y_hat)**2)
                    r2 = 1 - sse / (sst + eps)
                    return pd.Series([r2, resid], index=['r2', 'resi'])
                except Exception:
                    return pd.Series([np.nan, np.nan], index=['r2', 'resi'])
            
            # 对每只股票计算最后一个窗口
            result = y.groupby(level="instrument").apply(_calc_last_window)
            
            # groupby().apply() 返回 Series with MultiIndex: (instrument, ['r2', 'resi'])
            # 需要将其重塑为 DataFrame
            if isinstance(result, pd.Series) and isinstance(result.index, pd.MultiIndex):
                # result 是 MultiIndex Series: (instrument, metric)
                # 使用 unstack 将其转换为 DataFrame
                result_df = result.unstack()
                # 现在 result_df 是 DataFrame，index=instrument, columns=['r2', 'resi']
                r2_values = result_df['r2'].values
                resi_values = result_df['resi'].values
                instruments = result_df.index.values
            elif isinstance(result, pd.DataFrame) and len(result.columns) == 2:
                # 如果已经是 DataFrame（某些 pandas 版本可能直接返回 DataFrame）
                r2_values = result['r2'].values
                resi_values = result['resi'].values
                instruments = result.index.values
            else:
                raise ValueError(
                    f"groupby().apply() 返回了意外的结果类型: {type(result)}, "
                    f"index type: {type(result.index)}, "
                    f"shape: {result.shape if hasattr(result, 'shape') else 'N/A'}"
                )
            
            # 创建最后一天的 MultiIndex
            last_day_index = pd.MultiIndex.from_product(
                [[last_date], instruments],
                names=['datetime', 'instrument']
            )
            
            r2_series = pd.Series(r2_values, index=last_day_index)
            resi_series = pd.Series(resi_values, index=last_day_index)
            
            return r2_series, resi_series

        def _corr_series_last(a: pd.Series, b: pd.Series, win: int) -> pd.Series:
            """优化版本：只计算每只股票最后一个窗口的相关系数"""
            def _calc_last_corr(inst):
                a_inst = a.xs(inst, level="instrument")
                b_inst = b.xs(inst, level="instrument")
                arr_a = a_inst.values
                arr_b = b_inst.values
                if len(arr_a) < win or len(arr_b) < win:
                    return np.nan
                
                # 只取最后 win 个数据点
                a_win = arr_a[-win:]
                b_win = arr_b[-win:]
                
                try:
                    return np.corrcoef(a_win, b_win)[0, 1]
                except Exception:
                    return np.nan
            
            # 对每只股票计算最后一个窗口的相关系数
            instruments = a.index.get_level_values("instrument").unique()
            corr_values = [_calc_last_corr(inst) for inst in instruments]
            
            # 创建最后一天的 MultiIndex
            last_day_index = pd.MultiIndex.from_product(
                [[last_date], instruments],
                names=['datetime', 'instrument']
            )
            
            return pd.Series(corr_values, index=last_day_index)

        # 获取最后一天的数据（用于非滚动因子）
        last_day_data = df.loc[last_date]
        last_day_index = pd.MultiIndex.from_product(
            [[last_date], last_day_data.index],
            names=['datetime', 'instrument']
        )
        
        out: Dict[str, pd.Series] = {}
        
        # 非滚动因子：直接从最后一天计算
        if "KLEN" in col_list:
            klen_values = ((last_day_data[high_col] - last_day_data[low_col]) / 
                           last_day_data[open_col].replace(0.0, np.nan)).values
            out["KLEN"] = pd.Series(klen_values, index=last_day_index)
        
        if "KLOW" in col_list:
            min_val = pd.DataFrame({
                "open": last_day_data[open_col], 
                "close": last_day_data[close_col]
            }).min(axis=1)
            klow_values = ((min_val - last_day_data[low_col]) / 
                           last_day_data[open_col].replace(0.0, np.nan)).values
            out["KLOW"] = pd.Series(klow_values, index=last_day_index)

        # 资金流因子（如果有数据）
        if "MF_Main_Net_Amt_Ratio_5D" in col_list:
            lg_buy = df.get("mf_lg_buy_amt") or df.get("buy_lg_amount")
            elg_buy = df.get("mf_elg_buy_amt") or df.get("buy_elg_amount")
            lg_sell = df.get("mf_lg_sell_amt") or df.get("sell_lg_amount")
            elg_sell = df.get("mf_elg_sell_amt") or df.get("sell_elg_amount")
            amt = df.get("amount") or df.get("amount_li")
            
            if all(x is not None for x in [lg_buy, elg_buy, lg_sell, elg_sell, amt]):
                net = (lg_buy.fillna(0) + elg_buy.fillna(0) - lg_sell.fillna(0) - elg_sell.fillna(0)) / amt.replace(0.0, np.nan)
                
                def _calc_mf_ratio(group):
                    arr = group.values
                    if len(arr) < 5:
                        return np.nan
                    return np.mean(arr[-5:])
                
                mf_values = net.groupby(level="instrument").apply(_calc_mf_ratio).values
                instruments = net.index.get_level_values("instrument").unique()
                mf_ratio = pd.Series(
                    mf_values,
                    index=pd.MultiIndex.from_product(
                        [[last_date], instruments],
                        names=['datetime', 'instrument']
                    )
                )
                out["MF_Main_Net_Amt_Ratio_5D"] = mf_ratio
                out["mf_main_net_amt_ratio_5d"] = mf_ratio
            else:
                out["MF_Main_Net_Amt_Ratio_5D"] = pd.Series(0.0, index=last_day_index)
                out["mf_main_net_amt_ratio_5d"] = pd.Series(0.0, index=last_day_index)

        def _wvma_last(win: int) -> pd.Series:
            """优化版本：只计算最后一个窗口"""
            v = (ret.abs() * vol).fillna(0.0)
            
            def _calc_wvma(group):
                arr = group.values
                if len(arr) < win:
                    return np.nan
                arr_win = arr[-win:]
                num = np.std(arr_win)
                den = np.mean(arr_win) + eps
                return num / den
            
            instruments = v.index.get_level_values("instrument").unique()
            wvma_values = v.groupby(level="instrument").apply(_calc_wvma).values
            
            return pd.Series(
                wvma_values,
                index=pd.MultiIndex.from_product(
                    [[last_date], instruments],
                    names=['datetime', 'instrument']
                )
            )
        
        if "WVMA5" in col_list:
            out["WVMA5"] = _wvma_last(5)
        if "WVMA60" in col_list:
            out["WVMA60"] = _wvma_last(60)
        
        vol_log = np.log(vol.abs() + 1.0)
        if "CORR5" in col_list:
            out["CORR5"] = _corr_series_last(close, vol_log, 5)
        if "CORR10" in col_list:
            out["CORR10"] = _corr_series_last(close, vol_log, 10)
        if "CORR20" in col_list:
            out["CORR20"] = _corr_series_last(close, vol_log, 20)
        if "CORR60" in col_list:
            out["CORR60"] = _corr_series_last(close, vol_log, 60)

        vol_log_ret = np.log(vol / (g_vol.shift(1) + eps) + 1.0)
        close_ret = close / (g_close.shift(1) + eps)
        if "CORD5" in col_list:
            out["CORD5"] = _corr_series_last(close_ret, vol_log_ret, 5)
        if "CORD10" in col_list:
            out["CORD10"] = _corr_series_last(close_ret, vol_log_ret, 10)
        if "CORD60" in col_list:
            out["CORD60"] = _corr_series_last(close_ret, vol_log_ret, 60)

        # 回归因子（避免重复计算）
        if any(x in col_list for x in ["RSQR5", "RESI5"]):
            r2, resi = _rolling_reg_r2_and_resi_last(close, 5)
            if "RSQR5" in col_list:
                out["RSQR5"] = r2
            if "RESI5" in col_list:
                close_last = close.loc[last_date]
                close_last_series = pd.Series(
                    close_last.values,
                    index=pd.MultiIndex.from_product(
                        [[last_date], close_last.index],
                        names=['datetime', 'instrument']
                    )
                )
                out["RESI5"] = resi / close_last_series.replace(0.0, np.nan)
            del r2, resi
            gc.collect()
        
        if any(x in col_list for x in ["RSQR10", "RESI10"]):
            r2, resi = _rolling_reg_r2_and_resi_last(close, 10)
            if "RSQR10" in col_list:
                out["RSQR10"] = r2
            if "RESI10" in col_list:
                close_last = close.loc[last_date]
                close_last_series = pd.Series(
                    close_last.values,
                    index=pd.MultiIndex.from_product(
                        [[last_date], close_last.index],
                        names=['datetime', 'instrument']
                    )
                )
                out["RESI10"] = resi / close_last_series.replace(0.0, np.nan)
            del r2, resi
            gc.collect()

        if "RSQR20" in col_list:
            out["RSQR20"] = _rolling_reg_r2_and_resi_last(close, 20)[0]
        if "RSQR60" in col_list:
            out["RSQR60"] = _rolling_reg_r2_and_resi_last(close, 60)[0]
        
        # 标准差因子（只计算最后一个窗口）
        if "VSTD5" in col_list:
            def _calc_vstd5(group):
                arr = group.values
                if len(arr) < 5:
                    return np.nan
                return np.std(arr[-5:])
            
            vstd_values = vol.groupby(level="instrument").apply(_calc_vstd5).values
            vol_last = vol.loc[last_date]
            instruments = vol.index.get_level_values("instrument").unique()
            vstd_series = pd.Series(
                vstd_values,
                index=pd.MultiIndex.from_product(
                    [[last_date], instruments],
                    names=['datetime', 'instrument']
                )
            )
            vol_last_series = pd.Series(
                vol_last.values,
                index=pd.MultiIndex.from_product(
                    [[last_date], vol_last.index],
                    names=['datetime', 'instrument']
                )
            )
            out["VSTD5"] = vstd_series / (vol_last_series.abs() + eps)
        
        if "STD5" in col_list:
            def _calc_std5(group):
                arr = group.values
                if len(arr) < 5:
                    return np.nan
                return np.std(arr[-5:])
            
            std_values = close.groupby(level="instrument").apply(_calc_std5).values
            close_last = close.loc[last_date]
            instruments = close.index.get_level_values("instrument").unique()
            std_series = pd.Series(
                std_values,
                index=pd.MultiIndex.from_product(
                    [[last_date], instruments],
                    names=['datetime', 'instrument']
                )
            )
            close_last_series = pd.Series(
                close_last.values,
                index=pd.MultiIndex.from_product(
                    [[last_date], close_last.index],
                    names=['datetime', 'instrument']
                )
            )
            out["STD5"] = std_series / close_last_series.replace(0.0, np.nan)
        
        if "ROC60" in col_list:
            def _calc_roc60(group):
                arr = group.values
                if len(arr) < 61:
                    return np.nan
                return arr[-61] / arr[-1]
            
            roc_values = close.groupby(level="instrument").apply(_calc_roc60).values
            instruments = close.index.get_level_values("instrument").unique()
            out["ROC60"] = pd.Series(
                roc_values,
                index=pd.MultiIndex.from_product(
                    [[last_date], instruments],
                    names=['datetime', 'instrument']
                )
            )

        # 构建输出 DataFrame
        # 尝试从static_factors加载缺失的基本面因子
        # ⚠️ 关键修复：只加载col_list中请求的列，避免加载所有字段
        static_factors_path = Path("static_factors.parquet")
        if static_factors_path.exists():
            try:
                # 先读取parquet文件的列信息，只加载需要的列
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(static_factors_path)
                available_columns = parquet_file.schema.names
                
                # 找出col_list中在static_factors中存在但out中不存在的列
                missing_in_out = [col for col in col_list if col not in out]
                columns_to_load = [col for col in missing_in_out if col in available_columns]
                
                if columns_to_load:
                    logger.info(f"从static_factors加载缺失因子: {columns_to_load}")
                    # 只加载需要的列
                    df_static = pd.read_parquet(static_factors_path, columns=columns_to_load)
                    logger.info(f"从static_factors加载数据: {df_static.shape}, 列: {list(df_static.columns)}")
                    
                    # 尝试将static_factors中的列匹配到请求的Alpha158因子
                    for col in columns_to_load:
                        # 获取最后一天的数据
                        static_dates = _safe_get_datetime_level(df_static)
                        if last_date in static_dates:
                            static_last = df_static.loc[last_date]
                            if col in static_last.columns:
                                out[col] = static_last[col]
                                logger.info(f"从static_factors加载因子: {col}")
                        else:
                            # 使用static_factors中最近日期
                            closest_static_date = static_dates.max()
                            static_last = df_static.loc[closest_static_date]
                            if col in static_last.columns:
                                out[col] = static_last[col]
                                logger.info(f"从static_factors加载因子(使用最近日期): {col}")
                else:
                    logger.info("所有请求的Alpha158因子已计算完成，无需从static_factors加载")
            except Exception as e:
                logger.warning(f"从static_factors加载因子失败: {e}")
        
        # 检查是否还有缺失的因子
        missing_factors = [col for col in col_list if col not in out]
        if missing_factors:
            if _strict_inference_enabled():
                raise ValueError(
                    "strict inference Alpha158/static feature calculation is missing required factors: "
                    f"{missing_factors}"
                )
            logger.warning(f"Alpha158因子计算后仍缺失: {missing_factors}")
            # 对于缺失的因子，创建全NaN的Series
            for col in missing_factors:
                # 创建与现有因子相同索引的空Series
                if out:
                    sample_key = list(out.keys())[0]
                    sample_series = out[sample_key]
                    out[col] = pd.Series(np.nan, index=sample_series.index, name=col)
                    logger.info(f"为缺失因子创建空值: {col}")
        
        df_out = pd.DataFrame(out)
        
        # 确保索引正确
        if not df_out.empty:
            df_out = df_out.sort_index()
        
        # ⚠️ 关键修复：强制只返回col_list中请求的列，防止返回额外的列
        if not df_out.empty:
            # 检查是否有多余的列
            extra_cols = [col for col in df_out.columns if col not in col_list]
            if extra_cols:
                logger.warning(f"Alpha158计算返回了{len(extra_cols)}个多余的列，将被删除: {extra_cols[:20]}...")
                # 只保留col_list中的列
                df_out = df_out[[col for col in col_list if col in df_out.columns]]
            
            logger.info(f"Alpha158最终返回列数: {len(df_out.columns)}, 列名: {list(df_out.columns)}")
        
        # 释放内存
        gc.collect()
        
        logger.info(f"Alpha158 因子计算完成（优化版本），只返回最后一天 {last_date.date()} 的因子值")
        
        return df_out
    
    def _compute_alpha158_subset(self, df_history: pd.DataFrame, col_list: List[str]) -> pd.DataFrame:
        """计算 Alpha158 的子集特征（已自动使用优化版本）"""
        # 直接调用优化版本，不再发出警告（这是正常的调用路径）
        return self._compute_alpha158_last_day_only(df_history, col_list)

    def _load_task_manifest(self, task_id: str) -> Optional[Dict[str, Any]]:
        """加载本地任务 manifest.json"""
        manifest_path = self.assets_root / task_id / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载 manifest 失败: {e}")
        return None

    def _load_experiment_manifest(self, workspace_path: str) -> Optional[Dict[str, Any]]:
        """加载QE实验工作目录的 manifest.json
        
        QE实验的manifest结构与TASK相同，包含：
        - primary_assets: factor_entry_relpath, model_weight_relpath
        - assets: factor_order等
        """
        task_dir = _validate_qe_runtime_workspace_path(workspace_path)
        manifest_path = task_dir / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载实验 manifest 失败: {e}")
        return None

    def _infer_expected_features(self, task_dir: Path, manifest: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
        """从factor_order.json获取因子列表，区分Alpha158基线因子和SOTA动态因子
        
        Returns:
            Tuple[List[str], List[str], List[str]]: (完整因子顺序, Alpha158因子列表, SOTA动态因子列表)
        """
        assets = manifest.get("assets", {})
        fo_rel = assets.get("factor_order")
        
        if not fo_rel:
            raise RuntimeError(
                "manifest中缺少factor_order字段，无法获取因子顺序。"
                "要求：manifest.assets.factor_order必须存在。"
            )
        
        p = task_dir / fo_rel
        if not p.exists():
            raise RuntimeError(
                f"factor_order.json文件不存在: {p}。"
                "要求：factor_order.json必须在同步时生成。"
            )
        
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
            
            # 从factor_order.json获取完整的因子顺序列表
            factor_order = obj.get("factor_order")
            if not factor_order or not isinstance(factor_order, list):
                raise RuntimeError(
                    f"factor_order.json中的factor_order字段无效: {factor_order}。"
                    "要求：factor_order必须是非空列表。"
                )
            
            # 获取Alpha158基线因子列表（v2版本）
            alpha158_factors = obj.get("alpha158_factors", [])
            if not isinstance(alpha158_factors, list):
                alpha158_factors = []
            
            # 获取SOTA动态因子列表（v2版本）
            dynamic_factors = obj.get("dynamic_factors", [])
            if not isinstance(dynamic_factors, list):
                dynamic_factors = []
            
            # 验证：factor_order应该等于alpha158_factors + dynamic_factors
            expected_total = len(alpha158_factors) + len(dynamic_factors)
            if len(factor_order) != expected_total:
                logger.warning(
                    f"factor_order长度({len(factor_order)})与alpha158+dynamic总和({expected_total})不一致"
                )
            
            logger.info(
                f"从factor_order.json获取到{len(factor_order)}个因子: "
                f"Alpha158={len(alpha158_factors)}, SOTA动态={len(dynamic_factors)}"
            )
            
            return factor_order, alpha158_factors, dynamic_factors
            
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"解析factor_order.json失败: {e}")

    def _is_strict_inference(self) -> bool:
        return (os.environ.get("AISTOCK_STRICT_INFERENCE") or "").lower() in ["1", "true", "yes", "on"]

    def run_inference(
        self,
        strategy_id: str = "",
        version_tag: str = "replay",
        trade_date: Optional[datetime] = None,
        task_run_id: Optional[str] = None,
        loop_id: Optional[int] = None,
        cutoff_date: Optional[datetime] = None,
        experiment_id: Optional[str] = None,
        workspace_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            return self._run_inference_impl(
                strategy_id=strategy_id,
                version_tag=version_tag,
                trade_date=trade_date,
                task_run_id=task_run_id,
                loop_id=loop_id,
                cutoff_date=cutoff_date,
                experiment_id=experiment_id,
                workspace_path=workspace_path,
            )
        except Exception as e:
            import traceback
            error_msg = f"推理失败: {e}"
            stack_trace = traceback.format_exc()
            logger.error(f"{error_msg}\n完整堆栈:\n{stack_trace}")
            raise
    
    def _run_inference_impl(
        self,
        strategy_id: str = "",
        version_tag: str = "replay",
        trade_date: Optional[datetime] = None,
        task_run_id: Optional[str] = None,
        loop_id: Optional[int] = None,
        cutoff_date: Optional[datetime] = None,
        experiment_id: Optional[str] = None,
        workspace_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """执行完整的推理流程
        
        支持两种模式：
        1. TASK runtime mode: use task_run_id + loop_id, load from rdagent_assets
        2. QE runtime cache mode: experiment_id + AIstock-owned runtime cache workspace_path.
        """
        target_date = trade_date
        if cutoff_date and target_date.date() > cutoff_date.date():
            target_date = cutoff_date
        
        # actual_date 用于数据查询（归一化到最近的可用交易日）
        actual_date = self._get_latest_available_trade_date(target_date)
        # requested_trade_date 用于写入数据库（保持用户请求的原始日期）
        requested_trade_date = trade_date
        
        logger.info(
            f"推理日期设置: requested_trade_date={requested_trade_date.date()} "
            f"actual_date={actual_date.date()} "
            f"(actual_date用于数据查询，requested_trade_date用于写入数据库)"
        )
        
        # 核心逻辑：加载本地同步后的 v2 资产
        # Separate TASK runtime mode from QE runtime cache mode
        if experiment_id and workspace_path:
            # QE runtime cache mode: load from experiment workspace
            task_id = experiment_id
            task_dir = _validate_qe_runtime_workspace_path(workspace_path)
            manifest = self._load_experiment_manifest(str(task_dir))
            if not manifest:
                raise ValueError(f"未找到实验资产 manifest: {experiment_id} at {workspace_path}")
        else:
            # TASK runtime mode: load from rdagent_assets
            task_id = task_run_id or strategy_id
            manifest = self._load_task_manifest(task_id)
            if not manifest:
                raise ValueError(f"未找到本地任务资产 manifest: {task_id}")
            task_dir = self.assets_root / task_id

        primary = manifest.get("primary_assets", {})
        
        factor_file = task_dir / primary["factor_entry_relpath"]
        model_file = task_dir / primary["model_weight_relpath"]

        # 1. 加载因子模块
        # RDAgent原始因子接口: def calculate_xxx() 无参数，内部从 daily_pv.h5 读取数据
        # 推理引擎在调用前已将 daily_pv.h5 和 static_factors.parquet 写入临时工作目录
        factor_module = self.validator.validate_and_load(task_id, str(factor_file))
        # 收集所有 calculate_ 开头的函数（支持多SOTA因子场景）
        all_calc_funcs = {}
        for attr in sorted(dir(factor_module)):
            if attr.startswith("calculate_") and callable(getattr(factor_module, attr)):
                all_calc_funcs[attr] = getattr(factor_module, attr)

        factor_func = None
        factor_func_name = None
        if len(all_calc_funcs) == 1:
            # 单因子：直接使用
            factor_func_name, factor_func = next(iter(all_calc_funcs.items()))
        elif len(all_calc_funcs) > 1:
            # 多因子：使用第一个作为主函数（后续会逐个执行所有函数）
            factor_func_name, factor_func = next(iter(all_calc_funcs.items()))
            logger.info(f"发现 {len(all_calc_funcs)} 个因子计算函数: {list(all_calc_funcs.keys())}")

        # 兼容：如果没有 calculate_ 函数，尝试 compute（旧版包装）
        if not factor_func:
            factor_func = getattr(factor_module, "compute", None)
            if factor_func:
                factor_func_name = "compute"
        if not factor_func:
            raise ValueError(
                f"未找到合法的因子计算函数。"
                f"期望 calculate_* 或 compute 函数，"
                f"模块中可用属性: {[a for a in dir(factor_module) if not a.startswith('_')]}"
            )
        logger.info(f"找到因子计算函数: {factor_func_name}")

        # 2. 从 factor_order.json 读取特征清单，区分Alpha158基线因子和SOTA动态因子
        factor_order, alpha158_feats, dynamic_feats = self._infer_expected_features(task_dir, manifest)
        logger.info(
            f"从 factor_order.json 获取到 {len(alpha158_feats)} 个 Alpha158 基线因子 + "
            f"{len(dynamic_feats)} 个 SOTA 动态因子，总计 {len(factor_order)} 个"
        )
        
        # 3. 加载模型并获取其期望的特征数量
        # 模型类型判断基于反序列化对象的属性（确定性逻辑，不使用try/except推测）：
        #   - LGBModel: 有 self.model (lgb.Booster)，predict 接收 numpy 数组
        #   - GeneralPTNN: 有 self.dnn_model (nn.Module)，predict 接收 DatasetH
        #   - 其他Qlib Model子类: 统一 predict(dataset, segment) 接口
        
        # 将实验工作目录添加到sys.path，以便pickle能找到自定义模型类
        import sys
        task_dir_str = str(task_dir)
        if task_dir_str not in sys.path:
            sys.path.insert(0, task_dir_str)
            logger.info(f"已将实验目录添加到sys.path: {task_dir_str}")

        # 使用提取的模块级函数加载模型
        model, model_kind, inner_model, num_features_expected = load_model_from_pkl(model_file)
        
        # 4. 获取数据（支持内存缓存，同一交易日多次选股复用）
        universe = self._get_default_universe_excluding_st(actual_date)

        # 4.1 检查因子所需的数据窗口
        # factor_order 已在步骤2中通过 _infer_expected_features 获取
        required_window = get_required_data_window(factor_order)
        start_date, start_date_source = self._resolve_inference_start_date(
            actual_date, required_window, buffer_days=5
        )

        logger.info(
            "因子所需数据窗口: %s交易日 + 5日安全余量, start_date=%s, source=%s",
            required_window,
            start_date.date(),
            start_date_source,
        )

        # 4.2 尝试从缓存获取数据
        cache = get_selection_data_cache()
        cached_data = cache.get(actual_date.date(), universe)

        if cached_data is not None:
            df_history, df_fund_raw = cached_data
            cached_ok, cached_days, cached_msg = check_data_window_sufficient(
                df_history, required_window, buffer_days=5
            )
            if cached_ok:
                logger.info(f"✓ 从缓存获取数据: df_history={df_history.shape}, df_fund_raw={df_fund_raw.shape}")
            else:
                logger.info(
                    "缓存数据窗口不足，将按当前策略包窗口重新加载: actual_days=%s, msg=%s",
                    cached_days,
                    cached_msg,
                )
                cached_data = None

        if cached_data is None:
            # 4.3 缓存未命中或窗口不足，从数据库获取
            df_history = get_history_window(
                universe=universe,
                start=start_date,
                end=actual_date,
                fields=["open", "high", "low", "close", "volume", "amount", "factor"],
                freq="1d",
                adj="front",
            )
            if df_history.empty:
                raise ValueError("获取历史数据为空")

            # 从数据库获取基本面+资金流数据
            from .data_service import timescaledb_adapter
            df_fund_raw = timescaledb_adapter.fetch_fundamental_data_ts(
                universe=universe,
                start_date=start_date.date(),
                end_date=actual_date.date()
            )

            # 存入缓存
            cache.put(actual_date.date(), universe, df_history, df_fund_raw)
            logger.info(f"✓ 数据已缓存: df_history={df_history.shape}, df_fund_raw={df_fund_raw.shape}")
        
        # 🔍 诊断：检查df_history初始列数
        logger.info(f"🔍 df_history初始列数: {len(df_history.columns)}, 列名: {list(df_history.columns)}")
        
        # 写入诊断文件
        with open("f:/Dev/AIstock/debug_tools/qe_diagnosis.txt", "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"时间: {datetime.now()}\n")
            f.write(f"df_history初始列数: {len(df_history.columns)}\n")
            f.write(f"df_history列名: {list(df_history.columns)}\n")

        # 4.4 检查数据窗口是否充足
        is_sufficient, actual_days, window_msg = check_data_window_sufficient(
            df_history, required_window, buffer_days=5
        )
        if not is_sufficient:
            if _strict_inference_enabled():
                raise ValueError(f"strict inference data window is insufficient: {window_msg}")
            logger.warning(window_msg)
            # 不中断执行，但记录警告

        # 5. 验证数据时效性（修复：确保数据不滞后）
        self._validate_data_freshness(df_history, requested_trade_date, actual_date)

        tmp_dir = Path(tempfile.mkdtemp(prefix="aistock_inf_"))
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_dir)
            logger.info(f"切换工作目录到临时空间: {tmp_dir}")
            
            # 预先生成行情数据
            df_pv = df_history.copy()
            for col in ["open", "high", "low", "close", "volume", "amount", "factor"]:
                if col in df_pv.columns:
                    df_pv[f"${col}"] = df_pv[col]
            
            # 调试：记录df_history的instrument格式
            logger.info(f"df_history索引: {df_pv.index.names}")
            logger.info(f"df_history instrument样例: {df_pv.index.get_level_values('instrument').unique()[:3].tolist()}")

            t_h5 = time.time()
            df_pv.to_hdf("daily_pv.h5", key="data", mode="w")
            logger.info(f"✓ daily_pv.h5 写入完成，耗时: {time.time() - t_h5:.2f}s, 行数: {len(df_pv)}")
            
            # 6. 处理基本面+资金流数据
            # 重命名字段以匹配因子代码期望的字段名
            field_mapping = {
                # 资金流字段
                **MONEYFLOW_FIELD_MAP,
                # 基本面字段（保持db_前缀）
                'close': 'db_close',
                'turnover_rate': 'db_turnover_rate',
                'turnover_rate_f': 'db_turnover_rate_f',
                'volume_ratio': 'db_volume_ratio',
                'pe': 'db_pe',
                'pe_ttm': 'db_pe_ttm',
                'pb': 'db_pb',
                'ps': 'db_ps',
                'ps_ttm': 'db_ps_ttm',
                'dv_ratio': 'db_dv_ratio',
                'dv_ttm': 'db_dv_ttm',
                'total_share': 'db_total_share',
                'float_share': 'db_float_share',
                'free_share': 'db_free_share',
                'total_mv': 'db_total_mv',
                'circ_mv': 'db_circ_mv',
            }

            if not df_fund_raw.empty:
                df_fund = df_fund_raw.rename(columns=field_mapping)

                # 7. 调用预计算因子服务（集中计算所有派生字段）
                logger.info("调用预计算因子服务计算派生字段...")
                df_fund = compute_precomputed_factors(df_fund, df_history)

                # 验证预计算字段完整性
                is_valid, missing_fields = validate_precomputed_factors(df_fund)
                if not is_valid:
                    if _strict_inference_enabled():
                        raise ValueError(
                            "strict inference precomputed factor data is incomplete: "
                            f"missing={missing_fields}"
                        )
                    logger.warning(f"预计算字段不完整，缺失: {missing_fields}")

                fund_instruments = df_fund.index.get_level_values('instrument').unique()
                logger.info(f"预计算完成: {len(df_fund)}行, {len(fund_instruments)}只股票, {len(df_fund.columns)}个字段")
                logger.info(f"数据时间范围: {_safe_get_datetime_level(df_fund).min()} 到 {_safe_get_datetime_level(df_fund).max()}")
                # 🔍 诊断：检查df_fund列数
                logger.info(f"🔍 df_fund列数: {len(df_fund.columns)}, 前20列: {list(df_fund.columns)[:20]}")
                
                # 写入诊断文件
                with open("f:/Dev/AIstock/debug_tools/qe_diagnosis.txt", "a", encoding="utf-8") as f:
                    f.write(f"df_fund列数: {len(df_fund.columns)}\n")
                    f.write(f"df_fund前30列: {list(df_fund.columns)[:30]}\n")
            else:
                if _strict_inference_enabled():
                    raise ValueError("strict inference requires fundamental/moneyflow/sector DB data")
                logger.warning("数据库中没有找到资金流数据")
                df_fund = pd.DataFrame()
            
            if not df_fund.empty:
                # 确保索引名称正确（必须是datetime和instrument，与df_history一致）
                if isinstance(df_fund.index, pd.MultiIndex):
                    df_fund.index.names = ["datetime", "instrument"]
                
                # 关键修复：确保df_fund的instrument格式与df_history完全一致
                # 获取df_history的instrument格式作为标准
                history_instruments = df_pv.index.get_level_values("instrument").unique()
                fund_instruments_orig = df_fund.index.get_level_values("instrument").unique()
                
                # 检查格式是否匹配
                common = set(history_instruments) & set(fund_instruments_orig)
                
                if len(common) == 0:
                    # 格式不匹配，需要转换
                    logger.info("检测到instrument格式不匹配，开始转换...")
                    logger.info(f"df_history格式样例: {history_instruments[:3].tolist()}")
                    logger.info(f"df_fund格式样例: {fund_instruments_orig[:3].tolist()}")
                    
                    # df_fund使用.SH/.SZ后缀格式，需要转换为SH/SZ前缀格式
                    fund_inst_list = df_fund.index.get_level_values("instrument").astype(str)
                    if fund_inst_list.str.contains(r'\.(SH|SZ)$').any():
                        converted_instruments = []
                        for inst in fund_inst_list:
                            if '.' in inst:
                                code, exchange = inst.split('.')
                                converted_instruments.append(f"{exchange}{code}")
                            else:
                                converted_instruments.append(inst)
                        
                        df_fund = df_fund.copy()
                        df_fund.index = pd.MultiIndex.from_arrays(
                            [
                                _safe_get_datetime_level(df_fund),
                                pd.Index(converted_instruments, name="instrument"),
                            ],
                            names=["datetime", "instrument"],
                        )
                        
                        # 验证转换结果
                        fund_instruments_new = df_fund.index.get_level_values("instrument").unique()
                        common_after = set(history_instruments) & set(fund_instruments_new)
                        logger.info(f"转换后共同instrument数量: {len(common_after)}")
                        
                        if len(common_after) > 0:
                            logger.info("✓ instrument格式转换成功")
                        else:
                            if _strict_inference_enabled():
                                raise ValueError(
                                    "strict inference instrument format conversion failed: "
                                    "df_history and df_fund have no common instruments"
                                )
                            logger.error("❌ instrument格式转换后仍不匹配")
                            logger.error(f"df_history样例: {history_instruments[:3].tolist()}")
                            logger.error(f"df_fund转换后样例: {fund_instruments_new[:3].tolist()}")
                else:
                    logger.info(f"✓ instrument格式已匹配，共同数量: {len(common)}")
                
                logger.info(f"static_factors索引: {df_fund.index.names}, 形状: {df_fund.shape}")
                fund_inst_sample = df_fund.index.get_level_values('instrument').unique()[:5].tolist()
                logger.info(f"static_factors instrument样例: {fund_inst_sample}")
                
                # 关键验证：检查df_history和df_fund的instrument是否有交集
                history_instruments = set(df_pv.index.get_level_values('instrument').unique())
                fund_instruments = set(df_fund.index.get_level_values('instrument').unique())
                common_instruments = history_instruments & fund_instruments
                logger.info(f"df_history instrument数量: {len(history_instruments)}")
                logger.info(f"df_fund instrument数量: {len(fund_instruments)}")
                logger.info(f"共同instrument数量: {len(common_instruments)}")
                
                if len(common_instruments) == 0:
                    if _strict_inference_enabled():
                        raise ValueError("strict inference found no common instruments between price and static data")
                    logger.error("❌ df_history和df_fund没有共同的instrument，格式不匹配！")
                    logger.error(f"df_history样例: {list(history_instruments)[:3]}")
                    logger.error(f"df_fund样例: {list(fund_instruments)[:3]}")
                else:
                    logger.info(f"✓ 共同instrument样例: {list(common_instruments)[:3]}")
                
                t_parquet = time.time()
                df_fund.to_parquet("static_factors.parquet")
                logger.info(f"✓ static_factors.parquet 写入完成，耗时: {time.time() - t_parquet:.2f}s")
                
                # 🔍 诊断：检查df_history是否被df_fund污染
                # df_history应该只包含OHLCV列，不应该包含df_fund的列
                if hasattr(df_history, 'columns'):
                    logger.info(f"🔍 df_history当前列数: {len(df_history.columns)}, 列名: {list(df_history.columns)}")
                    # 检查是否有df_fund的列混入df_history
                    fund_cols_in_history = [col for col in df_history.columns if col in df_fund.columns]
                    if fund_cols_in_history:
                        logger.error(f"❌ df_history被污染！包含{len(fund_cols_in_history)}个df_fund的列: {fund_cols_in_history[:20]}")

            # 验证临时文件是否存在
            import os as _os
            logger.info(f"临时目录内容: {_os.listdir('.')}")
            
            # ⚠️ 关键修复：强制确保df_history只包含OHLCV列
            # 防止df_history被污染导致传递给模型的特征数量错误
            expected_ohlcv_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            actual_cols = list(df_history.columns)
            extra_cols = [col for col in actual_cols if col not in expected_ohlcv_cols]
            
            if extra_cols:
                logger.error(f"❌ df_history被污染！包含{len(extra_cols)}个额外列: {extra_cols[:20]}...")
                logger.error(f"df_history总列数: {len(df_history.columns)}")
                logger.error("强制只保留OHLCV列")
                # 只保留OHLCV列
                df_history = df_history[[col for col in expected_ohlcv_cols if col in df_history.columns]]
                logger.info(f"✓ df_history已清理，当前列数: {len(df_history.columns)}, 列名: {list(df_history.columns)}")

            t_start = time.time()
            # 多因子场景：逐个执行所有calculate_函数并合并结果
            if len(all_calc_funcs) > 1:
                logger.info(f"开始逐个执行 {len(all_calc_funcs)} 个SOTA因子计算函数")
                df_parts = []
                for fn_name, fn_obj in all_calc_funcs.items():
                    t_fn = time.time()
                    logger.info(f"开始执行SOTA因子计算函数: {fn_name}")
                    sig = inspect.signature(fn_obj)
                    if len(sig.parameters) == 0:
                        logger.info("调用无参数因子函数...")
                        part = fn_obj()
                    else:
                        logger.info("调用带参数因子函数...")
                        part = fn_obj(df_history)
                    logger.info(f"SOTA因子计算完成（{fn_name}），耗时: {time.time() - t_fn:.2f}s")
                    if isinstance(part, pd.DataFrame):
                        df_parts.append(part)
                    else:
                        raise ValueError(
                            f"SOTA因子计算函数 {fn_name} 必须返回DataFrame，"
                            f"实际返回类型: {type(part)}"
                        )
                # 合并所有因子DataFrame
                df_factors_raw = pd.concat(df_parts, axis=1)
                # 去除可能的重复列
                df_factors_raw = df_factors_raw.loc[:, ~df_factors_raw.columns.duplicated()]
                logger.info(
                    f"所有SOTA因子计算完成，合并后列数: {len(df_factors_raw.columns)}, "
                    f"列名: {list(df_factors_raw.columns)}, 总耗时: {time.time() - t_start:.2f}s"
                )
            else:
                # 单因子或compute兼容模式
                logger.info(f"开始执行SOTA因子计算函数: {factor_func_name}")
                # RDAgent原始因子: calculate_xxx() 无参数，内部从 daily_pv.h5 读取
                # 旧版兼容: compute(df_history) 接受DataFrame参数
                sig = inspect.signature(factor_func)
                if len(sig.parameters) == 0:
                    logger.info("调用无参数因子函数...")
                    df_factors_raw = factor_func()
                else:
                    logger.info("调用带参数因子函数...")
                    df_factors_raw = factor_func(df_history)
                logger.info(f"SOTA因子计算完成（{factor_func_name}），耗时: {time.time() - t_start:.2f}s")
            
            # 验证返回类型必须是DataFrame
            if not isinstance(df_factors_raw, pd.DataFrame):
                raise ValueError(
                    f"SOTA因子计算函数必须返回DataFrame，实际返回类型: {type(df_factors_raw)}"
                )
            
            # 检查是否为空DataFrame（QE实验可能只使用Alpha158基线因子）
            if df_factors_raw.empty:
                if _strict_inference_enabled() and dynamic_feats:
                    raise ValueError(
                        "strict inference dynamic factor output is empty while dynamic factors are required: "
                        f"{dynamic_feats}"
                    )
                logger.info("SOTA因子返回空DataFrame，将只使用Alpha158基线因子")
                df_factors = pd.DataFrame()
            else:
                # 获取最后一天的日期
                last_date = _safe_get_datetime_level(df_history).max()
                
                # 获取SOTA因子数据中的所有日期
                sota_dates = _safe_get_datetime_level(df_factors_raw)
                
                # 只保留最后一天的因子值（优化：选股只需要当天值）
                if last_date in sota_dates:
                    df_factors = df_factors_raw.loc[last_date]
                    logger.info(f"SOTA因子优化：使用目标日期 {last_date.date()} 的因子值")
                else:
                    if _strict_inference_enabled():
                        raise ValueError(
                            f"strict inference SOTA factors missing target date {last_date.date()}; "
                            "refusing to use an earlier factor date"
                        )
                    # 如果目标日期不存在，使用最近可用日期
                    available_dates = sota_dates.unique()
                    if len(available_dates) > 0:
                        closest_date = max([d for d in available_dates if d <= last_date], default=available_dates[-1])
                        df_factors = df_factors_raw.loc[closest_date]
                        logger.warning(f"SOTA因子：目标日期 {last_date.date()} 无数据，使用最近可用日期 {closest_date.date()}")
                    else:
                        raise ValueError(
                            "SOTA因子计算结果中没有任何日期数据。"
                            "请检查因子计算函数是否正确处理了输入数据。"
                        )
            
            # 确保返回的是MultiIndex格式
            if not isinstance(df_factors.index, pd.MultiIndex):
                used_date = last_date if last_date in sota_dates else closest_date
                # 重建MultiIndex
                df_factors.index = pd.MultiIndex.from_product(
                    [[used_date], df_factors.index],
                    names=['datetime', 'instrument']
                )
            
            # 验证所有列都是Series（不使用兜底方案）
            logger.info(f"SOTA因子列数: {len(df_factors.columns)}, 列名: {list(df_factors.columns)}")
            for col in df_factors.columns:
                col_data = df_factors[col]
                if not isinstance(col_data, pd.Series):
                    raise ValueError(
                        f"SOTA因子 {col} 的数据类型错误: {type(col_data)}，期望 pd.Series。"
                        f"请检查因子计算函数的返回值格式。"
                    )
            
        finally:
            os.chdir(old_cwd)
            shutil.rmtree(tmp_dir, ignore_errors=True)
        
        # 6. 计算 Alpha158 基线因子（优化版本：只计算最后一天）
        # 只计算alpha158_feats中的因子，不包含dynamic_feats
        if alpha158_feats:
            logger.info(f"开始计算 Alpha158 基线因子，请求的因子数量: {len(alpha158_feats)}")
            logger.info(f"请求的Alpha158因子列表: {alpha158_feats}")
            alpha_subset = self._compute_alpha158_subset(df_history, alpha158_feats)
            
            # 关键诊断：检查实际返回的列数
            logger.info(f"⚠️ Alpha158计算完成，实际返回列数: {len(alpha_subset.columns)}")
            logger.info(f"⚠️ Alpha158实际返回的列: {list(alpha_subset.columns)}")
            
            # 如果返回的列数不等于请求的列数，这是严重错误
            if len(alpha_subset.columns) != len(alpha158_feats):
                logger.error(f"❌ Alpha158列数不匹配！请求{len(alpha158_feats)}个，返回{len(alpha_subset.columns)}个")
                logger.error(f"请求的因子: {alpha158_feats}")
                logger.error(f"返回的因子: {list(alpha_subset.columns)}")
                if _strict_inference_enabled():
                    raise ValueError(
                        "strict inference Alpha158 feature count mismatch: "
                        f"requested={len(alpha158_feats)}, returned={len(alpha_subset.columns)}"
                    )
            
            # 验证所有列都是Series（不使用兜底方案）
            for col in alpha_subset.columns:
                col_data = alpha_subset[col]
                if not isinstance(col_data, pd.Series):
                    raise ValueError(
                        f"Alpha158 因子 {col} 的数据类型错误: {type(col_data)}，期望 pd.Series。"
                        f"这是内部错误，请检查 _compute_alpha158_last_day_only 方法。"
                    )
        else:
            # 如果没有Alpha158因子，创建空DataFrame（使用df_factors的索引）
            alpha_subset = pd.DataFrame(index=df_factors.index)
            logger.info("没有Alpha158基线因子，跳过计算")
        
        # 7. 按正确顺序组合特征（使用提取的模块级函数）
        df_factors_combined = assemble_features(
            alpha_subset, df_factors, factor_order, alpha158_feats, dynamic_feats
        )

        actual_count = len(df_factors_combined.columns)
        logger.info(f"特征组合完成: Alpha158={len(alpha158_feats)}, SOTA动态因子={actual_count - len(alpha158_feats)}, 总计={actual_count}")
        logger.info(f"最终特征列: {list(df_factors_combined.columns)}")

        # 写入诊断文件
        with open("f:/Dev/AIstock/debug_tools/qe_diagnosis.txt", "a", encoding="utf-8") as f:
            f.write(f"df_factors_combined列数: {len(df_factors_combined.columns)}\n")
            f.write(f"df_factors_combined列名: {list(df_factors_combined.columns)}\n")
            f.write(f"factor_order长度: {len(factor_order)}\n")
        
        # 7.6 处理特征数量不匹配的情况
        if num_features_expected > 0 and actual_count != num_features_expected:
            if _strict_inference_enabled():
                raise ValueError(
                    "strict inference model feature count mismatch; refusing to pad or truncate features: "
                    f"expected={num_features_expected}, actual={actual_count}"
                )
            logger.warning(
                f"特征数量不匹配: 模型期望 {num_features_expected} 个特征，实际提供 {actual_count} 个特征。"
            )
            
            if actual_count > num_features_expected:
                # 实际特征多于期望，截取前N个
                logger.warning(f"截取前 {num_features_expected} 个特征进行预测")
                df_factors_combined = df_factors_combined.iloc[:, :num_features_expected]
            else:
                # 实际特征少于期望，填充NaN列
                missing_count = num_features_expected - actual_count
                logger.warning(f"填充 {missing_count} 个空特征")
                for i in range(missing_count):
                    col_name = f"padding_{i}"
                    df_factors_combined[col_name] = pd.Series(0.0, index=df_factors_combined.index)
            
            actual_count = len(df_factors_combined.columns)
            logger.info(f"特征数量调整后: {actual_count} 个特征")
        
        unique_dates = _safe_get_datetime_level(df_factors_combined).unique()
        if actual_date not in unique_dates:
            if _strict_inference_enabled():
                raise ValueError(
                    f"strict inference factors do not contain exact actual_date {actual_date.date()}; "
                    "refusing to use an earlier date"
                )
            earlier_dates = unique_dates[unique_dates <= actual_date]
            if earlier_dates.empty:
                raise ValueError(f"推理日期 {actual_date} 无有效数据")
            actual_date = earlier_dates.max()

        df_today = df_factors_combined[_safe_get_datetime_level(df_factors_combined) == actual_date]
        if df_today.empty:
            raise ValueError(f"推理日期 {actual_date} 无有效因子数据")
        
        # 写入诊断文件
        with open("f:/Dev/AIstock/debug_tools/qe_diagnosis.txt", "a", encoding="utf-8") as f:
            f.write(f"df_today列数（索引过滤后）: {len(df_today.columns)}\n")
            f.write(f"df_today列名: {list(df_today.columns)[:50]}\n")

        # 7. 模型预测（使用提取的模块级函数）
        df_today = _apply_saved_qe_infer_processors(
            df_today,
            task_dir=task_dir,
            primary_assets=primary,
        )

        X = _drop_invalid_feature_rows_for_strict(df_today)
        logger.info(f"模型预测: model_kind={model_kind}, X.shape={X.shape}")

        scores = predict_scores(model, inner_model, model_kind, X)

        df_scores = _build_score_frame_for_scored_features(X, scores)

        # 保存信号到数据库（使用提取的模块级函数）
        save_signals_to_db(task_run_id, loop_id, requested_trade_date, df_scores)

        return df_scores

    def _save_signals_to_db(self, task_run_id: str, loop_id: int, trade_date: datetime, df_scores: pd.DataFrame):
        """将选股结果存入 trading.rdagent_signal（委托给模块级函数）"""
        save_signals_to_db(task_run_id, loop_id, trade_date, df_scores)

    def run_task_inference(
        self,
        *,
        task_id: str,
        task_dir: str,
        factor_entry_path: str,
        model_weight_path: str,
        task_run_id: Optional[str] = None,
        loop_id: Optional[int] = None,
        trade_date: Optional[str] = None,
        cutoff_date: Optional[str] = None,
        top_k: int = 50,
    ) -> Dict[str, Any]:
        """基于本地 task 资产执行推理并返回 TopK 结果。"""
        t_date = datetime.strptime(trade_date, "%Y-%m-%d") if trade_date else datetime.now()
        c_date = datetime.strptime(cutoff_date, "%Y-%m-%d") if cutoff_date else None
        
        scores = self.run_inference(
            strategy_id=task_id,
            version_tag="task_only",
            trade_date=t_date,
            task_run_id=task_run_id or task_id,
            loop_id=loop_id,
            cutoff_date=c_date,
        )

        scores = scores.sort_values(by="score", ascending=False).head(top_k)
        items = []
        for (dt, instrument), row in scores.iterrows():
            items.append({
                "symbol": instrument,
                "score": float(row["score"]),
                "rank": len(items) + 1
            })

        return {
            "task_id": task_id,
            "trade_date": t_date.strftime("%Y-%m-%d"),
            "items": items,
        }
