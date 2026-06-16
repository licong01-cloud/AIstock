"""
QuantEvolver Phase 2: ConfigComposer（配置组装器）

功能：
1. 生成QLib conf.yaml配置文件
2. 组装因子代码文件（factor.py）
3. 组装策略代码文件（如自定义策略）
4. 生成WSL执行命令
5. 管理实验目录和结果同步
"""
from __future__ import annotations

import json
import logging
import os
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


from ...db.pg_pool import get_conn
from ..strategy_package.workspace_policy import (
    ensure_aistock_artifact_path,
    ensure_not_forbidden_worker_workspace_path,
)
from .experiment_config import apply_qe_seed_to_model_params, ensure_qe_risk_policy, normalize_label_horizon
from .runtime_contract import merge_qe_minute_runtime_contract
from .payload_summary import compact_experiment_row

logger = logging.getLogger("aistock.quantevolver.config_composer")


AISTOCK_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _win_to_wsl_guess(path: Path) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


def _is_realtime_factor_cache_path(path_value: Optional[str]) -> bool:
    """Return True when a cache path points anywhere under factor_values_realtime."""
    if not path_value:
        return False
    normalized = str(path_value).strip().strip("\"'").replace("\\", "/").rstrip("/")
    return any(part.lower() == "factor_values_realtime" for part in normalized.split("/") if part)


def _env_path(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default)))


def _safe_artifact_root(name: str, default: Path, *, purpose: str) -> Path:
    """Resolve a local AIstock artifact root and reject worker/workspace paths."""
    raw_value = os.getenv(name)
    candidate = Path(raw_value) if raw_value else default
    try:
        return ensure_aistock_artifact_path(
            candidate,
            purpose=purpose,
            extra_roots=[default],
        )
    except Exception:
        if not raw_value:
            raise
        logger.warning(
            "%s points outside AIstock local artifact roots and will be ignored: %s",
            name,
            raw_value,
        )
        return ensure_aistock_artifact_path(
            default,
            purpose=purpose,
            extra_roots=[default],
        )


# QE workspace is a node-side path. Windows-side code must use node
# APIs/payloads instead of dereferencing worker files directly.
_DEFAULT_QE_WORKSPACE_LOCAL = AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_workspace"
QE_WORKSPACE_WSL = os.getenv("QE_WORKSPACE_WSL", _win_to_wsl_guess(_DEFAULT_QE_WORKSPACE_LOCAL))

QE_PROGRAMS_WIN = _safe_artifact_root(
    "QE_PROGRAMS_WIN",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_programs",
    purpose="QE program/template local root",
)
BUNDLED_QE_TEMPLATE_ROOT = Path(__file__).resolve().parent / "templates"
QE_PROGRAMS_WSL = os.getenv("QE_PROGRAMS_WSL", _win_to_wsl_guess(QE_PROGRAMS_WIN))

QE_EXPERIMENTS_ROOT = _safe_artifact_root(
    "QE_EXPERIMENTS_ROOT",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_experiments",
    purpose="QE experiment local artifact root",
)
FACTOR_CACHE_ROOT_WIN = _safe_artifact_root(
    "FACTOR_CACHE_ROOT_WIN",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "factor_values",
    purpose="QE factor-cache local artifact root",
)

RDAGENT_FACTOR_DATA_WSL = os.getenv("RDAGENT_FACTOR_DATA_WSL", "").strip()
QLIB_DATA_PATH_WSL = os.getenv("QLIB_DATA_PATH_WSL", "").strip()
QLIB_MINUTE_PATH_WSL = os.getenv("QLIB_MINUTE_PATH_WSL", "").strip()
RDAGENT_CODE_ROOT_WSL = os.getenv("QLIB_RDAGENT_ROOT_WSL", "").strip()


def _qe_experiment_dir(experiment_name: str) -> Path:
    return ensure_aistock_artifact_path(
        QE_EXPERIMENTS_ROOT / experiment_name,
        purpose=f"QE experiment local artifact directory: {experiment_name}",
        extra_roots=[QE_EXPERIMENTS_ROOT],
    )

# QE/RDAgent default data split.
#
# `test_end` is the last day used by the data handler/signals.  Qlib daily
# portfolio simulation needs one following calendar row, so the default
# portfolio backtest stops at 2026-04-27 while the provider still contains
# 2026-04-28 for signal/price lookups.
QE_DEFAULT_SIGNAL_END = "2026-04-28"
QE_DEFAULT_BACKTEST_END = "2026-04-27"

RDAGENT_DEFAULT_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": QE_DEFAULT_SIGNAL_END,
    "backtest_end": QE_DEFAULT_BACKTEST_END,
}

_LEGACY_QE_DEFAULT_SPLIT_MARKERS = (
    # Old QE UI/system defaults before the 2026-04-28 data refresh.  When a
    # caller sends this exact default split without an intentional override,
    # upgrade it so new single/evolution/strategy/custom runs do not require
    # manual date edits.
    {"test_end": "2026-03-10", "backtest_end": None},
    {"test_end": "2026-03-10", "backtest_end": "2026-03-09"},
    {"test_end": "2025-12-01", "backtest_end": None},
)

# ── RDAgent 默认 LGBModel 超参数（与 conf_baseline.yaml 一致） ──
RDAGENT_DEFAULT_LGB_KWARGS = {
    "loss": "mse",
    "device_type": "cpu",
    "max_bin": 63,
    "colsample_bytree": 0.8879,
    "learning_rate": 0.2,
    "subsample": 0.8789,
    "lambda_l1": 205.6999,
    "lambda_l2": 580.9768,
    "max_depth": 8,
    "num_leaves": 210,
    "num_threads": 24,
}


SUPPORTED_QE_EXECUTION_ALGOS = {
    "TWAP",
    "CLOSE_PRICE",
    "V24_PLAN",
    "V25_TWO_STAGE",
    "V25_1_SMALL_CAP",
}
DEFAULT_QE_EXECUTION_ALGO = "TWAP"
SUSPEND_FILTER_FILE = "qe_suspend_filter.json"
RISK_POLICY_FILE = "qe_event_risk_policy.json"
PRECOMPUTED_HMM_COEFF_JSON_PARAM = "_precomputed_hmm_coefficients_json"
QE_LOCAL_STRATEGY_ROOTS = [
    AISTOCK_PROJECT_ROOT / "backend" / "rebalance_strategies",
    AISTOCK_PROJECT_ROOT / "rdagent_assets" / "qe_strategies",
    AISTOCK_PROJECT_ROOT / "scripts",
]
AUTHORITATIVE_QE_HELPER_ASSETS = {
    # V25 is a strategy/model asset. Keep the authority in the AIstock repo
    # instead of probing an RDAgent/WSL workspace from Windows.
    "tail_twap_v25_strategy.py": AISTOCK_PROJECT_ROOT / "scripts" / "tail_twap_v25_strategy.py",
    "tail_twap_v25_1_strategy.py": AISTOCK_PROJECT_ROOT / "scripts" / "tail_twap_v25_1_strategy.py",
}


class ConfigComposer:
    """配置组装器。"""

    _workspace_config_cache: Optional[Dict[str, str]] = None
    _execution_algo_catalog_cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _validate_data_split(data_split: Dict[str, str]):
        """校验 data_split 日期合法性。"""
        from datetime import datetime
        required = ["train_start", "train_end", "valid_start", "valid_end", "test_start", "test_end"]
        for key in required:
            if key not in data_split:
                raise ValueError(f"data_split 缺少必填字段: {key}")
            try:
                datetime.strptime(data_split[key], "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(f"data_split[{key}] 日期格式错误: {data_split[key]}，应为 YYYY-MM-DD") from e

        dates = {k: datetime.strptime(data_split[k], "%Y-%m-%d") for k in required}
        if dates["train_end"] > dates["valid_start"]:
            raise ValueError(f"train_end ({data_split['train_end']}) 不能晚于 valid_start ({data_split['valid_start']})")
        if dates["valid_end"] > dates["test_start"]:
            raise ValueError(f"valid_end ({data_split['valid_end']}) 不能晚于 test_start ({data_split['test_start']})")
        if dates["test_end"] > datetime.now():
            raise ValueError(f"test_end ({data_split['test_end']}) 不能超过当前日期")
        if data_split.get("backtest_end"):
            try:
                backtest_end = datetime.strptime(data_split["backtest_end"], "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(
                    f"data_split[backtest_end] 日期格式错误: {data_split['backtest_end']}，应为 YYYY-MM-DD"
                ) from e
            if backtest_end < dates["test_start"]:
                raise ValueError(
                    f"backtest_end ({data_split['backtest_end']}) 不能早于 test_start ({data_split['test_start']})"
                )
            if backtest_end > dates["test_end"]:
                raise ValueError(
                    f"backtest_end ({data_split['backtest_end']}) 不能晚于 test_end ({data_split['test_end']})"
                )

    @staticmethod
    def _is_legacy_default_split(data_split: Dict[str, str]) -> bool:
        """Return True only for known stale system defaults, not arbitrary user windows."""
        base_matches = (
            data_split.get("train_start") == "2018-08-01"
            and data_split.get("train_end") == "2022-12-31"
            and data_split.get("valid_start") == "2023-01-01"
            and data_split.get("valid_end") == "2024-06-30"
            and data_split.get("test_start") == "2024-07-01"
        )
        if not base_matches:
            return False
        for marker in _LEGACY_QE_DEFAULT_SPLIT_MARKERS:
            if data_split.get("test_end") != marker["test_end"]:
                continue
            expected_backtest = marker["backtest_end"]
            if expected_backtest is None and not data_split.get("backtest_end"):
                return True
            if expected_backtest is not None and data_split.get("backtest_end") == expected_backtest:
                return True
        return False

    @staticmethod
    def _ensure_backtest_end(data_split: Dict[str, str]):
        """Ensure every QE path has a safe portfolio backtest end.

        Qlib daily backtest reads calendar[index+1].  With the official data
        currently ending on 2026-04-28, the safe default portfolio end is
        2026-04-27.  Data/signal coverage still uses `test_end`.
        """
        if not data_split.get("test_end"):
            return

        if ConfigComposer._is_legacy_default_split(data_split):
            data_split["test_end"] = QE_DEFAULT_SIGNAL_END
            data_split["backtest_end"] = QE_DEFAULT_BACKTEST_END
            return

        if data_split.get("backtest_end"):
            return

        # For the current latest signal date, cap portfolio simulation at the
        # preceding safe trading day.  For older user-selected windows, test_end
        # itself is safe because later Qlib calendar rows already exist.
        data_split["backtest_end"] = (
            QE_DEFAULT_BACKTEST_END
            if data_split["test_end"] >= QE_DEFAULT_SIGNAL_END
            else data_split["test_end"]
        )

    @staticmethod
    def _validate_historical_stock_pool_window(
        custom_params: Optional[Dict[str, Any]],
        data_split: Dict[str, str],
    ) -> None:
        stock_pool = (custom_params or {}).get("stock_pool")
        if not stock_pool:
            return
        match = re.search(r"filtered_pool[_-](\d{8})", str(stock_pool))
        if not match:
            return
        pool_date = datetime.strptime(match.group(1), "%Y%m%d")
        test_end = datetime.strptime(data_split["test_end"], "%Y-%m-%d")
        if pool_date > test_end:
            raise ValueError(
                "QE_STOCK_POOL_DATE_OUT_OF_WINDOW: "
                f"stock_pool={stock_pool!r} uses filtered_pool date {pool_date:%Y-%m-%d} "
                f"after data_split.test_end={data_split['test_end']}"
            )

    def _fetch_workspace_config(self, node_id: Optional[str] = None) -> Dict[str, str]:
        """
        获取 QE 工作区路径配置。
        所有路径直接从 AIstock 侧 .env 环境变量读取，不再依赖 RDAgent API。
        node_id: 指定节点时从 infra.compute_nodes 查询路径，None 则使用默认常量。
        """
        if node_id:
            return self._get_node_paths(node_id)

        if ConfigComposer._workspace_config_cache is not None:
            return ConfigComposer._workspace_config_cache

        ConfigComposer._workspace_config_cache = {
            "workspace_base": QE_WORKSPACE_WSL,
            "factor_data_dir": RDAGENT_FACTOR_DATA_WSL,
            "qlib_data_path": QLIB_DATA_PATH_WSL,
            "qlib_minute_path": QLIB_MINUTE_PATH_WSL,
            "qlib_rdagent_root": RDAGENT_CODE_ROOT_WSL,
        }
        return ConfigComposer._workspace_config_cache

    def _get_node_paths(self, node_id: str) -> Dict[str, str]:
        """查询目标节点的路径配置。所有路径字段必须已配置，缺失则报错。"""
        from ...db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT workspace_base, factor_data_dir, qlib_data_path, "
                    "       qlib_minute_path, qlib_rdagent_root, factor_cache_dir, "
                    "       api_base_url, ssh_user "
                    "FROM infra.compute_nodes WHERE node_id = %s",
                    (node_id,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            raise ValueError(f"节点 {node_id} 未配置 workspace_base，请先在节点管理中设置路径")
        missing = []
        if not row[1]:
            missing.append("factor_data_dir")
        if not row[2]:
            missing.append("qlib_data_path")
        if not row[3]:
            missing.append("qlib_minute_path")
        if not row[4]:
            missing.append("qlib_rdagent_root")
        if missing:
            raise ValueError(f"节点 {node_id} 缺少必要路径配置: {', '.join(missing)}，请在节点管理中补全")
        factor_cache_dir = row[5]
        if not factor_cache_dir:
            from urllib.parse import urlparse

            host = (urlparse(row[6] or "").hostname or "").lower()
            ssh_user = (row[7] or "").strip()
            if host not in {"", "127.0.0.1", "localhost", "::1"} and ssh_user:
                factor_cache_dir = f"/home/{ssh_user}/aistock_cache/factor_values"

        return {
            "workspace_base": row[0],
            "factor_data_dir": row[1],
            "qlib_data_path": row[2],
            "qlib_minute_path": row[3],
            "qlib_rdagent_root": row[4],
            "factor_cache_dir": factor_cache_dir,
        }

    def _generate_unique_experiment_id(self) -> str:
        """生成基于日期时间的唯一实验ID，格式: qe_YYYYMMDD_HHMMSS"""
        from datetime import datetime
        base_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                    (base_id,),
                )
                if not cur.fetchone():
                    return base_id
                # 极端冲突：追加序号
                for i in range(2, 100):
                    candidate = f"{base_id}_{i}"
                    cur.execute(
                        "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                        (candidate,),
                    )
                    if not cur.fetchone():
                        return candidate
        raise RuntimeError(f"无法生成唯一实验ID: {base_id}")


    @staticmethod
    def _sha256_file(path: Path) -> str:
        raw = path.read_bytes()
        if path.suffix.lower() == ".py":
            text = raw.decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
            return hashlib.sha256(text.encode("utf-8")).hexdigest()
        return hashlib.sha256(raw).hexdigest()

    @classmethod
    def _execution_algo_catalog_entry(cls, algo_code: str) -> Dict[str, Any]:
        algo = str(algo_code).strip().upper()
        cached = cls._execution_algo_catalog_cache.get(algo)
        if cached is not None:
            return dict(cached)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT default_config, is_enabled
                    FROM public.execution_algorithm_catalog
                    WHERE algo_code = %s
                    """,
                    (algo,),
                )
                row = cur.fetchone()
        if not row:
            raise ValueError(
                f"execution_algo='{algo}' is not registered in execution_algorithm_catalog; "
                "refusing to synthesize runtime defaults from code."
            )
        default_config, is_enabled = row
        if not is_enabled:
            raise ValueError(f"execution_algo='{algo}' is disabled in execution_algorithm_catalog")
        if default_config is None:
            default_config = {}
        if not isinstance(default_config, dict):
            raise ValueError(
                f"execution_algorithm_catalog.default_config for {algo} must be a JSON object"
            )
        entry = {"default_config": dict(default_config), "is_enabled": bool(is_enabled)}
        cls._execution_algo_catalog_cache[algo] = entry
        return dict(entry)

    @classmethod
    def _resolve_qe_helper_asset(cls, scripts_dir: Path, helper_name: str) -> Path:
        authoritative_path = AUTHORITATIVE_QE_HELPER_ASSETS.get(helper_name)
        if authoritative_path is None:
            return scripts_dir / helper_name
        if not authoritative_path.exists():
            raise FileNotFoundError(
                f"Authoritative QE helper asset missing for {helper_name}: {authoritative_path}"
            )
        local_path = scripts_dir / helper_name
        if local_path.exists():
            local_hash = cls._sha256_file(local_path)
            authoritative_hash = cls._sha256_file(authoritative_path)
            if local_hash != authoritative_hash:
                raise ValueError(
                    f"QE helper asset mismatch for {helper_name}: local={local_path} "
                    f"sha256={local_hash}, authoritative={authoritative_path} "
                    f"sha256={authoritative_hash}. Refusing to copy a divergent strategy asset."
                )
        return authoritative_path

    @staticmethod
    def _strategy_dependency_roots() -> list[Path]:
        roots = list(QE_LOCAL_STRATEGY_ROOTS)
        env_root = os.getenv("RDAGENT_FACTOR_TEMPLATE_WIN")
        if env_root:
            candidate = Path(env_root)
            ensure_not_forbidden_worker_workspace_path(
                candidate,
                purpose="QE strategy dependency source root",
            )
            raw = str(env_root).replace("\\", "/")
            if raw.startswith("/") or raw.startswith("//"):
                raise ValueError(
                    "RDAGENT_FACTOR_TEMPLATE_WIN must point to an AIstock-local directory; "
                    f"Linux/WSL paths are forbidden: {env_root}"
                )
            roots.append(candidate)
        return roots

    @classmethod
    def _resolve_strategy_dependency_path(
        cls,
        module_name: str,
        allowed_external_modules: set[str],
    ) -> Path | None:
        """Resolve strategy dependencies from AIstock-local code roots only."""
        for root in cls._strategy_dependency_roots():
            if root.name == "scripts" and module_name not in allowed_external_modules:
                continue
            candidate = root / f"{module_name}.py"
            ensure_not_forbidden_worker_workspace_path(
                candidate,
                purpose=f"QE strategy dependency {module_name}",
            )
            if candidate.exists():
                return candidate
        return None

    @classmethod
    def _resolve_strategy_dependency_code(
        cls,
        module_name: str,
        allowed_external_modules: set[str],
    ) -> str | None:
        """Resolve dependency source from AIstock-owned files or strategy catalog.

        Some catalog strategies, for example ``score_weighted_topk_v2``, import
        a base strategy that is stored in ``aistock_strategy_catalog`` rather
        than as an AIstock-local file.  Reading it from the DB keeps generation
        remote-safe: Windows never probes RD-Agent/WSL worker workspaces.
        """
        dep_path = cls._resolve_strategy_dependency_path(module_name, allowed_external_modules)
        if dep_path is not None:
            return dep_path.read_text(encoding="utf-8")

        if module_name not in allowed_external_modules:
            return None

        relpath = f"{module_name}.py"
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT source_code
                        FROM aistock_strategy_catalog
                        WHERE source_code_relpath = %s
                          AND source_code IS NOT NULL
                          AND source_code <> ''
                        ORDER BY updated_at DESC NULLS LAST,
                                 created_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (relpath,),
                    )
                    row = cur.fetchone()
        except Exception as exc:
            logger.debug(
                "QE strategy dependency catalog lookup skipped for %s: %s",
                module_name,
                exc,
            )
            return None
        if not row:
            return None
        return row[0]

    @staticmethod
    def _normalize_execution_algo(execution_algo: Optional[str]) -> str:
        """Return the exact QE execution algo or raise; never silently fallback."""
        raw = (execution_algo or DEFAULT_QE_EXECUTION_ALGO).strip().upper()
        aliases = {"": DEFAULT_QE_EXECUTION_ALGO, "NONE": DEFAULT_QE_EXECUTION_ALGO, "DEFAULT": DEFAULT_QE_EXECUTION_ALGO}
        raw = aliases.get(raw, raw)
        if raw == "VWAP":
            raise ValueError(
                "execution_algo='VWAP' is not implemented in QE minute execution. "
                "It is blocked instead of being silently mapped to TWAP."
            )
        if raw not in SUPPORTED_QE_EXECUTION_ALGOS:
            raise ValueError(
                f"execution_algo='{execution_algo}' is unsupported for QE; "
                f"allowed={sorted(SUPPORTED_QE_EXECUTION_ALGOS)}"
            )
        return raw

    @classmethod
    def _resolve_backtest_freq(
        cls,
        execution_algo: Optional[str],
        custom_params: Optional[Dict[str, Any]],
    ) -> str:
        """Resolve and validate the executor frequency for the requested algo.

        ``backtest_freq`` is a derived compatibility flag.  The execution algo is
        authoritative, and conflicting values fail fast instead of silently
        running a different executor stack than the UI requested.
        """
        params = custom_params or {}
        raw_freq = params.get("backtest_freq")
        algo = cls._normalize_execution_algo(execution_algo)
        if raw_freq in (None, ""):
            return "day" if algo == "CLOSE_PRICE" else "1min"
        freq = str(raw_freq).strip().lower()
        if freq not in {"1min", "day"}:
            raise ValueError("backtest_freq must be '1min' or 'day'")
        expected = "day" if algo == "CLOSE_PRICE" else "1min"
        if freq != expected:
            raise ValueError(
                f"execution_algo={execution_algo or DEFAULT_QE_EXECUTION_ALGO} requires "
                f"backtest_freq={expected}, got {raw_freq!r}; refusing inconsistent config"
            )
        return freq

    @staticmethod
    def _yaml_scalar(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value)
        if text == "<PRED>":
            return text
        safe = re.match(r"^[A-Za-z0-9_./:+\\-]+$", text) is not None
        return text if safe else json.dumps(text, ensure_ascii=False)

    @classmethod
    def _append_yaml_kwargs(cls, lines: List[str], kwargs: Dict[str, Any], indent: str) -> None:
        for k, v in kwargs.items():
            lines.append(f"{indent}{k}: {cls._yaml_scalar(v)}")

    @classmethod
    def _execution_algo_config(
        cls,
        execution_algo: Optional[str],
        execution_algo_params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        algo = cls._normalize_execution_algo(execution_algo)
        params = dict(execution_algo_params or {})
        if algo == "TWAP":
            return {
                "requested_algo": execution_algo or DEFAULT_QE_EXECUTION_ALGO,
                "effective_algo": algo,
                "class": "TailTWAPWithLimitStrategy",
                "module_path": "tail_twap_strategy",
                "kwargs": params,
            }
        if algo == "CLOSE_PRICE":
            return {
                "requested_algo": execution_algo,
                "effective_algo": algo,
                "class": "CloseExecutionStrategy",
                "module_path": "close_execution_strategy",
                "kwargs": params,
            }
        if algo == "V24_PLAN":
            if not params.get("model_path"):
                model_path = os.getenv("QE_V24_PLAN_MODEL_PATH", "").strip()
                if not model_path:
                    raise ValueError("QE_V24_PLAN_MODEL_PATH is required for V24_PLAN execution")
                params["model_path"] = model_path
            return {
                "requested_algo": execution_algo,
                "effective_algo": algo,
                "class": "TailTWAPWithV24PlanStrategy",
                "module_path": "tail_twap_v24_strategy",
                "kwargs": params,
            }
        if algo == "V25_TWO_STAGE":
            catalog_defaults = cls._execution_algo_catalog_entry(algo)["default_config"]
            for key, value in catalog_defaults.items():
                params.setdefault(key, value)
            return {
                "requested_algo": execution_algo,
                "effective_algo": algo,
                "class": "TailTWAPWithV25TwoStageStrategy",
                "module_path": "tail_twap_v25_strategy",
                "kwargs": params,
            }
        if algo == "V25_1_SMALL_CAP":
            catalog_defaults = cls._execution_algo_catalog_entry(algo)["default_config"]
            for key, value in catalog_defaults.items():
                params.setdefault(key, value)
            return {
                "requested_algo": execution_algo,
                "effective_algo": algo,
                "class": "TailTWAPWithV25_1SmallCapStrategy",
                "module_path": "tail_twap_v25_1_strategy",
                "kwargs": params,
            }
        raise AssertionError(f"unreachable execution algo: {algo}")

    @classmethod
    def _execution_algo_params_with_runtime_filters(
        cls,
        execution_algo: Optional[str],
        execution_algo_params: Optional[Dict[str, Any]],
        custom_params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Apply runtime filters that must also reach the minute inner_strategy."""

        params = dict(execution_algo_params or {})
        if cls._is_v25_execution(execution_algo):
            params.setdefault("filter_suspended_on_signal", True)
            params.setdefault(
                "suspend_filter_file",
                (custom_params or {}).get("suspend_filter_file") or SUSPEND_FILTER_FILE,
            )
            params.setdefault(
                "suspend_filter_strict",
                bool((custom_params or {}).get("suspend_filter_strict", True)),
            )
        return params

    @classmethod
    def _is_v25_execution(cls, execution_algo: Optional[str]) -> bool:
        return cls._normalize_execution_algo(execution_algo) in {
            "V25_TWO_STAGE",
            "V25_1_SMALL_CAP",
        }

    @staticmethod
    def _is_suspend_filter_enabled(custom_params: Optional[Dict[str, Any]]) -> bool:
        params = custom_params or {}
        return bool(
            params.get("filter_suspended_on_signal")
            or params.get("exclude_suspended")
            or params.get("filter_suspend_d")
        )

    @staticmethod
    def _parse_date(value: str):
        from datetime import date
        return date.fromisoformat(str(value)[:10])

    def _build_suspend_filter_artifact(
        self,
        data_split: Dict[str, str],
        *,
        strict_audit: bool = True,
    ) -> str:
        """Export suspend_d rows to a local JSON artifact for Qlib runtime."""
        backtest_start = self._parse_date(data_split["test_start"])
        backtest_end = self._parse_date(data_split["backtest_end"])
        if backtest_end < backtest_start:
            raise ValueError("backtest_end is earlier than test_start; cannot build suspend filter")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE
                      AND cal_date BETWEEN %s AND %s
                    ORDER BY cal_date
                    """,
                    (backtest_start, backtest_end),
                )
                trade_dates = [row[0] for row in cur.fetchall()]
                if not trade_dates:
                    raise RuntimeError(
                        f"No trading dates found for suspend filter: {backtest_start}..{backtest_end}"
                    )

                if strict_audit:
                    cur.execute(
                        """
                        SELECT trade_date, status
                        FROM market.dataset_date_refresh_audit
                        WHERE dataset = 'suspend_d'
                          AND trade_date = ANY(%s)
                        """,
                        (trade_dates,),
                    )
                    audit_rows = {row[0]: row[1] for row in cur.fetchall()}
                    missing = [d.isoformat() for d in trade_dates if audit_rows.get(d) != "success"]
                    if missing:
                        raise RuntimeError(
                            "suspend_d refresh audit is incomplete; "
                            f"missing_or_failed_dates={missing[:20]} total={len(missing)}. "
                            "Refresh/seed market.dataset_date_refresh_audit before enabling filter_suspended_on_signal."
                        )

                cur.execute(
                    """
                    SELECT trade_date, ts_code
                    FROM market.suspend_d
                    WHERE suspend_type = 'S'
                      AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date, ts_code
                    """,
                    (backtest_start, backtest_end),
                )
                suspended_by_date: Dict[str, List[str]] = {d.isoformat(): [] for d in trade_dates}
                for trade_date, ts_code in cur.fetchall():
                    key = trade_date.isoformat()
                    if key in suspended_by_date:
                        suspended_by_date[key].append(str(ts_code))

        payload = {
            "enabled": True,
            "source": "market.suspend_d",
            "audit_dataset": "suspend_d",
            "strict_audit": strict_audit,
            "start_date": backtest_start.isoformat(),
            "end_date": backtest_end.isoformat(),
            "trade_date_count": len(trade_dates),
            "suspended_row_count": sum(len(v) for v in suspended_by_date.values()),
            "suspended_by_date": suspended_by_date,
        }
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)

    def _get_strategy_class_name(self, strategy_info: Optional[Dict]) -> str:
        if not strategy_info:
            return "TopkDropoutStrategy"
        pc = strategy_info.get("portfolio_config")
        if isinstance(pc, str):
            pc = json.loads(pc)
        if isinstance(pc, dict) and pc.get("class"):
            return str(pc["class"])
        source_code = strategy_info.get("source_code") or ""
        match = re.search(r"class\s+(\w+)\s*\(", source_code)
        if match:
            return match.group(1)
        return strategy_info.get("strategy_name") or "TopkDropoutStrategy"

    def _ensure_suspend_filter_supported(self, strategy_class: str) -> None:
        supported = {
            "TopkDropoutStrategy",
            "ScoreWeightedTopkStrategy",
            "ScoreWeightedTopkStrategyV2",
            "ScoreWeightedTopkStrategyV2CapacityV1",
        }
        if strategy_class not in supported:
            raise ValueError(
                "filter_suspended_on_signal=True is not supported by strategy "
                f"'{strategy_class}'. Supported strategies: {sorted(supported)}. "
                "QE blocks this request instead of silently ignoring the UI configuration."
            )

    def _ensure_qe_risk_policy_supported(self, strategy_class: str) -> None:
        supported = {
            "TopkDropoutStrategy",
            "SuspendFilterTopkDropoutStrategy",
            "ScoreWeightedTopkStrategy",
            "SuspendFilterScoreWeightedTopkStrategy",
            "ScoreWeightedTopkStrategyV2",
            "SuspendFilterScoreWeightedTopkStrategyV2",
            "ScoreWeightedTopkStrategyV2CapacityV1",
            "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1",
        }
        if strategy_class not in supported:
            raise ValueError(
                "runtime risk_policy.enabled=True is not supported by strategy "
                f"'{strategy_class}'. Supported strategies: {sorted(supported)}. "
                "QE blocks this request instead of silently ignoring forced-exit semantics."
            )

    def _prepare_suspend_filter_runtime(
        self,
        *,
        custom_params: Optional[Dict[str, Any]],
        data_split: Dict[str, str],
        strategy_info: Optional[Dict],
        execution_algo: Optional[str],
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Build the suspend artifact needed by signal filtering or V25 execution."""

        # Mandatory event-risk policies can force exits or block buys before
        # ``filter_suspended_on_signal`` is explicitly set.  In that mode the
        # outer signal strategy must still remove confirmed suspend_d names so
        # Qlib does not emit orders for a fully suspended day.
        signal_filter_enabled = self._is_suspend_filter_enabled(
            custom_params
        ) or self._is_qe_risk_policy_enabled(custom_params)
        execution_filter_required = self._is_v25_execution(execution_algo)
        if not signal_filter_enabled and not execution_filter_required:
            return custom_params, None

        if custom_params is None:
            custom_params = {}

        if signal_filter_enabled:
            strategy_class_for_suspend = self._get_strategy_class_name(strategy_info)
            self._ensure_suspend_filter_supported(strategy_class_for_suspend)

        suspend_filter_json = self._build_suspend_filter_artifact(
            data_split,
            strict_audit=bool(custom_params.get("suspend_filter_strict", True)),
        )
        custom_params["suspend_filter_file"] = SUSPEND_FILTER_FILE
        custom_params.setdefault("suspend_filter_strict", True)

        if signal_filter_enabled:
            custom_params["filter_suspended_on_signal"] = True

        return custom_params, suspend_filter_json

    @staticmethod
    def _risk_policy_profile(custom_params: Optional[Dict[str, Any]]):
        raw_policy = (custom_params or {}).get("risk_policy")
        if raw_policy in (None, "", False):
            raw_policy = {"enabled": False}
        if isinstance(raw_policy, str):
            raw_policy = json.loads(raw_policy)
        if not isinstance(raw_policy, dict):
            raise ValueError("custom_params.risk_policy must be an object or JSON object string")
        from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile

        return RuntimeRiskPolicyProfile.model_validate(raw_policy)

    @classmethod
    def _is_qe_risk_policy_enabled(cls, custom_params: Optional[Dict[str, Any]]) -> bool:
        return bool(cls._risk_policy_profile(custom_params).enabled)

    def _build_qe_risk_policy_artifact(self, data_split: Dict[str, str], custom_params: Dict[str, Any]) -> str:
        """Export ST PIT spans to a local JSON artifact for Qlib runtime."""

        profile = self._risk_policy_profile(custom_params)
        if not profile.enabled:
            raise ValueError("risk policy artifact requested while risk_policy.enabled is false")
        if not profile.providers:
            raise ValueError("risk_policy.enabled=True requires at least one provider")
        if "announcement_risk" in profile.providers:
            raise ValueError(
                "risk_policy.providers includes announcement_risk, but QE announcement-risk runtime is not implemented yet"
            )
        if "st_pit" not in profile.providers:
            raise ValueError("risk_policy.providers must include st_pit for the current QE runtime")

        backtest_start = self._parse_date(data_split["test_start"])
        backtest_end = self._parse_date(data_split["backtest_end"])
        if backtest_end < backtest_start:
            raise ValueError("backtest_end is earlier than test_start; cannot build QE risk policy")

        from backend.services.stock_universe_pit_service import (
            DEFAULT_ST_PIT_START_DATE,
            StockUniversePitService,
        )

        StockUniversePitService().ensure_st_pit_universe(
            universe_key=profile.st_universe_key,
            start_date=DEFAULT_ST_PIT_START_DATE,
            end_date=backtest_end,
            strict=profile.strict_data_ready,
            refresh_policy="coverage",
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE
                      AND cal_date BETWEEN %s AND %s
                    ORDER BY cal_date
                    """,
                    (backtest_start, backtest_end),
                )
                trade_dates = [row[0] for row in cur.fetchall()]
                if not trade_dates:
                    raise RuntimeError(
                        f"No trading dates found for QE risk policy: {backtest_start}..{backtest_end}"
                    )
                cur.execute(
                    """
                    SELECT ts_code, eligible_start, eligible_end, entry_reason,
                           exit_reason, rule_version, metadata
                    FROM market.stock_universe_pit_spans
                    WHERE universe_key = %s
                      AND eligible_start <= %s
                      AND eligible_end >= %s
                    ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (profile.st_universe_key, backtest_end, backtest_start),
                )
                spans = [
                    {
                        "ts_code": row[0],
                        "eligible_start": row[1].isoformat(),
                        "eligible_end": row[2].isoformat(),
                        "entry_reason": row[3],
                        "exit_reason": row[4],
                        "rule_version": row[5],
                        "metadata": row[6] or {},
                    }
                    for row in cur.fetchall()
                ]
                cur.execute(
                    """
                    SELECT universe_key, rule_version, scope, status, dirty,
                           source_fingerprint_sha256, generated_at
                    FROM market.stock_universe_pit_state
                    WHERE universe_key = %s
                    """,
                    (profile.st_universe_key,),
                )
                state = cur.fetchone()

        payload = {
            "enabled": True,
            "contract": profile.policy_version,
            "source": "market.stock_universe_pit_spans",
            "providers": list(profile.providers),
            "hard_actions": list(profile.hard_actions),
            "visible_time_mode": profile.visible_time_mode,
            "strict_data_ready": profile.strict_data_ready,
            "st_universe_key": profile.st_universe_key,
            "start_date": backtest_start.isoformat(),
            "end_date": backtest_end.isoformat(),
            "trade_date_count": len(trade_dates),
            "span_count": len(spans),
            "active_spans": spans,
            "state": {
                "universe_key": state[0] if state else profile.st_universe_key,
                "rule_version": state[1] if state else None,
                "scope": state[2] if state else None,
                "status": state[3] if state else "missing",
                "dirty": bool(state[4]) if state else True,
                "source_fingerprint_sha256": state[5] if state else None,
                "generated_at": state[6].isoformat() if state and state[6] else None,
            },
        }
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)

    def _prepare_risk_policy_runtime(
        self,
        *,
        custom_params: Optional[Dict[str, Any]],
        data_split: Dict[str, str],
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        custom_params = ensure_qe_risk_policy(
            custom_params,
            source="ConfigComposer._prepare_risk_policy_runtime",
        )
        if not self._is_qe_risk_policy_enabled(custom_params):
            return custom_params, None
        risk_policy_json = self._build_qe_risk_policy_artifact(data_split, custom_params)
        profile = self._risk_policy_profile(custom_params)
        custom_params["risk_policy_file"] = RISK_POLICY_FILE
        custom_params["risk_policy_enabled"] = True
        custom_params["risk_policy_strict"] = profile.strict_data_ready
        payload = json.loads(risk_policy_json)
        quote_universe_codes = sorted(
            {
                str(span.get("ts_code") or "").strip().upper()
                for span in payload.get("active_spans", [])
                if str(span.get("ts_code") or "").strip()
            }
        )
        if quote_universe_codes:
            custom_params["quote_universe_codes"] = quote_universe_codes
        return custom_params, risk_policy_json

    @staticmethod
    def _validate_hmm_coefficients_json(content: str) -> None:
        data = json.loads(content)
        if "daily_coefficients" not in data or "stock_sector_map" not in data:
            raise RuntimeError(
                "precomputed HMM coefficients missing required fields: "
                f"keys={list(data.keys())}"
            )
        if not isinstance(data.get("daily_coefficients"), dict) or not data["daily_coefficients"]:
            raise RuntimeError("precomputed HMM coefficients contain no daily_coefficients")
        if not any(isinstance(day, dict) and day for day in data["daily_coefficients"].values()):
            raise RuntimeError("precomputed HMM coefficients contain no non-empty daily sector coefficients")
        if not isinstance(data.get("stock_sector_map"), dict) or not data["stock_sector_map"]:
            raise RuntimeError("precomputed HMM coefficients contain no stock_sector_map")

    def _resolve_hmm_coefficients_json(
        self,
        strategy_params: Dict[str, Any],
        data_split: Dict[str, str],
    ) -> str:
        precomputed = strategy_params.get(PRECOMPUTED_HMM_COEFF_JSON_PARAM)
        if precomputed:
            content = str(precomputed).strip()
            self._validate_hmm_coefficients_json(content)
            logger.info("Using precomputed HMM coefficients from source workspace")
            return content
        return self._precompute_hmm_coefficients(strategy_params, data_split)

    def _resolve_hmm_risk_gate_json(
        self,
        strategy_params: Dict[str, Any],
        data_split: Dict[str, str],
    ) -> str:
        """Resolve HMM risk gate artifact JSON for QE workspace injection.

        Looks for a precomputed risk gate artifact in the HMM model directory,
        matching the data_split window. Falls back to precomputed param if provided.
        """
        precomputed = strategy_params.get("precomputed_hmm_risk_gate_json")
        if precomputed:
            content = str(precomputed).strip()
            self._validate_hmm_risk_gate_json(content)
            logger.info("Using precomputed HMM risk gate from source workspace")
            return content

        model_path = strategy_params.get("sector_hmm_model_path")
        snapshot_id = strategy_params.get("hmm_model_version_id")

        if not model_path and snapshot_id:
            try:
                from ..hmm_training_service import HMMTrainingService
                hmm_svc = HMMTrainingService()
                snapshot = hmm_svc.get_snapshot(str(snapshot_id))
                if snapshot:
                    model_path = snapshot.get("model_path")
            except Exception as exc:
                raise RuntimeError(
                    f"failed to resolve HMM risk gate snapshot {snapshot_id!r}"
                ) from exc

        if not model_path:
            model_path = strategy_params.get(
                "hmm_risk_gate_model_path",
                "backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/models.json",
            )

        test_start = data_split.get("test_start", "")
        backtest_end = data_split.get("backtest_end", "")

        model_dir = Path(model_path).parent
        gate_pattern = f"hmm_risk_gate_*_{test_start}_{backtest_end}.json"
        candidates = list(model_dir.glob(gate_pattern))

        if not candidates:
            gate_pattern_any = "hmm_risk_gate_*.json"
            candidates = list(model_dir.glob(gate_pattern_any))

        if not candidates:
            project_root = Path(__file__).resolve().parents[3]
            fallback = project_root / ".codex_tmp" / "hmm_risk_gate_validation" / "hmm_risk_gate_duration_5d.json"
            if fallback.exists():
                candidates = [fallback]

        if not candidates:
            raise RuntimeError(
                f"HMM risk gate artifact not found. "
                f"Searched: {model_dir}/{gate_pattern} and fallback paths. "
                f"Run scripts/precompute_hmm_risk_gate.py to generate."
            )

        artifact_path = candidates[0]
        content = artifact_path.read_text(encoding="utf-8")
        self._validate_hmm_risk_gate_json(content)
        logger.info("Loaded HMM risk gate artifact: %s (%d bytes)", artifact_path, len(content))
        return content

    @staticmethod
    def _validate_hmm_risk_gate_json(content: str) -> None:
        data = json.loads(content)
        if data.get("artifact_type") != "hmm_risk_gate_v1":
            raise RuntimeError(
                f"Invalid HMM risk gate artifact_type: {data.get('artifact_type')}"
            )
        if "daily_gates" not in data or "stock_sector_map" not in data:
            raise RuntimeError(
                "HMM risk gate artifact missing required fields: "
                f"keys={list(data.keys())}"
            )
        if not isinstance(data.get("daily_gates"), dict) or not data["daily_gates"]:
            raise RuntimeError("HMM risk gate artifact contains no daily_gates")
        if not isinstance(data.get("stock_sector_map"), dict) or not data["stock_sector_map"]:
            raise RuntimeError("HMM risk gate artifact contains no stock_sector_map")

    def compose_experiment(
        self,
        factor_names: List[str],
        factor_sources: Optional[Dict[str, str]] = None,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
        custom_params: Optional[Dict[str, Any]] = None,
        experiment_name: Optional[str] = None,
        # ── 演进支持：生成目标 ──
        evolution_goal: Optional[str] = None,
        # evolution_goal: 本轮实验的目标描述
        # - AI生成时：用户输入的目标
        # - 手工配置时：可为空，由LLM根据上一轮结果自主发挥
        # ── 演进支持：LLM假设参数 ──
        llm_hypothesis: Optional[Dict[str, Any]] = None,
        # llm_hypothesis 结构:
        # {
        #   "hypothesis": "假设描述",
        #   "source": "llm_generate" | "manual",
        #   "reasoning_process": "LLM分析过程（可选）"
        # }
    ) -> Dict[str, Any]:
        """组装完整实验配置。

        实验文件直接写入 RDAgent 侧的 qe_workspace 目录，
        WSL 环境可直接 cd 到该目录执行 qrun。

        Returns:
            包含conf.yaml内容、factor.py内容、WSL命令、实验目录等信息
        """
        experiment_id = self._generate_unique_experiment_id()
        experiment_name = experiment_id  # 两者统一

        # 创建实验目录（在 AIstock 侧本地保存一份）
        exp_dir = _qe_experiment_dir(experiment_name)
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 通过API在RDAgent侧创建实验工作区并链接数据文件
        self._api_setup_experiment_workspace(experiment_name)

        # 默认数据划分（与 RDAgent conf_baseline.yaml 一致）
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)
        self._validate_historical_stock_pool_window(custom_params, data_split)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names, factor_sources)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型：是否包含需要执行代码的自定义因子
        has_custom_factors = any(
            f.get("code_text") for f in factors_info
        )
        has_alpha158 = any(
            f.get("source") == "alpha158" for f in factors_info
        )
        has_alpha360 = any(
            f.get("source") == "alpha360" for f in factors_info
        )

        # 判断模型类型（用于WSL命令环境变量）
        model_type_tag = None
        if model_info and model_info.get("code_text"):
            model_type_raw = model_info.get("model_type") or ""
            if model_type_raw in ("TimeSeries", "timeseries"):
                model_type_tag = "TimeSeries"
            elif model_type_raw in ("Tabular", "tabular"):
                model_type_tag = "Tabular"
            else:
                name_lower = (model_info.get("model_name") or "").lower()
                if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                    model_type_tag = "TimeSeries"
                else:
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        execution_algo = (custom_params or {}).get("execution_algo")
        backtest_freq = self._resolve_backtest_freq(execution_algo, custom_params)
        execution_algo_params = dict((custom_params or {}).get("execution_algo_params") or {})

        # 尾盘涨停未成交处理：从 custom_params 提取并注入到 execution_algo_params
        # （这些参数是给 inner_strategy TailTWAPWithLimitStrategy 的）
        _cp = custom_params or {}
        if _cp.get("unfilled_handler"):
            execution_algo_params["unfilled_handler"] = _cp["unfilled_handler"]
        if _cp.get("unfilled_trigger_minute"):
            execution_algo_params["unfilled_trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            execution_algo_params["unfilled_backup_depth"] = _cp["unfilled_backup_depth"]

        custom_params = merge_qe_minute_runtime_contract(
            custom_params,
            config={"backtest_freq": backtest_freq},
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
            source="config_composer",
            allow_default_execution_algo=True,
        )
        execution_algo = custom_params.get("execution_algo") or execution_algo
        execution_algo_params = dict(custom_params.get("execution_algo_params") or execution_algo_params or {})

        # HMM 预计算（必须在 conf.yaml 之前，使 hmm_coefficients_file 写入策略 kwargs）
        hmm_json_content: Optional[str] = None
        if _cp.get("enable_sector_hmm"):
            # 构造 strategy_params 供 _precompute_hmm_coefficients 使用
            _hmm_sp = dict(custom_params or {})
            hmm_json_content = self._resolve_hmm_coefficients_json(_hmm_sp, data_split)
            custom_params["hmm_coefficients_file"] = "hmm_sector_coefficients.json"

            # 严格验证：策略必须原生支持 HMM
            _hmm_supported_classes = {
                "TopkDropoutWithRiskControlStrategy",
                "ScoreWeightedTopkStrategy",
                "ScoreWeightedTopkStrategyV2",
                "ScoreWeightedTopkStrategyV2CapacityV1",
                "EnhancedTopkDropoutStrategy",
                "SmallCapTopkDropoutStrategy",
            }
            _strategy_class = None
            if strategy_info:
                _pc = strategy_info.get("portfolio_config")
                if isinstance(_pc, str):
                    import json as _json2
                    _pc = _json2.loads(_pc)
                if _pc:
                    _strategy_class = _pc.get("class")
                if not _strategy_class:
                    _strategy_class = strategy_info.get("strategy_name", "")
            if _strategy_class and _strategy_class not in _hmm_supported_classes:
                raise ValueError(
                    f"enable_sector_hmm=True 但策略 '{_strategy_class}' 不支持 HMM。"
                    f"支持的策略: {', '.join(sorted(_hmm_supported_classes))}"
                )

        custom_params, risk_policy_json = self._prepare_risk_policy_runtime(
            custom_params=custom_params,
            data_split=data_split,
        )
        custom_params, suspend_filter_json = self._prepare_suspend_filter_runtime(
            custom_params=custom_params,
            data_split=data_split,
            strategy_info=strategy_info,
            execution_algo=execution_algo,
        )

        # 生成conf.yaml
        conf_yaml = self._compose_conf_yaml(
            factors_info=factors_info,
            model_info=model_info,
            strategy_info=strategy_info,
            data_split=data_split,
            custom_params=custom_params,
            has_custom_factors=has_custom_factors,
            has_alpha158=has_alpha158,
            has_alpha360=has_alpha360,
            disable_alpha158=disable_alpha158,
            quick_train=quick_train,
            backtest_freq=backtest_freq,
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
        )

        # 生成因子文件和预处理脚本（如果有自定义因子）
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info, data_split=data_split)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")

        # 写入 HMM 系数文件（如果启用了 HMM）
        if hmm_json_content:
            hmm_path = exp_dir / "hmm_sector_coefficients.json"
            hmm_path.write_text(hmm_json_content, encoding="utf-8")

        if suspend_filter_json:
            (exp_dir / SUSPEND_FILTER_FILE).write_text(suspend_filter_json, encoding="utf-8")
        if risk_policy_json:
            (exp_dir / RISK_POLICY_FILE).write_text(risk_policy_json, encoding="utf-8")

        # 写入因子原始代码到 factors/ 子目录（保持原始格式不变）
        if has_factor_files:
            self._write_factor_files(exp_dir, factors_info)

        if prepare_factors_py:
            prepare_path = exp_dir / "prepare_factors.py"
            prepare_path.write_text(prepare_factors_py, encoding="utf-8")

        # 如果使用 DynamicFactorsOnlyLoader，复制 QE 独立的 loader 文件到实验目录
        if has_custom_factors and disable_alpha158:
            self._copy_qe_custom_loaders(exp_dir)

        # 复制read_exp_res.py模板
        self._copy_read_exp_res(exp_dir)

        # 复制 qrun_limit runner（分钟线使用 qrun_limit_minute.py，日线使用 qrun_limit.py）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        import shutil
        if backtest_freq != "day" or bool((custom_params or {}).get("_seed_ensemble_config")):
            # 分钟线：复制 qrun_limit_minute.py（含内存 patch + benchmark 注入）
            minute_src = scripts_dir / "qrun_limit_minute.py"
            if minute_src.exists():
                shutil.copy2(minute_src, exp_dir / "qrun_limit_minute.py")
            # 复制 TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_src = scripts_dir / "tail_twap_strategy.py"
            if twap_src.exists():
                shutil.copy2(twap_src, exp_dir / "tail_twap_strategy.py")
            # 复制 v24 Plan 执行策略（依赖 tail_twap_strategy.py）
            v24_src = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_src.exists():
                shutil.copy2(v24_src, exp_dir / "tail_twap_v24_strategy.py")
            for helper_name in ("tail_twap_v25_strategy.py", "tail_twap_v25_1_strategy.py", "qe_board_lot_exchange.py", "close_execution_strategy.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
                helper_src = self._resolve_qe_helper_asset(scripts_dir, helper_name)
                if helper_src.exists():
                    shutil.copy2(helper_src, exp_dir / helper_name)
            # benchmark parquet
            bench_src = scripts_dir / "benchmark_sh000300.parquet"
            if bench_src.exists():
                shutil.copy2(bench_src, exp_dir / "benchmark_sh000300.parquet")
        for helper_name in ("qe_board_lot_exchange.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
            helper_src = scripts_dir / helper_name
            if helper_src.exists():
                shutil.copy2(helper_src, exp_dir / helper_name)
        # 始终复制日线版作为 fallback
        qrun_limit_src = scripts_dir / "qrun_limit.py"
        if qrun_limit_src.exists():
            shutil.copy2(qrun_limit_src, exp_dir / "qrun_limit.py")
        # benchmark parquet 也复制到日线实验的 qe_workspace（qrun_limit.py 同样需要）
        bench_src = scripts_dir / "benchmark_sh000300.parquet"
        if bench_src.exists():
            local_benchmark = exp_dir / "benchmark_sh000300.parquet"
            if not local_benchmark.exists():
                shutil.copy2(bench_src, local_benchmark)

        # 如果模型使用自定义源码，写入实验目录（model.py + model_cls导出）
        if model_info and model_info.get("code_text"):
            self._write_custom_model(exp_dir, model_info)

        # 如果策略使用自定义源码，复制到实验目录
        if strategy_info and strategy_info.get("source_code"):
            self._write_custom_strategy(exp_dir, strategy_info)

        # 通过API同步文件到RDAgent侧（支持独立部署场景）
        self._api_sync_experiment_files(experiment_name, exp_dir)

        # 生成WSL命令（直接使用 qe_workspace WSL路径）
        wsl_path = f"{QE_WORKSPACE_WSL}/{experiment_name}"
        wsl_command = self._generate_wsl_command(
            wsl_path, has_custom_factors=has_custom_factors,
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            backtest_freq=backtest_freq,
            seed_ensemble_enabled=bool((custom_params or {}).get("_seed_ensemble_config")),
        )

        # 保存实验记录到数据库
        self._save_experiment_record(
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            exp_dir=str(exp_dir),
            factor_names=factor_names,
            model_id=model_id,
            strategy_id=strategy_id,
            data_split=data_split,
            custom_params=custom_params,
            evolution_goal=evolution_goal,
            llm_hypothesis=llm_hypothesis,
        )

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
            "experiment_dir": str(exp_dir),
            "conf_yaml_path": str(conf_path),
            "factors_dir": str(exp_dir / "factors") if has_factor_files else None,
            "prepare_factors_py_path": str(exp_dir / "prepare_factors.py") if prepare_factors_py else None,
            "wsl_path": wsl_path,
            "wsl_command": wsl_command,
            "conf_yaml_preview": conf_yaml[:2000],
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
            "data_split": data_split,
            "llm_hypothesis": llm_hypothesis,
        }

    def compose_experiment_in_memory(
        self,
        factor_names: List[str],
        factor_sources: Optional[Dict[str, str]] = None,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
        custom_params: Optional[Dict[str, Any]] = None,
        experiment_name: Optional[str] = None,
        evolution_goal: Optional[str] = None,
        llm_hypothesis: Optional[Dict[str, Any]] = None,
        skip_db_save: bool = False,
        execution_algo: Optional[str] = None,
        execution_algo_params: Optional[Dict[str, Any]] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
        train_only: bool = False,
    ) -> Dict[str, Any]:
        """组装实验配置到内存字典，不写入磁盘。

        复用现有生成逻辑，但将所有文件内容收集到 Dict[str, str]，
        供演进循环通过 RDAgent loop API 的 experiment_files 参数直接传递。

        Args:
            train_only: True 时生成 --train-only 命令，跳过回测（多Alpha从节点模式）。

        Returns:
            {
                "experiment_files": Dict[str, str],  # 相对路径 -> 文件内容
                "wsl_command": str,
                "experiment_name": str,
                "experiment_id": str,
                "factor_count": int,
                "has_custom_factors": bool,
            }
        """
        import uuid as _uuid

        experiment_id = str(_uuid.uuid4())[:8]
        if not experiment_name:
            experiment_name = f"qe_exp_{experiment_id}"

        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)
        self._validate_historical_stock_pool_window(custom_params, data_split)

        # ── 获取因子 / 模型 / 策略信息 ──
        factors_info = self._get_factors_info(factor_names, factor_sources)
        model_info = self._get_model_info(model_id) if model_id else None
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        has_custom_factors = any(f.get("code_text") for f in factors_info)
        has_alpha158 = any(f.get("source") == "alpha158" for f in factors_info)
        has_alpha360 = any(f.get("source") == "alpha360" for f in factors_info)

        model_type_tag = None
        if model_info and model_info.get("code_text"):
            model_type_raw = model_info.get("model_type") or ""
            if model_type_raw in ("TimeSeries", "timeseries"):
                model_type_tag = "TimeSeries"
            elif model_type_raw in ("Tabular", "tabular"):
                model_type_tag = "Tabular"
            else:
                name_lower = (model_info.get("model_name") or "").lower()
                if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                    model_type_tag = "TimeSeries"
                else:
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        disable_alpha158 = False
        quick_train = False
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        backtest_freq = self._resolve_backtest_freq(execution_algo, custom_params)

        # 尾盘涨停未成交处理：从 custom_params 提取并注入到 execution_algo_params
        # （这些参数是给 inner_strategy TailTWAPWithLimitStrategy 的）
        _cp = custom_params or {}
        if execution_algo_params is None:
            execution_algo_params = {}
        else:
            execution_algo_params = dict(execution_algo_params)
        if _cp.get("unfilled_handler"):
            execution_algo_params["unfilled_handler"] = _cp["unfilled_handler"]
        if _cp.get("unfilled_trigger_minute"):
            execution_algo_params["unfilled_trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            execution_algo_params["unfilled_backup_depth"] = _cp["unfilled_backup_depth"]

        custom_params = merge_qe_minute_runtime_contract(
            custom_params,
            config={"backtest_freq": backtest_freq},
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
            source="config_composer_in_memory",
            allow_default_execution_algo=True,
        )
        execution_algo = custom_params.get("execution_algo") or execution_algo
        execution_algo_params = dict(custom_params.get("execution_algo_params") or execution_algo_params or {})

        # ── 获取路径配置（支持多节点） ──
        rdagent_cfg = self._fetch_workspace_config(node_id)
        workspace_wsl = rdagent_cfg.get("workspace_base", QE_WORKSPACE_WSL)
        qlib_data_path = rdagent_cfg.get("qlib_data_path", QLIB_DATA_PATH_WSL)
        factor_data_dir = rdagent_cfg.get("factor_data_dir", RDAGENT_FACTOR_DATA_WSL)
        qlib_minute_path = rdagent_cfg.get("qlib_minute_path", QLIB_MINUTE_PATH_WSL)

        # ── 生成各文件内容到 dict ──
        experiment_files: Dict[str, str] = {}

        # 0) HMM 预计算（必须在 conf.yaml 之前，使 hmm_coefficients_file 写入策略 kwargs）
        # 与 compose_experiment() 一致，从 custom_params 检查 enable_sector_hmm
        if _cp.get("enable_sector_hmm"):
            # 构造 strategy_params 供 _precompute_hmm_coefficients 使用
            _hmm_sp = dict(_cp)
            hmm_json = self._resolve_hmm_coefficients_json(_hmm_sp, data_split)
            experiment_files["hmm_sector_coefficients.json"] = hmm_json
            # 注入到 custom_params 以便 _compose_conf_yaml 写入 strategy kwargs
            if custom_params is None:
                custom_params = {}
            custom_params["hmm_coefficients_file"] = "hmm_sector_coefficients.json"

            # 严格验证：策略必须原生支持 HMM，禁止静默替换策略
            _hmm_supported_classes = {
                "TopkDropoutWithRiskControlStrategy",
                "ScoreWeightedTopkStrategy",
                "ScoreWeightedTopkStrategyV2",
                "ScoreWeightedTopkStrategyV2CapacityV1",
                "EnhancedTopkDropoutStrategy",
                "SmallCapTopkDropoutStrategy",
            }
            _strategy_class = None
            if strategy_info:
                _pc = strategy_info.get("portfolio_config")
                if isinstance(_pc, str):
                    import json as _json
                    _pc = _json.loads(_pc)
                if _pc:
                    _strategy_class = _pc.get("class")
                if not _strategy_class:
                    _sc = strategy_info.get("source_code", "")
                    import re as _re
                    _class_match = _re.search(r'class\s+(\w+)\s*\(', _sc)
                    if _class_match:
                        _strategy_class = _class_match.group(1)

            if _strategy_class and _strategy_class not in _hmm_supported_classes:
                raise ValueError(
                    f"enable_sector_hmm=True 但策略 '{_strategy_class}' 不支持 HMM。"
                    f"已支持 HMM 的策略: {', '.join(sorted(_hmm_supported_classes))}。"
                    f"请切换到支持 HMM 的策略或关闭 HMM。"
                )

        # 0b) HMM Risk Gate 预计算注入
        if _cp.get("enable_hmm_risk_gate"):
            risk_gate_json = self._resolve_hmm_risk_gate_json(_cp, data_split)
            experiment_files["hmm_risk_gate.json"] = risk_gate_json
            if custom_params is None:
                custom_params = {}
            custom_params["hmm_risk_gate_file"] = "hmm_risk_gate.json"
            custom_params["enable_hmm_risk_gate"] = True

        custom_params, risk_policy_json = self._prepare_risk_policy_runtime(
            custom_params=custom_params,
            data_split=data_split,
        )
        if risk_policy_json:
            experiment_files[RISK_POLICY_FILE] = risk_policy_json
        custom_params, suspend_filter_json = self._prepare_suspend_filter_runtime(
            custom_params=custom_params,
            data_split=data_split,
            strategy_info=strategy_info,
            execution_algo=execution_algo,
        )
        if suspend_filter_json:
            experiment_files[SUSPEND_FILTER_FILE] = suspend_filter_json

        # 1) conf.yaml
        conf_yaml = self._compose_conf_yaml(
            factors_info=factors_info,
            model_info=model_info,
            strategy_info=strategy_info,
            data_split=data_split,
            custom_params=custom_params,
            has_custom_factors=has_custom_factors,
            has_alpha158=has_alpha158,
            has_alpha360=has_alpha360,
            disable_alpha158=disable_alpha158,
            quick_train=quick_train,
            qlib_data_path=qlib_data_path,
            qlib_minute_path=qlib_minute_path,
            backtest_freq=backtest_freq,
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
            initial_cash=(strategy_params or {}).get("initial_cash"),
        )
        experiment_files["conf.yaml"] = conf_yaml

        # 2) 因子文件 + prepare_factors.py
        if has_custom_factors:
            # 因子代码文件 → factors/<name>.py
            for f in factors_info:
                code = f.get("code_text")
                if code:
                    experiment_files[f"factors/{f['factor_name']}.py"] = code

            prepare_factors_py = self._compose_prepare_factors(factors_info, factor_data_dir=factor_data_dir, data_split=data_split)
            if prepare_factors_py:
                experiment_files["prepare_factors.py"] = prepare_factors_py

        # 3) qe_custom_loaders.py（仅 disable_alpha158 时需要）
        if has_custom_factors and disable_alpha158:
            qe_loader_source = Path(__file__).parent / "qe_custom_loaders.py"
            if qe_loader_source.exists():
                experiment_files["qe_custom_loaders.py"] = qe_loader_source.read_text(encoding="utf-8")

        # 4) read_exp_res.py
        experiment_files["read_exp_res.py"] = self._get_read_exp_res_content()

        # 4b) qrun_limit runner（分钟线/日线双模板）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        qrun_limit_path = scripts_dir / "qrun_limit.py"
        if qrun_limit_path.exists():
            experiment_files["qrun_limit.py"] = qrun_limit_path.read_text(encoding="utf-8")
        if backtest_freq != "day" or bool((custom_params or {}).get("_seed_ensemble_config")):
            minute_path = scripts_dir / "qrun_limit_minute.py"
            if minute_path.exists():
                experiment_files["qrun_limit_minute.py"] = minute_path.read_text(encoding="utf-8")
            # TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_path = scripts_dir / "tail_twap_strategy.py"
            if twap_path.exists():
                experiment_files["tail_twap_strategy.py"] = twap_path.read_text(encoding="utf-8")
            # v24 Plan 执行策略（继承 TailTWAPWithLimitStrategy）
            v24_path = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_path.exists():
                experiment_files["tail_twap_v24_strategy.py"] = v24_path.read_text(encoding="utf-8")
            for helper_name in ("tail_twap_v25_strategy.py", "tail_twap_v25_1_strategy.py", "qe_board_lot_exchange.py", "close_execution_strategy.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
                helper_path = self._resolve_qe_helper_asset(scripts_dir, helper_name)
                if helper_path.exists():
                    experiment_files[helper_name] = helper_path.read_text(encoding="utf-8")
            # benchmark parquet 是二进制文件，需要特殊处理
            bench_path = scripts_dir / "benchmark_sh000300.parquet"
            if bench_path.exists():
                import base64
                experiment_files["benchmark_sh000300.parquet.b64"] = base64.b64encode(
                    bench_path.read_bytes()
                ).decode("ascii")
        for helper_name in ("qe_board_lot_exchange.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
            helper_path = scripts_dir / helper_name
            if helper_path.exists():
                experiment_files[helper_name] = helper_path.read_text(encoding="utf-8")

        # 5) model.py（自定义模型）
        if model_info and model_info.get("code_text"):
            experiment_files["model.py"] = self._build_model_py_content(model_info)

        # 6) custom_strategy.py（自定义策略）
        if strategy_info and strategy_info.get("source_code"):
            strategy_content, strategy_deps = self._build_strategy_py_content(strategy_info)
            experiment_files["custom_strategy.py"] = strategy_content
            for dep_name, dep_content in strategy_deps.items():
                experiment_files[dep_name] = dep_content

        # 7) hmm_sector_coefficients.json — 已在步骤 0 提前处理

        # ── 生成 WSL 命令 ──
        wsl_path = f"{workspace_wsl}/{experiment_name}"
        factor_cache_dir = rdagent_cfg.get("factor_cache_dir")
        _, auto_core_parts = self._build_auto_wsl_command_parts(
            wsl_path,
            has_custom_factors=has_custom_factors,
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            backtest_freq=backtest_freq,
            train_only=train_only,
            factor_cache_dir=factor_cache_dir,
            seed_ensemble_enabled=bool((custom_params or {}).get("_seed_ensemble_config")),
        )
        wsl_command = self._generate_wsl_command(
            wsl_path,
            has_custom_factors=has_custom_factors,
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            mode="auto",
            backtest_freq=backtest_freq,
            train_only=train_only,
            factor_cache_dir=factor_cache_dir,
            seed_ensemble_enabled=bool((custom_params or {}).get("_seed_ensemble_config")),
        )

        # ── 保存 DB 记录（不写文件） ──
        if not skip_db_save:
            self._save_experiment_record(
                experiment_id=experiment_id,
                experiment_name=experiment_name,
                exp_dir=f"<in_memory>/{experiment_name}",
                factor_names=factor_names,
                model_id=model_id,
                strategy_id=strategy_id,
                data_split=data_split,
                custom_params=custom_params,
                evolution_goal=evolution_goal,
                llm_hypothesis=llm_hypothesis,
            )

        return {
            "experiment_files": experiment_files,
            "wsl_command": wsl_command,
            "wsl_command_core": " && ".join(auto_core_parts),
            "wsl_workdir": wsl_path,
            "experiment_name": experiment_name,
            "experiment_id": experiment_id,
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
        }

    # ── 内存生成辅助方法 ──

    def _extract_nn_model_class_name(self, code_text: str, fallback: str) -> str:
        """Extract the exported NN class name from custom model source."""

        model_cls_match = re.search(r"^\s*model_cls\s*=\s*([A-Za-z_]\w*)\s*$", code_text, re.MULTILINE)
        if model_cls_match:
            return model_cls_match.group(1)

        class_matches = re.findall(r"class\s+([A-Za-z_]\w*)\s*\(", code_text)
        if class_matches:
            return class_matches[-1]
        return fallback

    def _build_model_py_content(self, model_info: Dict) -> str:
        """生成 model.py 文件内容（不写磁盘）。"""
        code_text = model_info.get("code_text", "")
        model_name = model_info.get("model_name", "CustomModel")
        nn_class_name = self._extract_nn_model_class_name(code_text, model_name)
        return f'"""\nRDAgent SOTA模型: {model_name}\nQuantEvolver自动生成 - 由 GeneralPTNN 通过 pt_model_uri: "model.model_cls" 加载\n"""\n{code_text}\n\n# GeneralPTNN 通过此变量加载 NN 类\nmodel_cls = {nn_class_name}\n'

    def _build_strategy_py_content(self, strategy_info: Dict) -> tuple:
        """生成 custom_strategy.py 文件内容（不写磁盘）。

        复用 _write_custom_strategy 中的验证和 import 处理逻辑。

        Returns:
            (source_code, deps_dict) — deps_dict: {filename: content} 依赖文件
        """
        source_code = strategy_info.get("source_code", "")
        if not source_code:
            raise ValueError(
                f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码 (source_code)，"
                f"无法生成策略文件。请检查策略目录。"
            )

        validation_result = self._validate_strategy_code(source_code)
        if not validation_result["ok"]:
            raise ValueError(
                f"策略代码编译验证失败:\n{validation_result['error']}\n"
                f"请修复策略代码后再创建实验。"
            )

        # 处理相对导入：读取依赖文件，转换为本地导入
        import re as _re
        # 只允许从 AIstock 本地代码/资产目录复制策略类文件，避免直接读取
        # RDAgent/WSL worker workspace 或误复制运行时文件。
        _STRATEGY_DEP_WHITELIST = {"score_weighted_strategy", "score_weighted_strategy_v2",
                                   "tail_twap_strategy", "tail_twap_v24_strategy", "tail_twap_v25_strategy", "tail_twap_v25_1_strategy", "qe_board_lot_exchange", "close_execution_strategy", "qe_suspend_filter", "qe_event_risk_policy", "qe_suspend_filter_strategy", "qe_suspend_filter_score_weighted_strategy"}
        deps_dict: Dict[str, str] = {}

        def _resolve_deps(code: str, collected: set) -> str:
            """递归处理相对导入并收集依赖文件内容."""
            out_lines = []
            for ln in code.split("\n"):
                s = ln.strip()
                # 匹配 from .module import ... (相对导入)
                m = _re.match(r'^(\s*)from\s+\.(\w+)\s+import\s+(.+)$', s)
                if m:
                    indent, mod, imps = m.group(1), m.group(2), m.group(3)
                    dep_code = self._resolve_strategy_dependency_code(mod, _STRATEGY_DEP_WHITELIST)
                    if dep_code is None and mod in _STRATEGY_DEP_WHITELIST:
                        raise ValueError(f"策略依赖文件缺失，无法打包到 QE loop payload: {mod}.py")
                    if dep_code is not None and mod not in collected:
                        collected.add(mod)
                        dep_code = _resolve_deps(dep_code, collected)
                        deps_dict[f"{mod}.py"] = dep_code
                    out_lines.append(f"{indent}from {mod} import {imps}")
                    continue
                # 匹配 from module import ... (无点号，检查是否为本地策略包模块)
                m2 = _re.match(r'^(\s*)from\s+(\w+)\s+import\s+(.+)$', s)
                if m2:
                    indent, mod, imps = m2.group(1), m2.group(2), m2.group(3)
                    dep_code = self._resolve_strategy_dependency_code(mod, _STRATEGY_DEP_WHITELIST)
                    if dep_code is None and mod in _STRATEGY_DEP_WHITELIST:
                        raise ValueError(f"策略依赖文件缺失，无法打包到 QE loop payload: {mod}.py")
                    if dep_code is not None and mod not in collected:
                        collected.add(mod)
                        dep_code = _resolve_deps(dep_code, collected)
                        deps_dict[f"{mod}.py"] = dep_code
                    out_lines.append(ln)
                    continue
                if s == "@register":
                    continue
                out_lines.append(ln)
            return "\n".join(out_lines)

        collected_mods: set = set()
        source_code = _resolve_deps(source_code, collected_mods)

        # 自动添加缺失的 import
        required_imports = ["import pandas as pd", "import numpy as np"]
        import_lines = [imp for imp in required_imports if imp not in source_code]
        if import_lines:
            lines = source_code.split("\n")
            insert_pos = 0
            in_docstring = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                if not stripped:
                    insert_pos = i + 1
                    continue
                if stripped.startswith("#"):
                    insert_pos = i + 1
                    continue
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = not in_docstring
                    insert_pos = i + 1
                    continue
                if in_docstring:
                    insert_pos = i + 1
                    continue
                if stripped.startswith("import ") or stripped.startswith("from "):
                    insert_pos = i + 1
                    continue
                break
            for imp in import_lines:
                lines.insert(insert_pos, imp)
                insert_pos += 1
            source_code = "\n".join(lines)

        # 移除函数内部的重复 import
        lines = source_code.split("\n")
        cleaned_lines = []
        in_function = False
        indent_level = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.match(r'^(def |async def |class )', stripped):
                in_function = True
                indent_level = len(line) - len(line.lstrip())
            elif in_function and line and not line[0].isspace():
                in_function = False
            elif in_function and line:
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= indent_level and stripped and not stripped.startswith('#'):
                    in_function = False
            if in_function and (stripped.startswith("import pandas") or
                                stripped.startswith("import numpy")):
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines), deps_dict

    def sync_experiment_results(self, experiment_id: str) -> Dict[str, Any]:
        """同步实验结果。

        从实验目录读取QLib执行结果文件。
        """
        # 获取实验记录
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            return {"ok": False, "error": f"实验 {experiment_id} 不存在"}

        exp_dir = ensure_aistock_artifact_path(
            Path(exp_record.get("experiment_dir", "")),
            purpose=f"QE result sync local artifact directory: {experiment_id}",
            extra_roots=[QE_EXPERIMENTS_ROOT],
        )
        if not exp_dir.exists():
            return {"ok": False, "error": f"实验目录不存在: {exp_dir}"}

        results = {}

        # 读取qlib_res.csv
        qlib_res_path = exp_dir / "qlib_res.csv"
        if qlib_res_path.exists():
            try:
                import csv
                with open(qlib_res_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    results["qlib_res"] = list(reader)
            except Exception as e:
                raise RuntimeError(f"读取qlib_res.csv失败: {e}") from e

        # 读取回测指标
        metrics = self._extract_metrics_from_results(exp_dir)
        results["metrics"] = metrics

        # 更新数据库
        if metrics:
            self._update_experiment_metrics(experiment_id, metrics)
            # 为每个参与因子保存实验表现指标
            try:
                self._save_factor_experiment_metrics(experiment_id, metrics)
            except Exception as e:
                raise RuntimeError(f"保存因子实验指标失败: {e}") from e

        results["ok"] = True
        results["experiment_id"] = experiment_id
        return results

    def list_experiments(
        self,
        limit: int = 50,
        offset: int = 0,
        include_children: bool = False,
        detail: str = "summary",
    ) -> Dict[str, Any]:
        """获取实验列表；默认只返回适合列表/MCP 使用的标量摘要。"""
        if include_children:
            return self._list_experiment_history(limit=limit, offset=offset, detail=detail)

        full_detail = detail == "full"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM qe_experiments")
                total = cur.fetchone()[0]

                if full_detail:
                    cur.execute("""
                        SELECT experiment_id, experiment_name, status,
                               factor_names, model_id, strategy_id,
                               workspace_path, wsl_command,
                               result_metrics, qe_task_id, qe_loop_id,
                               loop_index, parent_experiment_id, is_evolution_loop,
                               ic, icir, rank_ic, rank_icir,
                               annualized_return, max_drawdown, information_ratio,
                               annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
                               created_at, updated_at, custom_params,
                               alpha_mode, multi_alpha_config, parent_multi_alpha_id
                        FROM qe_experiments
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                else:
                    cur.execute("""
                        SELECT experiment_id, experiment_name, status,
                               jsonb_array_length(COALESCE(factor_names, '[]'::jsonb)) AS factor_count,
                               model_id, strategy_id,
                               qe_task_id, qe_loop_id,
                               loop_index, parent_experiment_id, is_evolution_loop,
                               ic, icir, rank_ic, rank_icir,
                               annualized_return, max_drawdown, information_ratio,
                               annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
                               created_at, updated_at,
                               alpha_mode, parent_multi_alpha_id
                        FROM qe_experiments
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        items = rows if full_detail else [compact_experiment_row(row) for row in rows]
        return {"ok": True, "total": total, "items": items, "detail": "full" if full_detail else "summary"}

    @staticmethod
    def _normalize_history_parent_ids(
        rows: list[dict[str, Any]],
        selected_parent_ids: set[str],
    ) -> list[dict[str, Any]]:
        """Attach custom/strategy evolution loops to their real base experiment."""
        normalized: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            base_experiment_id = item.pop("_evolution_base_experiment_id", None)
            item.pop("_evolution_task_type", None)
            if item.get("is_evolution_loop") and base_experiment_id:
                current_parent = item.get("parent_experiment_id")
                parent_is_task_id = current_parent == item.get("qe_task_id")
                parent_not_in_page = bool(selected_parent_ids) and current_parent not in selected_parent_ids
                if parent_is_task_id or parent_not_in_page:
                    item["parent_experiment_id"] = base_experiment_id
            normalized.append(item)
        return normalized

    def _list_experiment_history(
        self,
        limit: int = 50,
        offset: int = 0,
        detail: str = "summary",
    ) -> Dict[str, Any]:
        """Return paged top-level QE history rows plus their evolution loops.

        The UI groups child loops by parent_experiment_id.  Standard auto
        evolution uses task_id == root experiment_id, but custom/strategy
        evolution creates a separate *_base experiment while loop rows were
        historically persisted with parent_experiment_id == task_id.  This view
        normalizes that relationship without mutating existing experiment rows.
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM qe_experiments WHERE parent_experiment_id IS NULL")
                total = cur.fetchone()[0]

                cur.execute("""
                    WITH parent_rows AS (
                        SELECT e.experiment_id,
                               GREATEST(
                                   COALESCE(e.updated_at, e.created_at),
                                   COALESCE((
                                       SELECT MAX(COALESCE(c.updated_at, c.created_at))
                                       FROM qe_experiments c
                                       LEFT JOIN qe_evolution_tasks et
                                         ON et.task_id = c.qe_task_id
                                       WHERE c.experiment_id <> e.experiment_id
                                         AND (
                                             c.parent_experiment_id = e.experiment_id
                                             OR et.base_experiment_id = e.experiment_id
                                         )
                                   ), COALESCE(e.updated_at, e.created_at))
                               ) AS history_updated_at
                        FROM qe_experiments e
                        WHERE e.parent_experiment_id IS NULL
                    )
                    SELECT experiment_id
                    FROM parent_rows
                    ORDER BY history_updated_at DESC NULLS LAST, experiment_id DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                parent_ids = [row[0] for row in cur.fetchall()]

                if not parent_ids:
                    return {"ok": True, "total": total, "items": []}

                select_fields = """
                           e.experiment_id, e.experiment_name, e.status,
                           e.factor_names, e.model_id, e.strategy_id,
                           e.qe_task_id, e.qe_loop_id,
                           e.loop_index, e.parent_experiment_id, e.is_evolution_loop,
                           e.ic, e.icir, e.rank_ic, e.rank_icir,
                           e.annualized_return, e.max_drawdown, e.information_ratio,
                           e.annualized_return_no_cost, e.max_drawdown_no_cost, e.information_ratio_no_cost,
                           e.created_at, e.updated_at,
                           e.alpha_mode, e.parent_multi_alpha_id,
                           et.base_experiment_id AS _evolution_base_experiment_id,
                           et.task_type AS _evolution_task_type
                    """
                if detail == "full":
                    select_fields = """
                           e.experiment_id, e.experiment_name, e.status,
                           e.factor_names, e.model_id, e.strategy_id,
                           e.workspace_path, e.wsl_command,
                           e.result_metrics, e.qe_task_id, e.qe_loop_id,
                           e.loop_index, e.parent_experiment_id, e.is_evolution_loop,
                           e.ic, e.icir, e.rank_ic, e.rank_icir,
                           e.annualized_return, e.max_drawdown, e.information_ratio,
                           e.annualized_return_no_cost, e.max_drawdown_no_cost, e.information_ratio_no_cost,
                           e.created_at, e.updated_at, e.custom_params,
                           e.alpha_mode, e.multi_alpha_config, e.parent_multi_alpha_id,
                           et.base_experiment_id AS _evolution_base_experiment_id,
                           et.task_type AS _evolution_task_type
                    """

                cur.execute(f"""
                    SELECT {select_fields}
                    FROM qe_experiments e
                    LEFT JOIN qe_evolution_tasks et
                      ON et.task_id = e.qe_task_id
                    WHERE e.experiment_id = ANY(%s)
                       OR e.parent_experiment_id = ANY(%s)
                       OR et.base_experiment_id = ANY(%s)
                    ORDER BY
                        array_position(%s::text[], COALESCE(e.parent_experiment_id, e.experiment_id, et.base_experiment_id)),
                        CASE WHEN e.parent_experiment_id IS NULL THEN 0 ELSE 1 END,
                        e.loop_index ASC NULLS LAST,
                        e.created_at ASC NULLS LAST
                """, (parent_ids, parent_ids, parent_ids, parent_ids))
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        normalized = self._normalize_history_parent_ids(rows, set(parent_ids))
        if detail != "full":
            normalized = [compact_experiment_row(row) for row in normalized]
        # Preserve parent page order, then place child loops under each parent.
        order = {experiment_id: idx for idx, experiment_id in enumerate(parent_ids)}
        normalized.sort(
            key=lambda exp: (
                order.get(exp.get("parent_experiment_id") or exp.get("experiment_id"), len(order)),
                0 if not exp.get("parent_experiment_id") else 1,
                exp.get("loop_index") if exp.get("loop_index") is not None else 10**9,
                str(exp.get("created_at") or ""),
            )
        )
        return {"ok": True, "total": total, "items": normalized, "detail": "full" if detail == "full" else "summary"}

    def get_experiment_detail(self, experiment_id: str, detail: str = "summary") -> Dict[str, Any]:
        """获取实验详情；默认排除 result_metrics 等大 JSONB。"""
        full_detail = detail == "full"
        with get_conn() as conn:
            with conn.cursor() as cur:
                if full_detail:
                    cur.execute("""
                        SELECT * FROM qe_experiments WHERE experiment_id = %s
                    """, (experiment_id,))
                else:
                    cur.execute("""
                        SELECT experiment_id, task_id, round_id, experiment_name, status,
                               factor_names, model_id, strategy_id,
                               data_split, custom_params, conf_yaml_path, workspace_path,
                               qe_task_id, qe_loop_id, loop_index, parent_experiment_id,
                               is_evolution_loop,
                               ic, icir, rank_ic, rank_icir,
                               annualized_return, max_drawdown, information_ratio,
                               excess_return_with_cost_mean, excess_return_without_cost_mean,
                               annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
                               model_catalog_id, alpha_mode, multi_alpha_config, parent_multi_alpha_id,
                               evolution_goal, llm_hypothesis, llm_feedback,
                               created_at, started_at, completed_at
                        FROM qe_experiments WHERE experiment_id = %s
                    """, (experiment_id,))
                row = cur.fetchone()
                if not row:
                    return {"ok": False, "error": "实验不存在"}
                cols = [desc[0] for desc in cur.description]
                experiment = dict(zip(cols, row))
                try:
                    from .blacklist_snapshot import enrich_blacklist_snapshot_for_display
                    custom_params = experiment.get("custom_params")
                    if isinstance(custom_params, str):
                        custom_params = json.loads(custom_params)
                    experiment["custom_params"] = enrich_blacklist_snapshot_for_display(custom_params)
                except Exception as e:
                    raise RuntimeError(f"行业黑名单快照解析失败: {e}") from e
                if not full_detail:
                    experiment["metrics_summary"] = compact_experiment_row(experiment).get("metrics_summary", {})
                    experiment["result_metrics_available"] = True
                return {"ok": True, "experiment": experiment, "detail": "full" if full_detail else "summary"}

    def regenerate_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """重新生成实验脚本（复用同一实验ID和名称）。

        从数据库读取实验的因子、模型、策略等信息，
        重新生成所有实验文件（conf.yaml, prepare_factors.py, read_exp_res.py等），
        但保留原有的实验ID和名称。
        """
        # 获取实验记录
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            return {"ok": False, "error": f"实验 {experiment_id} 不存在"}

        factor_names = exp_record.get("factor_names") or []
        model_id = exp_record.get("model_id")
        strategy_id = exp_record.get("strategy_id")
        data_split = exp_record.get("data_split")
        custom_params = exp_record.get("custom_params")
        experiment_name = exp_record.get("experiment_name")

        if not factor_names:
            return {"ok": False, "error": "实验没有关联因子"}

        # 解析data_split和custom_params（可能是JSON字符串）
        if isinstance(data_split, str):
            try:
                import json as _json
                data_split = _json.loads(data_split)
            except Exception as e:
                raise ValueError(f"data_split JSON 解析失败: {data_split!r}") from e
        if isinstance(custom_params, str):
            try:
                import json as _json
                custom_params = _json.loads(custom_params)
            except Exception as e:
                raise ValueError(f"custom_params JSON 解析失败: {custom_params!r}") from e

        # 创建实验目录 (本地)
        exp_dir = _qe_experiment_dir(experiment_name)
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 默认数据划分
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)
        self._validate_historical_stock_pool_window(custom_params, data_split)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型
        has_custom_factors = any(f.get("code_text") for f in factors_info)
        has_alpha158 = any(f.get("source") == "alpha158" for f in factors_info)
        has_alpha360 = any(f.get("source") == "alpha360" for f in factors_info)

        # 判断模型类型
        model_type_tag = None
        if model_info and model_info.get("code_text"):
            model_type_raw = model_info.get("model_type") or ""
            if model_type_raw in ("TimeSeries", "timeseries"):
                model_type_tag = "TimeSeries"
            elif model_type_raw in ("Tabular", "tabular"):
                model_type_tag = "Tabular"
            else:
                name_lower = (model_info.get("model_name") or "").lower()
                if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                    model_type_tag = "TimeSeries"
                else:
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        execution_algo = (custom_params or {}).get("execution_algo")
        backtest_freq = self._resolve_backtest_freq(execution_algo, custom_params)
        execution_algo_params = dict((custom_params or {}).get("execution_algo_params") or {})
        _cp = custom_params or {}
        if _cp.get("unfilled_handler"):
            execution_algo_params["unfilled_handler"] = _cp["unfilled_handler"]
        if _cp.get("unfilled_trigger_minute"):
            execution_algo_params["unfilled_trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            execution_algo_params["unfilled_backup_depth"] = _cp["unfilled_backup_depth"]
        custom_params, risk_policy_json = self._prepare_risk_policy_runtime(
            custom_params=custom_params,
            data_split=data_split,
        )
        custom_params, suspend_filter_json = self._prepare_suspend_filter_runtime(
            custom_params=custom_params,
            data_split=data_split,
            strategy_info=strategy_info,
            execution_algo=execution_algo,
        )

        # 生成conf.yaml
        conf_yaml = self._compose_conf_yaml(
            factors_info=factors_info,
            model_info=model_info,
            strategy_info=strategy_info,
            data_split=data_split,
            custom_params=custom_params,
            has_custom_factors=has_custom_factors,
            has_alpha158=has_alpha158,
            has_alpha360=has_alpha360,
            disable_alpha158=disable_alpha158,
            quick_train=quick_train,
            backtest_freq=backtest_freq,
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
        )

        # 生成因子文件和预处理脚本
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info, data_split=data_split)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")
        if suspend_filter_json:
            (exp_dir / SUSPEND_FILTER_FILE).write_text(suspend_filter_json, encoding="utf-8")
        if risk_policy_json:
            (exp_dir / RISK_POLICY_FILE).write_text(risk_policy_json, encoding="utf-8")

        if has_factor_files:
            self._write_factor_files(exp_dir, factors_info)

        if prepare_factors_py:
            prepare_path = exp_dir / "prepare_factors.py"
            prepare_path.write_text(prepare_factors_py, encoding="utf-8")

        # 如果使用 DynamicFactorsOnlyLoader，复制 QE 独立的 loader 文件到实验目录
        if has_custom_factors and disable_alpha158:
            self._copy_qe_custom_loaders(exp_dir)

        # 复制read_exp_res.py模板
        self._copy_read_exp_res(exp_dir)

        # 复制 qrun_limit runner（分钟线使用 qrun_limit_minute.py，日线使用 qrun_limit.py）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        import shutil
        if backtest_freq != "day" or bool((custom_params or {}).get("_seed_ensemble_config")):
            minute_src = scripts_dir / "qrun_limit_minute.py"
            if minute_src.exists():
                shutil.copy2(minute_src, exp_dir / "qrun_limit_minute.py")
            # TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_src = scripts_dir / "tail_twap_strategy.py"
            if twap_src.exists():
                shutil.copy2(twap_src, exp_dir / "tail_twap_strategy.py")
            # v24 Plan 执行策略（继承 TailTWAPWithLimitStrategy）
            v24_src = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_src.exists():
                shutil.copy2(v24_src, exp_dir / "tail_twap_v24_strategy.py")
            for helper_name in ("tail_twap_v25_strategy.py", "tail_twap_v25_1_strategy.py", "qe_board_lot_exchange.py", "close_execution_strategy.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
                helper_src = self._resolve_qe_helper_asset(scripts_dir, helper_name)
                if helper_src.exists():
                    shutil.copy2(helper_src, exp_dir / helper_name)
            bench_src = scripts_dir / "benchmark_sh000300.parquet"
            if bench_src.exists():
                shutil.copy2(bench_src, exp_dir / "benchmark_sh000300.parquet")
        for helper_name in ("qe_board_lot_exchange.py", "qe_suspend_filter.py", "qe_event_risk_policy.py", "qe_suspend_filter_strategy.py", "qe_suspend_filter_score_weighted_strategy.py"):
            helper_src = scripts_dir / helper_name
            if helper_src.exists():
                shutil.copy2(helper_src, exp_dir / helper_name)
        qrun_limit_src = scripts_dir / "qrun_limit.py"
        if qrun_limit_src.exists():
            shutil.copy2(qrun_limit_src, exp_dir / "qrun_limit.py")
        # benchmark parquet 也复制到日线实验的 qe_workspace
        bench_src = scripts_dir / "benchmark_sh000300.parquet"
        if bench_src.exists():
            local_benchmark = exp_dir / "benchmark_sh000300.parquet"
            if not local_benchmark.exists():
                shutil.copy2(bench_src, local_benchmark)

        # 如果模型使用自定义源码
        if model_info and model_info.get("code_text"):
            self._write_custom_model(exp_dir, model_info)

        # 如果策略使用自定义源码
        if strategy_info and strategy_info.get("source_code"):
            self._write_custom_strategy(exp_dir, strategy_info)

        # 同步文件到RDAgent侧
        self._api_sync_experiment_files(experiment_name, exp_dir)

        # 生成WSL命令
        wsl_path = f"{QE_WORKSPACE_WSL}/{experiment_name}"
        wsl_command = self._generate_wsl_command(
            wsl_path, has_custom_factors=has_custom_factors,
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            backtest_freq=backtest_freq,
            seed_ensemble_enabled=bool((custom_params or {}).get("_seed_ensemble_config")),
        )

        # 更新数据库中的WSL命令和状态
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET wsl_command = %s, status = 'created', updated_at = NOW()
                    WHERE experiment_id = %s
                """, (wsl_command, experiment_id))
            conn.commit()

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
            "experiment_dir": str(exp_dir),
            "wsl_command": wsl_command,
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
            "message": "实验脚本已重新生成",
        }

    # ---- conf.yaml 生成 ----

    def _compose_conf_yaml(
        self,
        factors_info: List[Dict],
        model_info: Optional[Dict],
        strategy_info: Optional[Dict],
        data_split: Dict[str, str],
        custom_params: Optional[Dict],
        has_custom_factors: bool,
        has_alpha158: bool,
        has_alpha360: bool = False,
        disable_alpha158: bool = False,
        quick_train: bool = False,  # 快速训练模式：训练时间缩短到20%
        qlib_data_path: Optional[str] = None,
        qlib_minute_path: Optional[str] = None,
        backtest_freq: str = "1min",  # "1min" 分钟线回测 | "day" 日线回测
        execution_algo: Optional[str] = None,  # None/"TWAP" → TailTWAPWithLimitStrategy | "CLOSE_PRICE" → CloseExecutionStrategy
        execution_algo_params: Optional[Dict[str, Any]] = None,
        initial_cash: Optional[int] = None,  # 初始资金，None → 100000000
    ) -> str:
        """生成QLib conf.yaml内容。

        结构与 RDAgent conf_baseline.yaml 完全一致，确保在 WSL 环境中
        使用相同的数据集、模型超参数、策略配置执行实验。
        """
        # ── 模型配置 ──
        # 与 RDAgent model_runner.py 完全一致的模型选择逻辑：
        # - 有源代码的自定义模型 → GeneralPTNN + pt_model_uri: "model.model_cls"
        # - LGB/LGBM/GBDT → LGBModel
        # - 无源代码的NN → GeneralPTNN（需要用户提供pt_model_uri）
        model_class = "LGBModel"
        model_module = "qlib.contrib.model.gbdt"
        model_kwargs = dict(RDAGENT_DEFAULT_LGB_KWARGS)  # 默认使用RDAgent超参
        use_custom_model = False  # 是否使用自定义模型源代码（GeneralPTNN + model.model_cls）
        model_type_tag = None  # "TimeSeries" | "Tabular" | None
        model_dataset_cls = "DatasetH"
        model_step_len: Optional[int] = None

        if model_info:
            model_type = (model_info.get("model_type") or "").upper()
            code_text = model_info.get("code_text")

            if code_text:
                # RDAgent自定义模型：使用 GeneralPTNN + pt_model_uri: "model.model_cls"
                # 与 RDAgent conf_sota_factors_model.yaml 完全一致
                use_custom_model = True
                model_class = "GeneralPTNN"
                model_module = "qlib.contrib.model.pytorch_general_nn"

                # 训练超参数
                thp = model_info.get("model_training_hyperparameters") or {}
                if isinstance(thp, str):
                    thp = json.loads(thp)

                # batch_size 对齐 Qlib 官方 (2000-8000) 和 QE 数据库验证结果:
                # batch=4096 → IC=0.051/AnnRet=89% (TOP1/TOP2 SOTA)
                # batch=16384+ → 梯度过度平滑，弱信号学习困难
                _DEFAULT_BATCH_SIZE = 4096

                raw_bs = thp.get("batch_size")
                if raw_bs is not None:
                    if isinstance(raw_bs, str):
                        raw_bs = int(raw_bs)
                    gpu_batch_size = raw_bs
                else:
                    gpu_batch_size = _DEFAULT_BATCH_SIZE

                model_kwargs = {
                    "n_epochs": thp.get("n_epochs", 200),
                    "lr": float(thp.get("lr", 1e-3)) if isinstance(thp.get("lr", 1e-3), str) else thp.get("lr", 1e-3),
                    "early_stop": thp.get("early_stop", 20),
                    "batch_size": gpu_batch_size,
                    "weight_decay": float(thp.get("weight_decay", 1e-4)) if isinstance(thp.get("weight_decay", 1e-4), str) else thp.get("weight_decay", 1e-4),
                    "metric": "loss",
                    "loss": "mse",
                    "n_jobs": 2,
                    "GPU": 0,
                    "use_amp": False,
                    "gradient_accumulation_steps": 1,
                    "pin_memory": True,
                    "prefetch_factor": 2,
                    "persistent_workers": False,
                    "pt_model_uri": "model.model_cls",
                }

                # 快速训练模式：训练时间缩短到20%
                if quick_train:
                    n_epochs_val = int(model_kwargs["n_epochs"]) if isinstance(model_kwargs["n_epochs"], str) else model_kwargs["n_epochs"]
                    early_stop_val = int(model_kwargs["early_stop"]) if isinstance(model_kwargs["early_stop"], str) else model_kwargs["early_stop"]
                    batch_size_val = int(model_kwargs["batch_size"]) if isinstance(model_kwargs["batch_size"], str) else model_kwargs["batch_size"]
                    model_kwargs["n_epochs"] = max(3, n_epochs_val // 5)
                    model_kwargs["early_stop"] = max(1, early_stop_val // 3)
                    model_kwargs["batch_size"] = batch_size_val * 2  # 增大batch加速

                # 判断模型类型（TimeSeries vs Tabular）
                model_type_raw = model_info.get("model_type") or ""
                if model_type_raw in ("TimeSeries", "timeseries"):
                    model_type_tag = "TimeSeries"
                    model_dataset_cls = "TSDatasetH"
                    model_step_len = 20
                elif model_type_raw in ("Tabular", "tabular"):
                    model_type_tag = "Tabular"
                    model_dataset_cls = "DatasetH"
                    model_step_len = None
                else:
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

            elif "LGB" in model_type or "LGBM" in model_type or "GBDT" in model_type:
                model_class = "LGBModel"
                model_module = "qlib.contrib.model.gbdt"
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "XGB" in model_type or "XGBOOST" in model_type:
                model_class = "AIStockXGBModel"
                model_module = "aistock_models.xgboost_model"
                model_kwargs = {
                    "num_boost_round": 500,
                    "max_depth": 8,
                    "learning_rate": 0.05,
                    "subsample": 0.8,
                    "colsample_bytree": 0.8,
                    "reg_alpha": 0.1,
                    "reg_lambda": 1.0,
                    "n_jobs": -1,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "CATBOOST" in model_type:
                model_class = "CatBoostModel"
                model_module = "qlib.contrib.model.catboost_model"
                model_kwargs = {
                    "iterations": 500,
                    "depth": 8,
                    "learning_rate": 0.05,
                    "l2_leaf_reg": 3.0,
                    "subsample": 0.8,
                    "bootstrap_type": "Bernoulli",
                    "task_type": "CPU",
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
                # Qlib's CatBoostModel.fit injects verbose_eval. CatBoost treats
                # verbose/logging_level/silent as mutually exclusive with it.
                for _cat_log_key in ("verbose", "logging_level", "silent"):
                    if _cat_log_key in model_kwargs:
                        model_kwargs.pop(_cat_log_key, None)
                        logger.info(
                            "移除 CatBoostModel 互斥日志参数 %s，避免与 Qlib verbose_eval 冲突",
                            _cat_log_key,
                        )
                if "subsample" in model_kwargs and "bootstrap_type" not in model_kwargs:
                    model_kwargs["bootstrap_type"] = "Bernoulli"
            elif "LINEAR" in model_type or "RIDGE" in model_type or "LASSO" in model_type:
                model_class = "LinearModel"
                model_module = "qlib.contrib.model.linear"
                model_kwargs = {
                    "estimator": "ridge",
                    "alpha": 0.05,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "PTNN" in model_type or "NN" in model_type:
                # 无源代码的 GeneralPTNN：必须在 model_hyperparameters 中提供 pt_model_uri
                model_class = "GeneralPTNN"
                model_module = "qlib.contrib.model.pytorch_general_nn"

                # 提供完整的默认训练参数（数值类型，不是字符串）
                model_dataset_cls = model_info.get("default_dataset_type") or "DatasetH"
                model_step_len = 20 if model_dataset_cls == "TSDatasetH" else None
                model_kwargs = {
                    "n_epochs": 30,
                    "lr": 1e-3,
                    "early_stop": 5,
                    "batch_size": 4096,
                    "weight_decay": 1e-4,
                    "metric": "loss",
                    "loss": "mse",
                    "n_jobs": 2,
                    "GPU": 0,
                }

                training_hp = model_info.get("model_training_hyperparameters")
                if training_hp:
                    if isinstance(training_hp, str):
                        training_hp = json.loads(training_hp)
                    if "lr" in training_hp and isinstance(training_hp["lr"], str):
                        training_hp["lr"] = float(training_hp["lr"])
                    if "weight_decay" in training_hp and isinstance(training_hp["weight_decay"], str):
                        training_hp["weight_decay"] = float(training_hp["weight_decay"])
                    model_kwargs.update(training_hp)

                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    # 确保 lr 和 weight_decay 是数值类型
                    if "lr" in hp and isinstance(hp["lr"], str):
                        hp["lr"] = float(hp["lr"])
                    if "weight_decay" in hp and isinstance(hp["weight_decay"], str):
                        hp["weight_decay"] = float(hp["weight_decay"])

                    pt_model_uri = hp.get("pt_model_uri")
                    pt_model_kwargs = {}
                    for arch_key in ("d_feat", "hidden_size", "num_layers", "dropout"):
                        if arch_key in hp:
                            pt_model_kwargs[arch_key] = hp[arch_key]

                    for key, value in hp.items():
                        if key in {"pt_model_uri", "d_feat", "hidden_size", "num_layers", "dropout"}:
                            continue
                        model_kwargs[key] = value

                    if pt_model_uri:
                        model_kwargs["pt_model_uri"] = pt_model_uri
                    if pt_model_kwargs:
                        model_kwargs["pt_model_kwargs"] = pt_model_kwargs

                # 检查是否提供了 pt_model_uri
                if "pt_model_uri" not in model_kwargs:
                    raise ValueError(
                        f"模型类型 '{model_type}' 无源代码，必须在 model_hyperparameters 中"
                        f"提供 pt_model_uri 参数指定模型类路径"
                    )
            elif "LAMBDARANK" in model_type or "LAMBDAMART" in model_type:
                # LambdaMART 排序模型 (LightGBM LGBMRanker + lambdarank objective)
                # 自定义 Qlib Model 类，实现 cross-sectional stock ranking
                model_class = "LambdaRankModel"
                model_module = "aistock_models.lambdarank"
                model_dataset_cls = "DatasetH"
                model_step_len = None
                model_kwargs = {
                    "objective": "lambdarank",
                    "metric": "ndcg",
                    "ndcg_eval_at": [10, 30, 50],
                    "num_leaves": 64,
                    "max_depth": 8,
                    "learning_rate": 0.05,
                    "n_estimators": 300,
                    "min_child_samples": 100,
                    "subsample": 0.8,
                    "colsample_bytree": 0.8,
                    "reg_alpha": 0.1,
                    "reg_lambda": 0.1,
                    "early_stopping_rounds": 20,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "TABPFN" in model_type:
                # TabPFN 表格基础模型 (Nature 2025)
                # 预训练 transformer，in-context learning，零梯度训练
                # TabPFNModel 实现 Qlib Model 接口，通过模块路径直接加载
                model_class = "TabPFNModel"
                model_module = "aistock_models.tabpfn_model"
                model_dataset_cls = "DatasetH"
                model_step_len = None
                model_kwargs = {
                    "n_estimators": 8,
                    "device": "cuda",
                    "max_context_size": 2000,
                    "predict_batch_size": 8192,
                    "n_bins": 10,
                    "random_state": 42,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            else:
                # 未知模型类型且无源代码，无法生成配置
                raise ValueError(
                    f"未知模型类型 '{model_type}'，无源代码可用，"
                    f"无法生成 Qlib 配置。请检查模型数据完整性。"
                )

        # ── 策略配置 ──
        # 默认值：仅在用户未选择策略时使用 qlib 内置 TopkDropoutStrategy
        strategy_class = "TopkDropoutStrategy"
        strategy_module = "qlib.contrib.strategy.signal_strategy"
        strategy_kwargs = {
            "signal": "<PRED>",
            "topk": 50,
            "n_drop": 5,
            "method_buy": "top",
            "hold_thresh": 2,
            "method_sell": "bottom",
            "risk_degree": 0.95,
            "only_tradable": True,
            "forbid_all_trade_at_limit": False,
        }
        if strategy_info:
            # 用户选择了策略 → 必须使用该策略的源代码
            source_code = strategy_info.get("source_code")
            if not source_code:
                raise ValueError(
                    f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码，"
                    f"无法生成实验配置。请先在策略管理页面编辑并保存策略源代码。"
                )
            # 有源代码 → module_path 指向本地 custom_strategy
            strategy_module = "custom_strategy"

            # 从源代码中提取策略类名（确保与实际定义匹配）
            import re
            class_match = re.search(r'class\s+(\w+)\s*\(', source_code)
            if class_match:
                extracted_class = class_match.group(1)
                logger.info(f"从源代码提取策略类名: {extracted_class}")

            pc = strategy_info.get("portfolio_config")
            if pc:
                if isinstance(pc, str):
                    pc = json.loads(pc)
                # 优先使用portfolio_config中的class，否则使用从源码提取的类名
                config_class = pc.get("class")
                if config_class:
                    strategy_class = config_class
                elif class_match:
                    strategy_class = extracted_class
                sk = pc.get("kwargs", {})
                strategy_kwargs.update(sk)
            elif class_match:
                # 没有portfolio_config，使用源码提取的类名
                strategy_class = extracted_class

            # 使用 default_kwargs 覆盖（UI 编辑的参数）
            dk = strategy_info.get("default_kwargs")
            if dk:
                if isinstance(dk, str):
                    dk = json.loads(dk)
                for k, v in dk.items():
                    strategy_kwargs[k] = v

        # ── 模型超参键白名单（始终可用，供策略安全过滤引用） ──
        _PTNN_HP_KEYS = {
            "n_epochs", "lr", "early_stop", "batch_size", "weight_decay",
            "optimizer",
        }
        # NOTE: hidden_size, num_layers, dropout 是模型架构参数，属于 pt_model_kwargs，
        # 由模型源码 (model.py) 硬编码，不能作为 GeneralPTNN.__init__() 的顶层参数传入。
        _SEED_ALIAS_KEYS = {"random_seed", "seed", "loop_seed", "random_state", "torch_seed", "numpy_seed"}
        _LGB_HP_KEYS = {
            "learning_rate", "max_depth", "num_leaves", "lambda_l1", "lambda_l2",
            "colsample_bytree", "subsample", "n_estimators", "min_child_samples",
        }
        _XGB_HP_KEYS = {
            "n_estimators", "num_boost_round", "early_stopping_rounds",
            "verbose_eval", "max_depth", "learning_rate", "subsample",
            "colsample_bytree", "reg_alpha", "reg_lambda", "n_jobs",
        }
        _CATBOOST_HP_KEYS = {
            "iterations", "depth", "learning_rate", "l2_leaf_reg",
            "subsample", "bootstrap_type", "task_type",
        }
        _TABPFN_HP_KEYS = {
            "n_estimators", "device", "max_context_size", "predict_batch_size",
            "min_predict_batch_size", "n_bins",
        }
        _LINEAR_HP_KEYS = {
            "estimator", "alpha",
        }
        _NON_STRATEGY_PARAMS = {
            "disable_alpha158", "disable_alpha360", "use_custom_model",
            "model_type", "dataset_cls", "step_len", "num_timesteps", "num_features",
            "quick_train",  # 快速训练模式：控制模型训练参数
            "label_type",   # 训练标签类型：close/open/vwap
            "label_horizon",  # Training label horizon: 1/3/5/10/20d
            "stock_pool",   # 股票池文件路径
            "runtime_mode",
            "bar_freq",
            "runtime_contract_version",
            "runtime_contract_source",
            "archive_policy",        # QE Archive policy metadata, not a strategy kwarg
            "archive_reason",
            "archive_allow_override",
            "backtest_freq",        # 回测频率（已在上层提取）
            "execution_algo",       # 执行算法（已在上层提取到 inner_strategy）
            "execution_algo_params",  # 执行算法参数（已在上层提取到 inner_strategy）
            "unfilled_handler",       # 尾盘涨停处理（已在上层提取到 inner_strategy.kwargs）
            "unfilled_trigger_minute", # 尾盘处理触发分钟（已在上层提取到 inner_strategy.kwargs）
            "unfilled_backup_depth",   # 替补候选深度（已在上层提取到 inner_strategy.kwargs）
            "initial_cash",         # 初始资金（已在上层处理）
            "hmm_model_version_id", # HMM 版本 ID（已在上层处理为 hmm_coefficients_file）
            "hmm_config_json",      # HMM 训练配置，仅用于动态系数预计算
            "quote_universe_codes", # Qlib Exchange 报价/卖出 universe，非策略构造参数
            "risk_policy",          # 统一事件风险策略配置，非当前 Qlib 策略构造参数
            "risk_policy_enabled",
            "risk_policy_file",
            "risk_policy_strict",
            "_seed_ensemble_config",
            # Industry blacklist metadata is persisted for UI/detail traceability.
            # The executable restriction is represented by stock_pool, not by
            # passing these metadata objects into the Qlib strategy constructor.
            "sector_blacklist",
            "sector_blacklist_enabled",
            "sector_blacklist_snapshot",
            "blacklist_enabled",
            "filter_suspended_on_signal",
            "exclude_suspended",
            "filter_suspend_d",
            "suspend_filter_file",
            "suspend_filter_strict",
            PRECOMPUTED_HMM_COEFF_JSON_PARAM,
        } | _SEED_ALIAS_KEYS | _PTNN_HP_KEYS | _LGB_HP_KEYS | _XGB_HP_KEYS | _CATBOOST_HP_KEYS | _TABPFN_HP_KEYS | _LINEAR_HP_KEYS

        if custom_params:
            # ── 模型超参透传: 从 custom_params 中提取模型超参 → model_kwargs ──
            hp_keys = set()
            if model_class in ("GeneralPTNN",):
                hp_keys = _PTNN_HP_KEYS
            elif model_class in ("LGBModel",):
                hp_keys = _LGB_HP_KEYS
            elif model_class in ("XGBModel", "AIStockXGBModel"):
                hp_keys = _XGB_HP_KEYS
            elif model_class in ("CatBoostModel",):
                hp_keys = _CATBOOST_HP_KEYS
            elif model_class in ("TabPFNModel",):
                hp_keys = _TABPFN_HP_KEYS
            elif model_class in ("LinearModel",):
                hp_keys = _LINEAR_HP_KEYS
            # 也包括有自定义代码的 PTNN 模型
            if use_custom_model and model_type_tag in ("TimeSeries", "Tabular"):
                hp_keys = hp_keys | _PTNN_HP_KEYS

            model_hp_overrides = {}
            for key in hp_keys:
                if key in custom_params:
                    val = custom_params[key]
                    # 确保 GeneralPTNN 的 lr 和 weight_decay 是数值类型
                    if model_class == "GeneralPTNN" and key in ("lr", "weight_decay") and isinstance(val, str):
                        val = float(val)
                    model_hp_overrides[key] = val
            if model_hp_overrides:
                logger.info(f"模型超参透传: {list(model_hp_overrides.keys())} → model_kwargs")
                model_kwargs.update(model_hp_overrides)

            runtime_seed = (custom_params or {}).get("random_seed")
            if runtime_seed is not None:
                model_kwargs = apply_qe_seed_to_model_params(
                    model_kwargs,
                    runtime_seed,
                    model_class=model_class,
                )

            # 过滤掉非策略参数（含模型超参、数据加载器配置等）
            filtered_params = {k: v for k, v in custom_params.items() if k not in _NON_STRATEGY_PARAMS}
            if set(custom_params.keys()) - set(filtered_params.keys()):
                logger.info(f"策略参数过滤: 移除非策略参数 {set(custom_params.keys()) - set(filtered_params.keys())}")
            strategy_kwargs.update(filtered_params)

        strategy_kwargs["signal"] = "<PRED>"

        suspend_filter_enabled = self._is_suspend_filter_enabled(
            custom_params
        ) or self._is_qe_risk_policy_enabled(custom_params)
        if suspend_filter_enabled:
            self._ensure_suspend_filter_supported(strategy_class)
            strategy_kwargs["filter_suspended_on_signal"] = True
            strategy_kwargs["suspend_filter_file"] = (custom_params or {}).get("suspend_filter_file") or SUSPEND_FILTER_FILE
            strategy_kwargs["suspend_filter_strict"] = bool((custom_params or {}).get("suspend_filter_strict", True))
            if strategy_class == "TopkDropoutStrategy":
                strategy_class = "SuspendFilterTopkDropoutStrategy"
                strategy_module = "qe_suspend_filter_strategy"
            elif strategy_class == "ScoreWeightedTopkStrategy":
                strategy_class = "SuspendFilterScoreWeightedTopkStrategy"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"
            elif strategy_class == "ScoreWeightedTopkStrategyV2":
                strategy_class = "SuspendFilterScoreWeightedTopkStrategyV2"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"
            elif strategy_class == "ScoreWeightedTopkStrategyV2CapacityV1":
                strategy_class = "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"

        risk_policy_enabled = self._is_qe_risk_policy_enabled(custom_params)
        if risk_policy_enabled:
            self._ensure_qe_risk_policy_supported(strategy_class)
            strategy_kwargs["risk_policy_enabled"] = True
            strategy_kwargs["risk_policy_file"] = (custom_params or {}).get("risk_policy_file") or RISK_POLICY_FILE
            strategy_kwargs["risk_policy_strict"] = bool((custom_params or {}).get("risk_policy_strict", True))
            if strategy_class in {"TopkDropoutStrategy", "SuspendFilterTopkDropoutStrategy"}:
                strategy_class = "SuspendFilterTopkDropoutStrategy"
                strategy_module = "qe_suspend_filter_strategy"
            elif strategy_class in {"ScoreWeightedTopkStrategy", "SuspendFilterScoreWeightedTopkStrategy"}:
                strategy_class = "SuspendFilterScoreWeightedTopkStrategy"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"
            elif strategy_class in {"ScoreWeightedTopkStrategyV2", "SuspendFilterScoreWeightedTopkStrategyV2"}:
                strategy_class = "SuspendFilterScoreWeightedTopkStrategyV2"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"
            elif strategy_class in {
                "ScoreWeightedTopkStrategyV2CapacityV1",
                "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1",
            }:
                strategy_class = "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1"
                strategy_module = "qe_suspend_filter_score_weighted_strategy"

        # 安全过滤：只保留策略支持的参数
        # 避免不支持的参数通过 **kwargs 传递到 BaseStrategy 导致 TypeError
        _HMM_KEYS = {
            "enable_sector_hmm", "sector_hmm_model_path",
            "hmm_signal_preset", "hmm_signal_presets",
            "hmm_coefficients_file",
        }
        _UNFILLED_KEYS = {
            "unfilled_handler", "unfilled_trigger_minute", "unfilled_backup_depth",
        }
        _SUSPEND_FILTER_KEYS = {
            "filter_suspended_on_signal", "suspend_filter_file", "suspend_filter_strict",
        }
        _RISK_POLICY_KEYS = {
            "risk_policy_enabled", "risk_policy_file", "risk_policy_strict",
        }
        _TOPK_DROPOUT_ALLOWED_KEYS = {
            "signal", "topk", "n_drop", "method_sell", "method_buy",
            "hold_thresh", "only_tradable", "forbid_all_trade_at_limit",
            "risk_degree",
        } | _UNFILLED_KEYS | _SUSPEND_FILTER_KEYS | _RISK_POLICY_KEYS
        # EnhancedTopkDropoutStrategy 支持的额外参数
        _ENHANCED_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "min_score", "max_position_ratio", "stop_loss", "max_market_cap",
        } | _HMM_KEYS
        # SmallCapTopkDropoutStrategy 支持的参数
        _SMALLCAP_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "max_market_cap",
        } | _HMM_KEYS
        # TopkDropoutWithRiskControlStrategy 支持的参数（含 HMM）
        _RC_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "stop_loss_pct", "max_daily_turnover_pct",
            "stock_pool",
        } | _HMM_KEYS
        # ScoreWeightedTopkStrategy 支持的参数（T1.1+T1.3, 2026-04-06）
        _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "weight_method", "temperature", "score_clip_quantile",
            "max_weight", "min_weight", "max_position_ratio",
            "enable_dynamic_ndrop", "max_n_drop", "min_n_drop",
            "threshold_method", "min_improvement", "adaptive_multiplier",
            "threshold_floor", "min_trade_price", "max_trade_price",
            "max_single_order_value", "lot_size",
        } | _UNFILLED_KEYS | _HMM_KEYS | _SUSPEND_FILTER_KEYS | _RISK_POLICY_KEYS

        if strategy_class in {"TopkDropoutStrategy", "SuspendFilterTopkDropoutStrategy"}:
            _removed = {k for k in strategy_kwargs if k not in _TOPK_DROPOUT_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_TOPK_DROPOUT_ALLOWED_KEYS)}"
                )
        elif strategy_class == "EnhancedTopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _ENHANCED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_ENHANCED_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "SmallCapTopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _SMALLCAP_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SMALLCAP_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "TopkDropoutWithRiskControlStrategy":
            _removed = {k for k in strategy_kwargs if k not in _RC_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_RC_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class in {"ScoreWeightedTopkStrategy", "SuspendFilterScoreWeightedTopkStrategy"}:
            _removed = {k for k in strategy_kwargs if k not in _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SCORE_WEIGHTED_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class in {
            "ScoreWeightedTopkStrategyV2",
            "SuspendFilterScoreWeightedTopkStrategyV2",
            "ScoreWeightedTopkStrategyV2CapacityV1",
            "SuspendFilterScoreWeightedTopkStrategyV2CapacityV1",
        }:
            # V2 and the capacity variant share parameters; only defaults differ.
            _removed = {k for k in strategy_kwargs if k not in _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SCORE_WEIGHTED_TOPK_ALLOWED_KEYS)}"
                )
        else:
            # 未知策略类型：只过滤已知的非策略参数（如 backtest_freq, execution_algo 等）
            _removed = {k for k in strategy_kwargs if k in _NON_STRATEGY_PARAMS}
            if _removed:
                logger.info(f"未知策略 '{strategy_class}': 移除非策略参数 {sorted(_removed)}")
                for k in _removed:
                    del strategy_kwargs[k]

        # 回测截止日（必须由 data_split 提供，禁止硬编码兜底）
        backtest_end = data_split.get("backtest_end")
        if not backtest_end:
            raise ValueError("data_split 缺少 backtest_end，无法生成回测配置")

        # ── 生成 YAML（与 RDAgent conf_baseline.yaml 结构一致） ──
        lines = []
        lines.append("qlib_init:")
        _day_uri = qlib_data_path or QLIB_DATA_PATH_WSL
        _min_uri = qlib_minute_path or QLIB_MINUTE_PATH_WSL
        lines.append("    provider_uri:")
        lines.append(f'        day: "{_day_uri}"')
        lines.append(f'        1min: "{_min_uri}"')
        lines.append("    region: cn")
        lines.append("    dataset_cache: null")
        lines.append("    expression_cache: null")
        lines.append("")
        stock_pool = (custom_params or {}).get("stock_pool", "all")
        # Training label selection. label_type controls price basis; label_horizon controls horizon.
        _LABEL_FIELDS = {
            "close": "$close",
            "open": "$open",
            "vwap": "$vwap",
        }
        _label_type = (custom_params or {}).get("label_type", "close")
        if _label_type not in _LABEL_FIELDS:
            raise ValueError(
                f"label_type='{_label_type}' invalid, must be one of {list(_LABEL_FIELDS.keys())}"
            )
        _label_horizon = normalize_label_horizon((custom_params or {}).get("label_horizon"))
        _label_field = _LABEL_FIELDS[_label_type]
        _label_formula = f"Ref({_label_field}, -{_label_horizon + 1}) / Ref({_label_field}, -1) - 1"
        if _label_type != "close" or _label_horizon != 1:
            logger.info(
                "Using non-default training label: "
                f"label_type={_label_type}, label_horizon={_label_horizon}, formula={_label_formula}"
            )
        _runtime_seed = (custom_params or {}).get("random_seed")
        _seed_ensemble_config = (custom_params or {}).get("_seed_ensemble_config")
        lines.append(f"market: &market {stock_pool}")
        lines.append("benchmark: &benchmark 000300.SH")
        if _runtime_seed is not None or _seed_ensemble_config:
            _seed_value = int(_runtime_seed if _runtime_seed is not None else _seed_ensemble_config["seeds"][0])
            lines.append("")
            lines.append("qe_runtime:")
            lines.append("    seed_policy: fixed")
            lines.append(f"    random_seed: {_seed_value}")
            lines.append("    deterministic_flags:")
            lines.append("        python_random: true")
            lines.append("        numpy_random: true")
            lines.append("        torch_random: true")
            lines.append("        torch_cuda_random: true")
            lines.append("        cudnn_deterministic: true")
            lines.append("        cudnn_benchmark: false")
            if _seed_ensemble_config:
                lines.append("    ensemble:")
                lines.append("        enabled: true")
                lines.append(f"        level: {self._yaml_scalar(_seed_ensemble_config.get('level') or 'score')}")
                lines.append(f"        agg: {self._yaml_scalar(_seed_ensemble_config.get('agg') or 'mean')}")
                lines.append("        seeds:")
                for seed in _seed_ensemble_config.get("seeds") or []:
                    lines.append(f"            - {int(seed)}")
        lines.append("")

        # data_handler_config
        lines.append("data_handler_config: &data_handler_config")
        lines.append(f"    start_time: {data_split['train_start']}")
        lines.append(f"    end_time: {data_split['test_end']}")
        # fit_start_time/fit_end_time 只有 Alpha158 接受，DataHandlerLP 不接受
        if not has_custom_factors:
            lines.append(f"    fit_start_time: {data_split['train_start']}")
            lines.append(f"    fit_end_time: {data_split['valid_end']}")
        lines.append("    instruments: *market")

        if has_custom_factors and not disable_alpha158:
            # 自定义因子：使用 NestedDataLoader（Alpha158DL + StaticDataLoader）
            # 与 RDAgent conf_sota_factors_model.yaml 完全一致
            # 需要先执行 prepare_factors.py 生成 combined_factors_df.parquet
            lines.append("    data_loader:")
            lines.append("        class: NestedDataLoader")
            lines.append("        kwargs:")
            lines.append("            dataloader_l:")
            lines.append("                - class: qlib.contrib.data.loader.Alpha158DL")
            lines.append("                  kwargs:")
            lines.append("                    config:")
            lines.append("                        label: ")
            lines.append(f'                            - ["{_label_formula}"]')
            lines.append('                            - ["LABEL0"]')
            lines.append("                        feature:")
            lines.append('                            - ["Resi($close, 5)/$close", "Std(Abs($close/Ref($close, 1)-1)*$volume, 5)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 5)+1e-12)",')
            lines.append('                               "Rsquare($close, 5)", "($high-$low)/$open", "Rsquare($close, 10)", "Corr($close, Log($volume+1), 5)",')
            lines.append('                               "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 5)", "Corr($close, Log($volume+1), 10)",')
            lines.append('                               "Ref($close, 60)/$close", "Resi($close, 10)/$close", "Std($volume, 5)/($volume+1e-12)",')
            lines.append('                               "Rsquare($close, 60)", "Corr($close, Log($volume+1), 60)", "Std(Abs($close/Ref($close, 1)-1)*$volume, 60)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 60)+1e-12)",')
            lines.append('                               "Std($close, 5)/$close", "Rsquare($close, 20)", "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 60)",')
            lines.append('                               "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 10)", "Corr($close, Log($volume+1), 20)",')
            lines.append('                               "(Less($open, $close)-$low)/$open"]')
            lines.append('                            - ["RESI5", "WVMA5", "RSQR5", "KLEN", "RSQR10", "CORR5", "CORD5", "CORR10", ')
            lines.append('                               "ROC60", "RESI10", "VSTD5", "RSQR60", "CORR60", "WVMA60", "STD5", ')
            lines.append('                               "RSQR20", "CORD60", "CORD10", "CORR20", "KLOW"]')
            lines.append("                - class: qlib.data.dataset.loader.StaticDataLoader")
            lines.append("                  kwargs:")
            lines.append('                    config: "combined_factors_df.parquet"')
            lines.append("    infer_processors:")
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append(f"              fit_start_time: {data_split['train_start']}")
            lines.append(f"              fit_end_time: {data_split['valid_end']}")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
        elif has_custom_factors and disable_alpha158:
            # 自定义因子 + 禁用Alpha158基线：使用 DynamicFactorsOnlyLoader
            # 注意：使用实验目录中的 qe_custom_loaders（QE独立版本），不影响RDAgent
            # DynamicFactorsOnlyLoader 会忽略 instruments 参数，直接加载 parquet 中所有数据
            # 同时从 QLib provider 加载 label 数据，确保包含 feature 和 label 列
            lines.append("    data_loader:")
            lines.append("        class: DynamicFactorsOnlyLoader")
            lines.append("        module_path: qe_custom_loaders")
            lines.append("        kwargs:")
            lines.append('            dynamic_path: "combined_factors_df.parquet"')
            lines.append(f'            label_type: "{_label_type}"')
            lines.append(f"            label_horizon: {_label_horizon}")
            lines.append("    infer_processors:")
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append(f"              fit_start_time: {data_split['train_start']}")
            lines.append(f"              fit_end_time: {data_split['valid_end']}")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
        else:
            # Alpha158 标准处理器（与 RDAgent conf_baseline.yaml 一致）
            lines.append("    infer_processors:")
            lines.append("        - class: FilterCol")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append('              col_list: ["RESI5", "WVMA5", "RSQR5", "KLEN", "RSQR10", "CORR5", "CORD5", "CORR10",')
            lines.append('                            "ROC60", "RESI10", "VSTD5", "RSQR60", "CORR60", "WVMA60", "STD5",')
            lines.append('                            "RSQR20", "CORD60", "CORD10", "CORR20", "KLOW"]')
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSRankNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
            lines.append(f'    label: ["{_label_formula}"]')

        lines.append("")

        # port_analysis_config — executor 根据 backtest_freq 切换
        lines.append("port_analysis_config: &port_analysis_config")
        lines.append("    executor:")
        if backtest_freq == "day":
            # 日线模式：单层 SimulatorExecutor
            effective_execution_algo_params = self._execution_algo_params_with_runtime_filters(
                execution_algo,
                execution_algo_params,
                custom_params,
            )
            algo_cfg = self._execution_algo_config(execution_algo, effective_execution_algo_params)
            if algo_cfg["effective_algo"] != "CLOSE_PRICE":
                raise ValueError(
                    "day backtest is only valid for execution_algo=CLOSE_PRICE; "
                    f"got {algo_cfg['effective_algo']}"
                )
            lines.append("        class: SimulatorExecutor")
            lines.append("        module_path: qlib.backtest.executor")
            lines.append("        kwargs:")
            lines.append("            time_per_step: day")
            lines.append("            # qe_execution_trace:")
            lines.append(f"            #   requested_algo: {algo_cfg['requested_algo']}")
            lines.append(f"            #   effective_algo: {algo_cfg['effective_algo']}")
            lines.append("            #   effective_class: SimulatorExecutor")
            lines.append("            #   effective_module_path: qlib.backtest.executor")
            lines.append("            generate_portfolio_metrics: true")
        else:
            # 分钟线模式：NestedExecutor + 执行算法策略
            lines.append("        class: NestedExecutor")
            lines.append("        module_path: qlib.backtest.executor")
            lines.append("        kwargs:")
            lines.append("            time_per_step: day")
            lines.append("            inner_executor:")
            lines.append("                class: SimulatorExecutor")
            lines.append("                module_path: qlib.backtest.executor")
            lines.append("                kwargs:")
            lines.append("                    time_per_step: 1min")
            lines.append("                    generate_portfolio_metrics: false")
            lines.append("            inner_strategy:")
            effective_execution_algo_params = self._execution_algo_params_with_runtime_filters(
                execution_algo,
                execution_algo_params,
                custom_params,
            )
            algo_cfg = self._execution_algo_config(execution_algo, effective_execution_algo_params)
            lines.append(f"                class: {algo_cfg['class']}")
            lines.append(f"                module_path: {algo_cfg['module_path']}")
            if algo_cfg.get("kwargs"):
                lines.append("                kwargs:")
                self._append_yaml_kwargs(lines, algo_cfg["kwargs"], "                    ")
            lines.append("            # qe_execution_trace:")
            lines.append(f"            #   requested_algo: {algo_cfg['requested_algo']}")
            lines.append(f"            #   effective_algo: {algo_cfg['effective_algo']}")
            lines.append(f"            #   effective_class: {algo_cfg['class']}")
            lines.append(f"            #   effective_module_path: {algo_cfg['module_path']}")
            lines.append("            generate_portfolio_metrics: true")
        lines.append("    strategy:")
        lines.append(f"        class: {strategy_class}")
        lines.append(f"        module_path: {strategy_module}")
        lines.append("        kwargs:")
        for k, v in strategy_kwargs.items():
            if k == "signal":
                lines.append(f"            {k}: <PRED>")
            elif isinstance(v, bool):
                lines.append(f"            {k}: {'true' if v else 'false'}")
            elif isinstance(v, str):
                lines.append(f"            {k}: {v}")
            else:
                lines.append(f"            {k}: {v}")
        lines.append("    backtest:")
        lines.append(f"        start_time: {data_split['test_start']}")
        lines.append(f"        end_time: {backtest_end}")
        _account_cash = int(initial_cash) if initial_cash else 100000000
        lines.append(f"        account: {_account_cash}")
        lines.append("        benchmark: ~")
        lines.append("        exchange_kwargs:")
        if backtest_freq == "day":
            lines.append('            freq: day')
            lines.append('            limit_threshold: ["$limit_up", "$limit_down"]')
        else:
            lines.append('            freq: 1min')
            lines.append('            limit_threshold: ["$limit_up", "$limit_down"]')
        lines.append("            deal_price: close")
        lines.append("            open_cost: 0.000095")
        lines.append("            close_cost: 0.000595")
        lines.append("            min_cost: 5")
        if backtest_freq != "day" and algo_cfg["effective_algo"] == "V25_1_SMALL_CAP":
            lines.append("            # V25.1 legalizes child orders with stock-aware board-lot rules.")
            lines.append("            # Disable Qlib's global 100-share rounding so STAR 200+1 orders survive.")
            lines.append("            trade_unit: ~")
            lines.append("            board_lot_trade_unit: true")
        else:
            lines.append("            trade_unit: 100")
        quote_universe_codes = (custom_params or {}).get("quote_universe_codes")
        if quote_universe_codes:
            if isinstance(quote_universe_codes, str):
                quote_universe_codes = [
                    item.strip()
                    for item in quote_universe_codes.replace(";", ",").split(",")
                    if item.strip()
                ]
            if not isinstance(quote_universe_codes, list) or not quote_universe_codes:
                raise ValueError("quote_universe_codes must be a non-empty list or comma-separated string")
            lines.append("            # Quote/sell universe can be wider than market buy universe.")
            lines.append("            codes:")
            for code in quote_universe_codes:
                lines.append(f"                - {self._yaml_scalar(str(code).upper())}")
        risk_policy = (custom_params or {}).get("risk_policy")
        if risk_policy:
            lines.append("        # risk_policy:")
            lines.append("        #   contract: stock_event_risk_policy_v1")
            lines.append("        #   note: QE strategy templates must consume the same hard-block/forced-exit")
            lines.append("        #         semantics as Paper v2; all.txt remains the buy universe.")
            for raw_line in json.dumps(risk_policy, ensure_ascii=False, sort_keys=True).splitlines():
                lines.append(f"        #   {raw_line}")

        # task（模型 + 数据集）
        lines.append("task:")
        lines.append("    model:")
        lines.append(f"        class: {model_class}")
        lines.append(f"        module_path: {model_module}")
        if model_kwargs:
            lines.append("        kwargs:")
            pt_model_kwargs = None
            if model_class == "GeneralPTNN":
                pt_model_kwargs = model_kwargs.get("pt_model_kwargs")
                if pt_model_kwargs is not None:
                    pt_model_kwargs = dict(pt_model_kwargs)
                    if model_dataset_cls == "TSDatasetH" and "d_feat" in pt_model_kwargs:
                        pt_model_kwargs["d_feat"] = "{{ num_features }}"
            for k, v in model_kwargs.items():
                if k == "pt_model_kwargs":
                    continue
                # pt_model_kwargs 使用Jinja2模板变量，与RDAgent一致
                if k == "pt_model_uri":
                    lines.append(f"            {k}: {v}")
                else:
                    lines.append(f"            {k}: {v}")
            # 自定义模型和内置 PTNN 模型都需要 pt_model_kwargs
            if model_class == "GeneralPTNN":
                if pt_model_kwargs is None and use_custom_model:
                    pt_model_kwargs = {"num_features": "{{ num_features }}"}
                    if model_type_tag == "TimeSeries":
                        pt_model_kwargs["num_timesteps"] = "{{ num_timesteps }}"
                if pt_model_kwargs is not None:
                    lines.append('            pt_model_kwargs: {')
                    first_item = True
                    for pt_key, pt_value in pt_model_kwargs.items():
                        prefix = "                " if first_item else "                , "
                        if isinstance(pt_value, str) and pt_value.startswith("{{"):
                            rendered_value = pt_value
                        elif isinstance(pt_value, str):
                            rendered_value = f'"{pt_value}"'
                        else:
                            rendered_value = str(pt_value)
                        lines.append(f"{prefix}\"{pt_key}\": {rendered_value}")
                        first_item = False
                    lines.append('            }')

        # 数据集配置
        if model_class == "GeneralPTNN":
            lines.append("    dataset:")
            lines.append(f'        class: {model_dataset_cls}')
            lines.append("        module_path: qlib.data.dataset")
        else:
            lines.append("    dataset:")
            lines.append("        class: DatasetH")
            lines.append("        module_path: qlib.data.dataset")
        lines.append("        kwargs:")
        lines.append("            handler:")
        if has_custom_factors:
            lines.append("                class: DataHandlerLP")
            lines.append("                module_path: qlib.contrib.data.handler")
            lines.append("                kwargs: *data_handler_config")
        else:
            lines.append("                class: Alpha158")
            lines.append("                module_path: qlib.contrib.data.handler")
            lines.append("                kwargs: *data_handler_config")
        lines.append("            segments:")
        lines.append(f"                train: [{data_split['train_start']}, {data_split['train_end']}]")
        lines.append(f"                valid: [{data_split['valid_start']}, {data_split['valid_end']}]")
        lines.append(f"                test: [{data_split['test_start']}, {data_split['test_end']}]")
        if model_class == "GeneralPTNN" and model_step_len:
            lines.append(f"            step_len: {model_step_len}")

        # record
        lines.append("    record:")
        lines.append("        - class: SignalRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            model: <MODEL>")
        lines.append("            dataset: <DATASET>")
        lines.append("        - class: SigAnaRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            ana_long_short: False")
        lines.append("            ann_scaler: 252")
        lines.append("        - class: PortAnaRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            config: *port_analysis_config")
        lines.append("")

        return "\n".join(lines)

    def _compose_factor_file(self, factors_info: List[Dict]) -> Optional[str]:
        """保存因子原始代码到 factors/ 子目录。

        与 RDAgent 完全一致：因子代码保持原始格式不做任何修改，
        每个因子作为独立的 factor.py 脚本直接执行。
        因子代码自己负责读取数据（daily_pv.h5, static_factors.parquet 等）
        并将结果写入 result.h5。

        Returns:
            返回一个标记字符串表示有因子文件（实际文件在compose_experiment中写入）
        """
        custom_factors = [f for f in factors_info if f.get("code_text")]
        if not custom_factors:
            return None

        # 返回标记，实际文件写入在 compose_experiment 中通过 _write_factor_files 完成
        return "__FACTOR_FILES_WRITTEN__"

    def _write_factor_files(self, exp_dir: Path, factors_info: List[Dict]) -> None:
        """将每个因子的原始代码写入 factors/ 子目录。

        与 RDAgent 完全一致：因子代码保持原始格式不做任何修改。
        prepare_factors.py 会从 factors/ 目录读取每个因子代码，
        在独立的临时目录中执行（python factor.py），读取 result.h5。
        """
        custom_factors = [f for f in factors_info if f.get("code_text")]
        if not custom_factors:
            return

        factors_dir = exp_dir / "factors"
        factors_dir.mkdir(parents=True, exist_ok=True)

        for f in custom_factors:
            code = f.get("code_text")
            if not code:
                continue
            fname = f["factor_name"]
            factor_path = factors_dir / f"{fname}.py"
            factor_path.write_text(code, encoding="utf-8")
            logger.info(f"写入因子文件: {factor_path}")

    def _resolve_factor_cache_universe_metadata(
        self,
        *,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """Return official ST PIT metadata that generated factor caches must match."""

        from .factor_universe_mask_service import (
            OFFICIAL_FACTOR_COVERAGE_SEMANTICS,
            OFFICIAL_FACTOR_INDEX_POLICY,
            OFFICIAL_FACTOR_UNIVERSE_KEY,
            OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION,
            QE_BACKTEST_FRESHNESS_PROFILE,
            FactorUniverseMaskService,
        )

        fallback = {
            "data_freshness_profile": QE_BACKTEST_FRESHNESS_PROFILE,
            "universe_key": OFFICIAL_FACTOR_UNIVERSE_KEY,
            "universe_rule_version": OFFICIAL_FACTOR_UNIVERSE_RULE_VERSION,
            "universe_fingerprint_sha256": "",
            "index_policy": OFFICIAL_FACTOR_INDEX_POLICY,
            "coverage_semantics": OFFICIAL_FACTOR_COVERAGE_SEMANTICS,
        }
        try:
            metadata = FactorUniverseMaskService().metadata(
                start_date=start_date,
                end_date=end_date,
                refresh_policy="coverage",
            )
        except Exception as exc:
            logger.warning(
                "Unable to resolve ST PIT universe fingerprint for QE prepare_factors.py; "
                "generated script will continue with the explicit QE backtest coverage "
                "cache policy and validate universe key/index/date coverage without a "
                "fingerprint: %s",
                exc,
            )
            return fallback

        return {key: metadata.get(key) if metadata.get(key) is not None else fallback[key] for key in fallback}

    def _compose_prepare_factors(
        self,
        factors_info: List[Dict],
        factor_data_dir: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """生成 prepare_factors.py 预处理脚本（含因子值缓存集成）。

        与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式：
        1. 从 RDAgent 数据目录链接所有数据文件到实验目录
        2. 对每个因子先检查缓存 (source_hash + 日期覆盖) → 命中则读 parquet
        3. 未命中则执行 python factor.py (timeout=1200s) → 成功后回写缓存
        4. 合并所有因子结果为 combined_factors_df.parquet
        """
        # 提取缓存窗口（用于全周期缓存命中判断）
        train_start = RDAGENT_DEFAULT_DATA_SPLIT["train_start"]
        test_end = QE_DEFAULT_SIGNAL_END
        if data_split:
            train_start = data_split.get("train_start", train_start)
            test_end = data_split.get("test_end", test_end)
        custom_factors = [f for f in factors_info if f.get("code_text")]
        if not custom_factors:
            return None

        factor_names = [f["factor_name"] for f in custom_factors]
        factor_cache_universe_metadata = self._resolve_factor_cache_universe_metadata(
            start_date=train_start,
            end_date=test_end,
        )

        lines: list[str] = []
        lines.append('"""')
        lines.append("QuantEvolver 因子预处理脚本")
        lines.append("与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式")
        lines.append("在 qrun 之前执行，生成 combined_factors_df.parquet")
        lines.append('"""')
        lines.append("import os")
        lines.append("import sys")
        lines.append("import shutil")
        lines.append("import logging")
        lines.append("import subprocess")
        lines.append("import tempfile")
        lines.append("import textwrap")
        lines.append("import re")
        lines.append("import numpy as np")
        lines.append("import pandas as pd")
        lines.append("")
        lines.append("logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')")
        lines.append("logger = logging.getLogger('prepare_factors')")
        lines.append("")
        lines.append(f"FACTOR_DATA_DIR = os.environ.get('RDAGENT_FACTOR_DATA_DIR', {repr(factor_data_dir or RDAGENT_FACTOR_DATA_WSL)})")
        lines.append("")
        lines.append("# ── 因子值缓存 ──────────────────────────────────────────")
        lines.append("import hashlib")
        lines.append("import json as _json")
        lines.append("def _is_forbidden_factor_cache_path(path_value):")
        lines.append("    _parts = [p.lower() for p in re.split(r'[/\\\\]+', str(path_value).strip().strip(\"\\\"'\").rstrip('/\\\\')) if p]")
        lines.append("    return 'factor_values_realtime' in _parts")
        lines.append("")
        lines.append("RAW_FACTOR_CACHE_DIR = os.environ.get('FACTOR_CACHE_DIR', '')")
        lines.append("if RAW_FACTOR_CACHE_DIR:")
        lines.append(r"    _cache_base = RAW_FACTOR_CACHE_DIR.rstrip('/\\')")
        lines.append("    if _is_forbidden_factor_cache_path(_cache_base):")
        lines.append("        raise RuntimeError('QE backtest must not use factor_values_realtime as FACTOR_CACHE_DIR')")
        lines.append("    if os.path.basename(_cache_base) == 'single':")
        lines.append("        FACTOR_CACHE_SINGLE_DIR = _cache_base")
        lines.append("        FACTOR_CACHE_META = os.path.join(os.path.dirname(_cache_base), '_meta.json')")
        lines.append("    else:")
        lines.append("        FACTOR_CACHE_SINGLE_DIR = os.path.join(_cache_base, 'single')")
        lines.append("        FACTOR_CACHE_META = os.path.join(_cache_base, '_meta.json')")
        lines.append("else:")
        lines.append("    FACTOR_CACHE_SINGLE_DIR = ''")
        lines.append("    FACTOR_CACHE_META = ''")
        lines.append(f"TRAIN_START = '{train_start}'")
        lines.append(f"TEST_END = '{test_end}'")
        lines.append(
            "FACTOR_CACHE_EXPECTED_UNIVERSE_META = "
            + repr(
                {
                    key: factor_cache_universe_metadata.get(key)
                    for key in (
                        "data_freshness_profile",
                        "universe_key",
                        "universe_rule_version",
                        "universe_fingerprint_sha256",
                        "index_policy",
                        "coverage_semantics",
                    )
                }
            )
        )
        lines.append("")
        lines.append("")
        lines.append("def _expected_universe_meta():")
        lines.append("    meta = dict(FACTOR_CACHE_EXPECTED_UNIVERSE_META)")
        lines.append("    env_map = {")
        lines.append("        'universe_key': 'FACTOR_CACHE_EXPECTED_UNIVERSE_KEY',")
        lines.append("        'universe_rule_version': 'FACTOR_CACHE_EXPECTED_UNIVERSE_RULE_VERSION',")
        lines.append("        'universe_fingerprint_sha256': 'FACTOR_CACHE_EXPECTED_UNIVERSE_FINGERPRINT_SHA256',")
        lines.append("        'index_policy': 'FACTOR_CACHE_EXPECTED_INDEX_POLICY',")
        lines.append("        'coverage_semantics': 'FACTOR_CACHE_EXPECTED_COVERAGE_SEMANTICS',")
        lines.append("    }")
        lines.append("    for key, env_name in env_map.items():")
        lines.append("        value = os.environ.get(env_name)")
        lines.append("        if value:")
        lines.append("            meta[key] = value")
        lines.append("    return meta")
        lines.append("")
        lines.append("")
        lines.append("def _cache_universe_mismatch(entry, expected):")
        lines.append("    required_keys = ('universe_key', 'index_policy')")
        lines.append("    for key in required_keys:")
        lines.append("        if expected.get(key) and entry.get(key) != expected.get(key):")
        lines.append("            return key")
        lines.append("    expected_fp = expected.get('universe_fingerprint_sha256')")
        lines.append("    if expected_fp and entry.get('universe_fingerprint_sha256') != expected_fp:")
        lines.append("        return 'universe_fingerprint_sha256'")
        lines.append("    return ''")
        lines.append("")
        lines.append("")
        lines.append('def _official_cache_miss_reasons():')
        lines.append('    return {')
        lines.append("        'missing_from_cache': [],")
        lines.append("        'missing_meta': [],")
        lines.append("        'as_of_date_mismatch': [],")
        lines.append("        'window_not_covered': [],")
        lines.append("        'universe_mismatch': [],")
        lines.append("        'index_policy_mismatch': [],")
        lines.append("        'hash_mismatch': [],")
        lines.append("        'schema_invalid': [],")
        lines.append('    }')
        lines.append('')
        lines.append('')
        lines.append('def _validate_official_cache_hit_contract(factor_name, factor_code):')
        lines.append("    cache_root = os.path.dirname(FACTOR_CACHE_SINGLE_DIR) if FACTOR_CACHE_SINGLE_DIR else ''")
        lines.append("    cache_path = os.path.join(FACTOR_CACHE_SINGLE_DIR, f'{factor_name}.parquet') if FACTOR_CACHE_SINGLE_DIR else ''")
        lines.append('    miss_reasons = _official_cache_miss_reasons()')
        lines.append('    top_level_errors = []')
        lines.append('    contract = {')
        lines.append("        'schema_version': 'official_factor_cache_hit_validation_v1',")
        lines.append("        'gate_status': 'failed',")
        lines.append("        'official_cache_hit': False,")
        lines.append("        'factor_name': factor_name,")
        lines.append("        'cache_root': cache_root,")
        lines.append("        'single_dir': FACTOR_CACHE_SINGLE_DIR,")
        lines.append("        'meta_path': FACTOR_CACHE_META,")
        lines.append("        'start_date': TRAIN_START,")
        lines.append("        'end_date': TEST_END,")
        lines.append("        'expected_as_of_date': TEST_END,")
        lines.append("        'requested_factor_count': 1,")
        lines.append("        'hit_factor_count': 0,")
        lines.append("        'miss_factor_count': 1,")
        lines.append("        'hit_factors': [],")
        lines.append("        'miss_factors': [factor_name],")
        lines.append("        'miss_reasons': miss_reasons,")
        lines.append("        'top_level_errors': top_level_errors,")
        lines.append('    }')
        lines.append('    if not FACTOR_CACHE_SINGLE_DIR:')
        lines.append("        top_level_errors.append('cache_dir_missing')")
        lines.append('        return contract')
        lines.append('    if _is_forbidden_factor_cache_path(FACTOR_CACHE_SINGLE_DIR):')
        lines.append("        top_level_errors.append('realtime_cache_forbidden')")
        lines.append('        return contract')
        lines.append('    if not os.path.exists(cache_path):')
        lines.append("        miss_reasons['missing_from_cache'].append(factor_name)")
        lines.append('        return contract')
        lines.append('    if not FACTOR_CACHE_META or not os.path.exists(FACTOR_CACHE_META):')
        lines.append("        miss_reasons['missing_meta'].append(factor_name)")
        lines.append('        return contract')
        lines.append('    try:')
        lines.append("        with open(FACTOR_CACHE_META, 'r', encoding='utf-8') as _meta_f:")
        lines.append('            meta = _json.load(_meta_f)')
        lines.append('    except Exception as e:')
        lines.append("        logger.warning(f'  {factor_name}: cache meta read failed: {e}')")
        lines.append("        miss_reasons['missing_meta'].append(factor_name)")
        lines.append('        return contract')
        lines.append("    entry = meta.get('factors', {}).get(factor_name)")
        lines.append('    if not isinstance(entry, dict):')
        lines.append("        miss_reasons['missing_meta'].append(factor_name)")
        lines.append('        return contract')
        lines.append("    data_mode = entry.get('data_source_mode') or meta.get('data_source_mode') or meta.get('source_system')")
        lines.append("    allowed_modes = {'backtest_factor_data_dir', 'official_offline_backtest_factor_data'}")
        lines.append('    if data_mode and data_mode not in allowed_modes:')
        lines.append("        logger.info(f'  {factor_name}: cache data mode mismatch ({data_mode})')")
        lines.append("        miss_reasons['schema_invalid'].append(factor_name)")
        lines.append('    expected_universe = _expected_universe_meta()')
        lines.append('    universe_mismatch = _cache_universe_mismatch(entry, expected_universe)')
        lines.append('    if universe_mismatch:')
        lines.append("        logger.info(f'  {factor_name}: cache universe mismatch ({universe_mismatch})')")
        lines.append("        if universe_mismatch == 'index_policy':")
        lines.append("            miss_reasons['index_policy_mismatch'].append(factor_name)")
        lines.append('        else:')
        lines.append("            miss_reasons['universe_mismatch'].append(factor_name)")
        lines.append('    code_hash = hashlib.sha256(factor_code.encode()).hexdigest()[:16]')
        lines.append("    cached_hash = entry.get('source_hash_raw') or entry.get('code_hash')")
        lines.append("    contract['expected_code_hashes'] = {factor_name: code_hash}")
        lines.append('    if cached_hash != code_hash:')
        lines.append("        logger.info(f'  {factor_name}: cache hash mismatch (cached={cached_hash}, current={code_hash})')")
        lines.append("        miss_reasons['hash_mismatch'].append(factor_name)")
        lines.append("    cached_range = entry.get('date_range', '')")
        lines.append("    contract['date_range'] = cached_range")
        lines.append("    if '~' not in cached_range:")
        lines.append("        miss_reasons['schema_invalid'].append(factor_name)")
        lines.append('    else:')
        lines.append("        c_start, c_end = cached_range.split('~')")
        lines.append('        _LOOKBACK_TOLERANCE_DAYS = 60')
        lines.append('        _ts = pd.Timestamp(TRAIN_START)')
        lines.append("        _window_start = entry.get('window_train_start')")
        lines.append('        if _window_start:')
        lines.append('            _gap_ok = pd.Timestamp(_window_start) <= _ts')
        lines.append('        else:')
        lines.append('            _gap_ok = (pd.Timestamp(c_start) - _ts).days <= _LOOKBACK_TOLERANCE_DAYS if c_start > TRAIN_START else True')
        lines.append('        if (not _gap_ok) or c_end < TEST_END:')
        lines.append("            logger.info(f'  {factor_name}: cache date insufficient ({cached_range} vs {TRAIN_START}~{TEST_END})')")
        lines.append("            miss_reasons['window_not_covered'].append(factor_name)")
        lines.append("        if entry.get('as_of_date') and pd.Timestamp(entry.get('as_of_date')) < pd.Timestamp(TEST_END):")
        lines.append("            miss_reasons['as_of_date_mismatch'].append(factor_name)")
        lines.append('    miss_factor_names = sorted({name for names in miss_reasons.values() for name in names})')
        lines.append('    if not top_level_errors and not miss_factor_names:')
        lines.append("        contract.update({'gate_status': 'passed', 'official_cache_hit': True, 'hit_factor_count': 1, 'miss_factor_count': 0, 'hit_factors': [factor_name], 'miss_factors': []})")
        lines.append('    return contract')
        lines.append('')
        lines.append('')
        lines.append('def _try_cache_hit(factor_name, factor_code):')
        lines.append('    """Load a factor only when the official cache-hit contract passes."""')
        lines.append('    contract = _validate_official_cache_hit_contract(factor_name, factor_code)')
        lines.append('    logger.info(f\'  {factor_name}: cache contract {contract["schema_version"]} gate={contract["gate_status"]}\')')
        lines.append("    if not contract.get('official_cache_hit'):")
        lines.append('        logger.info(f\'  {factor_name}: cache miss reasons={contract.get("miss_reasons")} top_errors={contract.get("top_level_errors")}\')')
        lines.append('        return None')
        lines.append("    cache_path = os.path.join(FACTOR_CACHE_SINGLE_DIR, f'{factor_name}.parquet')")
        lines.append('    df = pd.read_parquet(cache_path)')
        lines.append('    dates = df.index.get_level_values(0)')
        lines.append('    df = df[(dates >= pd.Timestamp(TRAIN_START)) & (dates <= pd.Timestamp(TEST_END))]')
        lines.append("    if 'value' in df.columns:")
        lines.append("        df = df.rename(columns={'value': factor_name})")
        lines.append('    logger.info(f\'  {factor_name}: CACHE HIT ({len(df)} rows, {contract.get("date_range")})\')')
        lines.append('    return df')
        lines.append("")
        lines.append("")
        lines.append("def _write_cache(factor_name, factor_code, result_df):")
        lines.append("    \"\"\"执行成功后回写因子值缓存。\"\"\"")
        lines.append("    if not FACTOR_CACHE_SINGLE_DIR:")
        lines.append("        return")
        lines.append("    try:")
        lines.append("        os.makedirs(FACTOR_CACHE_SINGLE_DIR, exist_ok=True)")
        lines.append("        cache_path = os.path.join(FACTOR_CACHE_SINGLE_DIR, f'{factor_name}.parquet')")

        lines.append("        code_hash = hashlib.sha256(factor_code.encode()).hexdigest()[:16]")
        lines.append("        save_df = result_df.copy()")
        lines.append("        if len(save_df.columns) == 1:")
        lines.append("            save_df = save_df.rename(columns={save_df.columns[0]: 'value'})")
        lines.append("        save_df.to_parquet(cache_path, engine='pyarrow', compression='snappy')")
        lines.append("        # 原子更新 _meta.json")
        lines.append("        import tempfile as _tmpf")
        lines.append("        meta = {}")
        lines.append("        if os.path.exists(FACTOR_CACHE_META):")
        lines.append("            try:")
        lines.append("                with open(FACTOR_CACHE_META, 'r', encoding='utf-8') as _meta_f:")
        lines.append("                    meta = _json.load(_meta_f)")
        lines.append("            except Exception as e:")
        lines.append("                logger.warning(f'  {factor_name}: cache meta read failed before write, skip cache write: {e}')")
        lines.append("                return")
        lines.append("        factors = meta.get('factors', {})")
        lines.append("        universe_meta = _expected_universe_meta()")
        lines.append("        dates = result_df.index.get_level_values(0)")
        lines.append("        d_min = str(dates.min().date())")
        lines.append("        d_max = str(dates.max().date())")
        lines.append("        factors[factor_name] = {")
        lines.append("            'computed_at': __import__('datetime').datetime.now().isoformat(),")
        lines.append("            'rows': len(result_df),")
        lines.append("            'date_range': f'{d_min}~{d_max}',")
        lines.append("            'as_of_date': d_max,")
        lines.append("            'source_hash_raw': code_hash,")
        lines.append("            'data_source_mode': 'backtest_factor_data_dir',")
        lines.append("            'window_train_start': TRAIN_START,")
        lines.append("            'window_backtest_end': TEST_END,")
        lines.append("        }")
        lines.append("        factors[factor_name].update(universe_meta)")
        lines.append("        meta['factors'] = factors")
        lines.append("        for _k, _v in universe_meta.items():")
        lines.append("            if _v is not None:")
        lines.append("                meta[_k] = _v")
        lines.append("        tmp_fd, tmp_path = _tmpf.mkstemp(dir=os.path.dirname(FACTOR_CACHE_META), suffix='.json')")

        lines.append("        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:")
        lines.append("            _json.dump(meta, f, indent=2, ensure_ascii=False)")
        lines.append("        os.replace(tmp_path, FACTOR_CACHE_META)")
        lines.append("        logger.info(f'  {factor_name}: cache WRITTEN ({len(result_df)} rows, {d_min}~{d_max}, window={TRAIN_START}~{TEST_END})')")

        lines.append("    except Exception as e:")
        lines.append("        logger.warning(f'  {factor_name}: cache write failed: {e}')")
        lines.append("")
        lines.append("")
        lines.append("ALLOWED_DATA_FILES = ('daily_pv.h5', 'daily_basic.h5', 'moneyflow.h5', 'bak_basic.h5', 'cyq_perf.h5', 'sector_data.h5', 'margin_detail.h5', 'static_factors.parquet')")
        lines.append("")
        lines.append("")
        lines.append("def link_all_files_to_dir(src_dir, dst_dir):")
        lines.append('    """仅链接 execution layer 允许的历史数据文件，避免混入实时数据。"""')
        lines.append("    src_dir = os.path.abspath(src_dir)")
        lines.append("    for item in ALLOWED_DATA_FILES:")
        lines.append("        src_path = os.path.join(src_dir, item)")
        lines.append("        dst_path = os.path.join(dst_dir, item)")
        lines.append("        if not os.path.isfile(src_path):")
        lines.append("            continue")
        lines.append("        if os.path.exists(dst_path) or os.path.islink(dst_path):")
        lines.append("            os.remove(dst_path)")
        lines.append("        try:")
        lines.append("            os.link(src_path, dst_path)")
        lines.append("            continue")
        lines.append("        except OSError:")
        lines.append("            pass")
        lines.append("        try:")
        lines.append("            os.symlink(src_path, dst_path)")
        lines.append("            continue")
        lines.append("        except OSError:")
        lines.append("            pass")
        lines.append("        # 检查文件大小")
        lines.append("        file_size = os.path.getsize(src_path)")
        lines.append("        if file_size >= 50 * 1024 * 1024:  # >= 50MB 大文件")
        lines.append("            # 大文件：创建路径映射文件，绝不复制")
        lines.append("            mapping_file = dst_path + '.pathmap'")
        lines.append("            with open(mapping_file, 'w') as f:")
        lines.append("                f.write(src_path)")
        lines.append("            logger.info(f'Large file {item} ({file_size/1024/1024:.0f}MB): created pathmap -> {src_path}')")
        lines.append("        else:")
        lines.append("            # 小文件：最后手段复制")
        lines.append("            shutil.copy2(src_path, dst_path)")
        lines.append("")
        lines.append("")
        lines.append("def execute_factor(factor_name, factor_code, work_dir):")
        lines.append('    """因子执行（含缓存读取/回写）。')
        lines.append('    1. 检查缓存：source_hash 匹配 + 日期覆盖 → 读 parquet')
        lines.append('    2. 未命中：执行 factor.py (timeout=1200s)')
        lines.append('    3. 成功后回写缓存')
        lines.append('    """')
        lines.append("    # ── 缓存检查 ──")
        lines.append("    cached = _try_cache_hit(factor_name, factor_code)")
        lines.append("    if cached is not None:")
        lines.append("        return cached")
        lines.append("")
        lines.append("    factor_dir = os.path.join(work_dir, f'_factor_{factor_name}')")
        lines.append("    os.makedirs(factor_dir, exist_ok=True)")
        lines.append("")
        lines.append("    # 链接所有数据文件到因子执行目录")
        lines.append("    link_all_files_to_dir(FACTOR_DATA_DIR, factor_dir)")
        lines.append("")
        lines.append("    # 直接使用因子库 code_text 源码，不做任何修改")
        lines.append("    factor_py_path = os.path.join(factor_dir, 'factor.py')")
        lines.append("    with open(factor_py_path, 'w', encoding='utf-8') as f:")
        lines.append("        f.write(factor_code)")
        lines.append("")
        lines.append("    # 执行因子代码 (超时 1800s, 适配720万行 × rolling.cov 等重型算子)")
        lines.append("    _factor_timeout = int(os.environ.get('AISTOCK_FACTOR_TIMEOUT', '1800'))")
        lines.append("    try:")
        lines.append("        output = subprocess.check_output(")
        lines.append("            [sys.executable, 'factor.py'],")
        lines.append("            cwd=factor_dir,")
        lines.append("            stderr=subprocess.STDOUT,")
        lines.append("            timeout=_factor_timeout,")
        lines.append("        )")
        lines.append("        logger.info(f'  {factor_name}: execution succeeded')")
        lines.append("    except subprocess.CalledProcessError as e:")
        lines.append("        err_msg = e.output.decode('utf-8', errors='replace')[-500:]")
        lines.append("        logger.error(f'  {factor_name}: execution failed: {err_msg}')")
        lines.append("        return None")
        lines.append("    except subprocess.TimeoutExpired:")
        lines.append("        logger.error(f'  {factor_name}: execution timeout ({_factor_timeout}s)')")
        lines.append("        return None")
        lines.append("")
        lines.append("    # 读取结果")
        lines.append("    result_path = os.path.join(factor_dir, 'result.h5')")
        lines.append("    if not os.path.exists(result_path):")
        lines.append("        logger.error(f'  {factor_name}: result.h5 not found')")
        lines.append("        return None")
        lines.append("")
        lines.append("    try:")
        lines.append("        df = pd.read_hdf(result_path)")
        lines.append("        if df is None or (hasattr(df, 'empty') and df.empty):")
        lines.append("            logger.warning(f'  {factor_name}: empty result')")
        lines.append("            return None")
        lines.append("        # 确保是 DataFrame 或 Series")
        lines.append("        if isinstance(df, pd.Series):")
        lines.append("            df = df.to_frame(name=factor_name)")
        lines.append("        logger.info(f'  {factor_name}: {df.shape[0]} rows, {df.shape[1]} cols')")
        lines.append("        # 回写缓存")
        lines.append("        _write_cache(factor_name, factor_code, df)")
        lines.append("        return df")
        lines.append("    except Exception as e:")
        lines.append("        logger.error(f'  {factor_name}: failed to read result.h5: {e}')")
        lines.append("        return None")
        lines.append("")
        lines.append("")
        lines.append("def main():")
        lines.append("    script_dir = os.path.dirname(os.path.abspath(__file__))")
        lines.append("    os.chdir(script_dir)")
        lines.append("")
        lines.append("    logger.info(f'Factor data dir: {FACTOR_DATA_DIR}')")
        lines.append("    logger.info(f'Work dir: {script_dir}')")
        lines.append("")
        lines.append("    # 同时将数据文件链接到实验根目录（供 qrun 时 StaticDataLoader 使用）")
        lines.append("    link_all_files_to_dir(FACTOR_DATA_DIR, script_dir)")
        lines.append("    logger.info('Linked all data files to experiment dir')")
        lines.append("")

        # 读取因子代码（从 factors/ 子目录中的独立文件）
        lines.append("    # 因子代码定义")
        lines.append("    factor_codes = {}")
        for fname in factor_names:
            lines.append(f"    factor_codes[{fname!r}] = open(os.path.join(script_dir, 'factors', {fname!r} + '.py'), 'r', encoding='utf-8').read()")

        lines.append("")
        lines.append("    # 逐个执行因子")
        lines.append("    factor_results = []")
        lines.append("    for factor_name, factor_code in factor_codes.items():")
        lines.append("        logger.info(f'Executing factor: {factor_name}...')")
        lines.append("        result = execute_factor(factor_name, factor_code, script_dir)")
        lines.append("        if result is not None:")
        lines.append("            factor_results.append(result)")
        lines.append("")
        lines.append("    # 合并为 combined_factors_df.parquet")
        lines.append("    if not factor_results:")
        lines.append("        logger.error('No factors computed successfully!')")
        lines.append("        sys.exit(1)")
        lines.append("")
        lines.append("    logger.info(f'Combining {len(factor_results)} factor results...')")
        lines.append("    combined = pd.concat(factor_results, axis=1)")
        lines.append("    combined = combined.sort_index()")
        lines.append("    combined = combined.loc[:, ~combined.columns.duplicated(keep='last')]")
        lines.append("")
        lines.append("    # 确保索引格式正确：MultiIndex(datetime, instrument)")
        lines.append("    # StaticDataLoader 要求索引必须是 MultiIndex(datetime, instrument)")
        lines.append("    if not isinstance(combined.index, pd.MultiIndex):")
        lines.append("        # 检查是否有 datetime 和 instrument 列")
        lines.append("        if 'datetime' in combined.columns and 'instrument' in combined.columns:")
        lines.append("            logger.info('Setting index from datetime/instrument columns...')")
        lines.append("            combined['datetime'] = pd.to_datetime(combined['datetime'], errors='coerce')")
        lines.append("            combined = combined.set_index(['datetime', 'instrument'])")
        lines.append("        else:")
        lines.append("            logger.error('Factor data must have MultiIndex(datetime, instrument) or datetime/instrument columns!')")
        lines.append("            sys.exit(1)")
        lines.append("")
        lines.append("    # 确保索引名称顺序正确：[""datetime"", ""instrument""]")
        lines.append("    if isinstance(combined.index, pd.MultiIndex):")
        lines.append("        names = list(combined.index.names)")
        lines.append("        if set(names) == {'datetime', 'instrument'} and names != ['datetime', 'instrument']:")
        lines.append("            logger.info('Swapping index levels to (datetime, instrument)...')")
        lines.append("            combined = combined.swaplevel('datetime', 'instrument')")
        lines.append("        combined = combined.sort_index()")
        lines.append("")
        lines.append("    # 添加 'feature' 多级列索引（与 RDAgent factor_runner.py 一致）")
        lines.append("    new_columns = pd.MultiIndex.from_product([['feature'], combined.columns])")
        lines.append("    combined.columns = new_columns")
        lines.append("")
        lines.append("    output_path = os.path.join(script_dir, 'combined_factors_df.parquet')")
        lines.append("    combined.to_parquet(output_path, engine='pyarrow')")
        lines.append("    logger.info(f'Saved combined_factors_df.parquet: shape={combined.shape}')")
        lines.append("")
        lines.append("    # 计算 num_features：根据 conf.yaml 中的 data_loader 类型决定")
        lines.append("    # - 如果使用 DynamicFactorsOnlyLoader（disable_alpha158=True）：num_features = 自定义因子数")
        lines.append("    # - 如果使用 NestedDataLoader（disable_alpha158=False）：num_features = Alpha158数 + 自定义因子数")
        lines.append("    import yaml")
        lines.append("    conf_path = os.path.join(script_dir, 'conf.yaml')")
        lines.append("    alpha158_count = 0")
        lines.append("    use_alpha158 = False")
        lines.append("    try:")
        lines.append("        with open(conf_path, 'r', encoding='utf-8') as cf:")
        lines.append("            conf_text = cf.read()")
        lines.append("        # conf.yaml 包含 Jinja2 模板语法，先做简单替换以便 yaml 解析")
        lines.append("        import re as _re")
        lines.append("        conf_text = _re.sub(r'\\{\\{.*?\\}\\}', '0', conf_text)")
        lines.append("        conf_text = _re.sub(r'\\{%.*?%\\}', '', conf_text)")
        lines.append("        conf_obj = yaml.safe_load(conf_text)")
        lines.append("        # 检查 data_loader 类型")
        lines.append("        dhc = conf_obj.get('data_handler_config', {})")
        lines.append("        dl = dhc.get('data_loader', {})")
        lines.append("        dl_cls = dl.get('class', '')")
        lines.append("        # 如果使用 DynamicFactorsOnlyLoader，说明 disable_alpha158=True")
        lines.append("        if 'DynamicFactorsOnlyLoader' in dl_cls:")
        lines.append("            use_alpha158 = False")
        lines.append("            logger.info('检测到 DynamicFactorsOnlyLoader，disable_alpha158=True，只使用自定义因子')")
        lines.append("        else:")
        lines.append("            # 使用 NestedDataLoader，遍历 dataloader_l 找 Alpha158DL")
        lines.append("            use_alpha158 = True")
        lines.append("            dl_list = dl.get('kwargs', {}).get('dataloader_l', [])")
        lines.append("            for dl_item in dl_list:")
        lines.append("                dl_item_cls = dl_item.get('class', '')")
        lines.append("                if 'Alpha158' in dl_item_cls:")
        lines.append("                    feat_cfg = dl_item.get('kwargs', {}).get('config', {}).get('feature', [])")
        lines.append("                    if len(feat_cfg) >= 2:")
        lines.append("                        # feat_cfg[0] = 表达式列表, feat_cfg[1] = 名称列表")
        lines.append("                        alpha158_count = len(feat_cfg[0])")
        lines.append("                        logger.info(f'从 conf.yaml 解析到 Alpha158 特征数: {alpha158_count}')")
        lines.append("                    break")
        lines.append("    except Exception as e:")
        lines.append("        logger.warning(f'解析 conf.yaml 失败: {e}')")
        lines.append("    # 如果使用 Alpha158 但未解析到特征数，使用默认值")
        lines.append("    if use_alpha158 and alpha158_count == 0:")
        lines.append("        alpha158_count = int(os.environ.get('INITIAL_FACTOR_LIBRARY_SIZE', '20'))")
        lines.append("        logger.warning(f'未能从 conf.yaml 解析 Alpha158 特征数，使用默认值: {alpha158_count}')")
        lines.append("    custom_factor_cols = len([c for c in combined.columns if c[0] == 'feature'])")
        lines.append("    if use_alpha158:")
        lines.append("        num_features = alpha158_count + custom_factor_cols")
        lines.append("        logger.info(f'num_features = {alpha158_count} (Alpha158) + {custom_factor_cols} (custom) = {num_features}')")
        lines.append("    else:")
        lines.append("        num_features = custom_factor_cols")
        lines.append("        logger.info(f'num_features = {custom_factor_cols} (custom only, disable_alpha158=True)')")
        lines.append("")
        lines.append("    # 写入环境变量文件，供后续 qrun 使用")
        lines.append("    env_file = os.path.join(script_dir, '.factor_env')")
        lines.append("    with open(env_file, 'w') as f:")
        lines.append("        f.write(f'export num_features={num_features}\\n')")
        lines.append("    logger.info(f'Wrote .factor_env: num_features={num_features}')")
        lines.append("")
        lines.append("    logger.info('Factor preparation completed successfully.')")
        lines.append("")
        lines.append("if __name__ == '__main__':")
        lines.append("    main()")

        return "\n".join(lines)

    # ---- 辅助方法 ----

    def _get_factors_info(self, factor_names: List[str],
                          factor_sources: Optional[Dict[str, str]] = None) -> List[Dict]:
        """获取因子详细信息。
        
        QE 回测只使用因子库 aistock_factor_catalog.code_text 作为唯一权威源码。
        不在实验运行时回连 RDAgent API 获取历史源码，避免已删除实验、格式差异
        或远端 API 状态变化导致同名因子缓存 hash 不稳定。
        """
        if not factor_names:
            return []

        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT factor_name, source, expression, code_text,
                           ic, sharpe, annualized_return,
                           best_loop_task_run_id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                """, factor_names)
                cols = [desc[0] for desc in cur.description]
                results = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 如果指定了source映射，过滤
        if factor_sources:
            results = [r for r in results if r["factor_name"] not in factor_sources
                       or r["source"] == factor_sources[r["factor_name"]]]

        missing_code = [r.get("factor_name") for r in results if not r.get("code_text")]
        if missing_code:
            logger.warning(
                f"[QE] {len(missing_code)} 个因子缺少因子库 code_text，将无法作为自定义因子使用: "
                f"{missing_code[:10]}"
            )

        # 补充未在数据库中的因子
        found_names = {r["factor_name"] for r in results}
        for fn in factor_names:
            if fn not in found_names:
                results.append({"factor_name": fn, "source": "unknown"})

        return results

    def _fetch_factor_source_from_api(self, task_id: str, factor_name: str) -> Optional[str]:
        """从 RDAgent API 获取因子原始源码。
        
        Args:
            task_id: RDAgent 任务 ID
            factor_name: 因子名称
            
        Returns:
            因子源码字符串，失败时返回 None
        """
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            result = client.get_factor_source_code(task_id, factor_name)
            if result.get("success"):
                return result.get("source_code")
            else:
                raise RuntimeError(
                    f"[QE] 获取因子源码失败: factor={factor_name}, task={task_id}, "
                    f"error={result.get('error', 'unknown')}"
                )
        except Exception as e:
            raise RuntimeError(f"[QE] 获取因子源码异常: factor={factor_name}, task={task_id}, {e}") from e

    # Qlib 内置模型定义（不需要自定义代码，仅超参数）
    #
    # Multi-Alpha 架构新增说明:
    #   - 每个模型可附带 default_hyperparameters（composer 兜底使用）
    #   - default_dataset_type 暗示 Multi-Alpha 分组时的首选 dataset (TSDatasetH / DatasetH)
    #   - 实际 dataset_cls 由 Multi-Alpha engine 在 Jinja 渲染阶段注入，此处仅作为元信息
    _BUILTIN_MODELS = {
        "__builtin_LGBModel__": {
            "model_name": "LGBModel", "model_type": "LGB",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_XGBModel__": {
            "model_name": "XGBModel", "model_type": "XGB",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_CatBoostModel__": {
            "model_name": "CatBoostModel", "model_type": "CATBOOST",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_LinearModel__": {
            "model_name": "LinearModel", "model_type": "LINEAR",
            "default_dataset_type": "DatasetH",
            "default_hyperparameters": {"estimator": "ridge", "alpha": 0.1},
        },
        # 兼容旧数据中的小写 ID
        "__builtin_lgbmodel__": {
            "model_name": "LGBModel", "model_type": "LGB",
            "default_dataset_type": "DatasetH",
        },
        # ── Multi-Alpha 新增: 时序/非线性模型 ──────────────────────
        "__builtin_ALSTM__": {
            "model_name": "ALSTM", "model_type": "PTNN",
            "default_dataset_type": "TSDatasetH",
            "default_hyperparameters": {
                "pt_model_uri": "qlib.contrib.model.pytorch_alstm_ts.ALSTMModel",
                "d_feat": 20, "hidden_size": 64, "num_layers": 1,
                "dropout": 0.0,
            },
            "default_training_hyperparameters": {
                "n_epochs": 200, "lr": 3e-4,
                "early_stop": 20, "batch_size": 4096,
                "weight_decay": 1e-5,
            },
        },
        "__builtin_GRU2__": {
            "model_name": "GRU2", "model_type": "PTNN",
            "default_dataset_type": "TSDatasetH",
            "default_hyperparameters": {
                "pt_model_uri": "qlib.contrib.model.pytorch_gru_ts.GRUModel",
                "d_feat": 20, "hidden_size": 128, "num_layers": 2,
                "dropout": 0.2,
            },
            "default_training_hyperparameters": {
                "n_epochs": 200, "lr": 2e-4,
                "early_stop": 20, "batch_size": 4096,
                "weight_decay": 1e-4,
            },
        },
        "__builtin_Ridge__": {
            "model_name": "Ridge", "model_type": "LINEAR",
            "default_dataset_type": "DatasetH",
            "default_hyperparameters": {"estimator": "ridge", "alpha": 0.1},
        },
    }

    def _get_model_info(self, model_id: str) -> Optional[Dict]:
        """获取模型信息（含源代码）。

        Multi-Alpha 解析优先级:
          1. aistock_model_catalog (用户可见/可管理)
          2. _BUILTIN_MODELS fallback (系统内置兜底)

        Catalog 中 model_hyperparameters 为空时，会自动从 _BUILTIN_MODELS 补齐 defaults。
        """
        # 1. 先查 catalog
        catalog_row = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type,
                           model_hyperparameters, model_training_hyperparameters,
                           ic, annualized_return, is_sota,
                           code_text
                    FROM aistock_model_catalog
                    WHERE model_id = %s
                """, (model_id,))
                row = cur.fetchone()
                if row:
                    cols = [desc[0] for desc in cur.description]
                    catalog_row = dict(zip(cols, row))

        # 2. 如果 catalog 没有，尝试 builtin
        if catalog_row is None:
            builtin = self._BUILTIN_MODELS.get(model_id)
            if not builtin:
                return None
            return {
                "model_id": model_id,
                "model_name": builtin["model_name"],
                "model_type": builtin["model_type"],
                "model_hyperparameters": builtin.get("default_hyperparameters"),
                "model_training_hyperparameters": builtin.get("default_training_hyperparameters"),
                "ic": None,
                "annualized_return": None,
                "is_sota": False,
                "code_text": None,
                "default_dataset_type": builtin.get("default_dataset_type"),
            }

        # 3. catalog 命中：补充 builtin default 作为 fallback（仅当对应字段为空）
        # 通过 model_name 匹配最接近的 builtin
        builtin_match = None
        for bid, bmeta in self._BUILTIN_MODELS.items():
            if bmeta["model_name"] == catalog_row.get("model_name"):
                builtin_match = bmeta
                break
        if builtin_match:
            if not catalog_row.get("model_hyperparameters") and builtin_match.get("default_hyperparameters"):
                catalog_row["model_hyperparameters"] = builtin_match["default_hyperparameters"]
            if not catalog_row.get("model_training_hyperparameters") and builtin_match.get("default_training_hyperparameters"):
                catalog_row["model_training_hyperparameters"] = builtin_match["default_training_hyperparameters"]
            if not catalog_row.get("model_type") and builtin_match.get("model_type"):
                catalog_row["model_type"] = builtin_match["model_type"]
            catalog_row["default_dataset_type"] = builtin_match.get("default_dataset_type")
        return catalog_row

    def _get_strategy_info(self, strategy_id: str) -> Optional[Dict]:
        """获取策略信息（含源码和参数）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_id, display_name, portfolio_config,
                           source_code, default_kwargs, param_schema
                    FROM aistock_strategy_catalog
                    WHERE strategy_id = %s
                """, (strategy_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _windows_to_wsl_path(self, win_path: str) -> str:
        """Windows路径转WSL路径。"""
        path = win_path.replace("\\", "/")
        if len(path) >= 2 and path[1] == ":":
            drive = path[0].lower()
            path = f"/mnt/{drive}{path[2:]}"
        return path

    @staticmethod
    def _local_hmm_artifact_path(path_text: str) -> Optional[Path]:
        """Resolve AIstock-local HMM artifact paths without probing worker filesystems."""
        text = str(path_text or "").strip()
        if not text:
            return None
        normalized = text.replace("\\", "/")
        if normalized.startswith("/") or normalized.startswith("//"):
            return None
        if len(normalized) >= 2 and normalized[1] == ":":
            return Path(normalized)
        candidate = Path(text)
        return candidate if candidate.is_absolute() else None

    @staticmethod
    def _build_conda_activate_chain(default_env: str = "rdagent-gpu") -> str:
        """构造稳健的 conda 初始化命令。

        兼容以下场景：
        - QLIB_WSL_CONDA_SH 未设置
        - QLIB_WSL_CONDA_SH 配成 ~/miniconda3/...（在引号内需手动展开）
        - 节点 conda 安装路径与本机不同
        """
        return (
            'if [ -n "${QLIB_WSL_CONDA_SH:-}" ] && [ -f "${QLIB_WSL_CONDA_SH}" ]; then '
            '. "${QLIB_WSL_CONDA_SH}"; '
            'elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then '
            '. "$HOME/miniconda3/etc/profile.d/conda.sh"; '
            'elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then '
            '. "$HOME/anaconda3/etc/profile.d/conda.sh"; '
            'elif [ -f "/opt/conda/etc/profile.d/conda.sh" ]; then '
            '. "/opt/conda/etc/profile.d/conda.sh"; '
            'else echo "conda.sh not found" >&2; exit 1; fi && '
            f'conda activate "${{QLIB_WSL_CONDA_ENV:-{default_env}}}"'
        )

    def _build_auto_wsl_command_parts(
        self,
        wsl_path: str,
        has_custom_factors: bool = False,
        use_custom_model: bool = False,
        model_type_tag: Optional[str] = None,
        backtest_freq: str = "1min",
        train_only: bool = False,
        factor_cache_dir: Optional[str] = None,
        seed_ensemble_enabled: bool = False,
    ) -> tuple[list[str], list[str]]:
        """构造 auto 模式命令片段。

        Args:
            train_only: True 时生成 --train-only 命令，跳过回测（多Alpha从节点模式）。
            factor_cache_dir: 节点配置的因子缓存目录（远端节点使用 rsync 同步的路径）。
        """
        env_lines = []
        if has_custom_factors or use_custom_model:
            env_lines.append(f'export PYTHONPATH="{wsl_path}:${{QLIB_RDAGENT_ROOT_WSL:-.}}:$PYTHONPATH"')

        if use_custom_model and model_type_tag:
            if model_type_tag == "TimeSeries":
                env_lines.append("export dataset_cls=TSDatasetH")
                env_lines.append("export step_len=20")
                env_lines.append("export num_timesteps=20")
            else:
                env_lines.append("export dataset_cls=DatasetH")

        if use_custom_model and not has_custom_factors:
            env_lines.append("export num_features=20")
        elif has_custom_factors:
            env_lines.append("# num_features 将在 qrun 时由 conf.yaml Jinja2 模板自动计算")

        # 因子缓存目录：QE 回测只允许 backtest factor_values，不能继承或指向 realtime 缓存。
        if factor_cache_dir:
            if _is_realtime_factor_cache_path(factor_cache_dir):
                raise ValueError(
                    "QE backtest factor_cache_dir must not point to factor_values_realtime"
                )
            # 远端节点：直接使用配置的绝对路径
            env_lines.append(f'export FACTOR_CACHE_DIR="{factor_cache_dir}"')
        else:
            # 本地节点：强制使用 Windows 路径转换后的 QE 回测缓存，覆盖任何继承环境变量。
            factor_cache_wsl = self._windows_to_wsl_path(str(FACTOR_CACHE_ROOT_WIN))
            if _is_realtime_factor_cache_path(factor_cache_wsl):
                raise RuntimeError("QE backtest FACTOR_CACHE_ROOT_WIN resolves to factor_values_realtime")
            env_lines.append(f'export FACTOR_CACHE_DIR="{factor_cache_wsl}"')
        env_lines.append('export FACTOR_CACHE_DATA_MODE="backtest_factor_data_dir"')

        link_data_cmd = (
            '_FDD="${RDAGENT_FACTOR_DATA_WSL:-.}" && '
            'for f in daily_basic.h5 daily_pv.h5 moneyflow.h5 bak_basic.h5 cyq_perf.h5 sector_data.h5 static_factors.parquet; do '
            '[ ! -e "$f" ] && [ -e "$_FDD/$f" ] && ln -sf "$_FDD/$f" .; done; true'
        )

        runner = "qrun_limit_minute.py" if seed_ensemble_enabled or backtest_freq != "day" else "qrun_limit.py"

        core_parts = [
            self._build_conda_activate_chain(),
            "export MALLOC_ARENA_MAX=4",
            "export PYTHONUNBUFFERED=1",
        ]
        if train_only:
            core_parts.append("export TRAIN_ONLY=1")
        core_parts.extend([line for line in env_lines if line and not line.startswith("#")])
        core_parts.append(link_data_cmd)
        if has_custom_factors:
            core_parts.append("python prepare_factors.py")
            core_parts.append(". ./.factor_env")
        runner_cmd = f"python {runner} conf.yaml"
        if train_only:
            runner_cmd += " --train-only"
        core_parts.append(runner_cmd)
        core_parts.append("QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py")
        return env_lines, core_parts

    def _generate_wsl_command(self, wsl_path: str,
                              has_custom_factors: bool = False,
                              use_custom_model: bool = False,
                              model_type_tag: Optional[str] = None,
                              mode: str = "manual",
                              backtest_freq: str = "1min",
                              train_only: bool = False,
                              factor_cache_dir: Optional[str] = None,
                              seed_ensemble_enabled: bool = False) -> str:
        """生成WSL执行命令。

        Args:
            mode: "manual" — 面向用户手动复制执行（含注释、conda activate）
                  "auto"   — 面向子进程自动执行（纯净命令链，用 && 连接）
            train_only: True 时生成 --train-only 命令（多Alpha从节点模式）
            factor_cache_dir: 节点配置的因子缓存目录
        """
        env_lines, core_parts = self._build_auto_wsl_command_parts(
            wsl_path,
            has_custom_factors=has_custom_factors,
            use_custom_model=use_custom_model,
            model_type_tag=model_type_tag,
            backtest_freq=backtest_freq,
            train_only=train_only,
            factor_cache_dir=factor_cache_dir,
            seed_ensemble_enabled=seed_ensemble_enabled,
        )
        env_block = "\n".join(env_lines)

        # 手动模式的数据链接步骤（可读格式）
        _link_data_manual = f"""# 链接策略所需数据文件到实验目录（幂等）
_FDD="${{RDAGENT_FACTOR_DATA_WSL:-{RDAGENT_FACTOR_DATA_WSL}}}"
for f in daily_basic.h5 daily_pv.h5 moneyflow.h5 bak_basic.h5 cyq_perf.h5 sector_data.h5 static_factors.parquet; do
  [ ! -e "$f" ] && [ -e "$_FDD/$f" ] && ln -sf "$_FDD/$f" .
done"""

        # 分钟线使用 qrun_limit_minute.py（含内存 patch + benchmark），日线使用 qrun_limit.py
        runner = "qrun_limit_minute.py" if seed_ensemble_enabled or backtest_freq != "day" else "qrun_limit.py"

        # ── auto 模式：纯净命令链，供子进程直接执行 ──
        if mode == "auto":
            return " && ".join([f"cd {wsl_path}", *core_parts])

        # ── manual 模式：面向用户手动复制执行 ──
        train_only_flag = " --train-only" if train_only else ""
        if has_custom_factors:
            return f"""# QuantEvolver 实验执行命令（含自定义因子预处理）
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

{_link_data_manual}

# 步骤1: 预计算因子 -> 生成 combined_factors_df.parquet
python prepare_factors.py

# 步骤2: 设置环境变量
{env_block}
# 加载因子预处理输出的 num_features（由 prepare_factors.py 自动计算）
. .factor_env

# 步骤3: 运行QLib回测
python {runner} conf.yaml{train_only_flag}

# 步骤4: 读取结果
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""
        elif use_custom_model:
            return f"""# QuantEvolver 实验执行命令（含自定义模型）
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

# 设置环境变量
{env_block}

{_link_data_manual}

# 运行QLib回测
python {runner} conf.yaml{train_only_flag}

# 读取结果
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""
        else:
            return f"""# QuantEvolver 实验执行命令
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

{_link_data_manual}

python {runner} conf.yaml{train_only_flag}
QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""

    def _api_sync_experiment_files(self, experiment_name: str, exp_dir: Path) -> None:
        """通过API将实验文件同步到RDAgent侧。
        
        当AIstock和RDAgent在同一台机器上时，文件已通过直接写入共享目录完成同步。
        此方法额外通过HTTP API同步一份，确保独立部署场景下也能正常工作。
        若API同步失败则抛错中断，避免RDAgent侧缺文件导致实验执行失败。
        """
        exp_dir = ensure_aistock_artifact_path(
            exp_dir,
            purpose=f"QE experiment local files for API sync: {experiment_name}",
            extra_roots=[QE_EXPERIMENTS_ROOT],
        )
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            # 递归收集实验目录中的所有文件（包含 factors/ 子目录）
            # 文本文件直接同步，二进制文件（parquet 等）以 base64 编码同步
            import base64 as _b64
            files_to_sync = {}
            text_suffixes = {".yaml", ".yml", ".py", ".txt", ".json"}
            binary_suffixes = {".parquet"}
            for f in exp_dir.rglob("*"):
                try:
                    if f.is_file() and f.suffix in text_suffixes:
                        try:
                            rel_path = f.relative_to(exp_dir).as_posix()
                            files_to_sync[rel_path] = f.read_text(encoding="utf-8")
                        except Exception as e:
                            raise RuntimeError(f"读取文件 {f} 失败: {e}") from e
                    elif f.is_file() and f.suffix in binary_suffixes:
                        try:
                            rel_path = f.relative_to(exp_dir).as_posix()
                            files_to_sync[rel_path + ".b64"] = _b64.b64encode(f.read_bytes()).decode("ascii")
                        except Exception as e:
                            raise RuntimeError(f"读取二进制文件 {f} 失败: {e}") from e
                except OSError as e:
                    raise RuntimeError(f"检查文件状态失败: {f} - {e}") from e
            
            if not files_to_sync:
                return
            
            result = client.sync_experiment_files(experiment_name, files_to_sync)
            if result.get("success"):
                logger.info(
                    f"[QESync] API同步成功: {experiment_name}, "
                    f"{result.get('success_count', 0)} 个文件"
                )
            else:
                logger.error(
                    f"[QESync] API同步失败: {experiment_name}, "
                    f"full_result={result}"
                )
                raise RuntimeError(
                    f"[QESync] API同步失败: {experiment_name}, "
                    f"error={result.get('error', 'unknown')}, "
                    f"failed={result.get('failed', [])}"
                )
        except Exception as e:
            # 独立部署场景必须保证同步成功，否则RDAgent侧可能缺文件导致实验失败
            logger.error(f"[QESync] API同步异常: {e}")
            raise

    def _api_setup_experiment_workspace(self, experiment_name: str) -> None:
        """通过API在RDAgent侧创建实验工作区并链接数据文件。
        
        调用RDAgent的POST /api/qe/experiments/{exp_id}/setup API：
        1. 在WSL环境中创建实验工作区目录
        2. 链接数据文件（daily_pv.h5, static_factors.parquet等）
        
        若API调用失败则抛错中断，确保RDAgent侧工作区和数据链接已就绪。
        """
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            result = client.setup_experiment_workspace(
                exp_id=experiment_name,
                link_data_files=True,
                overwrite=False,
            )
            if result.get("success"):
                logger.info(
                    f"[QESetup] 实验工作区创建成功: {experiment_name}, "
                    f"workspace={result.get('workspace_path')}, "
                    f"linked_files={result.get('linked_files_count', 0)}"
                )
            else:
                raise RuntimeError(
                    f"[QESetup] 实验工作区创建失败: {experiment_name}, "
                    f"error={result.get('error', 'unknown')}"
                )
        except Exception as e:
            # 必须确保RDAgent侧工作区和数据链接创建成功，否则后续实验不可可靠执行
            logger.error(f"[QESetup] API创建实验工作区异常: {e}")
            raise

    def _write_custom_model(self, exp_dir: Path, model_info: Dict) -> None:
        """将RDAgent自定义模型源码写入实验目录的 model.py。

        与 RDAgent 完全一致的方式：
        1. model.py 只包含纯 PyTorch NN 类代码
        2. 文件末尾导出 model_cls 变量指向 NN 类
        3. GeneralPTNN 通过 pt_model_uri: "model.model_cls" 自动加载并包装

        这样 GeneralPTNN 负责所有训练/推理逻辑（分批处理、GPU管理、早停等），
        无需 QE 自行实现 QLib Model 接口，支持所有 QLib 模型类型。
        """
        code_text = model_info.get("code_text", "")
        if not code_text:
            return

        model_name = model_info.get("model_name", "CustomModel")

        # 从源代码中提取 NN 主类名。优先尊重代码中显式导出的 model_cls，
        # 否则使用最后一个 class，避免 TCN/辅助层类被误选为模型入口。
        nn_class_name = self._extract_nn_model_class_name(code_text, model_name)

        # 构建 model.py：纯NN类 + model_cls 导出
        # 与 RDAgent 中 model.py 的格式完全一致
        model_py_content = f'''"""
RDAgent SOTA模型: {model_name}
QuantEvolver自动生成 - 由 GeneralPTNN 通过 pt_model_uri: "model.model_cls" 加载
"""
{code_text}

# GeneralPTNN 通过此变量加载 NN 类
model_cls = {nn_class_name}
'''

        (exp_dir / "model.py").write_text(model_py_content, encoding="utf-8")
        logger.info(f"写入模型文件: {exp_dir / 'model.py'} (NN类: {nn_class_name}, 由GeneralPTNN加载)")

    def _precompute_hmm_coefficients(
        self, strategy_params: Dict[str, Any], data_split: Dict[str, str],
    ) -> str:
        """获取 HMM 行业热度系数 JSON 字符串.

        只读取训练时预生成的本地系数文件，或使用调用方注入的预计算 JSON。
        文件不存在时 fail-fast；Windows FastAPI 不再启动 WSL 子进程或读取 worker 路径。
        系数文件命名: coefficients_{preset}_{test_start}_{backtest_end}.json
        存放在 model_path 同级目录。

        Raises ValueError/RuntimeError on any failure — no silent fallback.
        """
        model_path = strategy_params.get("sector_hmm_model_path")
        if not model_path:
            raise ValueError("enable_sector_hmm=True 但未提供 sector_hmm_model_path")

        test_start = data_split.get("test_start")
        backtest_end = data_split.get("backtest_end")
        if not test_start or not backtest_end:
            raise ValueError(f"data_split 缺少 test_start 或 backtest_end: {data_split}")

        preset_key = strategy_params.get("hmm_signal_preset", "preset_A")
        hmm_config_json = strategy_params.get("hmm_config_json")
        if isinstance(hmm_config_json, str) and hmm_config_json.strip():
            hmm_config_json = json.loads(hmm_config_json)
        if not isinstance(hmm_config_json, dict):
            hmm_config_json = None
        if hmm_config_json is None:
            snapshot_id = strategy_params.get("hmm_model_version_id")
            if snapshot_id and snapshot_id != "from_resolved_model_path":
                try:
                    from ..hmm_training_service import HMMTrainingService
                    hmm_svc = HMMTrainingService()
                    snapshot = hmm_svc.get_snapshot(str(snapshot_id))
                    if snapshot is not None:
                        config_id = snapshot.get("config_id")
                        cfg = None
                        if config_id:
                            get_config = getattr(hmm_svc, "get_config", None)
                            if callable(get_config):
                                cfg = get_config(str(config_id))
                            else:
                                private_get_config = getattr(hmm_svc, "_get_config", None)
                                if callable(private_get_config):
                                    try:
                                        cfg = private_get_config(str(config_id))
                                    except ValueError:
                                        cfg = None
                        if cfg:
                            cj = cfg.get("config_json") or {}
                            if isinstance(cj, str):
                                cj = json.loads(cj)
                            if isinstance(cj, dict):
                                hmm_config_json = cj
                except Exception as exc:
                    logger.warning("HMM config_json 自动解析失败，将仅使用预生成文件: %s", exc)

        # ── 优先读取训练时预生成的系数文件 ──
        coeff_filename = f"coefficients_{preset_key}_{test_start}_{backtest_end}.json"
        strict_no_leakage = False
        if hmm_config_json:
            strict_value = hmm_config_json.get("strict_no_leakage")
            strict_no_leakage = (
                strict_value is True
                or (isinstance(strict_value, str) and strict_value.lower() == "true")
            )
        if strict_no_leakage:
            allowed_windows = hmm_config_json.get("coefficient_windows") or []
            strict_window_ok = any(
                str(window.get("preset")) == str(preset_key)
                and str(window.get("test_start")) == str(test_start)
                and str(window.get("backtest_end")) == str(backtest_end)
                and (
                    window.get("strict_no_leakage") is True
                    or (
                        isinstance(window.get("strict_no_leakage"), str)
                        and window.get("strict_no_leakage").lower() == "true"
                    )
                )
                for window in allowed_windows
                if isinstance(window, dict)
            )
            if not strict_window_ok:
                raise ValueError(
                    "strict_no_leakage HMM 只能用于已登记的无泄漏系数窗口: "
                    f"preset={preset_key}, test_start={test_start}, backtest_end={backtest_end}"
                )

        local_model_path = self._local_hmm_artifact_path(str(model_path))
        local_coeff_text = ""
        if local_model_path is not None:
            try:
                coeff_local_path = ensure_aistock_artifact_path(
                    local_model_path.parent / coeff_filename,
                    purpose="QE HMM coefficient artifact",
                    extra_roots=[AISTOCK_PROJECT_ROOT / "backend" / "data" / "hmm_models"],
                )
                if coeff_local_path.exists() and coeff_local_path.is_file():
                    local_coeff_text = coeff_local_path.read_text(encoding="utf-8").strip()
            except Exception as exc:
                logger.debug("HMM local coefficient lookup skipped: %s", exc)
        if local_coeff_text:
            data = json.loads(local_coeff_text)
            if "daily_coefficients" in data and "stock_sector_map" in data:
                self._validate_hmm_coefficients_json(local_coeff_text)
                logger.info(
                    f"HMM 系数文件本地命中: {coeff_filename} "
                    f"({data.get('sector_count', '?')} 行业, "
                    f"{len(data['daily_coefficients'])} 天)"
                )
                return local_coeff_text

        searched_path = (
            str(local_model_path.parent / coeff_filename)
            if local_model_path is not None
            else f"{model_path}/{coeff_filename}"
        )
        raise RuntimeError(
            "HMM coefficients must be provided as a precomputed AIstock-local artifact "
            "or via _precomputed_hmm_coefficients_json; Windows FastAPI must not invoke "
            "WSL or probe worker paths during QE generation/retry/clone flows. "
            f"missing={searched_path}"
        )

    def _write_custom_strategy(self, exp_dir: Path, strategy_info: Dict) -> None:
        """将自定义策略源码写入实验目录的 custom_strategy.py。
        
        写入前会进行编译验证，确保代码无语法错误。
        并自动添加必要的import语句，移除函数内部的重复import。
        """
        source_code = strategy_info.get("source_code", "")
        if not source_code:
            raise ValueError(
                f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码 (source_code)，"
                f"无法写入策略文件。请检查策略目录。"
            )
        
        # 编译验证：检查语法错误
        validation_result = self._validate_strategy_code(source_code)
        if not validation_result["ok"]:
            raise ValueError(
                f"策略代码编译验证失败:\n{validation_result['error']}\n"
                f"请修复策略代码后再创建实验。"
            )
        
        # 处理相对导入：递归复制依赖文件到实验目录，将相对导入转为本地导入
        import re as _re
        # 只允许从 AIstock 本地代码/资产目录复制策略类文件，避免直接读取
        # RDAgent/WSL worker workspace 或误复制运行时文件。
        _STRATEGY_DEP_WHITELIST = {"score_weighted_strategy", "score_weighted_strategy_v2",
                                   "tail_twap_strategy", "tail_twap_v24_strategy", "tail_twap_v25_strategy", "tail_twap_v25_1_strategy", "qe_board_lot_exchange", "close_execution_strategy", "qe_suspend_filter", "qe_event_risk_policy", "qe_suspend_filter_strategy", "qe_suspend_filter_score_weighted_strategy"}

        def _copy_deps_recursive(code: str, copied: set) -> str:
            """递归处理相对导入：转换为本地导入并复制依赖文件."""
            out_lines = []
            for ln in code.split("\n"):
                s = ln.strip()
                # 匹配 from .module import ... (相对导入)
                m = _re.match(r'^(\s*)from\s+\.(\w+)\s+import\s+(.+)$', s)
                if m:
                    indent, mod, imps = m.group(1), m.group(2), m.group(3)
                    dep_code = self._resolve_strategy_dependency_code(mod, _STRATEGY_DEP_WHITELIST)
                    if dep_code is None and mod in _STRATEGY_DEP_WHITELIST:
                        raise ValueError(f"策略依赖文件缺失，无法写入 QE 实验目录: {mod}.py")
                    if dep_code is not None and mod not in copied:
                        copied.add(mod)
                        dep_code = _copy_deps_recursive(dep_code, copied)
                        (exp_dir / f"{mod}.py").write_text(dep_code, encoding="utf-8")
                        logger.info(f"复制策略依赖文件: {mod}.py")
                    out_lines.append(f"{indent}from {mod} import {imps}")
                    continue
                # 匹配 from module import ... (无点号，检查是否为本地策略包模块)
                m2 = _re.match(r'^(\s*)from\s+(\w+)\s+import\s+(.+)$', s)
                if m2:
                    indent, mod, imps = m2.group(1), m2.group(2), m2.group(3)
                    dep_code = self._resolve_strategy_dependency_code(mod, _STRATEGY_DEP_WHITELIST)
                    if dep_code is None and mod in _STRATEGY_DEP_WHITELIST:
                        raise ValueError(f"策略依赖文件缺失，无法写入 QE 实验目录: {mod}.py")
                    if dep_code is not None and mod not in copied:
                        copied.add(mod)
                        dep_code = _copy_deps_recursive(dep_code, copied)
                        (exp_dir / f"{mod}.py").write_text(dep_code, encoding="utf-8")
                        logger.info(f"复制策略依赖文件: {mod}.py")
                    out_lines.append(ln)
                    continue
                if s == "@register":
                    continue
                out_lines.append(ln)
            return "\n".join(out_lines)

        copied_deps: set = set()
        source_code = _copy_deps_recursive(source_code, copied_deps)
        if copied_deps:
            logger.info(f"共复制 {len(copied_deps)} 个策略依赖文件: {copied_deps}")
        
        # 自动添加必要的import语句（如果源码中没有）
        required_imports = [
            "import pandas as pd",
            "import numpy as np",
        ]
        
        # 检查并添加缺失的import
        import_lines = []
        for imp in required_imports:
            # 检查是否已存在该import（检查模块级import）
            if imp not in source_code:
                import_lines.append(imp)
        
        # 如果需要添加import，在文件开头插入
        if import_lines:
            # 找到现有import语句后的位置
            lines = source_code.split("\n")
            insert_pos = 0
            
            # 跳过文件开头的注释和docstring
            in_docstring = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                
                # 跳过空行
                if not stripped:
                    insert_pos = i + 1
                    continue
                
                # 跳过注释
                if stripped.startswith("#"):
                    insert_pos = i + 1
                    continue
                
                # 跳过docstring
                if '"""' in stripped or "'''" in stripped:
                    if in_docstring:
                        in_docstring = False
                        insert_pos = i + 1
                    else:
                        in_docstring = True
                        insert_pos = i + 1
                    continue
                
                if in_docstring:
                    insert_pos = i + 1
                    continue
                
                # 遇到import语句，继续跳过
                if stripped.startswith("import ") or stripped.startswith("from "):
                    insert_pos = i + 1
                    continue
                
                # 遇到其他代码，停止
                break
            
            # 在合适位置插入import
            for imp in import_lines:
                lines.insert(insert_pos, imp)
                insert_pos += 1
            
            source_code = "\n".join(lines)
            logger.info(f"自动添加了缺失的import: {import_lines}")
        
        # 移除函数内部的重复import（避免UnboundLocalError）
        # 检测函数/方法内部的import语句并移除
        import re
        lines = source_code.split("\n")
        cleaned_lines = []
        in_function = False
        indent_level = 0
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # 检测函数/方法定义开始
            if re.match(r'^(def |async def |class )', stripped):
                in_function = True
                indent_level = len(line) - len(line.lstrip())
            # 检测缩进回到函数外部
            elif in_function and line and not line[0].isspace():
                in_function = False
            elif in_function and line:
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= indent_level and stripped and not stripped.startswith('#'):
                    in_function = False
            
            # 如果在函数内部且是import语句，跳过（移除）
            if in_function and (stripped.startswith("import pandas") or 
                                stripped.startswith("import numpy") or
                                stripped.startswith("import pandas as pd") or
                                stripped.startswith("import numpy as np")):
                logger.info(f"移除函数内部的重复import: 第{i+1}行 '{stripped}'")
                continue
            
            cleaned_lines.append(line)
        
        source_code = "\n".join(cleaned_lines)
        
        (exp_dir / "custom_strategy.py").write_text(source_code, encoding="utf-8")
        logger.info(f"写入自定义策略文件: {exp_dir / 'custom_strategy.py'}")
    
    def _validate_strategy_code(self, source_code: str) -> Dict[str, Any]:
        """验证策略代码是否能正确编译。
        
        检查内容：
        1. Python语法是否正确
        2. 是否能正确导入qlib相关模块
        3. 策略类是否继承自正确的基类
        4. __init__方法参数是否能正确处理
        """
        import ast
        
        result = {"ok": True, "error": None, "warnings": []}
        
        # 1. 语法检查
        try:
            ast.parse(source_code)
        except SyntaxError as e:
            result["ok"] = False
            result["error"] = f"语法错误 (行 {e.lineno}): {e.msg}\n{e.text}"
            return result
        
        # 2. 检查类定义
        try:
            tree = ast.parse(source_code)
            class_defs = [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
            
            if not class_defs:
                result["ok"] = False
                result["error"] = "未找到策略类定义（class关键字）"
                return result
            
            # 检查是否继承自BaseSignalStrategy或其子类
            valid_base_classes = {"BaseSignalStrategy", "BaseStrategy", "TopkDropoutStrategy",
                                   "ScoreWeightedTopkStrategy"}
            found_valid_class = False
            
            for class_def in class_defs:
                for base in class_def.bases:
                    base_name = None
                    if isinstance(base, ast.Name):
                        base_name = base.id
                    elif isinstance(base, ast.Attribute):
                        base_name = base.attr
                    
                    if base_name in valid_base_classes:
                        found_valid_class = True
                        break
                
                if found_valid_class:
                    break
            
            if not found_valid_class:
                result["warnings"].append(
                    f"策略类应继承自 BaseSignalStrategy 或 TopkDropoutStrategy，"
                    f"当前继承自: {[ast.unparse(b) for b in class_defs[0].bases]}"
                )
            
            # 3. 检查__init__方法是否正确处理**kwargs
            for class_def in class_defs:
                for item in class_def.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                        # 检查是否有**kwargs参数
                        has_kwargs = any(
                            arg.arg == "kwargs" and arg.arg == "**" 
                            for arg in item.args.kwonlyargs or []
                        )
                        # 更准确的方式检查kwargs
                        if item.args.kwarg:
                            has_kwargs = True
                        
                        if not has_kwargs:
                            result["warnings"].append(
                                f"策略类 {class_def.name} 的 __init__ 方法没有 **kwargs 参数，"
                                f"可能导致无法接收Qlib传递的额外参数。建议添加 **kwargs 参数。"
                            )
                        break
            
        except Exception as e:
            result["warnings"].append(f"代码结构分析警告: {str(e)}")
        
        # 4. 尝试在隔离环境中编译（不执行）
        try:
            compile(source_code, "<strategy>", "exec")
        except Exception as e:
            result["ok"] = False
            result["error"] = f"编译错误: {str(e)}"
            return result
        
        if result["warnings"]:
            logger.warning(f"策略代码验证警告: {result['warnings']}")

        # 5. 检查 HMM 调用顺序：_apply_hmm_adjustment 必须在 None 检查之后
        if "_apply_hmm_adjustment" in source_code:
            lines = source_code.split("\n")
            hmm_line = next((i for i, line in enumerate(lines) if "_apply_hmm_adjustment" in line), None)
            none_check_line = next((i for i, line in enumerate(lines)
                                    if ("is None" in line or "if not" in line) and
                                    ("pred_score" in line or "all_pred_scores" in line or "scores" in line)
                                    and i < (hmm_line or 9999)), None)
            if hmm_line is not None and none_check_line is None:
                result["warnings"].append(
                    f"[HMM顺序警告] _apply_hmm_adjustment (行{hmm_line+1}) 在 None 检查之前调用，"
                    f"当 get_signal 返回 None 时会触发 AttributeError。"
                    f"请确保 None 检查在 HMM 调用之前。"
                )

        return result

    def _copy_qe_custom_loaders(self, exp_dir: Path) -> None:
        """复制QE独立的custom_loaders.py到实验目录。
        
        这样实验目录中有独立的loader文件，不会影响RDAgent的原有custom_loaders.py。
        """
        import shutil
        from pathlib import Path
        
        # QE独立的loader源文件
        qe_loader_source = Path(__file__).parent / "qe_custom_loaders.py"
        
        if not qe_loader_source.exists():
            logger.warning(f"QE custom loaders源文件不存在: {qe_loader_source}")
            return
        
        # 复制到实验目录，命名为qe_custom_loaders.py
        qe_loader_dest = exp_dir / "qe_custom_loaders.py"
        shutil.copy2(qe_loader_source, qe_loader_dest)
        logger.info(f"复制QE独立loader: {qe_loader_dest}")

    def _get_read_exp_res_content(self) -> str:
        """从 qe_programs/templates/ 读取 read_exp_res.py 模板内容。

        模板文件维护在 QE 专用程序目录中，不再内嵌于本文件。
        这样 Phase 3 增强的诊断提取功能可以直接生效。
        """
        external_template_path = ensure_aistock_artifact_path(
            QE_PROGRAMS_WIN / "templates" / "read_exp_res.py",
            purpose="QE read_exp_res.py local template",
            extra_roots=[QE_PROGRAMS_WIN],
        )
        candidate_paths = [
            BUNDLED_QE_TEMPLATE_ROOT / "read_exp_res.py",
            external_template_path,
        ]
        template_path = next((path for path in candidate_paths if path.exists()), candidate_paths[0])
        try:
            return template_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.error(f"QE read_exp_res.py 模板文件不存在: {template_path}")
            raise FileNotFoundError(
                f"QE 模板文件缺失: {template_path}，"
                f"请确认 qe_programs/templates/read_exp_res.py 已部署"
            )

    def _copy_read_exp_res(self, exp_dir: Path) -> None:
        """复制read_exp_res.py模板到实验目录。"""
        template_content = self._get_read_exp_res_content()
        (exp_dir / "read_exp_res.py").write_text(template_content, encoding="utf-8")

    # NOTE: _READ_EXP_RES_TEMPLATE removed, now read from qe_programs/templates/read_exp_res.py


    def _save_experiment_record(self, experiment_id: str, experiment_name: str,
                                exp_dir: str, factor_names: List[str],
                                model_id: Optional[str], strategy_id: Optional[str],
                                data_split: Dict, custom_params: Optional[Dict],
                                evolution_goal: Optional[str] = None,
                                llm_hypothesis: Optional[Dict] = None) -> None:
        """保存实验记录到数据库。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                        (experiment_id, experiment_name, status,
                         factor_names, model_id, strategy_id,
                         data_split, custom_params, workspace_path,
                         evolution_goal, llm_hypothesis, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (experiment_id) DO UPDATE SET
                        experiment_name = EXCLUDED.experiment_name,
                        factor_names = EXCLUDED.factor_names,
                        model_id = EXCLUDED.model_id,
                        strategy_id = EXCLUDED.strategy_id,
                        data_split = EXCLUDED.data_split,
                        custom_params = EXCLUDED.custom_params,
                        evolution_goal = EXCLUDED.evolution_goal,
                        llm_hypothesis = EXCLUDED.llm_hypothesis
                """, (
                    experiment_id, experiment_name, "created",
                    json.dumps(factor_names),
                    model_id, strategy_id,
                    json.dumps(data_split),
                    json.dumps(custom_params) if custom_params else None,
                    exp_dir,
                    evolution_goal,
                    json.dumps(llm_hypothesis) if llm_hypothesis else None,
                ))

    def _get_experiment_record(self, experiment_id: str) -> Optional[Dict]:
        """获取实验记录。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                record = dict(zip(cols, row))
                # 兼容字段名映射
                if "workspace_path" in record:
                    record["experiment_dir"] = record["workspace_path"]
                return record

    def _extract_metrics_from_results(self, exp_dir: Path) -> Dict[str, Any]:
        """从实验结果目录提取指标。优先读取qlib_results.json（包含完整交易统计）。"""
        metrics = {}

        # 优先读取qlib_results.json（包含summary + daily_win_stats + stock_trade_stats）
        json_path = exp_dir / "qlib_results.json"
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    full_results = json.load(f)
                # 合并所有指标到一个扁平dict
                if "summary" in full_results:
                    metrics.update(full_results["summary"])
                if "daily_win_stats" in full_results:
                    metrics.update(full_results["daily_win_stats"])
                if "stock_trade_stats" in full_results:
                    metrics.update(full_results["stock_trade_stats"])
                if "calmar_ratio" in full_results and full_results["calmar_ratio"] is not None:
                    metrics["calmar_ratio"] = full_results["calmar_ratio"]
                # 保留完整原始数据
                metrics["_raw_json"] = full_results
                logger.info(f"从qlib_results.json读取到 {len(metrics)} 个指标")
                return metrics
            except Exception as e:
                raise RuntimeError(f"读取qlib_results.json失败: {e}") from e

        # 回退：读取qlib_res.csv
        csv_path = exp_dir / "qlib_res.csv"
        if csv_path.exists():
            try:
                import csv
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        metrics.update(row)
                        break  # 只取第一行
            except Exception as e:
                raise RuntimeError(f"读取qlib_res.csv失败: {e}") from e

        return metrics

    def _update_experiment_metrics(self, experiment_id: str, metrics: Dict) -> None:
        """更新实验指标。"""
        # 保存到qe_experiments表（排除_raw_json避免过大）
        save_metrics = {k: v for k, v in metrics.items() if k != "_raw_json"}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET result_metrics = %s, status = 'completed', completed_at = NOW(), updated_at = NOW()
                    WHERE experiment_id = %s
                """, (json.dumps(save_metrics, default=str), experiment_id))

    def _save_factor_experiment_metrics(self, experiment_id: str, metrics: Dict) -> None:
        """为实验中的每个因子保存实验表现指标到qe_factor_experiment_metrics表。"""
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            logger.warning(f"保存因子实验指标失败：实验 {experiment_id} 不存在")
            return

        factor_names = exp_record.get("factor_names") or []
        if isinstance(factor_names, str):
            try:
                factor_names = json.loads(factor_names)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("factor_names JSON 解析失败，作为单字符串处理: %s (error: %s)", factor_names[:80], e)
                factor_names = [factor_names]

        if not factor_names:
            logger.info(f"实验 {experiment_id} 没有关联因子，跳过因子指标保存")
            return

        experiment_name = exp_record.get("experiment_name", "")
        model_id = exp_record.get("model_id")
        data_split = exp_record.get("data_split")

        # 提取各类指标
        def _float(val):
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def _int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # 原始完整指标（备份）
        raw_metrics = metrics.get("_raw_json") or {k: v for k, v in metrics.items() if k != "_raw_json"}

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 解析 model_catalog_id（可选）
                model_catalog_id = None
                qe_task_id = exp_record.get("qe_task_id")
                if qe_task_id:
                    cur.execute("""
                        SELECT mc.id FROM qe_evolution_tasks et
                        JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
                        WHERE et.task_id = %s LIMIT 1
                    """, (qe_task_id,))
                    mc_row = cur.fetchone()
                    if mc_row:
                        model_catalog_id = mc_row[0]

                for factor_name in factor_names:
                    # 确定因子来源和 catalog_id
                    factor_source, factor_catalog_id = self._detect_factor_source(cur, factor_name)
                    if factor_catalog_id is None:
                        logger.warning(f"因子 {factor_name} 未在 catalog 中找到，跳过指标保存")
                        continue
                    other_factors = [f for f in factor_names if f != factor_name]

                    try:
                        cur.execute("""
                            INSERT INTO qe_factor_experiment_metrics (
                                factor_name, factor_source, experiment_id, experiment_name,
                                ic, icir, rank_ic, rank_icir,
                                ann_return_no_cost, info_ratio_no_cost, max_drawdown_no_cost,
                                ann_return_with_cost, info_ratio_with_cost, max_drawdown_with_cost,
                                daily_win_rate, weekly_win_rate, max_consecutive_win, max_consecutive_loss,
                                total_trades, winning_trades, losing_trades, stock_win_rate,
                                avg_profit_pct, avg_loss_pct, profit_loss_ratio,
                                max_single_profit_pct, max_single_loss_pct,
                                sharpe_ratio, calmar_ratio, avg_turnover, total_trading_days,
                                model_id, other_factors, data_split, raw_metrics,
                                factor_catalog_id, model_catalog_id
                            ) VALUES (
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s
                            )
                            ON CONFLICT (factor_name, factor_source, experiment_id)
                            DO UPDATE SET
                                experiment_name = EXCLUDED.experiment_name,
                                ic = EXCLUDED.ic, icir = EXCLUDED.icir,
                                rank_ic = EXCLUDED.rank_ic, rank_icir = EXCLUDED.rank_icir,
                                ann_return_no_cost = EXCLUDED.ann_return_no_cost,
                                info_ratio_no_cost = EXCLUDED.info_ratio_no_cost,
                                max_drawdown_no_cost = EXCLUDED.max_drawdown_no_cost,
                                ann_return_with_cost = EXCLUDED.ann_return_with_cost,
                                info_ratio_with_cost = EXCLUDED.info_ratio_with_cost,
                                max_drawdown_with_cost = EXCLUDED.max_drawdown_with_cost,
                                daily_win_rate = EXCLUDED.daily_win_rate,
                                weekly_win_rate = EXCLUDED.weekly_win_rate,
                                max_consecutive_win = EXCLUDED.max_consecutive_win,
                                max_consecutive_loss = EXCLUDED.max_consecutive_loss,
                                total_trades = EXCLUDED.total_trades,
                                winning_trades = EXCLUDED.winning_trades,
                                losing_trades = EXCLUDED.losing_trades,
                                stock_win_rate = EXCLUDED.stock_win_rate,
                                avg_profit_pct = EXCLUDED.avg_profit_pct,
                                avg_loss_pct = EXCLUDED.avg_loss_pct,
                                profit_loss_ratio = EXCLUDED.profit_loss_ratio,
                                max_single_profit_pct = EXCLUDED.max_single_profit_pct,
                                max_single_loss_pct = EXCLUDED.max_single_loss_pct,
                                sharpe_ratio = EXCLUDED.sharpe_ratio,
                                calmar_ratio = EXCLUDED.calmar_ratio,
                                avg_turnover = EXCLUDED.avg_turnover,
                                total_trading_days = EXCLUDED.total_trading_days,
                                model_id = EXCLUDED.model_id,
                                other_factors = EXCLUDED.other_factors,
                                data_split = EXCLUDED.data_split,
                                raw_metrics = EXCLUDED.raw_metrics,
                                factor_catalog_id = EXCLUDED.factor_catalog_id,
                                model_catalog_id = EXCLUDED.model_catalog_id,
                                collected_at = NOW()
                        """, (
                            factor_name, factor_source, experiment_id, experiment_name,
                            _float(metrics.get("IC")),
                            _float(metrics.get("ICIR")),
                            _float(metrics.get("Rank_IC")),
                            _float(metrics.get("Rank_ICIR")),
                            _float(metrics.get("excess_return_without_cost_annualized")),
                            _float(metrics.get("excess_return_without_cost_IR")),
                            _float(metrics.get("excess_return_without_cost_max_drawdown")),
                            _float(metrics.get("excess_return_with_cost_annualized")),
                            _float(metrics.get("excess_return_with_cost_IR")),
                            _float(metrics.get("excess_return_with_cost_max_drawdown")),
                            _float(metrics.get("daily_win_rate")),
                            _float(metrics.get("weekly_win_rate")),
                            _int(metrics.get("max_consecutive_win")),
                            _int(metrics.get("max_consecutive_loss")),
                            _int(metrics.get("total_trades")),
                            _int(metrics.get("winning_trades")),
                            _int(metrics.get("losing_trades")),
                            _float(metrics.get("stock_win_rate")),
                            _float(metrics.get("avg_profit_pct")),
                            _float(metrics.get("avg_loss_pct")),
                            _float(metrics.get("profit_loss_ratio")),
                            _float(metrics.get("max_single_profit_pct")),
                            _float(metrics.get("max_single_loss_pct")),
                            _float(metrics.get("sharpe_ratio")),
                            _float(metrics.get("calmar_ratio")),
                            _float(metrics.get("avg_turnover")),
                            _int(metrics.get("total_trading_days")),
                            model_id,
                            json.dumps(other_factors) if other_factors else None,
                            json.dumps(data_split, default=str) if data_split else None,
                            json.dumps(raw_metrics, default=str),
                            factor_catalog_id,
                            model_catalog_id,
                        ))
                        logger.info(f"保存因子实验指标: {factor_name} @ {experiment_id}")
                    except Exception as e:
                        raise RuntimeError(f"保存因子 {factor_name} 实验指标失败: {e}") from e

    def _detect_factor_source(self, cur, factor_name: str) -> tuple:
        """检测因子来源并返回 catalog_id。Returns (source, catalog_id)."""
        try:
            cur.execute("""
                SELECT source, id FROM aistock_factor_catalog
                WHERE factor_name = %s
                ORDER BY CASE WHEN source = 'rdagent_task_sync' THEN 0 ELSE 1 END, id
                LIMIT 1
            """, (factor_name,))
            row = cur.fetchone()
            if row:
                return row[0], row[1]
            return (None, None)
        except Exception as e:
            raise RuntimeError(f"[QE] 因子来源检测失败: factor={factor_name}, {e}") from e

