"""
QE Unified Engine — ExperimentConfig data model.

Single source of truth for all parameters passed to compose_experiment_in_memory().
Replaces the ad-hoc loop_custom_params dicts scattered across the four call paths.
"""
from __future__ import annotations

import json
from typing import Any, Mapping

from pydantic import BaseModel, model_validator

ALLOWED_LABEL_HORIZONS = {1, 3, 5, 10, 20}
DEFAULT_LABEL_HORIZON = 1

QE_DEFAULT_RISK_POLICY: dict[str, Any] = {
    "enabled": True,
    "policy_version": "stock_event_risk_policy_v1",
    "providers": ["st_pit"],
    "st_universe_key": "shsz_st_pit_active_v1",
    "hard_actions": ["block_buy", "force_exit"],
    "visible_time_mode": "next_trading_session",
    "strict_data_ready": True,
    "score_overlay": {
        "enabled": False,
        "negative_multiplier_floor": 0.7,
        "positive_multiplier_cap": 1.1,
    },
}

_QE_RISK_POLICY_RUNTIME_KEYS = {
    "risk_policy_enabled",
    "risk_policy_file",
    "risk_policy_strict",
    "quote_universe_codes",
}

QE_RUNTIME_METADATA_KEYS = frozenset(
    {
        "archive_policy",
        "archive_reason",
        "archive_allow_override",
        "random_seed",
        "seed",
        "loop_seed",
        "random_state",
        "torch_seed",
        "numpy_seed",
    }
)


def split_qe_runtime_metadata(params: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split runtime-only metadata from executable strategy/model params."""

    clean: dict[str, Any] = {}
    metadata: dict[str, Any] = {}
    for key, value in dict(params or {}).items():
        if key in QE_RUNTIME_METADATA_KEYS:
            metadata[key] = value
        else:
            clean[key] = value
    return clean, metadata


def strip_qe_runtime_metadata(params: dict[str, Any] | None) -> dict[str, Any]:
    """Return params without metadata that must not reach Qlib strategies."""

    clean, _ = split_qe_runtime_metadata(params)
    return clean


def normalize_qe_random_seed(value: Any, *, field_name: str = "random_seed") -> int:
    """Return a fixed integer seed or fail fast for non-reproducible loops."""

    if isinstance(value, bool) or value in (None, ""):
        raise ValueError(f"{field_name} is required and must be an integer fixed seed")
    try:
        seed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer fixed seed, got {value!r}") from exc
    if seed < 0 or seed > 2**32 - 1:
        raise ValueError(f"{field_name} must be between 0 and 4294967295, got {seed}")
    return seed


def extract_qe_random_seed(*sources: Mapping[str, Any] | None) -> int | None:
    """Find random_seed/seed/loop_seed from explicit runtime sources."""

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in ("random_seed", "seed", "loop_seed", "random_state", "torch_seed", "numpy_seed"):
            if key in source and source.get(key) not in (None, ""):
                return normalize_qe_random_seed(source.get(key), field_name=key)
    return None


def require_qe_random_seed(*sources: Mapping[str, Any] | None, context: str = "qe_loop") -> int:
    seed = extract_qe_random_seed(*sources)
    if seed is None:
        raise ValueError(f"{context}: runtime_flags.random_seed is required for trainable QE loops")
    return seed


def model_seed_param_keys(model_class: str | None) -> tuple[str, ...]:
    """Return constructor-safe seed kwargs for a concrete Qlib model class."""

    normalized = str(model_class or "").strip()
    if normalized in {"LGBModel", "AIStockXGBModel", "XGBModel"}:
        return ("seed", "random_state")
    if normalized == "CatBoostModel":
        return ("random_seed",)
    if normalized in {"TabPFNModel", "LambdaRankModel"}:
        return ("random_state",)
    return ()


def apply_qe_seed_to_model_params(
    params: dict[str, Any] | None,
    seed: int | None,
    *,
    model_class: str | None = None,
) -> dict[str, Any]:
    """Inject only model-constructor-supported seed kwargs."""

    seeded = dict(params or {})
    if seed is None:
        return seeded
    fixed_seed = normalize_qe_random_seed(seed)
    allowed_keys = set(model_seed_param_keys(model_class))
    for key in ("random_seed", "seed", "loop_seed", "random_state", "torch_seed", "numpy_seed"):
        if key not in allowed_keys:
            seeded.pop(key, None)
    for key in allowed_keys:
        seeded[key] = fixed_seed
    return seeded


def default_qe_risk_policy() -> dict[str, Any]:
    """Return the mandatory ST PIT event-risk policy for new QE runs."""

    return json.loads(json.dumps(QE_DEFAULT_RISK_POLICY, ensure_ascii=False))


def ensure_qe_risk_policy(custom_params: dict[str, Any] | None, *, source: str = "qe") -> dict[str, Any]:
    """Inject and validate the mandatory QE ST PIT risk policy.

    This is called only while constructing a new runnable QE config.  It does
    not migrate completed historical experiment rows.
    """

    params: dict[str, Any] = dict(custom_params or {})
    for runtime_key in _QE_RISK_POLICY_RUNTIME_KEYS:
        params.pop(runtime_key, None)

    raw_policy = params.get("risk_policy")
    if raw_policy in (None, "", False):
        params["risk_policy"] = default_qe_risk_policy()
        return params

    if isinstance(raw_policy, str):
        try:
            raw_policy = json.loads(raw_policy)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{source}: risk_policy must be a JSON object string") from exc
    if not isinstance(raw_policy, dict):
        raise ValueError(f"{source}: risk_policy must be an object")

    policy = dict(raw_policy)
    if policy.get("enabled") is False:
        raise ValueError(f"{source}: risk_policy.enabled=false is not allowed for new QE runs")
    policy["enabled"] = True

    if "policy_version" not in policy:
        policy["policy_version"] = QE_DEFAULT_RISK_POLICY["policy_version"]
    if "st_universe_key" not in policy:
        policy["st_universe_key"] = QE_DEFAULT_RISK_POLICY["st_universe_key"]
    if "visible_time_mode" not in policy:
        policy["visible_time_mode"] = QE_DEFAULT_RISK_POLICY["visible_time_mode"]
    if "strict_data_ready" not in policy:
        policy["strict_data_ready"] = QE_DEFAULT_RISK_POLICY["strict_data_ready"]
    if "score_overlay" not in policy:
        policy["score_overlay"] = default_qe_risk_policy()["score_overlay"]

    providers = policy.get("providers")
    if providers in (None, ""):
        providers = list(QE_DEFAULT_RISK_POLICY["providers"])
    if not isinstance(providers, list):
        raise ValueError(f"{source}: risk_policy.providers must be a list")
    providers = [str(item).strip() for item in providers if str(item or "").strip()]
    if "st_pit" not in providers:
        raise ValueError(f"{source}: risk_policy.providers must include st_pit")
    policy["providers"] = providers

    hard_actions = policy.get("hard_actions")
    if hard_actions in (None, ""):
        hard_actions = list(QE_DEFAULT_RISK_POLICY["hard_actions"])
    if not isinstance(hard_actions, list):
        raise ValueError(f"{source}: risk_policy.hard_actions must be a list")
    hard_actions = [str(item).strip() for item in hard_actions if str(item or "").strip()]
    required_actions = {"block_buy", "force_exit"}
    if not required_actions.issubset(set(hard_actions)):
        raise ValueError(
            f"{source}: risk_policy.hard_actions must include {sorted(required_actions)}"
        )
    policy["hard_actions"] = hard_actions

    params["risk_policy"] = policy
    return params


def normalize_label_horizon(value: Any, *, field_name: str = "label_horizon") -> int:
    """Return a validated label horizon.

    Missing values are the legacy 1d mode. Explicit invalid values fail fast;
    this prevents accidental fallback to 1d for non-legacy requests.
    """
    if value is None or value == "":
        return DEFAULT_LABEL_HORIZON
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer in {sorted(ALLOWED_LABEL_HORIZONS)}")
    if value not in ALLOWED_LABEL_HORIZONS:
        raise ValueError(f"{field_name}={value!r} invalid, must be one of {sorted(ALLOWED_LABEL_HORIZONS)}")
    return value


def extract_label_horizon(raw: Any, *, field_name: str = "label_horizon") -> int:
    """Extract label_horizon from a dict-like object or raw value."""
    if isinstance(raw, dict):
        return normalize_label_horizon(raw.get("label_horizon"), field_name=field_name)
    return normalize_label_horizon(raw, field_name=field_name)


class HmmConfig(BaseModel):
    """HMM sector filter configuration.

    sector_hmm_model_path must be pre-resolved from hmm_model_version_id
    by the builder before constructing this object.
    """

    enable_sector_hmm: bool = False
    hmm_model_version_id: str | None = None
    sector_hmm_model_path: str | None = None
    hmm_signal_preset: str | None = None
    # Path 1 only: per-sector preset overrides loaded from DB config_json
    hmm_signal_presets: dict[str, Any] | None = None
    # Full HMM training config; dynamic HMM coefficient generation requires it.
    hmm_config_json: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_hmm_consistency(self) -> "HmmConfig":
        if self.enable_sector_hmm:
            if not self.hmm_model_version_id:
                raise ValueError(
                    "enable_sector_hmm=True requires hmm_model_version_id"
                )
            if not self.sector_hmm_model_path:
                raise ValueError(
                    "enable_sector_hmm=True requires sector_hmm_model_path "
                    "(resolve via HMMTrainingService before constructing HmmConfig)"
                )
        return self


class ExperimentConfig(BaseModel):
    """Unified experiment configuration for all QE call paths.

    Constructed by one of the four builder functions in experiment_config_builders.py.
    Call build_custom_params() to produce the loop_custom_params dict expected by
    compose_experiment_in_memory(), and use the top-level fields for the remaining
    keyword arguments.
    """

    # ── Core identity ──────────────────────────────────────────────────────────
    factor_names: list[str]
    model_id: str
    strategy_id: str | None = None

    # ── Training / backtest window ─────────────────────────────────────────────
    data_split: dict[str, str] | None = None

    # ── Stock universe ─────────────────────────────────────────────────────────
    label_type: str | None = None
    label_horizon: int | None = None
    stock_pool: str | None = None
    sector_blacklist: list[str] | None = None

    # ── HMM sector filter ─────────────────────────────────────────────────────
    hmm: HmmConfig | None = None

    # ── Execution ─────────────────────────────────────────────────────────────
    execution_algo: str | None = None
    execution_algo_params: dict[str, Any] | None = None
    filter_suspended_on_signal: bool = False
    suspend_filter_strict: bool = True

    # ── Unfilled order handling ────────────────────────────────────────────────
    # Raw params dict: {"trigger_minute": int, "backup_depth": int}
    unfilled_handler: str | None = None
    unfilled_handler_params: dict[str, Any] | None = None

    # ── Strategy parameters ────────────────────────────────────────────────────
    # Passed as strategy_params= to compose_experiment_in_memory.
    # Also merged into custom_params (minus initial_cash).
    strategy_params: dict[str, Any] | None = None

    # Runtime-only archive/seed/provenance metadata. Never pass this to Qlib
    # strategy constructors.
    runtime_flags: dict[str, Any] | None = None

    # ── Model hyperparameters base ─────────────────────────────────────────────
    # For paths that start from model_params (Paths 2 & 3).
    # Merged into custom_params before strategy_params overlay.
    model_params_base: dict[str, Any] | None = None

    # ── Escape hatch ───────────────────────────────────────────────────────────
    extra_params: dict[str, Any] | None = None

    # ── Backtest-only mode (reuse trained model, skip training) ──────────────
    # Requires model_source_task_id + model_source_loop_index pointing to a
    # previously trained loop.  Factor list MUST match the source loop.
    backtest_only: bool = False
    model_source_task_id: str | None = None
    model_source_loop_index: int | None = None

    # ── Metadata (not passed to compose) ──────────────────────────────────────
    node_id: str | None = None
    experiment_name: str | None = None

    # ── Multi-Alpha (Phase 3) ─────────────────────────────────────────────────
    # alpha_mode="single" → 现有路径零影响；"multi" → 启用多 Alpha 引擎。
    alpha_mode: str = "single"
    multi_alpha_config: "MultiAlphaConfig | None" = None

    @model_validator(mode="after")
    def _validate_factor_names(self) -> "ExperimentConfig":
        if self.alpha_mode == "single" and not self.factor_names:
            raise ValueError("factor_names cannot be empty in single-alpha mode")
        if self.alpha_mode == "multi" and not self.multi_alpha_config:
            raise ValueError("multi_alpha_config required when alpha_mode='multi'")
        self.label_horizon = normalize_label_horizon(self.label_horizon)
        return self

    def build_custom_params(self) -> dict[str, Any]:
        """Produce the loop_custom_params dict for compose_experiment_in_memory().

        Mirrors the construction logic of submit_custom_evo_loop (Path 4),
        which is the most complete reference path.

        Order of precedence (later entries win):
          model_params_base -> strategy_params -> HMM keys -> sector_blacklist
          -> stock_pool -> label_type -> unfilled_handler keys -> extra_params
          then initial_cash is popped.
        """
        params: dict[str, Any] = {}

        # 1. Base model hyperparameters (Paths 2 & 3 start here)
        if self.model_params_base:
            params.update(self.model_params_base)

        # 2. Strategy params overlay (Path 4 starts here; Paths 2 & 3 merge on top)
        if self.strategy_params:
            params.update(strip_qe_runtime_metadata(self.strategy_params))

        # 3. HMM keys
        if self.hmm and self.hmm.enable_sector_hmm:
            params["enable_sector_hmm"] = True
            params["hmm_model_version_id"] = self.hmm.hmm_model_version_id
            params["sector_hmm_model_path"] = self.hmm.sector_hmm_model_path
            if self.hmm.hmm_signal_preset:
                params["hmm_signal_preset"] = self.hmm.hmm_signal_preset
            if self.hmm.hmm_signal_presets:
                params["hmm_signal_presets"] = self.hmm.hmm_signal_presets
            if self.hmm.hmm_config_json:
                params["hmm_config_json"] = self.hmm.hmm_config_json

        # 4. Sector blacklist
        if self.sector_blacklist:
            params["sector_blacklist"] = self.sector_blacklist

        # 5. Stock pool
        if self.stock_pool:
            params["stock_pool"] = self.stock_pool

        # 6. Label type
        if self.label_type:
            params["label_type"] = self.label_type

        # 7. Label horizon; omit legacy 1d to preserve old custom_params shape.
        effective_label_horizon = normalize_label_horizon(self.label_horizon)
        if effective_label_horizon != DEFAULT_LABEL_HORIZON:
            params["label_horizon"] = effective_label_horizon

        # 8. suspend_d signal filter
        if self.filter_suspended_on_signal:
            params["filter_suspended_on_signal"] = True
            params["suspend_filter_strict"] = self.suspend_filter_strict

        # 9. Unfilled handler - flatten params dict into top-level keys
        if self.unfilled_handler:
            params["unfilled_handler"] = self.unfilled_handler
            uf_params = self.unfilled_handler_params or {}
            if uf_params.get("trigger_minute"):
                params["unfilled_trigger_minute"] = uf_params["trigger_minute"]
            if uf_params.get("backup_depth"):
                params["unfilled_backup_depth"] = uf_params["backup_depth"]

        # 10. Extra catch-all params
        if self.extra_params:
            extra_params = strip_qe_runtime_metadata(self.extra_params)
            if "label_horizon" in extra_params:
                extra_horizon = normalize_label_horizon(
                    extra_params["label_horizon"],
                    field_name="extra_params.label_horizon",
                )
                if extra_horizon != effective_label_horizon:
                    raise ValueError(
                        "extra_params.label_horizon conflicts with ExperimentConfig.label_horizon"
                    )
                # Keep label_horizon controlled by the unified field above.
                extra_params.pop("label_horizon", None)
            params.update(extra_params)

        # 11. initial_cash must NOT flow into custom_params
        params.pop("initial_cash", None)
        for runtime_key in QE_RUNTIME_METADATA_KEYS:
            params.pop(runtime_key, None)

        return ensure_qe_risk_policy(params, source="ExperimentConfig.build_custom_params")

    def build_runtime_flags(self) -> dict[str, Any]:
        """Return archive/seed metadata separated from executable parameters."""

        flags = dict(self.runtime_flags or {})
        for source in (self.model_params_base, self.strategy_params, self.extra_params):
            _, metadata = split_qe_runtime_metadata(source)
            for key, value in metadata.items():
                flags.setdefault(key, value)
        return flags

    def build_strategy_params(self) -> dict[str, Any]:
        """Return the strategy_params dict for compose_experiment_in_memory().

        This is a copy of strategy_params (so the caller's dict is not mutated).
        initial_cash is intentionally kept here — it belongs in strategy_params,
        not in custom_params.
        """
        return strip_qe_runtime_metadata(self.strategy_params)


# ── Multi-Alpha Architecture: Phase 3 Data Models ─────────────────────────
#
# 与 ExperimentConfig 通过 alpha_mode / multi_alpha_config 关联。
# alpha_mode="single" 时整个 Multi-Alpha 配置不参与任何逻辑（零影响）。


class AlphaGroup(BaseModel):
    """单个 Alpha 组的配置。

    每组包含一组因子、一个模型和对应的数据集类型。
    Multi-Alpha 引擎为每组独立调用 compose_experiment_in_memory()
    生成子实验的 conf.yaml。
    """

    group_name: str                           # "pv_medium", "mf", "fundamental"
    factor_names: list[str]
    model_id: str                             # catalog model_id 或 __builtin_xxx__
    dataset_type: str = "DatasetH"            # "DatasetH" / "TSDatasetH"
    model_params: dict[str, Any] | None = None  # 可选超参覆盖
    compute_resource: str = "cpu"             # "cpu" / "gpu"
    preferred_node_id: str | None = None      # "wsl2-5080" / "rdagent-node1"
    holding_period_hint: str | None = None    # "short" / "medium" / "long" (信息标注)

    # ── 模型复用 (v1.1 backtest-only) ──────────────────────────────
    model_source_experiment_id: str | None = None   # 复用来源实验 ID
    model_source_group_name: str | None = None      # 复用来源组名（默认同 group_name）
    reuse_mode: str = "retrain"                     # "retrain" / "reuse_prediction" / "reuse_model"


class MetaModelConfig(BaseModel):
    """Meta-Model 合成配置。"""

    method: str = "ic_weighted"               # "ic_weighted" / "ols" / "stacking"
    cv_strategy: str = "train_valid_test"     # "train_valid_test" (回测) / "walk_forward" (实盘)
    lookback_days: int = 60
    cv_params: dict[str, Any] | None = None   # purge_days, embargo_days, n_splits


class MultiAlphaConfig(BaseModel):
    """多 Alpha 信号架构的完整配置。

    由 Multi-Alpha 因子选择引擎自动生成，或用户在 UI 手动编辑。
    """

    alpha_groups: list[AlphaGroup]
    meta_model: MetaModelConfig = MetaModelConfig()
    execution_mode: str = "serial"            # "serial" / "local_parallel" / "distributed"
    auto_selected: bool = False               # 是否由自动选因子引擎生成

    @model_validator(mode="after")
    def _validate_groups(self) -> "MultiAlphaConfig":
        if len(self.alpha_groups) < 2:
            raise ValueError("Multi-Alpha requires at least 2 alpha groups")
        names = [g.group_name for g in self.alpha_groups]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate group names: {names}")
        for g in self.alpha_groups:
            if not g.factor_names:
                raise ValueError(f"Group '{g.group_name}' has no factors")
        return self
