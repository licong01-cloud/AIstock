"""
QE Unified Engine — ExperimentConfig data model.

Single source of truth for all parameters passed to compose_experiment_in_memory().
Replaces the ad-hoc loop_custom_params dicts scattered across the four call paths.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, model_validator

ALLOWED_LABEL_HORIZONS = {1, 3, 5, 10, 20}
DEFAULT_LABEL_HORIZON = 1


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
            params.update(self.strategy_params)

        # 3. HMM keys
        if self.hmm and self.hmm.enable_sector_hmm:
            params["enable_sector_hmm"] = True
            params["hmm_model_version_id"] = self.hmm.hmm_model_version_id
            params["sector_hmm_model_path"] = self.hmm.sector_hmm_model_path
            if self.hmm.hmm_signal_preset:
                params["hmm_signal_preset"] = self.hmm.hmm_signal_preset
            if self.hmm.hmm_signal_presets:
                params["hmm_signal_presets"] = self.hmm.hmm_signal_presets

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
            extra_params = dict(self.extra_params)
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

        return params

    def build_strategy_params(self) -> dict[str, Any]:
        """Return the strategy_params dict for compose_experiment_in_memory().

        This is a copy of strategy_params (so the caller's dict is not mutated).
        initial_cash is intentionally kept here — it belongs in strategy_params,
        not in custom_params.
        """
        return dict(self.strategy_params) if self.strategy_params else {}


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
