"""
QE Unified Engine — ExperimentConfig data model.

Single source of truth for all parameters passed to compose_experiment_in_memory().
Replaces the ad-hoc loop_custom_params dicts scattered across the four call paths.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, model_validator


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
    stock_pool: str | None = None
    sector_blacklist: list[str] | None = None

    # ── HMM sector filter ─────────────────────────────────────────────────────
    hmm: HmmConfig | None = None

    # ── Execution ─────────────────────────────────────────────────────────────
    execution_algo: str | None = None
    execution_algo_params: dict[str, Any] | None = None

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

    @model_validator(mode="after")
    def _validate_factor_names(self) -> "ExperimentConfig":
        if not self.factor_names:
            raise ValueError("factor_names cannot be empty")
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

        # 7. Unfilled handler — flatten params dict into top-level keys
        if self.unfilled_handler:
            params["unfilled_handler"] = self.unfilled_handler
            uf_params = self.unfilled_handler_params or {}
            if uf_params.get("trigger_minute"):
                params["unfilled_trigger_minute"] = uf_params["trigger_minute"]
            if uf_params.get("backup_depth"):
                params["unfilled_backup_depth"] = uf_params["backup_depth"]

        # 8. Extra catch-all params
        if self.extra_params:
            params.update(self.extra_params)

        # 9. initial_cash must NOT flow into custom_params
        params.pop("initial_cash", None)

        return params

    def build_strategy_params(self) -> dict[str, Any]:
        """Return the strategy_params dict for compose_experiment_in_memory().

        This is a copy of strategy_params (so the caller's dict is not mutated).
        initial_cash is intentionally kept here — it belongs in strategy_params,
        not in custom_params.
        """
        return dict(self.strategy_params) if self.strategy_params else {}
