from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.contracts import PredictionArtifactDescriptor
from backend.services.advisory_list_transition import AdvisoryTransitionPolicyV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class AdvisoryPolicyCostV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_policy_cost_v1"] = "advisory_policy_cost_v1"
    buy_cost_bps: float = Field(ge=0)
    sell_cost_bps: float = Field(ge=0)
    minimum_commission_bps: Literal[0] = 0
    cash_return_bps_per_day: Literal[0] = 0
    benchmark_instrument: str = "000300.SH"

    @model_validator(mode="after")
    def validate_benchmark(self) -> "AdvisoryPolicyCostV1":
        value = self.benchmark_instrument.strip().upper()
        if not value:
            raise ValueError("benchmark_instrument is required")
        object.__setattr__(self, "benchmark_instrument", value)
        return self

    @property
    def policy_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class AdvisoryPolicySplitV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_policy_split_v1"] = "advisory_policy_split_v1"
    group_count: int = Field(default=8, ge=4)
    validation_group_count: int = Field(default=2, ge=1)
    embargo_trading_days: int = Field(default=20, ge=0)
    pbo_primary_metric: Literal["mean_net_excess_return_bps"] = "mean_net_excess_return_bps"
    pbo_tie_break: Literal["trial_id_ascending"] = "trial_id_ascending"
    random_seed: int = 20260813

    @model_validator(mode="after")
    def validate_shape(self) -> "AdvisoryPolicySplitV1":
        if self.group_count % 2:
            raise ValueError("group_count must be even for complementary PBO partitions")
        if self.validation_group_count >= self.group_count:
            raise ValueError("validation_group_count must be smaller than group_count")
        return self

    @property
    def policy_sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json"))


class FrozenAdvisoryPolicyDatasetRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_policy_dataset_request_v1"] = (
        "frozen_advisory_policy_dataset_request_v1"
    )
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    program_id: str
    binding_version_id: str
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_asset_closure_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    selection_runtime_semantics_id: str
    selection_runtime_semantics_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    selection_runtime_semantics: dict[str, Any]
    representative_seed_run_ids: dict[str, str]
    representative_model_asset_sha256: dict[str, str]
    prediction_artifacts: dict[str, PredictionArtifactDescriptor]
    terminal_weights: dict[str, float]
    qlib_daily_root: str
    suspend_data_root: str
    prediction_store_root: str
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    decision_date_start: str
    decision_date_end: str
    data_cutoff: str
    baseline_policy: dict[str, Any]
    baseline_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    shadow_policy: dict[str, Any]
    shadow_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    price_semantics_version: Literal["advisory_previous_rank_next_open_v1"] = (
        "advisory_previous_rank_next_open_v1"
    )
    policy_rank_semantics_version: Literal["advisory_exact_weighted_top40_v1"] = (
        "advisory_exact_weighted_top40_v1"
    )
    cost_policy: AdvisoryPolicyCostV1
    cost_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    split_policy: AdvisoryPolicySplitV1 = Field(default_factory=AdvisoryPolicySplitV1)
    split_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    output_root: str
    resource_max_rss_bytes: int = Field(default=8 * 1024**3, gt=0)

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdvisoryPolicyDatasetRequestV1":
        if self.baseline_policy_sha256 != canonical_json_sha256(self.baseline_policy):
            raise ValueError("baseline_policy_sha256 mismatch")
        if self.shadow_policy_sha256 != canonical_json_sha256(self.shadow_policy):
            raise ValueError("shadow_policy_sha256 mismatch")
        if self.cost_policy_sha256 != self.cost_policy.policy_sha256:
            raise ValueError("cost_policy_sha256 mismatch")
        if self.split_policy_sha256 != self.split_policy.policy_sha256:
            raise ValueError("split_policy_sha256 mismatch")
        baseline_policy = transition_policy_from_payload(self.baseline_policy)
        policy = transition_policy_from_payload(self.shadow_policy)
        if baseline_policy.target_count != 20 or baseline_policy.rank_enter_threshold != 20:
            raise ValueError("baseline policy must use target_count=rank_enter_threshold=20")
        if policy.target_count != 5 or policy.rank_enter_threshold != 5:
            raise ValueError("shadow policy must use target_count=rank_enter_threshold=5")
        if policy.rank_exit_threshold < 40:
            raise ValueError("shadow policy rank_exit_threshold must be at least 40")
        inherited = (
            "rank_exit_threshold",
            "rank_exit_confirm_days",
            "daily_replacement_budget",
            "stop_loss_bps",
            "take_profit_bps",
            "trailing_stop_bps",
            "time_stop_days",
            "take_profit_mode",
        )
        if any(getattr(policy, name) != getattr(baseline_policy, name) for name in inherited):
            raise ValueError("shadow policy must inherit every non-capacity transition field from baseline")
        if self.shadow_policy.get("entry_price_basis") != self.baseline_policy.get("entry_price_basis"):
            raise ValueError("shadow entry price basis differs from baseline")
        if self.shadow_policy.get("exit_price_basis") != self.baseline_policy.get("exit_price_basis"):
            raise ValueError("shadow exit price basis differs from baseline")
        if self.split_policy.embargo_trading_days < policy.time_stop_days:
            raise ValueError("split embargo must cover shadow policy time_stop_days")
        if set(self.representative_seed_run_ids) != set(self.terminal_weights):
            raise ValueError("representative leg roster differs from terminal weights")
        if set(self.prediction_artifacts) != set(self.representative_seed_run_ids.values()):
            raise ValueError("prediction artifacts must be the exact representative run roster")
        weight_sum = sum(float(value) for value in self.terminal_weights.values())
        if abs(weight_sum - 1.0) > 1e-10 or any(float(value) <= 0 for value in self.terminal_weights.values()):
            raise ValueError("terminal weights must be positive and sum to one")
        start = datetime.fromisoformat(self.decision_date_start).date()
        end = datetime.fromisoformat(self.decision_date_end).date()
        cutoff = datetime.fromisoformat(self.data_cutoff).date()
        if start > end or end > cutoff:
            raise ValueError("policy dataset date range must satisfy start <= end <= data_cutoff")
        expected = canonical_json_sha256(self.functional_payload())
        if expected != self.request_sha256:
            raise ValueError(f"request_sha256 mismatch: expected={expected} actual={self.request_sha256}")
        if self.request_id != f"advpolreq_{expected[:24]}":
            raise ValueError("request_id does not match request_sha256")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )

    def write_json(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        temporary.replace(target)


def build_frozen_policy_dataset_request(**values: Any) -> FrozenAdvisoryPolicyDatasetRequestV1:
    values = dict(values)
    values.setdefault("cost_policy_sha256", values["cost_policy"].policy_sha256)
    values.setdefault("split_policy_sha256", values["split_policy"].policy_sha256)
    values.setdefault("baseline_policy_sha256", canonical_json_sha256(values["baseline_policy"]))
    values.setdefault("shadow_policy_sha256", canonical_json_sha256(values["shadow_policy"]))
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = FrozenAdvisoryPolicyDatasetRequestV1.model_construct(
        schema_version="frozen_advisory_policy_dataset_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryPolicyDatasetRequestV1(
        schema_version="frozen_advisory_policy_dataset_request_v1",
        request_id=f"advpolreq_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def transition_policy_from_payload(payload: dict[str, Any]) -> AdvisoryTransitionPolicyV1:
    required = {
        "target_count",
        "rank_enter_threshold",
        "rank_exit_threshold",
        "rank_exit_confirm_days",
        "daily_replacement_budget",
        "stop_loss_bps",
        "take_profit_bps",
        "trailing_stop_bps",
        "time_stop_days",
        "take_profit_mode",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"review policy is missing fields: {missing}")
    if payload.get("entry_price_basis") != "next_open_executable":
        raise ValueError("entry_price_basis must be next_open_executable")
    if payload.get("exit_price_basis") != "next_open_executable":
        raise ValueError("exit_price_basis must be next_open_executable")
    return AdvisoryTransitionPolicyV1(**{key: payload[key] for key in required})
