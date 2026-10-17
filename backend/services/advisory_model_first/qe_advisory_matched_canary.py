from __future__ import annotations

from statistics import fmean, pstdev
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SOURCE_TASK_ID = "qe_20260907_204335_1fb0"
EXACT_RETRY_SEEDS = (314, 2718)
MATCHED_REPORT_SEEDS = (123, 314, 2718)
PROFILE_READY_MARKER = "QE_PROFILE_RUNTIME_READY"
EXPECTED_MODEL_ID = "__seed_LSTM_10D_hs64_d02__"
EXPECTED_FACTOR_NAMES = (
    "m_intraday_range_60d_min_ratio",
    "m_turnover_acceleration",
    "m_sector_mf_divergence_lg",
)
EXPECTED_FACTOR_DIGEST = "07fd78abfede758204f6c084cc6d9aaf3c07a57eae82ec73f965b1854a301345"
EXPECTED_DATA_SPLIT = {
    "train_start": "2022-03-23",
    "train_end": "2025-05-08",
    "valid_start": "2025-06-10",
    "valid_end": "2025-12-02",
    "test_start": "2026-01-05",
    "test_end": "2026-06-30",
    "backtest_end": "2026-06-29",
}
EXPECTED_LABEL_HORIZON = 20
EXPECTED_STRATEGY_ID = "score_weighted_topk_v2"
EXPECTED_OBSERVATION_PANEL_ID = "ma_e19_ce3_reference_v1"
EXPECTED_DATASET_RELEASE_ID = "qe_hmm_full_v2_20260831"
EXPECTED_DATASET_GENERATION = "20260907-v2"
EXPECTED_DATASET_CUTOFF = "2026-08-31"
EXPECTED_RISK_POLICY = {
    "enabled": True,
    "providers": ["st_pit"],
    "hard_actions": ["block_buy", "force_exit"],
    "score_overlay": {
        "enabled": False,
        "positive_multiplier_cap": 1.1,
        "negative_multiplier_floor": 0.7,
    },
    "policy_version": "stock_event_risk_policy_v1",
    "st_universe_key": "shsz_st_pit_active_v1",
    "strict_data_ready": True,
    "visible_time_mode": "next_trading_session",
}


class QECanaryLoopSnapshotV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_canary_loop_snapshot_v1"] = (
        "advisory_qe_canary_loop_snapshot_v1"
    )
    source_task_id: str
    loop_id: str
    loop_index: int = Field(ge=1)
    source_task_status: Literal["failed"] = "failed"
    loop_status: Literal["failed"] = "failed"
    experiment_id: None = None
    model_id: str
    factor_names: tuple[str, ...] = Field(min_length=1)
    factor_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    random_seed: int
    data_split: dict[str, str]
    label_horizon: int = Field(gt=0)
    strategy_id: str
    topk: Literal[50] = 50
    n_drop: Literal[1] = 1
    min_n_drop: Literal[0] = 0
    max_n_drop: Literal[1] = 1
    risk_policy: dict[str, Any]
    execution_algo: str
    execution_algo_params: dict[str, Any]
    bar_freq: Literal["1m"] = "1m"
    backtest_freq: Literal["1min"] = "1min"
    model_family: Literal["LSTM"] = "LSTM"
    refit_mode: Literal["rolling"] = "rolling"
    vintage: Literal["2026H1"] = "2026H1"
    rolling_train_days: Literal[756] = 756
    refit_cadence_trading_days: Literal[120] = 120
    observation_panel_id: str
    dataset_release_id: str
    dataset_generation: str
    dataset_cutoff: str
    universe_mode: str
    execution_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_identity(self) -> "QECanaryLoopSnapshotV1":
        required_split = {
            "train_start",
            "train_end",
            "valid_start",
            "valid_end",
            "test_start",
            "test_end",
            "backtest_end",
        }
        if set(self.data_split) != required_split:
            raise ValueError("data_split must contain the exact frozen date roles")
        if self.execution_algo.strip().upper() != "TWAP":
            raise ValueError("execution_algo must be TWAP")
        if self.universe_mode != "stock_universe":
            raise ValueError("universe_mode must be stock_universe")
        if len(set(self.factor_names)) != len(self.factor_names):
            raise ValueError("factor_names contain duplicates")
        return self

    def shared_contract_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={
                "source_task_id",
                "loop_id",
                "loop_index",
                "source_task_status",
                "loop_status",
                "experiment_id",
                "random_seed",
                "dataset_generation",
                "execution_manifest_sha256",
            },
        )

    @property
    def shared_contract_sha256(self) -> str:
        return canonical_json_sha256(self.shared_contract_payload())


class QECanaryExactRetryPreflightV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_qe_canary_exact_retry_preflight_v1"] = (
        "advisory_qe_canary_exact_retry_preflight_v1"
    )
    source_task_id: Literal[SOURCE_TASK_ID] = SOURCE_TASK_ID
    source_task_action: Literal["PRESERVE_FAILED_RECORD"] = "PRESERVE_FAILED_RECORD"
    new_task_identity_required: Literal[True] = True
    expected_seed_roster: tuple[int, ...] = EXACT_RETRY_SEEDS
    loops: tuple[QECanaryLoopSnapshotV1, ...]
    shared_contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_status_marker: str
    status: Literal["BLOCKED_QE_PROFILE_NOT_READY", "READY_FOR_NEW_QE_TASK_CREATION"]
    reason_code: str | None = None
    qe_task_submission_performed: Literal[False] = False
    source_task_mutated: Literal[False] = False
    qe_public_response_only: Literal[True] = True
    receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_receipt(self) -> "QECanaryExactRetryPreflightV1":
        expected = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"receipt_sha256"})
        )
        if expected != self.receipt_sha256:
            raise ValueError("receipt_sha256 mismatch")
        if self.status == "BLOCKED_QE_PROFILE_NOT_READY":
            if self.reason_code != "ADVISORY_QE_CANARY_PROFILE_NOT_READY":
                raise ValueError("blocked preflight requires the profile-not-ready reason")
        elif self.reason_code is not None:
            raise ValueError("ready preflight must not carry a reason_code")
        return self


class AdvisoryMatchedPolicyIdentityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_matched_policy_identity_v1"] = (
        "advisory_matched_policy_identity_v1"
    )
    policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cost_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pit_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_count: Literal[5] = 5
    rank_enter_threshold: Literal[5] = 5
    rank_exit_threshold: int = Field(ge=40)
    daily_replacement_budget: int = Field(ge=0)
    entry_price_basis: Literal["next_open_executable"] = "next_open_executable"
    exit_price_basis: Literal["next_open_executable"] = "next_open_executable"
    dynamic_position_weights: Literal[False] = False
    silent_backfill_allowed: Literal[False] = False


class AdvisoryMatchedSeedObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_matched_seed_observation_v1"] = (
        "advisory_qe_matched_seed_observation_v1"
    )
    random_seed: int
    qe_experiment_id: str = Field(min_length=1)
    qe_status: Literal["completed"] = "completed"
    candidate_source_lineage_id: str = Field(min_length=1)
    parent_package_id: str = Field(min_length=1)
    dataset_release_id: str = Field(min_length=1)
    dataset_cutoff: str = Field(min_length=1)
    universe_mode: str = Field(min_length=1)
    factor_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision_date_start: str
    decision_date_end: str
    advisory_decision_day_count: int = Field(gt=0)
    advisory_intervention_day_count: int = Field(ge=0)
    advisory_intervention_day_coverage: float = Field(ge=0, le=1)
    policy: AdvisoryMatchedPolicyIdentityV1
    qe_top50_net_return_bps: float
    qe_top50_turnover: float = Field(ge=0)
    qe_top50_max_drawdown: float = Field(ge=0)
    advisory_candidate_net_excess_return_bps: float
    advisory_parent_net_excess_return_bps: float
    advisory_candidate_coverage: float = Field(ge=0, le=1)
    advisory_parent_coverage: float = Field(ge=0, le=1)
    advisory_candidate_turnover: float = Field(ge=0)
    advisory_parent_turnover: float = Field(ge=0)
    advisory_candidate_max_drawdown: float = Field(ge=0)
    advisory_parent_max_drawdown: float = Field(ge=0)
    sealed_holdout_accessed: Literal[False] = False

    @model_validator(mode="after")
    def validate_intervention_support(self) -> "AdvisoryMatchedSeedObservationV1":
        expected = self.advisory_intervention_day_count / self.advisory_decision_day_count
        if abs(expected - self.advisory_intervention_day_coverage) > 1e-12:
            raise ValueError(
                "advisory_intervention_day_coverage does not match intervention/day counts"
            )
        return self


class AdvisoryQEMatchedCanaryReportV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_matched_canary_report_v1"] = (
        "advisory_qe_matched_canary_report_v1"
    )
    objective_contract: Literal["ALPHA_RANKING"] = "ALPHA_RANKING"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    result_class: Literal["CANDIDATE_EVIDENCE_ONLY_NOT_ACTIVATED"] = (
        "CANDIDATE_EVIDENCE_ONLY_NOT_ACTIVATED"
    )
    seed_roster: tuple[int, ...] = MATCHED_REPORT_SEEDS
    matched_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    observations: tuple[AdvisoryMatchedSeedObservationV1, ...]
    advisory_delta_bps_by_seed: dict[int, float]
    advisory_delta_mean_bps: float
    advisory_delta_std_bps: float
    advisory_delta_min_bps: float
    advisory_delta_max_bps: float
    positive_delta_seed_count: int = Field(ge=0)
    all_seeds_positive: bool
    qe_top50_net_return_mean_bps: float
    qe_top50_turnover_mean: float = Field(ge=0)
    qe_top50_max_drawdown_max: float = Field(ge=0)
    advisory_candidate_coverage_min: float = Field(ge=0, le=1)
    advisory_parent_coverage_min: float = Field(ge=0, le=1)
    advisory_intervention_day_count_min: int = Field(ge=0)
    advisory_intervention_day_coverage_min: float = Field(ge=0, le=1)
    advisory_candidate_turnover_mean: float = Field(ge=0)
    advisory_parent_turnover_mean: float = Field(ge=0)
    advisory_candidate_max_drawdown_max: float = Field(ge=0)
    advisory_parent_max_drawdown_max: float = Field(ge=0)
    activation_authorized: Literal[False] = False
    strategy_package_mutated: Literal[False] = False
    qe_task_submission_performed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_report_hash(self) -> "AdvisoryQEMatchedCanaryReportV1":
        expected = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"report_sha256"})
        )
        if expected != self.report_sha256:
            raise ValueError("report_sha256 mismatch")
        return self


def build_qe_exact_retry_preflight(
    task_payload: Mapping[str, Any],
    loop_config_payloads: Sequence[Mapping[str, Any]],
    *,
    profile_status_marker: str,
) -> QECanaryExactRetryPreflightV1:
    task = _unwrap_public_payload(task_payload)
    task_id = str(task.get("task_id") or "")
    if (
        task_id != SOURCE_TASK_ID
        or task.get("status") != "failed"
        or task.get("task_type") != "custom_evo"
        or task.get("max_loops") != 2
        or task.get("current_loop") != 0
    ):
        _raise(
            "QE source task is not the frozen failed canary",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            task_id=task_id,
            status=task.get("status"),
            task_type=task.get("task_type"),
            max_loops=task.get("max_loops"),
            current_loop=task.get("current_loop"),
        )
    task_loops = task.get("loops")
    if not isinstance(task_loops, list) or len(task_loops) != len(EXACT_RETRY_SEEDS):
        _raise(
            "QE source task loop roster is invalid",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_count=len(task_loops) if isinstance(task_loops, list) else None,
        )
    summary_by_index: dict[int, Mapping[str, Any]] = {}
    for item in task_loops:
        if not isinstance(item, Mapping):
            _raise("QE task loop summary is invalid", "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID")
        index = _required_int(item, "loop_index")
        if index in summary_by_index:
            _raise("QE task has duplicate loop indexes", "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID")
        summary_by_index[index] = item

    snapshots: list[QECanaryLoopSnapshotV1] = []
    for payload in loop_config_payloads:
        snapshots.append(_project_loop_snapshot(task, summary_by_index, payload))
    snapshots.sort(key=lambda item: item.loop_index)
    if len(snapshots) != len(EXACT_RETRY_SEEDS):
        _raise(
            "QE loop config roster is incomplete",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_count=len(snapshots),
        )
    indexes = tuple(item.loop_index for item in snapshots)
    if indexes != (1, 2):
        _raise(
            "QE exact-retry loop index roster drifted",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            expected_loop_indexes=(1, 2),
            actual_loop_indexes=indexes,
        )
    seeds = tuple(item.random_seed for item in snapshots)
    if seeds != EXACT_RETRY_SEEDS:
        _raise(
            "QE exact-retry seed roster drifted",
            "ADVISORY_QE_CANARY_CONTRACT_DRIFT",
            expected_seeds=EXACT_RETRY_SEEDS,
            actual_seeds=seeds,
        )
    hashes = {item.shared_contract_sha256 for item in snapshots}
    if len(hashes) != 1:
        _raise(
            "QE exact-retry loops do not share one frozen experiment contract",
            "ADVISORY_QE_CANARY_CONTRACT_DRIFT",
            shared_contract_hashes=sorted(hashes),
        )
    shared_contract_sha256 = next(iter(hashes))
    for snapshot in snapshots:
        _validate_frozen_source_contract(snapshot)
    ready = profile_status_marker == PROFILE_READY_MARKER
    values = {
        "loops": tuple(snapshots),
        "shared_contract_sha256": shared_contract_sha256,
        "profile_status_marker": str(profile_status_marker),
        "status": (
            "READY_FOR_NEW_QE_TASK_CREATION" if ready else "BLOCKED_QE_PROFILE_NOT_READY"
        ),
        "reason_code": None if ready else "ADVISORY_QE_CANARY_PROFILE_NOT_READY",
    }
    seed = QECanaryExactRetryPreflightV1.model_construct(
        **values,
        receipt_sha256="0" * 64,
    )
    digest = canonical_json_sha256(seed.model_dump(mode="json", exclude={"receipt_sha256"}))
    return QECanaryExactRetryPreflightV1(**values, receipt_sha256=digest)


def build_qe_advisory_matched_canary_report(
    observations: Sequence[Mapping[str, Any] | AdvisoryMatchedSeedObservationV1],
) -> AdvisoryQEMatchedCanaryReportV1:
    parsed: list[AdvisoryMatchedSeedObservationV1] = []
    try:
        for item in observations:
            parsed.append(
                item
                if isinstance(item, AdvisoryMatchedSeedObservationV1)
                else AdvisoryMatchedSeedObservationV1.model_validate(item)
            )
    except ValidationError as exc:
        reason_code = (
            "ADVISORY_QE_CANARY_SEALED_ACCESS_FORBIDDEN"
            if "sealed_holdout_accessed" in str(exc)
            else "ADVISORY_QE_CANARY_OBSERVATION_INVALID"
        )
        _raise(
            "QE/Advisory matched seed observation is invalid",
            reason_code,
            error_type=type(exc).__name__,
        )
    parsed.sort(key=lambda item: item.random_seed)
    seeds = tuple(item.random_seed for item in parsed)
    if seeds != MATCHED_REPORT_SEEDS:
        _raise(
            "QE/Advisory matched report requires the frozen three-seed roster",
            "ADVISORY_QE_CANARY_OBSERVATION_INVALID",
            expected_seeds=MATCHED_REPORT_SEEDS,
            actual_seeds=seeds,
        )
    for item in parsed:
        _validate_observation_source_contract(item)
    matched_payloads = [_matched_identity_payload(item) for item in parsed]
    matched_hashes = {canonical_json_sha256(payload) for payload in matched_payloads}
    if len(matched_hashes) != 1:
        policy_hashes = {item.policy.policy_sha256 for item in parsed}
        cost_hashes = {item.policy.cost_policy_sha256 for item in parsed}
        pit_hashes = {item.policy.pit_identity_sha256 for item in parsed}
        reason_code = (
            "ADVISORY_QE_CANARY_POLICY_MISMATCH"
            if len(policy_hashes) > 1 or len(cost_hashes) > 1 or len(pit_hashes) > 1
            else "ADVISORY_QE_CANARY_CONTRACT_DRIFT"
        )
        _raise(
            "QE/Advisory matched observations do not share one evaluation identity",
            reason_code,
            matched_identity_hashes=sorted(matched_hashes),
        )
    matched_identity_sha256 = next(iter(matched_hashes))
    deltas = {
        item.random_seed: (
            item.advisory_candidate_net_excess_return_bps
            - item.advisory_parent_net_excess_return_bps
        )
        for item in parsed
    }
    delta_values = list(deltas.values())
    values = {
        "observations": tuple(parsed),
        "matched_identity_sha256": matched_identity_sha256,
        "advisory_delta_bps_by_seed": deltas,
        "advisory_delta_mean_bps": fmean(delta_values),
        "advisory_delta_std_bps": pstdev(delta_values),
        "advisory_delta_min_bps": min(delta_values),
        "advisory_delta_max_bps": max(delta_values),
        "positive_delta_seed_count": sum(value > 0 for value in delta_values),
        "all_seeds_positive": all(value > 0 for value in delta_values),
        "qe_top50_net_return_mean_bps": fmean(
            item.qe_top50_net_return_bps for item in parsed
        ),
        "qe_top50_turnover_mean": fmean(item.qe_top50_turnover for item in parsed),
        "qe_top50_max_drawdown_max": max(
            item.qe_top50_max_drawdown for item in parsed
        ),
        "advisory_candidate_coverage_min": min(
            item.advisory_candidate_coverage for item in parsed
        ),
        "advisory_parent_coverage_min": min(
            item.advisory_parent_coverage for item in parsed
        ),
        "advisory_intervention_day_count_min": min(
            item.advisory_intervention_day_count for item in parsed
        ),
        "advisory_intervention_day_coverage_min": min(
            item.advisory_intervention_day_coverage for item in parsed
        ),
        "advisory_candidate_turnover_mean": fmean(
            item.advisory_candidate_turnover for item in parsed
        ),
        "advisory_parent_turnover_mean": fmean(
            item.advisory_parent_turnover for item in parsed
        ),
        "advisory_candidate_max_drawdown_max": max(
            item.advisory_candidate_max_drawdown for item in parsed
        ),
        "advisory_parent_max_drawdown_max": max(
            item.advisory_parent_max_drawdown for item in parsed
        ),
    }
    seed = AdvisoryQEMatchedCanaryReportV1.model_construct(
        **values,
        report_sha256="0" * 64,
    )
    digest = canonical_json_sha256(seed.model_dump(mode="json", exclude={"report_sha256"}))
    return AdvisoryQEMatchedCanaryReportV1(**values, report_sha256=digest)


def _project_loop_snapshot(
    task: Mapping[str, Any],
    summary_by_index: Mapping[int, Mapping[str, Any]],
    payload: Mapping[str, Any],
) -> QECanaryLoopSnapshotV1:
    data = _unwrap_public_payload(payload)
    config = data.get("config_json") if isinstance(data.get("config_json"), Mapping) else data
    if not isinstance(config, Mapping):
        _raise("QE loop config_json is missing", "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID")
    index = _required_int(data, "loop_index")
    summary = summary_by_index.get(index)
    if summary is None:
        _raise(
            "QE loop config is not present in the source task",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_index=index,
        )
    model_params = _required_mapping(config, "model_params")
    runtime_flags = _required_mapping(config, "runtime_flags")
    registration = config.get("_qe_run_registration")
    if not isinstance(registration, Mapping):
        registration = model_params.get("_qe_run_registration")
    if not isinstance(registration, Mapping):
        _raise(
            "QE loop registration is missing",
            "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
            loop_index=index,
        )
    if registration.get("schema_version") != "qe_run_registration_v1":
        _raise("QE registration schema is unsupported", "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID")
    if registration.get("task_id") != task.get("task_id") or registration.get("loop_index") != index:
        _raise(
            "QE task and loop registration identities disagree",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_index=index,
        )
    if data.get("task_id") != task.get("task_id") or data.get("loop_id") != summary.get("loop_id"):
        _raise(
            "QE public task and loop config identities disagree",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_index=index,
        )
    if summary.get("status") != "failed" or data.get("status") != "failed":
        _raise(
            "QE source loop is not failed",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_index=index,
        )
    if "experiment_id" not in summary or summary.get("experiment_id") is not None:
        _raise(
            "QE source loop experiment identity must be explicitly null",
            "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID",
            loop_index=index,
        )
    factor_names = tuple(str(item) for item in _required_sequence(config, "factor_list"))
    data_split_raw = _required_mapping(config, "data_split")
    data_split = {str(key): str(value) for key, value in data_split_raw.items()}
    try:
        return QECanaryLoopSnapshotV1(
            source_task_id=str(task["task_id"]),
            loop_id=str(data["loop_id"]),
            loop_index=index,
            model_id=str(config["model_id"]),
            factor_names=factor_names,
            factor_digest=str(registration["factor_digest"]),
            random_seed=int(runtime_flags["random_seed"]),
            data_split=data_split,
            label_horizon=int(config["label_horizon"]),
            strategy_id=str(config["strategy_id"]),
            topk=int(model_params["topk"]),
            n_drop=int(model_params["n_drop"]),
            min_n_drop=int(model_params["min_n_drop"]),
            max_n_drop=int(model_params["max_n_drop"]),
            risk_policy=dict(_required_mapping(model_params, "risk_policy")),
            execution_algo=str(config["execution_algo"]),
            execution_algo_params=dict(config.get("execution_algo_params") or {}),
            bar_freq=str(config["bar_freq"]),
            backtest_freq=str(config["backtest_freq"]),
            model_family=str(runtime_flags["model_family"]),
            refit_mode=str(runtime_flags["refit_mode"]),
            vintage=str(runtime_flags["vintage"]),
            rolling_train_days=int(runtime_flags["rolling_train_days"]),
            refit_cadence_trading_days=int(runtime_flags["refit_cadence_trading_days"]),
            observation_panel_id=str(runtime_flags["observation_panel_id"]),
            dataset_release_id=str(registration["dataset_release_id"]),
            dataset_generation=str(registration["dataset_generation"]),
            dataset_cutoff=str(registration["dataset_cutoff"]),
            universe_mode=str(registration["universe_mode"]),
            execution_manifest_sha256=str(config["execution_manifest_sha256"]),
        )
    except (KeyError, TypeError, ValueError, ValidationError) as exc:
        _raise(
            "QE public loop config cannot be projected to the Advisory canary contract",
            "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
            loop_index=index,
            error_type=type(exc).__name__,
        )


def _matched_identity_payload(item: AdvisoryMatchedSeedObservationV1) -> dict[str, Any]:
    return item.model_dump(
        mode="json",
        include={
            "candidate_source_lineage_id",
            "parent_package_id",
            "dataset_release_id",
            "dataset_cutoff",
            "universe_mode",
            "factor_digest",
            "decision_date_start",
            "decision_date_end",
            "advisory_decision_day_count",
            "policy",
        },
    )


def _validate_frozen_source_contract(snapshot: QECanaryLoopSnapshotV1) -> None:
    expected = {
        "model_id": EXPECTED_MODEL_ID,
        "factor_names": EXPECTED_FACTOR_NAMES,
        "factor_digest": EXPECTED_FACTOR_DIGEST,
        "data_split": EXPECTED_DATA_SPLIT,
        "label_horizon": EXPECTED_LABEL_HORIZON,
        "strategy_id": EXPECTED_STRATEGY_ID,
        "topk": 50,
        "n_drop": 1,
        "min_n_drop": 0,
        "max_n_drop": 1,
        "execution_algo": "TWAP",
        "bar_freq": "1m",
        "backtest_freq": "1min",
        "model_family": "LSTM",
        "refit_mode": "rolling",
        "vintage": "2026H1",
        "rolling_train_days": 756,
        "refit_cadence_trading_days": 120,
        "observation_panel_id": EXPECTED_OBSERVATION_PANEL_ID,
        "dataset_release_id": EXPECTED_DATASET_RELEASE_ID,
        "dataset_generation": EXPECTED_DATASET_GENERATION,
        "dataset_cutoff": EXPECTED_DATASET_CUTOFF,
        "universe_mode": "stock_universe",
    }
    actual = {key: getattr(snapshot, key) for key in expected}
    drifted = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if actual[key] != expected[key]
    }
    if snapshot.risk_policy != EXPECTED_RISK_POLICY:
        drifted["risk_policy"] = {
            "expected": EXPECTED_RISK_POLICY,
            "actual": snapshot.risk_policy,
        }
    if snapshot.execution_algo_params:
        drifted["execution_algo_params"] = {
            "expected": {},
            "actual": snapshot.execution_algo_params,
        }
    if drifted:
        _raise(
            "QE source loop differs from the frozen exact-retry contract",
            "ADVISORY_QE_CANARY_CONTRACT_DRIFT",
            loop_index=snapshot.loop_index,
            drifted=drifted,
        )


def _validate_observation_source_contract(
    item: AdvisoryMatchedSeedObservationV1,
) -> None:
    expected = {
        "dataset_release_id": EXPECTED_DATASET_RELEASE_ID,
        "dataset_cutoff": EXPECTED_DATASET_CUTOFF,
        "universe_mode": "stock_universe",
        "factor_digest": EXPECTED_FACTOR_DIGEST,
        "decision_date_start": EXPECTED_DATA_SPLIT["test_start"],
        "decision_date_end": EXPECTED_DATA_SPLIT["test_end"],
    }
    actual = {key: getattr(item, key) for key in expected}
    drifted = {
        key: {"expected": expected[key], "actual": actual[key]}
        for key in expected
        if actual[key] != expected[key]
    }
    if drifted:
        _raise(
            "QE/Advisory observation differs from the frozen canary source contract",
            "ADVISORY_QE_CANARY_CONTRACT_DRIFT",
            random_seed=item.random_seed,
            drifted=drifted,
        )


def _unwrap_public_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        _raise("QE public payload is not an object", "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID")
    data = payload.get("data")
    if data is not None:
        if not isinstance(data, Mapping):
            _raise("QE public payload data is not an object", "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID")
        return data
    return payload


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        _raise(
            "QE public payload mapping is missing",
            "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
            key=key,
        )
    return value


def _required_sequence(payload: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = payload.get(key)
    if not isinstance(value, (list, tuple)) or isinstance(value, (str, bytes)):
        _raise(
            "QE public payload sequence is missing",
            "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
            key=key,
        )
    return value


def _required_int(payload: Mapping[str, Any], key: str) -> int:
    try:
        return int(payload[key])
    except (KeyError, TypeError, ValueError) as exc:
        _raise(
            "QE public payload integer is missing",
            "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
            key=key,
            error_type=type(exc).__name__,
        )


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "EXPECTED_DATASET_CUTOFF",
    "EXPECTED_DATASET_GENERATION",
    "EXPECTED_DATASET_RELEASE_ID",
    "EXPECTED_DATA_SPLIT",
    "EXPECTED_FACTOR_NAMES",
    "EXPECTED_FACTOR_DIGEST",
    "EXPECTED_MODEL_ID",
    "EXPECTED_RISK_POLICY",
    "EXACT_RETRY_SEEDS",
    "MATCHED_REPORT_SEEDS",
    "PROFILE_READY_MARKER",
    "SOURCE_TASK_ID",
    "AdvisoryMatchedPolicyIdentityV1",
    "AdvisoryMatchedSeedObservationV1",
    "AdvisoryQEMatchedCanaryReportV1",
    "QECanaryExactRetryPreflightV1",
    "QECanaryLoopSnapshotV1",
    "build_qe_advisory_matched_canary_report",
    "build_qe_exact_retry_preflight",
]
