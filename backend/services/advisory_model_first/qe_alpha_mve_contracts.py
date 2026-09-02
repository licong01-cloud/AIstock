from __future__ import annotations

from datetime import date, datetime, timezone
from math import isfinite
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
N3_EXPERIMENT_ID = "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1"
N3_HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1"
N3_SELECTED_ROUTE = "N3_QE_UPSTREAM_ALPHA_MVE"
N3_ROUTE_MINIMUM_RECALL = 0.20
MVE_SIGNAL_START = date(2024, 7, 4)
MVE_SIGNAL_END = date(2026, 2, 2)
MVE_OUTCOME_CUTOFF = date(2026, 3, 10)
MVE_PROPOSAL_COUNT = 24
MVE_MAX_RSS_BYTES = 16 * 1024**3
MVE_MAX_TEMP_BYTES = 32 * 1024**3
MVE_BLOCK_LENGTH = 20
MVE_BOOTSTRAP_REPETITIONS = 2000
MVE_RANDOM_SEED = 20260902

MVE_FAMILIES = (
    "PRICE_VOLUME_BEHAVIOR",
    "MONEYFLOW_BEHAVIOR",
    "FUNDAMENTAL_CHANGE",
    "SECTOR_RELATIVE",
    "CROWDING_DISPERSION",
    "REGIME_CONDITIONED",
)

EXPRESSION_OPERATORS = frozenset(
    {
        "FIELD",
        "CONST",
        "ADD",
        "SUBTRACT",
        "MULTIPLY",
        "SAFE_DIVIDE",
        "ABS",
        "SIGN",
        "LOG1P_ABS",
        "SQRT_ABS",
        "CLIP",
        "LAG",
        "DELTA",
        "TRAILING_SUM",
        "TRAILING_MEAN",
        "TRAILING_STD",
        "TRAILING_MIN",
        "TRAILING_MAX",
        "SAME_DATE_RANK",
        "SAME_DATE_ZSCORE",
    }
)

DAILY_FIELDS = frozenset({"open", "high", "low", "close", "volume", "amount"})
STATIC_FIELDS = frozenset(
    {
        "db_turnover_rate",
        "db_volume_ratio",
        "db_pb",
        "mf_main_net_amt_ratio_5d",
        "mf_main_net_amt_ratio_20d",
        "mf_elg_net_amt_ratio_5d",
        "mf_elg_net_amt_ratio_20d",
        "mf_total_net_amt_ratio_5d",
        "mf_total_net_amt_ratio_20d",
        "mf_elg_share_in_main_amt",
        "mf_sm_buy_amt",
        "mf_sm_sell_amt",
        "cp_winner_rate",
        "bb_rev_yoy",
        "bb_profit_yoy",
        "bb_gpr",
        "bb_npr",
        "value_pb_inv",
        "bb_liquid_assets",
        "bb_total_assets",
        "sw2_pct_change",
        "sw2_vol",
        "sw2_mf_net_amt",
        "sw2_pb",
    }
)
DERIVED_FIELDS = frozenset({"market_regime"})
ALLOWED_FIELDS = DAILY_FIELDS | STATIC_FIELDS | DERIVED_FIELDS


def field(name: str) -> dict[str, Any]:
    return {"op": "FIELD", "field": name}


def const(value: float) -> dict[str, Any]:
    return {"op": "CONST", "value": float(value)}


def operation(op: str, *args: dict[str, Any], **params: Any) -> dict[str, Any]:
    return {"op": op, "args": list(args), **params}


def _return(window: int) -> dict[str, Any]:
    return operation(
        "SUBTRACT",
        operation("SAFE_DIVIDE", field("close"), operation("LAG", field("close"), periods=window)),
        const(1.0),
    )


def _volume_trend() -> dict[str, Any]:
    return operation(
        "SAFE_DIVIDE",
        operation("TRAILING_MEAN", field("volume"), window=5),
        operation("TRAILING_MEAN", field("volume"), window=20),
    )


def _rank(value: dict[str, Any]) -> dict[str, Any]:
    return operation("SAME_DATE_RANK", value)


def _zscore(value: dict[str, Any]) -> dict[str, Any]:
    return operation("SAME_DATE_ZSCORE", value)


class QEAlphaProposalV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_proposal_v1"] = "advisory_qe_alpha_proposal_v1"
    proposal_id: str = Field(pattern=r"^N3_[A-Z_]+_[0-9]{2}$")
    family: str = Field(min_length=1)
    economic_hypothesis: str = Field(min_length=1)
    expression: dict[str, Any]
    expression_sha256: str = Field(pattern=SHA256_PATTERN)
    source_fields: tuple[str, ...]
    direction_frozen: Literal[True] = True

    @model_validator(mode="after")
    def validate_proposal(self) -> "QEAlphaProposalV1":
        if self.family not in MVE_FAMILIES:
            raise ValueError("QE alpha proposal family is not frozen")
        stats = validate_expression(self.expression)
        if self.source_fields != tuple(sorted(stats["fields"])):
            raise ValueError("QE alpha proposal source field roster drift")
        digest = canonical_json_sha256(self.expression)
        if self.expression_sha256 != digest:
            raise ValueError("QE alpha proposal expression hash drift")
        return self


class AdvisoryN3RouteReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_n3_route_receipt_v1"] = "advisory_n3_route_receipt_v1"
    receipt_id: str = Field(pattern=r"^advn3route_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    candidate_top50_winner_recall: float = Field(ge=0, le=1)
    candidate_top50_winner_recall_upper: float = Field(ge=0, le=1)
    minimum_required_recall: Literal[0.20] = N3_ROUTE_MINIMUM_RECALL
    candidate_recall_state: Literal["INSUFFICIENT"] = "INSUFFICIENT"
    n1_direction_ready: Literal[False] = False
    entry_confirmatory_positive: Literal[False] = False
    exit_learnability_high: Literal[False] = False
    selected_route: Literal["N3_QE_UPSTREAM_ALPHA_MVE"] = N3_SELECTED_ROUTE
    active_main_line_count: Literal[1] = 1
    active_auxiliary_line_count: Literal[0] = 0
    reason_codes: tuple[str, ...]
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    study_type: Literal[ResearchStudyType.EXPLORATORY_SCREEN] = ResearchStudyType.EXPLORATORY_SCREEN
    decision_use: Literal[DecisionUse.DIRECTION_GATE] = DecisionUse.DIRECTION_GATE
    planned_trial_count: Literal[0] = 0
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_route(self) -> "AdvisoryN3RouteReceiptV1":
        if self.candidate_top50_winner_recall_upper >= self.minimum_required_recall:
            raise ValueError("N3 upstream route requires recall upper below the frozen minimum")
        required = {
            "GLOBAL_WINNER_RECALL_INSUFFICIENT",
            "N1_RANKER_NOT_DIRECTION_READY",
            "ENTRY_HAS_NO_CONFIRMATORY_POSITIVE_ARM",
            "EXIT_FIXED_INFORMATION_NOT_CONFIRMED_LEARNABLE",
        }
        if set(self.reason_codes) != required:
            raise ValueError("N3 route reason roster drift")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advn3route_{digest[:24]}":
            raise ValueError("N3 route receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


class FrozenAdvisoryQEAlphaMVERequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["frozen_advisory_qe_alpha_mve_request_v1"] = "frozen_advisory_qe_alpha_mve_request_v1"
    request_id: str = Field(pattern=r"^advqemvereq_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    objective_contract: Literal[ObjectiveContract.ALPHA_RANKING] = ObjectiveContract.ALPHA_RANKING
    study_type: Literal[ResearchStudyType.EXPLORATORY_SCREEN] = ResearchStudyType.EXPLORATORY_SCREEN
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    planned_trial_count: Literal[24] = MVE_PROPOSAL_COUNT
    generated_trial_count: Literal[0] = 0
    evaluated_trial_count: Literal[0] = 0
    selected_trial_count: Literal[0] = 0
    route_receipt: AdvisoryN3RouteReceiptV1
    proposals: tuple[QEAlphaProposalV1, ...]
    evidence_refs: tuple[EvidenceReferenceV1, ...]
    preparation_path: str = Field(min_length=1)
    n2b_bundle_path: str = Field(min_length=1)
    outcomes_path: str = Field(min_length=1)
    outcomes_ref: EvidenceReferenceV1
    factor_root: str = Field(min_length=1)
    static_factor_ref: EvidenceReferenceV1
    static_schema_sha256: str = Field(pattern=SHA256_PATTERN)
    qlib_daily_root: str = Field(min_length=1)
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    signal_start: date = MVE_SIGNAL_START
    signal_end: date = MVE_SIGNAL_END
    outcome_cutoff: date = MVE_OUTCOME_CUTOFF
    lookback_trading_days: Literal[252] = 252
    minimum_evaluable_days: Literal[382] = 382
    minimum_finite_fraction: Literal[0.95] = 0.95
    maximum_parent_spearman: Literal[0.80] = 0.80
    block_length_trading_days: Literal[20] = MVE_BLOCK_LENGTH
    bootstrap_repetitions: Literal[2000] = MVE_BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[20260902] = MVE_RANDOM_SEED
    familywise_trial_count: Literal[24] = MVE_PROPOSAL_COUNT
    registry_path: str = Field(min_length=1)
    route_path: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    output_root: str = Field(min_length=1)
    resource_max_rss_bytes: Literal[MVE_MAX_RSS_BYTES] = MVE_MAX_RSS_BYTES
    resource_max_temp_bytes: Literal[MVE_MAX_TEMP_BYTES] = MVE_MAX_TEMP_BYTES
    resource_max_wall_seconds: Literal[None] = None
    database_read_allowed: Literal[False] = False
    network_read_allowed: Literal[False] = False
    factor_catalog_write_allowed: Literal[False] = False
    runtime_activation_allowed: Literal[False] = False
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "FrozenAdvisoryQEAlphaMVERequestV1":
        if len(self.proposals) != MVE_PROPOSAL_COUNT:
            raise ValueError("QE alpha MVE requires exactly 24 proposals")
        ids = [item.proposal_id for item in self.proposals]
        hashes = [item.expression_sha256 for item in self.proposals]
        if len(set(ids)) != len(ids) or len(set(hashes)) != len(hashes):
            raise ValueError("QE alpha MVE proposal identity is duplicated")
        counts = {family: sum(item.family == family for item in self.proposals) for family in MVE_FAMILIES}
        if any(value != 4 for value in counts.values()):
            raise ValueError("QE alpha MVE proposal family budget drift")
        if self.signal_start != MVE_SIGNAL_START or self.signal_end != MVE_SIGNAL_END:
            raise ValueError("QE alpha MVE signal window drift")
        roles = [item.role for item in self.evidence_refs]
        if len(roles) != len(set(roles)):
            raise ValueError("QE alpha MVE evidence roles are duplicated")
        required_roles = {
            "n3_n1_oracle_receipt",
            "n3_n1_learnability_receipt",
            "n3_n1_quadrant_receipt",
            "n3_n2a_audit_receipt",
            "n3_n2a_arm_summary",
            "n3_n2b_audit_receipt",
            "n3_n2b_arm_summary",
            "n3_n2b_pairwise_summary",
            "n3_n2_action_receipt",
            "n3_n2_entry_summary",
            "n3_n2_entry_support",
            "n3_n2_exit_summary",
            "n3_n2_exit_support",
            "n3_exit_learnability_receipt",
            "n3_qe_alpha_preparation",
            "n3_trial_registry_before",
        }
        if set(roles) != required_roles:
            raise ValueError("QE alpha MVE evidence role roster drift")
        if self.static_factor_ref.role != "n3_static_factors_parquet":
            raise ValueError("QE alpha MVE static factor ref role drift")
        if self.outcomes_ref.role != "n3_current_parent_signal_outcomes":
            raise ValueError("QE alpha MVE outcome ref role drift")
        if self.outcomes_ref.artifact_uri.replace("\\", "/") != self.outcomes_path.replace("\\", "/"):
            raise ValueError("QE alpha MVE outcome path/ref drift")
        if self.route_receipt.selected_route != N3_SELECTED_ROUTE:
            raise ValueError("QE alpha MVE request requires the upstream N3 route")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advqemvereq_{digest[:24]}":
            raise ValueError("QE alpha MVE request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


class AdvisoryQEAlphaMVEReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["advisory_qe_alpha_mve_receipt_v1"] = "advisory_qe_alpha_mve_receipt_v1"
    receipt_id: str = Field(pattern=r"^advqemvercpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["COMPLETE"] = "COMPLETE"
    planned_trial_count: Literal[24] = MVE_PROPOSAL_COUNT
    generated_trial_count: Literal[24] = MVE_PROPOSAL_COUNT
    evaluated_trial_count: Literal[24] = MVE_PROPOSAL_COUNT
    selected_trial_count: int = Field(ge=0, le=1)
    selected_proposal_id: str | None
    eligible_proposal_ids: tuple[str, ...]
    result_class: Literal[ResearchResultClass.EXPLORATORY] = ResearchResultClass.EXPLORATORY
    decision_use: Literal[DecisionUse.NAVIGATION_ONLY] = DecisionUse.NAVIGATION_ONLY
    next_task: Literal[
        "N3_ALPHA_CANDIDATE_CONFIRMATION_DESIGN",
        "N3_ALPHA_INFORMATION_SET_REVIEW",
    ]
    source_identity_sha256: str = Field(pattern=SHA256_PATTERN)
    result_files_sha256: str = Field(pattern=SHA256_PATTERN)
    resource_report_sha256: str = Field(pattern=SHA256_PATTERN)
    sealed_holdout_accessed: Literal[False] = False
    deployable: Literal[False] = False
    runtime_eligible: Literal[False] = False
    factor_catalog_written: Literal[False] = False
    created_at: datetime

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryQEAlphaMVEReceiptV1":
        expected_count = 1 if self.selected_proposal_id is not None else 0
        expected_next = (
            "N3_ALPHA_CANDIDATE_CONFIRMATION_DESIGN" if expected_count else "N3_ALPHA_INFORMATION_SET_REVIEW"
        )
        if self.selected_trial_count != expected_count or self.next_task != expected_next:
            raise ValueError("QE alpha MVE selection/next-task relation drift")
        if self.selected_proposal_id is not None and self.selected_proposal_id not in self.eligible_proposal_ids:
            raise ValueError("QE alpha MVE selected proposal is not eligible")
        digest = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != digest or self.receipt_id != f"advqemvercpt_{digest[:24]}":
            raise ValueError("QE alpha MVE receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


def validate_expression(expression: dict[str, Any]) -> dict[str, Any]:
    fields: set[str] = set()
    nodes = 0

    def visit(node: Any, depth: int) -> None:
        nonlocal nodes
        if not isinstance(node, dict) or depth > 8:
            raise ValueError("QE alpha expression node/depth is invalid")
        nodes += 1
        if nodes > 64:
            raise ValueError("QE alpha expression exceeds 64 nodes")
        op = node.get("op")
        if op not in EXPRESSION_OPERATORS:
            raise ValueError("QE alpha expression operator is not implemented")
        allowed_keys = {"op"}
        if op == "FIELD":
            allowed_keys.add("field")
            name = node.get("field")
            if name not in ALLOWED_FIELDS:
                raise ValueError("QE alpha expression field is not allowed")
            fields.add(str(name))
            if "args" in node:
                raise ValueError("FIELD cannot contain args")
        elif op == "CONST":
            allowed_keys.add("value")
            value = node.get("value")
            if not isinstance(value, (int, float)) or not isfinite(float(value)):
                raise ValueError("CONST value is invalid")
            if "args" in node:
                raise ValueError("CONST cannot contain args")
        else:
            allowed_keys.add("args")
            args = node.get("args")
            if not isinstance(args, list):
                raise ValueError("QE alpha operator args are invalid")
            binary = {"ADD", "SUBTRACT", "MULTIPLY", "SAFE_DIVIDE"}
            unary = {
                "ABS",
                "SIGN",
                "LOG1P_ABS",
                "SQRT_ABS",
                "SAME_DATE_RANK",
                "SAME_DATE_ZSCORE",
            }
            if op in binary and len(args) != 2:
                raise ValueError("binary QE alpha operator arity drift")
            if op in unary and len(args) != 1:
                raise ValueError("unary QE alpha operator arity drift")
            if op in {"LAG", "DELTA"}:
                allowed_keys.add("periods")
                periods = node.get("periods")
                if len(args) != 1 or not isinstance(periods, int) or not 1 <= periods <= 252:
                    raise ValueError("QE alpha lag/delta periods are invalid")
            if op.startswith("TRAILING_"):
                allowed_keys.add("window")
                window = node.get("window")
                if len(args) != 1 or not isinstance(window, int) or not 2 <= window <= 252:
                    raise ValueError("QE alpha trailing window is invalid")
            if op == "CLIP":
                allowed_keys.update({"lower", "upper"})
                if (
                    len(args) != 1
                    or not isinstance(node.get("lower"), (int, float))
                    or not isinstance(node.get("upper"), (int, float))
                    or float(node["lower"]) >= float(node["upper"])
                ):
                    raise ValueError("QE alpha clip bounds are invalid")
            for child in args:
                visit(child, depth + 1)
        if set(node) != allowed_keys:
            raise ValueError("QE alpha expression node has unknown/missing keys")

    visit(expression, 1)
    if len(fields) > 8:
        raise ValueError("QE alpha expression exceeds eight raw fields")
    return {"fields": fields, "node_count": nodes}


def build_default_proposals() -> tuple[QEAlphaProposalV1, ...]:
    ret1 = _return(1)
    ret5 = _return(5)
    ret10 = _return(10)
    ret20 = _return(20)
    range_ratio = operation(
        "TRAILING_MEAN",
        operation(
            "SAFE_DIVIDE",
            operation("SUBTRACT", field("high"), field("low")),
            field("close"),
        ),
        window=20,
    )
    specs: list[tuple[str, str, str, dict[str, Any]]] = [
        (
            "N3_PRICE_VOLUME_BEHAVIOR_01",
            "PRICE_VOLUME_BEHAVIOR",
            "20日收益除以20日日收益波动，捕捉风险调整动量",
            _rank(operation("SAFE_DIVIDE", ret20, operation("TRAILING_STD", ret1, window=20))),
        ),
        (
            "N3_PRICE_VOLUME_BEHAVIOR_02",
            "PRICE_VOLUME_BEHAVIOR",
            "短期反转与换手异常共振",
            _rank(operation("MULTIPLY", operation("MULTIPLY", const(-1), ret5), _zscore(field("db_turnover_rate")))),
        ),
        (
            "N3_PRICE_VOLUME_BEHAVIOR_03",
            "PRICE_VOLUME_BEHAVIOR",
            "10日动量由5/20日成交量趋势确认",
            _rank(operation("MULTIPLY", ret10, _volume_trend())),
        ),
        (
            "N3_PRICE_VOLUME_BEHAVIOR_04",
            "PRICE_VOLUME_BEHAVIOR",
            "低区间波动下的10日动量",
            _rank(operation("SAFE_DIVIDE", ret10, range_ratio)),
        ),
        (
            "N3_MONEYFLOW_BEHAVIOR_01",
            "MONEYFLOW_BEHAVIOR",
            "主力资金5日相对20日加速",
            _rank(operation("SUBTRACT", field("mf_main_net_amt_ratio_5d"), field("mf_main_net_amt_ratio_20d"))),
        ),
        (
            "N3_MONEYFLOW_BEHAVIOR_02",
            "MONEYFLOW_BEHAVIOR",
            "超大单资金5日相对20日加速",
            _rank(operation("SUBTRACT", field("mf_elg_net_amt_ratio_5d"), field("mf_elg_net_amt_ratio_20d"))),
        ),
        (
            "N3_MONEYFLOW_BEHAVIOR_03",
            "MONEYFLOW_BEHAVIOR",
            "总净流入加速由换手确认",
            _rank(
                operation(
                    "MULTIPLY",
                    operation("SUBTRACT", field("mf_total_net_amt_ratio_5d"), field("mf_total_net_amt_ratio_20d")),
                    _zscore(field("db_turnover_rate")),
                )
            ),
        ),
        (
            "N3_MONEYFLOW_BEHAVIOR_04",
            "MONEYFLOW_BEHAVIOR",
            "超大单占主力比例与筹码胜率联合",
            _rank(operation("MULTIPLY", field("mf_elg_share_in_main_amt"), field("cp_winner_rate"))),
        ),
        (
            "N3_FUNDAMENTAL_CHANGE_01",
            "FUNDAMENTAL_CHANGE",
            "营收与利润同比联合质量",
            _rank(operation("ADD", _zscore(field("bb_rev_yoy")), _zscore(field("bb_profit_yoy")))),
        ),
        (
            "N3_FUNDAMENTAL_CHANGE_02",
            "FUNDAMENTAL_CHANGE",
            "毛利率与净利率联合质量",
            _rank(operation("ADD", _zscore(field("bb_gpr")), _zscore(field("bb_npr")))),
        ),
        (
            "N3_FUNDAMENTAL_CHANGE_03",
            "FUNDAMENTAL_CHANGE",
            "低PB与利润同比的交互",
            _rank(operation("MULTIPLY", field("value_pb_inv"), _zscore(field("bb_profit_yoy")))),
        ),
        (
            "N3_FUNDAMENTAL_CHANGE_04",
            "FUNDAMENTAL_CHANGE",
            "流动资产占比与营收同比联合",
            _rank(
                operation(
                    "MULTIPLY",
                    operation("SAFE_DIVIDE", field("bb_liquid_assets"), field("bb_total_assets")),
                    _zscore(field("bb_rev_yoy")),
                )
            ),
        ),
        (
            "N3_SECTOR_RELATIVE_01",
            "SECTOR_RELATIVE",
            "个股20日动量减行业20日收益",
            _rank(
                operation(
                    "SUBTRACT",
                    ret20,
                    operation("TRAILING_SUM", operation("SAFE_DIVIDE", field("sw2_pct_change"), const(100)), window=20),
                )
            ),
        ),
        (
            "N3_SECTOR_RELATIVE_02",
            "SECTOR_RELATIVE",
            "个股与行业5/20日成交量趋势差",
            _rank(
                operation(
                    "SUBTRACT",
                    _zscore(_volume_trend()),
                    _zscore(
                        operation(
                            "SAFE_DIVIDE",
                            operation("TRAILING_MEAN", field("sw2_vol"), window=5),
                            operation("TRAILING_MEAN", field("sw2_vol"), window=20),
                        )
                    ),
                )
            ),
        ),
        (
            "N3_SECTOR_RELATIVE_03",
            "SECTOR_RELATIVE",
            "个股主力流入相对行业净流入",
            _rank(
                operation(
                    "SUBTRACT",
                    _zscore(field("mf_main_net_amt_ratio_5d")),
                    _zscore(field("sw2_mf_net_amt")),
                )
            ),
        ),
        (
            "N3_SECTOR_RELATIVE_04",
            "SECTOR_RELATIVE",
            "个股PB逆数相对行业PB逆数",
            _rank(
                operation(
                    "SUBTRACT",
                    field("value_pb_inv"),
                    operation("SAFE_DIVIDE", const(1), field("sw2_pb")),
                )
            ),
        ),
        (
            "N3_CROWDING_DISPERSION_01",
            "CROWDING_DISPERSION",
            "60日换手拥挤程度反向",
            _rank(
                operation(
                    "MULTIPLY",
                    const(-1),
                    operation(
                        "SAFE_DIVIDE",
                        operation(
                            "SUBTRACT",
                            field("db_turnover_rate"),
                            operation("TRAILING_MEAN", field("db_turnover_rate"), window=60),
                        ),
                        operation("TRAILING_STD", field("db_turnover_rate"), window=60),
                    ),
                )
            ),
        ),
        (
            "N3_CROWDING_DISPERSION_02",
            "CROWDING_DISPERSION",
            "日收益波动之波动反向",
            _rank(
                operation(
                    "MULTIPLY",
                    const(-1),
                    operation("TRAILING_STD", operation("TRAILING_STD", ret1, window=5), window=20),
                )
            ),
        ),
        (
            "N3_CROWDING_DISPERSION_03",
            "CROWDING_DISPERSION",
            "成交量比率偏离1的绝对值反向",
            _rank(
                operation(
                    "MULTIPLY", const(-1), operation("ABS", operation("SUBTRACT", field("db_volume_ratio"), const(1)))
                )
            ),
        ),
        (
            "N3_CROWDING_DISPERSION_04",
            "CROWDING_DISPERSION",
            "筹码胜率与小单净买强度联合",
            _rank(
                operation(
                    "MULTIPLY",
                    field("cp_winner_rate"),
                    operation(
                        "SAFE_DIVIDE",
                        operation("SUBTRACT", field("mf_sm_buy_amt"), field("mf_sm_sell_amt")),
                        operation(
                            "ADD", operation("ABS", field("mf_sm_buy_amt")), operation("ABS", field("mf_sm_sell_amt"))
                        ),
                    ),
                )
            ),
        ),
        (
            "N3_REGIME_CONDITIONED_01",
            "REGIME_CONDITIONED",
            "上行市场中的20日动量",
            _rank(operation("MULTIPLY", ret20, operation("CLIP", field("market_regime"), lower=0, upper=1))),
        ),
        (
            "N3_REGIME_CONDITIONED_02",
            "REGIME_CONDITIONED",
            "下行市场中的5日反转",
            _rank(
                operation(
                    "MULTIPLY",
                    operation("MULTIPLY", const(-1), ret5),
                    operation("MULTIPLY", const(-1), operation("CLIP", field("market_regime"), lower=-1, upper=0)),
                )
            ),
        ),
        (
            "N3_REGIME_CONDITIONED_03",
            "REGIME_CONDITIONED",
            "下行市场中的主力资金加速",
            _rank(
                operation(
                    "MULTIPLY",
                    operation("SUBTRACT", field("mf_main_net_amt_ratio_5d"), field("mf_main_net_amt_ratio_20d")),
                    operation("MULTIPLY", const(-1), operation("CLIP", field("market_regime"), lower=-1, upper=0)),
                )
            ),
        ),
        (
            "N3_REGIME_CONDITIONED_04",
            "REGIME_CONDITIONED",
            "上行市场中的价值质量联合",
            _rank(
                operation(
                    "MULTIPLY",
                    operation("MULTIPLY", field("value_pb_inv"), _zscore(field("bb_profit_yoy"))),
                    operation("CLIP", field("market_regime"), lower=0, upper=1),
                )
            ),
        ),
    ]
    proposals: list[QEAlphaProposalV1] = []
    for proposal_id, family_name, hypothesis, expression in specs:
        stats = validate_expression(expression)
        proposals.append(
            QEAlphaProposalV1(
                proposal_id=proposal_id,
                family=family_name,
                economic_hypothesis=hypothesis,
                expression=expression,
                expression_sha256=canonical_json_sha256(expression),
                source_fields=tuple(sorted(stats["fields"])),
            )
        )
    return tuple(proposals)


def build_n3_route_receipt(**values: Any) -> AdvisoryN3RouteReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_n3_route_receipt_v1",
        "created_at": created_at,
        "minimum_required_recall": N3_ROUTE_MINIMUM_RECALL,
        "candidate_recall_state": "INSUFFICIENT",
        "n1_direction_ready": False,
        "entry_confirmatory_positive": False,
        "exit_learnability_high": False,
        "selected_route": N3_SELECTED_ROUTE,
        "active_main_line_count": 1,
        "active_auxiliary_line_count": 0,
        "reason_codes": (
            "ENTRY_HAS_NO_CONFIRMATORY_POSITIVE_ARM",
            "EXIT_FIXED_INFORMATION_NOT_CONFIRMED_LEARNABLE",
            "GLOBAL_WINNER_RECALL_INSUFFICIENT",
            "N1_RANKER_NOT_DIRECTION_READY",
        ),
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.EXPLORATORY_SCREEN,
        "decision_use": DecisionUse.DIRECTION_GATE,
        "planned_trial_count": 0,
        "sealed_holdout_accessed": False,
        "deployable": False,
        **values,
    }
    draft = AdvisoryN3RouteReceiptV1.model_construct(
        receipt_id="advn3route_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryN3RouteReceiptV1(
        receipt_id=f"advn3route_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


def build_qe_alpha_mve_request(**values: Any) -> FrozenAdvisoryQEAlphaMVERequestV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "frozen_advisory_qe_alpha_mve_request_v1",
        "created_at": created_at,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "study_type": ResearchStudyType.EXPLORATORY_SCREEN,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "planned_trial_count": MVE_PROPOSAL_COUNT,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "proposals": build_default_proposals(),
        "signal_start": MVE_SIGNAL_START,
        "signal_end": MVE_SIGNAL_END,
        "outcome_cutoff": MVE_OUTCOME_CUTOFF,
        "resource_max_rss_bytes": MVE_MAX_RSS_BYTES,
        "resource_max_temp_bytes": MVE_MAX_TEMP_BYTES,
        "resource_max_wall_seconds": None,
        "database_read_allowed": False,
        "network_read_allowed": False,
        "factor_catalog_write_allowed": False,
        "runtime_activation_allowed": False,
        "sealed_holdout_accessed": False,
        "deployable": False,
        **values,
    }
    draft = FrozenAdvisoryQEAlphaMVERequestV1.model_construct(
        request_id="advqemvereq_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return FrozenAdvisoryQEAlphaMVERequestV1(
        request_id=f"advqemvereq_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_qe_alpha_mve_receipt(**values: Any) -> AdvisoryQEAlphaMVEReceiptV1:
    created_at = values.pop("created_at", datetime.now(timezone.utc))
    payload = {
        "schema_version": "advisory_qe_alpha_mve_receipt_v1",
        "status": "COMPLETE",
        "planned_trial_count": 24,
        "generated_trial_count": 24,
        "evaluated_trial_count": 24,
        "result_class": ResearchResultClass.EXPLORATORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
        "created_at": created_at,
        **values,
    }
    draft = AdvisoryQEAlphaMVEReceiptV1.model_construct(
        receipt_id="advqemvercpt_" + "0" * 24,
        receipt_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(draft.functional_payload())
    return AdvisoryQEAlphaMVEReceiptV1(
        receipt_id=f"advqemvercpt_{digest[:24]}",
        receipt_sha256=digest,
        **payload,
    )


__all__ = [
    "ALLOWED_FIELDS",
    "AdvisoryN3RouteReceiptV1",
    "AdvisoryQEAlphaMVEReceiptV1",
    "DAILY_FIELDS",
    "DERIVED_FIELDS",
    "EXPRESSION_OPERATORS",
    "FrozenAdvisoryQEAlphaMVERequestV1",
    "MVE_FAMILIES",
    "N3_EXPERIMENT_ID",
    "N3_HYPOTHESIS_FAMILY_ID",
    "N3_SELECTED_ROUTE",
    "QEAlphaProposalV1",
    "STATIC_FIELDS",
    "build_default_proposals",
    "build_n3_route_receipt",
    "build_qe_alpha_mve_receipt",
    "build_qe_alpha_mve_request",
    "validate_expression",
]
