from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import comb, log
from typing import Any, Iterable

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicySplitV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class PolicyCPCVResult:
    paths: tuple[dict[str, Any], ...]
    block_by_date: dict[str, int]


def build_policy_cpcv_paths(
    labels: pd.DataFrame,
    *,
    split_policy: AdvisoryPolicySplitV1,
    trading_calendar: Iterable[pd.Timestamp],
    request_sha256: str,
) -> PolicyCPCVResult:
    required = {
        "decision_as_of_trade_date",
        "label_information_start",
        "label_information_end",
        "label_status",
        "take_label",
    }
    if not required.issubset(labels.columns):
        raise AdvisoryModelFirstError(
            "policy labels omit CPCV columns",
            reason_code="ADVISORY_POLICY_CPCV_INVALID",
            context={"missing_columns": sorted(required - set(labels.columns))},
        )
    matured = labels.loc[labels["label_status"] == "MATURED"].copy()
    matured["decision_as_of_trade_date"] = pd.to_datetime(matured["decision_as_of_trade_date"]).dt.normalize()
    matured["label_information_start"] = pd.to_datetime(matured["label_information_start"]).dt.normalize()
    matured["label_information_end"] = pd.to_datetime(matured["label_information_end"]).dt.normalize()
    invalid_intervals = (
        matured["label_information_start"].isna()
        | matured["label_information_end"].isna()
        | (matured["label_information_end"] < matured["label_information_start"])
    )
    if invalid_intervals.any():
        raise AdvisoryModelFirstError(
            "mature policy labels contain invalid information intervals",
            reason_code="ADVISORY_POLICY_CPCV_INVALID",
            context={"invalid_row_count": int(invalid_intervals.sum())},
        )
    decisions = pd.DatetimeIndex(matured["decision_as_of_trade_date"].unique()).sort_values()
    if len(decisions) < split_policy.group_count:
        return PolicyCPCVResult(
            paths=(_not_computable_path("INSUFFICIENT_MATURE_DECISION_DATES", request_sha256),),
            block_by_date={},
        )
    blocks = [pd.DatetimeIndex(values) for values in np.array_split(decisions, split_policy.group_count)]
    if any(len(values) == 0 for values in blocks):
        return PolicyCPCVResult(
            paths=(_not_computable_path("EMPTY_TIME_BLOCK", request_sha256),),
            block_by_date={},
        )
    block_by_timestamp = {value: index for index, values in enumerate(blocks) for value in values}
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    calendar_pos = {value: index for index, value in enumerate(calendar)}
    paths: list[dict[str, Any]] = []
    for validation_blocks in combinations(range(split_policy.group_count), split_policy.validation_group_count):
        validation_dates = pd.DatetimeIndex(
            sorted(value for block in validation_blocks for value in blocks[block])
        )
        validation_mask = matured["decision_as_of_trade_date"].isin(validation_dates)
        validation = matured.loc[validation_mask]
        train = matured.loc[~validation_mask].copy()
        purge_mask = _information_overlap(train, validation)
        embargo_dates = _embargo_dates(
            validation_dates,
            calendar=calendar,
            calendar_pos=calendar_pos,
            days=split_policy.embargo_trading_days,
        )
        embargo_mask = train["decision_as_of_trade_date"].isin(embargo_dates)
        retained = train.loc[~purge_mask & ~embargo_mask]
        status, reason = _path_status(retained, validation)
        functional = {
            "schema_version": "advisory_policy_cpcv_path_v1",
            "request_sha256": request_sha256,
            "validation_blocks": list(validation_blocks),
            "train_dates": _dates(retained["decision_as_of_trade_date"]),
            "validation_dates": _dates(validation["decision_as_of_trade_date"]),
            "purged_dates": _dates(train.loc[purge_mask, "decision_as_of_trade_date"]),
            "embargo_dates": _dates(train.loc[embargo_mask, "decision_as_of_trade_date"]),
            "status": status,
            "reason_code": reason,
        }
        paths.append(
            {
                **functional,
                "path_id": "advpcpv_" + canonical_json_sha256(functional)[:24],
                "train_row_count": int(len(retained)),
                "validation_row_count": int(len(validation)),
                "train_positive_count": int((retained["take_label"] == 1).sum()),
                "validation_positive_count": int((validation["take_label"] == 1).sum()),
            }
        )
    expected = comb(split_policy.group_count, split_policy.validation_group_count)
    if len(paths) != expected:
        raise AdvisoryModelFirstError(
            "CPCV path enumeration is incomplete",
            reason_code="ADVISORY_POLICY_CPCV_INVALID",
        )
    return PolicyCPCVResult(
        paths=tuple(paths),
        block_by_date={value.date().isoformat(): block for value, block in block_by_timestamp.items()},
    )


def calculate_policy_pbo(
    trial_block_scores: pd.DataFrame,
    *,
    group_count: int,
    metric_column: str = "mean_net_excess_return_bps",
) -> dict[str, Any]:
    required = {"trial_id", "block_id", metric_column}
    if not required.issubset(trial_block_scores.columns):
        raise AdvisoryModelFirstError(
            "PBO score matrix omits required columns",
            reason_code="ADVISORY_POLICY_PBO_INVALID",
            context={"missing_columns": sorted(required - set(trial_block_scores.columns))},
        )
    if group_count < 4 or group_count % 2:
        raise ValueError("PBO group_count must be even and at least four")
    frame = trial_block_scores[["trial_id", "block_id", metric_column]].copy()
    frame["trial_id"] = frame["trial_id"].astype(str)
    frame["block_id"] = pd.to_numeric(frame["block_id"], errors="raise").astype(int)
    frame[metric_column] = pd.to_numeric(frame[metric_column], errors="coerce")
    trials = tuple(sorted(frame["trial_id"].unique()))
    expected_blocks = set(range(group_count))
    if len(trials) < 2:
        return _pbo_not_computable("NOT_COMPUTABLE_INSUFFICIENT_TRIALS", len(trials), group_count)
    if frame.duplicated(["trial_id", "block_id"]).any():
        raise AdvisoryModelFirstError(
            "PBO score matrix has duplicate trial-block rows",
            reason_code="ADVISORY_POLICY_PBO_INVALID",
        )
    if set(frame["block_id"]) != expected_blocks or frame[metric_column].isna().any():
        return _pbo_not_computable("NOT_COMPUTABLE_INCOMPLETE_BLOCK_MATRIX", len(trials), group_count)
    if any(set(group["block_id"]) != expected_blocks for _, group in frame.groupby("trial_id")):
        return _pbo_not_computable("NOT_COMPUTABLE_INCOMPLETE_BLOCK_MATRIX", len(trials), group_count)
    matrix = frame.pivot(index="block_id", columns="trial_id", values=metric_column).reindex(
        index=range(group_count), columns=list(trials)
    )
    records: list[dict[str, Any]] = []
    half = group_count // 2
    for in_blocks in combinations(range(group_count), half):
        out_blocks = tuple(sorted(expected_blocks - set(in_blocks)))
        in_scores = matrix.loc[list(in_blocks)].mean(axis=0)
        winner = sorted(trials, key=lambda item: (-float(in_scores[item]), item))[0]
        out_scores = matrix.loc[list(out_blocks)].mean(axis=0)
        ordered = sorted(trials, key=lambda item: (float(out_scores[item]), item))
        rank = ordered.index(winner) + 1
        percentile = (rank - 0.5) / len(trials)
        logit = log(percentile / (1.0 - percentile))
        records.append(
            {
                "in_sample_blocks": list(in_blocks),
                "out_of_sample_blocks": list(out_blocks),
                "selected_trial_id": winner,
                "out_of_sample_rank": rank,
                "out_of_sample_percentile": percentile,
                "out_of_sample_logit": logit,
            }
        )
    return {
        "schema_version": "advisory_policy_pbo_receipt_v1",
        "status": "COMPUTED",
        "method": "advisory_block_score_cscv_pbo_v1",
        "metric": metric_column,
        "tie_break": "trial_id_ascending",
        "trial_count": len(trials),
        "group_count": group_count,
        "partition_count": len(records),
        "pbo": float(np.mean([record["out_of_sample_logit"] < 0 for record in records])),
        "partitions": records,
    }


def _information_overlap(train: pd.DataFrame, validation: pd.DataFrame) -> pd.Series:
    distinct = (
        validation[["label_information_start", "label_information_end"]]
        .drop_duplicates()
        .sort_values(["label_information_start", "label_information_end"])
    )
    merged: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in distinct.itertuples(index=False, name=None):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    overlap = pd.Series(False, index=train.index)
    starts = train["label_information_start"]
    ends = train["label_information_end"]
    for validation_start, validation_end in merged:
        overlap |= (starts <= validation_end) & (ends >= validation_start)
    return overlap


def _embargo_dates(
    validation_dates: pd.DatetimeIndex,
    *,
    calendar: pd.DatetimeIndex,
    calendar_pos: dict[pd.Timestamp, int],
    days: int,
) -> set[pd.Timestamp]:
    result: set[pd.Timestamp] = set()
    for value in validation_dates:
        position = calendar_pos.get(value)
        if position is None:
            raise AdvisoryModelFirstError(
                "CPCV validation date is absent from the trading calendar",
                reason_code="ADVISORY_POLICY_CPCV_INVALID",
            )
        lower = max(0, position - days)
        upper = min(len(calendar), position + days + 1)
        result.update(calendar[lower:upper])
    return result


def _path_status(train: pd.DataFrame, validation: pd.DataFrame) -> tuple[str, str | None]:
    if train.empty or validation.empty:
        return "NOT_COMPUTABLE", "EMPTY_TRAIN_OR_VALIDATION"
    if train["decision_as_of_trade_date"].nunique() < 2 or validation["decision_as_of_trade_date"].nunique() < 1:
        return "NOT_COMPUTABLE", "INSUFFICIENT_DECISION_GROUPS"
    if train["take_label"].nunique() < 2 or validation["take_label"].nunique() < 2:
        return "NOT_COMPUTABLE", "SINGLE_CLASS_TAKE_LABEL"
    return "READY", None


def _dates(values: pd.Series) -> list[str]:
    return sorted({pd.Timestamp(value).date().isoformat() for value in values})


def _not_computable_path(reason: str, request_sha256: str) -> dict[str, Any]:
    payload = {
        "schema_version": "advisory_policy_cpcv_path_v1",
        "request_sha256": request_sha256,
        "status": "NOT_COMPUTABLE",
        "reason_code": reason,
    }
    return {**payload, "path_id": "advpcpv_" + canonical_json_sha256(payload)[:24]}


def _pbo_not_computable(reason: str, trial_count: int, group_count: int) -> dict[str, Any]:
    return {
        "schema_version": "advisory_policy_pbo_receipt_v1",
        "status": "NOT_COMPUTABLE",
        "reason_code": reason,
        "method": "advisory_block_score_cscv_pbo_v1",
        "trial_count": trial_count,
        "group_count": group_count,
        "partition_count": 0,
        "pbo": None,
        "partitions": [],
    }
