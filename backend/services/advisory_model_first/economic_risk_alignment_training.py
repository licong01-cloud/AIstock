"""One fixed entry-loss head; weight reuse stops on eligibility drift."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingRequestV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail
from backend.services.advisory_model_first.economic_entry_training import (
    EconomicEntryTrainingResult, prepare_economic_training_rows,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class EntryLossTrainingResultV2:
    return_model: Any
    risk_model: Any
    original_request: EconomicEntryTrainingRequestV1
    parent_return_model_sha256: str
    label_sha256: str
    feature_bounds: dict[str, tuple[float, float]]
    price_support: dict[int, dict[str, float | int]]
    diagnostics: dict[str, Any]
    split_receipt: pd.DataFrame


def prepare_entry_loss_training_v2(
    *, features: pd.DataFrame, labels: tuple[EntryLossLabelV2, ...], parent: EconomicEntryTrainingResult,
    expected_parent_rows_sha256: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    labels = tuple(EntryLossLabelV2.model_validate(label.model_dump()) for label in labels)
    rows = prepare_economic_training_rows(features=features, labels=tuple(value.original for value in labels),
                                         request=parent.request)
    if return_reuse_rows_sha256(rows, parent.request) != expected_parent_rows_sha256:
        _fail("v2 original fitted/calibrated matrix or return targets changed")
    risk_by_id = {value.original.episode_label_id: value for value in labels}
    if len(risk_by_id) != len(labels):
        _fail("v2 risk labels contain duplicate original IDs")
    # prepare_economic_training_rows joins by keys, not input order. Bind explicitly.
    risk_by_key = {(pd.Timestamp(value.original.decision_date), pd.Timestamp(value.original.target_date),
                    value.original.instrument): value for value in labels}
    ordered = [risk_by_key[key] for key in rows[KEY].itertuples(index=False, name=None)]
    rows["risk_status_v2"] = [value.status for value in ordered]
    rows["entry_net_max_loss_bps"] = [value.entry_net_max_loss_bps for value in ordered]
    rows["risk_reason_v2"] = [value.reason_code for value in ordered]
    rows["training_eligible_v2"] = rows.training_eligible & rows.risk_status_v2.eq("AVAILABLE")
    parent_receipt = parent.split_receipt.loc[:, KEY + ["split", "status", "training_eligible", "label_information_end"]]
    current = rows.loc[:, parent_receipt.columns]
    try:
        pd.testing.assert_frame_equal(current.sort_values(KEY).reset_index(drop=True),
                                      parent_receipt.sort_values(KEY).reset_index(drop=True), check_dtype=False)
    except AssertionError:
        _fail("v2 cannot reproduce the frozen parent split/eligibility receipt")
    differences = rows.loc[rows.split.isin(["train", "validation"]) &
                           rows.training_eligible.ne(rows.training_eligible_v2)]
    summary = {
        "weight_reuse_status": "BLOCKED_ELIGIBILITY_DRIFT" if len(differences) else "ELIGIBILITY_VERIFIED",
        "differences_by_split": {name: int(differences.split.eq(name).sum()) for name in ("train", "validation")},
        "risk_unavailable_by_reason": rows.loc[rows.risk_status_v2.eq("UNAVAILABLE"), "risk_reason_v2"].value_counts().to_dict(),
        "candidate_rows_preserved": len(rows), "original_return_targets_changed": False,
        "test_used_for_training_or_calibration": False,
        "decision_use": "NAVIGATION_ONLY", "deployable": False,
    }
    return rows, summary


def return_reuse_rows_sha256(rows: pd.DataFrame, request: EconomicEntryTrainingRequestV1) -> str:
    selected = rows.loc[rows.split.isin(["train", "validation"]) & rows.training_eligible,
                        KEY + ["split", "label_information_end", *request.feature_names, "return_target_bps"]]
    return canonical_json_sha256([
        {name: value.isoformat() if isinstance(value, pd.Timestamp) else value for name, value in row.items()}
        for row in selected.sort_values(KEY).to_dict("records")
    ])


def train_entry_loss_head_v2(
    *, features: pd.DataFrame, labels: tuple[EntryLossLabelV2, ...], parent: EconomicEntryTrainingResult,
    expected_parent_return_sha256: str, expected_parent_rows_sha256: str, expected_lightgbm_version: str,
) -> EntryLossTrainingResultV2:
    """Caller must register exact sources/code/config BEFORE invoking this function."""
    import hashlib
    import lightgbm as lgb

    if lgb.__version__ != expected_lightgbm_version:
        _fail("v2 runtime LightGBM version differs from frozen parent")
    rows, summary = prepare_entry_loss_training_v2(features=features, labels=labels, parent=parent,
                                                 expected_parent_rows_sha256=expected_parent_rows_sha256)
    if summary["weight_reuse_status"] != "ELIGIBILITY_VERIFIED":
        _fail("v2 risk eligibility differs; no return reuse, risk fit or silent cohort change",
              "ADVISORY_ENTRY_LOSS_REUSE_BLOCKED")
    if tuple(parent.return_model.feature_name()) != parent.request.feature_names:
        _fail("v2 parent return feature order differs")
    digest = hashlib.sha256(parent.return_model.model_to_string().encode("utf-8")).hexdigest()
    if digest != expected_parent_return_sha256:
        _fail("v2 parent return weights differ from registered identity")
    train = rows.loc[rows.split.eq("train") & rows.training_eligible_v2]
    validation = rows.loc[rows.split.eq("validation") & rows.training_eligible_v2]
    if len(train) < parent.request.minimum_bin_observations or validation.empty:
        _fail("v2 risk head lacks past-only mature train/validation")
    parameters = dict(parent.request.effective_parameters)
    rounds = parameters.pop("num_boost_round")
    parameters.pop("return_objective")
    parameters["objective"], parameters["alpha"] = parameters.pop("risk_objective"), parameters.pop("risk_alpha")
    model = lgb.train(parameters, lgb.Dataset(train.loc[:, parent.request.feature_names].astype(float),
                                            label=train.entry_net_max_loss_bps.astype(float)), num_boost_round=rounds)
    predicted = np.asarray(model.predict(validation.loc[:, parent.request.feature_names].astype(float), num_threads=2))
    if not np.isfinite(predicted).all() or (predicted < 0).any() or (predicted > 10000).any():
        _fail("v2 risk head returned invalid entry-loss estimates")
    diagnostics = {**summary, "train_rows": len(train), "validation_rows": len(validation),
                   "validation_entry_loss_q90_coverage": float(np.mean(validation.entry_net_max_loss_bps <= predicted)),
                   "uncertainty_semantics": parent.diagnostics["uncertainty_semantics"],
                   "validation_return_abs_error_p90_bps": parent.diagnostics["validation_return_abs_error_p90_bps"],
                   "economic_effectiveness": "NOT_EVALUATED", "new_risk_heads": 1, "new_return_heads": 0}
    return EntryLossTrainingResultV2(
        parent.return_model, model, parent.request, digest,
        canonical_json_sha256([value.model_dump(mode="json") for value in labels]),
        parent.feature_bounds, parent.price_support, diagnostics,
        rows.loc[:, KEY + ["split", "status", "training_eligible", "training_eligible_v2", "risk_status_v2",
                          "risk_reason_v2", "label_information_end"]].copy(),
    )
