from __future__ import annotations

import copy
import hashlib
import inspect
from dataclasses import dataclass
from datetime import date

import pandas as pd
import pytest

from backend.services.hmm_risk import qe_assistance_adapter as subject
from scripts.hmm_risk import build_qe_assistance_artifact as cli

MODEL_HASH = "a" * 64
AUTHORITY_HASH = "b" * 64
ROW_HASH = "c" * 64
MODEL_CONTRACT = "hmm_risk_rotation_l1_g2a_v1_6"


@dataclass(frozen=True)
class Projection:
    status: str
    canonical_symbol: str
    trade_date: date
    l1_code: str | None
    reason_code: str | None
    classification_receipt_hash: str = AUTHORITY_HASH
    index_membership_receipt_hash: str = AUTHORITY_HASH
    classification_row_hashes: tuple[str, ...] = (ROW_HASH,)
    index_membership_row_hashes: tuple[str, ...] = (ROW_HASH,)


class Adapter:
    def __init__(self, *, unavailable: set[tuple[date, str]] | None = None, bad_reason: bool = False):
        self.unavailable = unavailable or set()
        self.bad_reason = bad_reason

    def resolve(self, symbol: str, trade_date: date) -> Projection:
        if (trade_date, symbol) in self.unavailable:
            return Projection(
                status="unavailable",
                canonical_symbol=symbol,
                trade_date=trade_date,
                l1_code=None,
                reason_code=(
                    "classification:membership_boundary_unavailable"
                    if self.bad_reason
                    else subject.APPROVED_UNAVAILABLE_REASON
                ),
                classification_row_hashes=(),
                index_membership_row_hashes=(),
            )
        return Projection(
            status="resolved",
            canonical_symbol=symbol,
            trade_date=trade_date,
            l1_code="801010.SI",
            reason_code=None,
        )


def _sectors() -> list[str]:
    return [f"801{index:03d}.SI" for index in range(10, 41)]


def _states(days: tuple[date, ...]) -> list[dict]:
    states = ("fading", "neutral", "trending")
    return [
        {
            "trade_date": day.isoformat(),
            "sector_code": sector,
            "availability": "available",
            "reason_code": None,
            "forecast_state": states[index % 3],
            "model_hash": MODEL_HASH,
            "model_contract": MODEL_CONTRACT,
        }
        for day in days
        for index, sector in enumerate(_sectors())
    ]


def _rows() -> list[dict]:
    return [
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000001.SZ", "score": 2.0},
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000002.SZ", "score": -2.0},
        {"source_date": "2024-07-02", "trade_date": "2024-07-03", "instrument": "000001.SZ", "score": 0.0},
    ]


def _build(*, rows=None, states=None, adapter=None, calendar=None):
    return subject._build_qe_assistance_artifact(
        raw_prediction_rows=rows if rows is not None else _rows(),
        state_rows=states if states is not None else _states((date(2024, 7, 2), date(2024, 7, 3))),
        calendar=calendar if calendar is not None else (date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)),
        industry_adapter=adapter if adapter is not None else Adapter(),
        source_model_contract=MODEL_CONTRACT,
        model_hash=MODEL_HASH,
        source_mapping_sha256=subject.EXPECTED_MAPPING_SHA256,
        source_prediction_file_sha256=subject.EXPECTED_SOURCE_FILE_SHA256,
        authority_identity={"bundle_hash": subject.EXPECTED_AUTHORITY_BUNDLE_HASH},
        canonical_l1_codes=_sectors(),
        window_start=date(2024, 7, 2),
        window_end=date(2024, 7, 3),
        formal_cardinality=None,
    )


def test_sign_safe_adjustment_handles_positive_negative_and_zero_scores() -> None:
    assert subject.apply_sign_safe_adjustment(2.0, 1.02) == pytest.approx(2.04)
    assert subject.apply_sign_safe_adjustment(-2.0, 1.02) == pytest.approx(-1.96)
    assert subject.apply_sign_safe_adjustment(2.0, 0.98) == pytest.approx(1.96)
    assert subject.apply_sign_safe_adjustment(-2.0, 0.98) == pytest.approx(-2.04)
    assert subject.apply_sign_safe_adjustment(0.0, 1.02) == 0.0


def test_artifact_binds_model_mapping_source_window_and_formula_hashes() -> None:
    artifact = _build()
    subject.validate_qe_assistance_artifact(artifact)
    assert artifact["schema_version"] == subject.SCHEMA_VERSION
    assert artifact["source_mapping_sha256"] == subject.EXPECTED_MAPPING_SHA256
    assert artifact["source_prediction_file_sha256"] == subject.EXPECTED_SOURCE_FILE_SHA256
    assert artifact["model_hash"] == MODEL_HASH
    assert artifact["adapter_formula"]["sha256"] == subject.FORMULA_SHA256
    assert artifact["date_count"] == 2
    assert all(len(value) == 31 for value in artifact["daily_coefficients"].values())


def test_explicit_authority_unavailable_is_not_missing_or_neutral() -> None:
    key = (date(2024, 7, 2), "000002.SZ")
    artifact = _build(adapter=Adapter(unavailable={key}))
    entry = artifact["stock_sector_applicability_by_date"]["2024-07-02"]["000002.SZ"]
    assert entry == {
        "status": subject.NOT_APPLICABLE,
        "sector_code": None,
        "reason_code": subject.APPROVED_UNAVAILABLE_REASON,
        "adjustment_applied": False,
        "classification_receipt_hash": AUTHORITY_HASH,
        "index_membership_receipt_hash": AUTHORITY_HASH,
        "classification_row_hashes": [],
        "index_membership_row_hashes": [],
    }
    assert artifact["not_applicable_row_count"] == 1
    assert artifact["applied_row_count"] == 2
    raw = -2.0
    assert subject.apply_artifact_entry(raw, entry, artifact["daily_coefficients"]["2024-07-02"]) == raw


def test_applied_entry_uses_sign_safe_formula_and_missing_entry_fails() -> None:
    artifact = _build()
    entry = artifact["stock_sector_applicability_by_date"]["2024-07-02"]["000001.SZ"]
    coefficients = artifact["daily_coefficients"]["2024-07-02"]
    assert subject.apply_artifact_entry(-2.0, entry, coefficients) == pytest.approx(-2.04)
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        subject.apply_artifact_entry(1.0, {}, coefficients)
    assert exc_info.value.reason_code == subject.REASON_MAPPING


def test_input_row_order_does_not_change_canonical_artifact() -> None:
    first = _build()
    second = _build(rows=list(reversed(_rows())), states=list(reversed(_states((date(2024, 7, 2), date(2024, 7, 3))))))
    assert first == second


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda rows: rows + [dict(rows[0])], subject.REASON_INPUT),
        (
            lambda rows: [{**rows[0], "trade_date": "2024-07-03"}, *rows[1:]],
            subject.REASON_CALENDAR,
        ),
        (
            lambda rows: [{**rows[0], "score": float("nan")}, *rows[1:]],
            subject.REASON_INPUT,
        ),
    ],
)
def test_duplicate_calendar_drift_and_non_finite_score_fail_closed(mutate, reason) -> None:
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        _build(rows=mutate(_rows()))
    assert exc_info.value.reason_code == reason


def test_unknown_unavailable_reason_fails_closed() -> None:
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        _build(
            adapter=Adapter(
                unavailable={(date(2024, 7, 2), "000002.SZ")},
                bad_reason=True,
            )
        )
    assert exc_info.value.reason_code == subject.REASON_MAPPING


def test_missing_or_unknown_state_fails_closed() -> None:
    states = _states((date(2024, 7, 2), date(2024, 7, 3)))
    states.pop()
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        _build(states=states)
    assert exc_info.value.reason_code == subject.REASON_STATE

    states = _states((date(2024, 7, 2), date(2024, 7, 3)))
    states[0]["forecast_state"] = "unknown"
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        _build(states=states)
    assert exc_info.value.reason_code == subject.REASON_STATE


def test_artifact_readback_rejects_missing_key_unknown_status_and_hash_drift() -> None:
    artifact = _build()
    missing = copy.deepcopy(artifact)
    del missing["stock_sector_applicability_by_date"]["2024-07-02"]["000001.SZ"]
    missing_body = {key: value for key, value in missing.items() if key != "artifact_sha256"}
    missing["artifact_sha256"] = subject._sha256(missing_body)
    with pytest.raises(subject.QEAssistanceContractError, match="counts differ"):
        subject.validate_qe_assistance_artifact(missing)

    unknown = copy.deepcopy(artifact)
    unknown["stock_sector_applicability_by_date"]["2024-07-02"]["000001.SZ"]["status"] = "neutral"
    unknown_body = {key: value for key, value in unknown.items() if key != "artifact_sha256"}
    unknown["artifact_sha256"] = subject._sha256(unknown_body)
    with pytest.raises(subject.QEAssistanceContractError, match="status is unknown"):
        subject.validate_qe_assistance_artifact(unknown)

    drift = copy.deepcopy(artifact)
    drift["model_hash"] = "d" * 64
    with pytest.raises(subject.QEAssistanceContractError, match="canonical hash differs"):
        subject.validate_qe_assistance_artifact(drift)


def test_formal_builder_rejects_sample_cardinality_and_tail() -> None:
    assert "formal_cardinality" not in inspect.signature(subject.build_qe_assistance_artifact).parameters
    assert "window_start" not in inspect.signature(subject.build_qe_assistance_artifact).parameters
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        subject.build_qe_assistance_artifact(
            raw_prediction_rows=_rows(),
            state_rows=_states((date(2024, 7, 2), date(2024, 7, 3))),
            calendar=(date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)),
            industry_adapter=Adapter(),
            source_model_contract=MODEL_CONTRACT,
            model_hash=MODEL_HASH,
            source_mapping_sha256=subject.EXPECTED_MAPPING_SHA256,
            source_prediction_file_sha256=subject.EXPECTED_SOURCE_FILE_SHA256,
            authority_identity={"bundle_hash": subject.EXPECTED_AUTHORITY_BUNDLE_HASH},
            canonical_l1_codes=_sectors(),
        )
    assert exc_info.value.reason_code == subject.REASON_MAPPING

    rows = [{"source_date": "2026-03-31", "trade_date": "2026-04-01", "instrument": "000001.SZ", "score": 1.0}]
    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        _build(
            rows=rows,
            states=_states((date(2026, 4, 1),)),
            calendar=(date(2026, 3, 31), date(2026, 4, 1)),
        )
    assert exc_info.value.reason_code == subject.REASON_INPUT


def test_cli_filters_frozen_pickle_and_v16_rows_to_approved_window(tmp_path, monkeypatch) -> None:
    prediction_path = tmp_path / "pred.pkl"
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-07-01"), "SZ000001"),
            (pd.Timestamp("2024-07-02"), "SH600000"),
            (pd.Timestamp("2026-04-01"), "SZ000002"),
        ]
    )
    pd.DataFrame({"score": [1.0, -1.0, 3.0]}, index=index).to_pickle(prediction_path)
    monkeypatch.setattr(cli, "EXPECTED_SOURCE_FILE_SHA256", hashlib.sha256(prediction_path.read_bytes()).hexdigest())
    rows = cli._prediction_rows(
        prediction_path,
        ["2024-07-01", "2024-07-02", "2024-07-03", "2026-04-01", "2026-04-02"],
    )
    assert rows == [
        {
            "source_date": "2024-07-01",
            "trade_date": "2024-07-02",
            "instrument": "000001.SZ",
            "score": 1.0,
        },
        {
            "source_date": "2024-07-02",
            "trade_date": "2024-07-03",
            "instrument": "600000.SH",
            "score": -1.0,
        },
    ]

    report_rows = [
        {"trade_date": "2024-07-01", "model_hash": MODEL_HASH},
        {"trade_date": "2024-07-02", "model_hash": MODEL_HASH},
        {"trade_date": "2026-04-01", "model_hash": MODEL_HASH},
    ]
    states, model_hash = cli._state_rows({"oof_prediction_rows": report_rows}, MODEL_CONTRACT)
    assert states == [{**report_rows[1], "model_contract": MODEL_CONTRACT}]
    assert model_hash == MODEL_HASH
