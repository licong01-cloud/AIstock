from __future__ import annotations

import copy
import inspect
from dataclasses import dataclass
from datetime import date

import pytest

from backend.services.hmm_risk import qe_assistance_adapter as subject

MODEL_HASH = subject.EXPECTED_MODEL_HASH
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
    def __init__(
        self,
        *,
        unavailable: set[tuple[date, str]] | None = None,
        bad_reason: bool = False,
        sector_by_symbol: dict[str, str] | None = None,
        resolved_index_lineage: bool = True,
    ):
        self.unavailable = unavailable or set()
        self.bad_reason = bad_reason
        self.sector_by_symbol = sector_by_symbol or {}
        self.resolved_index_lineage = resolved_index_lineage

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
            l1_code=self.sector_by_symbol.get(symbol, "801010.SI"),
            reason_code=None,
            index_membership_row_hashes=(ROW_HASH,) if self.resolved_index_lineage else (),
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


def test_resolved_classification_does_not_fabricate_index_membership_lineage() -> None:
    artifact = _build(adapter=Adapter(resolved_index_lineage=False))
    subject.validate_qe_assistance_artifact(artifact)
    entry = artifact["stock_sector_applicability_by_date"]["2024-07-02"]["000001.SZ"]
    assert entry["classification_row_hashes"] == [ROW_HASH]
    assert entry["index_membership_row_hashes"] == []


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


def test_consumer_effect_reports_score_rank_and_topk_changes_without_labels() -> None:
    rows = [
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000001.SZ", "score": 1.0},
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000002.SZ", "score": 0.99},
    ]
    calendar = (date(2024, 7, 1), date(2024, 7, 2))
    artifact = _build(
        rows=rows,
        states=_states((date(2024, 7, 2),)),
        calendar=calendar,
        adapter=Adapter(sector_by_symbol={"000001.SZ": "801010.SI", "000002.SZ": "801012.SI"}),
    )
    first = subject._evaluate_consumer_effect(
        raw_prediction_rows=rows,
        calendar=calendar,
        artifact=artifact,
        topk=1,
        formal_cardinality=None,
    )
    second = subject._evaluate_consumer_effect(
        raw_prediction_rows=list(reversed(rows)),
        calendar=calendar,
        artifact=artifact,
        topk=1,
        formal_cardinality=None,
    )

    assert first == second
    assert first["status"] == subject.CONSUMER_EFFECT_OBSERVED
    assert first["changed_score_count"] == 2
    assert first["rank_changed_row_count"] == 2
    assert first["topk_changed_date_count"] == 1
    assert first["topk_entered_count"] == 1
    assert first["topk_dropped_count"] == 1
    assert first["daily_summary"][0]["topk_entered_symbols"] == ["000002.SZ"]
    assert first["daily_summary"][0]["topk_dropped_symbols"] == ["000001.SZ"]
    assert first["raw_topk_not_applicable_count"] == 0
    assert first["tail_accessed"] is False
    assert first["database_write_performed"] is False
    assert first["runtime_action_performed"] is False


def test_consumer_effect_no_change_and_prediction_identity_fail_closed() -> None:
    rows = [
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000001.SZ", "score": 1.0},
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000002.SZ", "score": 0.99},
    ]
    calendar = (date(2024, 7, 1), date(2024, 7, 2))
    artifact = _build(
        rows=rows,
        states=_states((date(2024, 7, 2),)),
        calendar=calendar,
        adapter=Adapter(sector_by_symbol={"000001.SZ": "801011.SI", "000002.SZ": "801011.SI"}),
    )
    result = subject._evaluate_consumer_effect(
        raw_prediction_rows=rows,
        calendar=calendar,
        artifact=artifact,
        topk=1,
        formal_cardinality=None,
    )
    assert result["status"] == subject.NO_CONSUMER_EFFECT
    assert result["changed_score_count"] == 0
    assert result["rank_changed_row_count"] == 0
    assert result["topk_changed_date_count"] == 0

    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        subject._evaluate_consumer_effect(
            raw_prediction_rows=rows[:-1],
            calendar=calendar,
            artifact=artifact,
            topk=1,
            formal_cardinality=None,
        )
    assert exc_info.value.reason_code == subject.REASON_REPLAY

    with pytest.raises(subject.QEAssistanceContractError) as exc_info:
        subject.evaluate_consumer_effect(
            raw_prediction_rows=rows,
            calendar=calendar,
            artifact=artifact,
        )
    assert exc_info.value.reason_code == subject.REASON_REPLAY


def test_consumer_effect_preserves_explicit_non_applicable_rows() -> None:
    rows = [
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000001.SZ", "score": 1.0},
        {"source_date": "2024-07-01", "trade_date": "2024-07-02", "instrument": "000002.SZ", "score": 2.0},
    ]
    calendar = (date(2024, 7, 1), date(2024, 7, 2))
    artifact = _build(
        rows=rows,
        states=_states((date(2024, 7, 2),)),
        calendar=calendar,
        adapter=Adapter(unavailable={(date(2024, 7, 2), "000002.SZ")}),
    )
    result = subject._evaluate_consumer_effect(
        raw_prediction_rows=rows,
        calendar=calendar,
        artifact=artifact,
        topk=1,
        formal_cardinality=None,
    )
    assert result["applied_row_count"] == 1
    assert result["not_applicable_row_count"] == 1
    assert result["changed_score_count"] == 1
    assert result["raw_topk_not_applicable_count"] == 1
    assert result["raw_topk_not_applicable_date_count"] == 1
    assert result["daily_summary"][0]["not_applicable_row_count"] == 1


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

    rehashed_drift = copy.deepcopy(artifact)
    rehashed_drift["model_hash"] = "d" * 64
    rehashed_body = {key: value for key, value in rehashed_drift.items() if key != "artifact_sha256"}
    rehashed_drift["artifact_sha256"] = subject._sha256(rehashed_body)
    with pytest.raises(subject.QEAssistanceContractError, match="fixed identity differs"):
        subject.validate_qe_assistance_artifact(rehashed_drift)


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


def test_streamed_sequence_hash_matches_canonical_list_hash() -> None:
    values = [["2024-07-01", "2024-07-02", "000001.SZ", 1.0], ["中文", None, -0.0]]
    assert subject._sequence_sha256(iter(values)) == subject._sha256(values)
