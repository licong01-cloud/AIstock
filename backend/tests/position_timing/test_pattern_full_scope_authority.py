from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.action_value_corporate_actions import (
    CorporateAction,
    CorporateActionBook,
)
from backend.services.position_timing.action_value_data import file_reference
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256
from backend.services.position_timing.pattern_full_scope_authority import (
    ACCOUNT_CLASS,
    AUTHORITY_SCHEMA,
    EXPECTED_CLASSIFICATION_COUNTS,
    EXPECTED_CONSUMER_CONTRACT,
    apply_full_scope_corporate_action_authority,
    open_full_scope_corporate_action_authority,
)


class _Candidate:
    def __init__(
        self,
        root: Path,
        frames: dict[str, pd.DataFrame],
        references: dict[str, dict[str, object]],
    ) -> None:
        self.root = root
        self._frames = frames
        self.references = references
        self.spans = pd.DataFrame(
            {
                "symbol": list(frames),
                "start": [frame.index[0] for frame in frames.values()],
                "end": [pd.Timestamp("2020-01-03")] * len(frames),
            }
        )

    def bars(self, symbol: str) -> pd.DataFrame:
        return self._frames[symbol]


def _authority_fixture(tmp_path: Path):
    root = tmp_path / "candidate"
    root.mkdir()
    manifest = root / "qe_dataset_manifest.json"
    manifest.write_bytes(canonical_json_bytes({"schema_version": "fixture"}))
    manifest_reference = file_reference(manifest)
    classifications = (
        ["DUPLICATE_SOURCE_RECORD"]
        + ["SPECIAL_RESTRUCTURING_NON_PRO_RATA"] * 7
        + ["TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION"]
        + ["PREHISTORY_BOUNDARY_ACTION"] * 6
    )
    frames: dict[str, pd.DataFrame] = {}
    references: dict[str, dict[str, object]] = {}
    resolutions: list[dict[str, object]] = []
    for ordinal, classification in enumerate(classifications, start=1):
        symbol = f"{ordinal:06d}.SZ"
        factor_path = root / f"{symbol.lower()}.factor.bin"
        factor_path.write_bytes(f"factor-{symbol}".encode())
        references[f"{symbol}:factor"] = file_reference(factor_path)
        prehistory = classification == "PREHISTORY_BOUNDARY_ACTION"
        index = pd.to_datetime(["2020-01-02"] if prehistory else ["2020-01-01", "2020-01-02"])
        values = [1.1] if prehistory else [1.0, 1.1]
        frames[symbol] = pd.DataFrame({"factor": values}, index=index)
        source_row = {"ts_code": symbol, "ex_date": "2020-01-02", "ordinal": ordinal}
        if classification == "DUPLICATE_SOURCE_RECORD":
            replay = "APPLY_ACCOUNT_ACTION"
            account = {"quantity_multiplier": "1.1", "cash_yuan_per_share": "0"}
            reference = {"factor_ratio": "1.1"}
            accumulation = "ONE_CANONICAL_ACCOUNT_ACTION"
        elif classification == "SPECIAL_RESTRUCTURING_NON_PRO_RATA":
            replay = "REFERENCE_PRICE_ONLY"
            account = {"quantity_multiplier": "1", "cash_yuan_per_share": "0"}
            reference = {"factor_ratio": "1.1"}
            accumulation = "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT"
        elif classification == "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION":
            replay = "APPLY_ACCOUNT_ACTION_WITH_DISTINCT_REFERENCE_BASIS"
            account = {"quantity_multiplier": "1.2", "cash_yuan_per_share": "0.1"}
            reference = {
                "quantity_multiplier": "1.1",
                "cash_yuan_per_share": "0.05",
                "factor_ratio": "1.1",
            }
            accumulation = "APPLY_ELIGIBLE_ACCOUNT_BASIS_ONCE"
        else:
            replay = "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION"
            account = {"quantity_multiplier": "1.5", "cash_yuan_per_share": "0.1"}
            reference = None
            accumulation = "RETAIN_SOURCE_DO_NOT_REPLAY"
        evidence = {
            "factor_file": references[f"{symbol}:factor"],
            "first_valid_factor_date": "2020-01-02" if prehistory else "2020-01-01",
            "first_pit_eligible_date": "2020-01-02" if prehistory else "2020-01-01",
            "boundary_type": (
                "NO_PRE_ACTION_FACTOR_IN_CANDIDATE" if prehistory else "MATERIAL_FACTOR_INTERVAL"
            ),
            "previous_factor_date": None if prehistory else "2020-01-01",
            "current_factor_date": "2020-01-02",
            "observed_factor_ratio": None if prehistory else "1.1",
        }
        resolutions.append(
            {
                "symbol": symbol,
                "effective_trade_date": "2020-01-02",
                "classification": classification,
                "accumulation_policy": accumulation,
                "replay_application": replay,
                "account_class": ACCOUNT_CLASS,
                "account_economics": account,
                "reference_price_economics": reference,
                "source_row_count": 1,
                "source_rows_sha256": canonical_sha256([source_row]),
                "row_classifications": [
                    {
                        "source_row": source_row,
                        "source_row_sha256": canonical_sha256(source_row),
                        "classification": classification,
                        "may_accumulate_as_separate_account_action": False,
                    }
                ],
                "factor_boundary_evidence": evidence,
                "announcement_url": None,
                "source_record_key": f"market.dividend:{symbol}:2020-01-02",
            }
        )
    candidate = _Candidate(root, frames, references)
    identity = {
        "schema_version": AUTHORITY_SCHEMA,
        "request_id": "PT-NEXT-020",
        "candidate_manifest": manifest_reference,
        "account_class": ACCOUNT_CLASS,
        "scope": {
            "resolution_count": 15,
            "symbols": sorted(frames),
            "start": "2020-01-01",
            "end": "2020-01-03",
        },
        "classification_counts": dict(EXPECTED_CLASSIFICATION_COUNTS),
        "resolutions": resolutions,
        "captured_at": "2026-09-16T00:00:00+00:00",
        "consumer_contract": dict(EXPECTED_CONSUMER_CONTRACT),
        "safety": {
            "database_write_performed": False,
            "candidate_write_performed": False,
            "adj_factor_write_performed": False,
            "outcomes_read": False,
            "runtime_action_performed": False,
        },
    }
    payload = {**identity, "canonical_sha256": canonical_sha256(identity)}
    path = tmp_path / "authority.json"
    path.write_bytes(canonical_json_bytes(payload))
    return candidate, path, payload


def _open(candidate: _Candidate, path: Path, payload: dict[str, object]):
    return open_full_scope_corporate_action_authority(
        path,
        candidate=candidate,
        expected_candidate_manifest_sha256=file_reference(
            candidate.root / "qe_dataset_manifest.json"
        )["sha256"],
        expected_authority_canonical_sha256=str(payload["canonical_sha256"]),
        expected_authority_file_sha256=file_reference(path)["sha256"],
        expected_start=date(2020, 1, 1),
        expected_end=date(2020, 1, 3),
    )


def test_reader_and_adapter_apply_all_typed_semantics(tmp_path: Path) -> None:
    candidate, path, payload = _authority_fixture(tmp_path)
    authority = _open(candidate, path, payload)
    actions = tuple(
        CorporateAction(
            symbol=item.symbol,
            effective_trade_date=item.effective_trade_date,
            quantity_multiplier=Decimal("2"),
            cashflow_yuan_per_share=Decimal("0.2"),
            reference_price_cash_yuan_per_share=Decimal("0.2"),
            cash_pay_date=item.effective_trade_date,
            share_listing_date=item.effective_trade_date,
            source_available_at=datetime(2019, 12, 31, tzinfo=timezone.utc),
            source_row_count=item.source_row_count,
            source_rows_sha256="f" * 64,
        )
        for item in authority.resolutions
    )
    applied, audit = apply_full_scope_corporate_action_authority(
        CorporateActionBook(actions, "e" * 64),
        authority,
        candidate_source_sha256="c" * 64,
    )
    assert len(applied.actions) == 9
    assert audit["excluded_prehistory_action_count"] == 6
    assert audit["duplicate_economic_accumulation_count"] == 0
    duplicate = applied.on("000001.SZ", date(2020, 1, 2))
    assert duplicate is not None and str(duplicate.quantity_multiplier) == "1.1"
    for ordinal in range(2, 9):
        restructure = applied.on(f"{ordinal:06d}.SZ", date(2020, 1, 2))
        assert restructure is not None and str(restructure.quantity_multiplier) == "1"
    dual_basis = applied.on("000009.SZ", date(2020, 1, 2))
    assert dual_basis is not None
    assert str(dual_basis.cashflow_yuan_per_share) == "0.1"
    assert str(dual_basis.reference_price_cash_yuan_per_share) == "0.05"
    assert authority.resolution_audit["unresolved_typed_resolution_count"] == 0


@pytest.mark.parametrize(
    "mutate,reason",
    [
        (lambda payload: payload.update(schema_version="wrong"), "IDENTITY_MISMATCH"),
        (
            lambda payload: payload["classification_counts"].update(
                SPECIAL_RESTRUCTURING_NON_PRO_RATA=6
            ),
            "IDENTITY_MISMATCH",
        ),
        (
            lambda payload: payload["resolutions"][0].update(
                source_rows_sha256="0" * 64
            ),
            "SOURCE_ROWS_INVALID",
        ),
        (
            lambda payload: payload["resolutions"][0]["factor_boundary_evidence"].update(
                current_factor_date="2020-01-03"
            ),
            "FACTOR_EVIDENCE_INVALID",
        ),
    ],
)
def test_reader_fails_closed_on_authority_drift(
    tmp_path: Path, mutate, reason: str
) -> None:
    candidate, path, payload = _authority_fixture(tmp_path)
    mutate(payload)
    payload["canonical_sha256"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "canonical_sha256"}
    )
    path.write_bytes(canonical_json_bytes(payload))
    with pytest.raises(ActionValueError, match=reason):
        _open(candidate, path, payload)
