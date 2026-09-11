from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.action_value import ActionValueError, PositionState, TZ, cutoff_on
from backend.services.position_timing.action_value_corporate_actions import (
    AVAILABILITY_SNAPSHOT_SCHEMA,
    CorporateAction,
    CorporateActionBook,
    IDENTITY_SNAPSHOT_SCHEMA,
    LEGACY_SNAPSHOT_SCHEMA,
    SAME_DAY_SNAPSHOT_SCHEMA,
    _snapshot_payload,
    apply_corporate_action,
    apply_corporate_action_with_audit,
    freeze_corporate_action_snapshot,
)
from backend.services.position_timing.contracts import canonical_json_bytes, canonical_sha256


def _row(**overrides):
    values = {
        "symbol": "000001.SZ",
        "end_date": date(2025, 12, 31),
        "ann_date": date(2026, 5, 1),
        "imp_ann_date": date(2026, 5, 20),
        "ex_date": date(2026, 5, 27),
        "stk_div": Decimal("0.3"),
        "stk_bo_rate": Decimal("0.1"),
        "stk_co_rate": Decimal("0.2"),
        "cash_div": Decimal("0.5"),
        "cash_div_tax": Decimal("0.5"),
        "record_date": date(2026, 5, 26),
        "pay_date": date(2026, 5, 27),
        "div_listdate": date(2026, 5, 28),
        "base_date": date(2025, 12, 31),
        "base_share": Decimal("1000000"),
    }
    values.update(overrides)
    return tuple(values[name] for name in (
        "symbol", "end_date", "ann_date", "imp_ann_date", "ex_date",
        "stk_div", "stk_bo_rate", "stk_co_rate", "cash_div", "cash_div_tax",
        "record_date", "pay_date", "div_listdate", "base_date", "base_share",
    ))


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.arguments = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _query, arguments):
        self.arguments = arguments

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)

    def cursor(self):
        return self.cursor_instance


def test_freeze_snapshot_uses_scoped_read_and_content_addressed_path(tmp_path: Path) -> None:
    connection = _Connection([_row()])

    path = freeze_corporate_action_snapshot(
        connection,
        symbols=("000001.SZ", "000001.SZ"),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
        timing_root=tmp_path,
    )

    book = CorporateActionBook.open(path)
    assert path.name == f"{book.snapshot_sha256}.json"
    assert connection.cursor_instance.arguments[0] == ["000001.SZ"]
    assert connection.cursor_instance.arguments[1:3] == (
        date(2026, 1, 1),
        date(2026, 12, 31),
    )


def test_snapshot_collapses_equivalent_rows_and_round_trips(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row(), _row(ann_date=None)],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    assert payload["raw_source_row_count"] == 2
    assert payload["canonical_action_count"] == 1
    assert payload["canonical_economic_action_count"] == 1
    assert payload["canonicalized_equivalent_revision_count"] == 1
    assert payload["combined_same_day_economic_action_count"] == 0
    assert payload["snapshot_sha256"] == canonical_sha256(
        {key: value for key, value in payload.items() if key != "snapshot_sha256"}
    )
    path = tmp_path / "actions.json"
    path.write_bytes(canonical_json_bytes(payload))
    book = CorporateActionBook.open(path)
    assert book.snapshot_sha256 == payload["snapshot_sha256"]
    assert book.actions[0].quantity_multiplier == Decimal("1.3")
    assert book.actions[0].cashflow_yuan_per_share == Decimal("0.5")
    assert book.actions[0].reference_price_cash_yuan_per_share == Decimal("0.5")


def test_snapshot_sums_distinct_same_day_distributions_after_revision_collapse(
    tmp_path: Path,
) -> None:
    second = {
        "end_date": date(2026, 3, 31),
        "base_date": date(2026, 3, 31),
        "imp_ann_date": date(2026, 5, 22),
        "stk_div": Decimal("0.1"),
        "stk_bo_rate": Decimal("0.04"),
        "stk_co_rate": Decimal("0.06"),
        "cash_div": Decimal("0.2"),
        "cash_div_tax": Decimal("0.25"),
    }
    payload = _snapshot_payload(
        [
            _row(),
            _row(ann_date=None),
            _row(**second),
            _row(ann_date=None, **{**second, "imp_ann_date": date(2026, 5, 23)}),
        ],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["raw_source_row_count"] == 4
    assert payload["canonical_action_count"] == 1
    assert payload["canonical_economic_action_count"] == 2
    assert payload["canonicalized_equivalent_revision_count"] == 2
    assert payload["combined_same_day_economic_action_count"] == 1
    path = tmp_path / "same-day-actions.json"
    path.write_bytes(canonical_json_bytes(payload))
    action = CorporateActionBook.open(path).actions[0]
    assert action.quantity_multiplier == Decimal("1.4")
    assert action.cashflow_yuan_per_share == Decimal("0.7")
    assert action.reference_price_cash_yuan_per_share == Decimal("0.75")
    assert action.source_row_count == 4
    assert action.source_economic_action_count == 2
    assert action.source_available_at == cutoff_on(date(2026, 5, 22))


def test_snapshot_collapses_same_distribution_when_only_base_date_differs() -> None:
    payload = _snapshot_payload(
        [
            _row(base_date=date(2025, 12, 31)),
            _row(ann_date=date(2026, 5, 2), base_date=date(2026, 5, 20)),
        ],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["raw_source_row_count"] == 2
    assert payload["canonical_economic_action_count"] == 1
    assert payload["canonicalized_equivalent_revision_count"] == 1
    assert payload["combined_same_day_economic_action_count"] == 0
    assert payload["actions"][0]["quantity_multiplier"] == "1.3"
    assert payload["actions"][0]["cashflow_yuan_per_share"] == "0.5"


def test_snapshot_implemented_terms_supersede_preliminary_terms_for_same_distribution() -> None:
    payload = _snapshot_payload(
        [
            _row(
                imp_ann_date=None,
                ann_date=date(2026, 3, 1),
                base_date=date(2025, 12, 31),
                stk_div=Decimal("0"),
                stk_bo_rate=Decimal("0"),
                stk_co_rate=Decimal("0"),
                cash_div=Decimal("0.81"),
                cash_div_tax=Decimal("0.81"),
                div_listdate=None,
            ),
            _row(
                base_date=date(2026, 5, 26),
                stk_div=Decimal("0"),
                stk_bo_rate=Decimal("0"),
                stk_co_rate=Decimal("0"),
                cash_div=Decimal("1.03"),
                cash_div_tax=Decimal("1.03"),
                div_listdate=None,
            ),
        ],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["canonical_economic_action_count"] == 1
    assert payload["canonicalized_equivalent_revision_count"] == 1
    assert payload["actions"][0]["quantity_multiplier"] == "1"
    assert payload["actions"][0]["cashflow_yuan_per_share"] == "1.03"
    assert payload["actions"][0]["source_available_at"] == cutoff_on(
        date(2026, 5, 20)
    ).isoformat()


def test_snapshot_collapses_same_distribution_when_only_end_date_differs() -> None:
    payload = _snapshot_payload(
        [
            _row(end_date=date(2025, 12, 31), base_date=date(2026, 5, 20)),
            _row(
                end_date=date(2026, 5, 1),
                ann_date=date(2026, 5, 2),
                base_date=date(2026, 5, 20),
            ),
        ],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["canonical_economic_action_count"] == 1
    assert payload["canonicalized_equivalent_revision_count"] == 1
    assert payload["combined_same_day_economic_action_count"] == 0
    assert payload["actions"][0]["quantity_multiplier"] == "1.3"


def test_v1_snapshot_remains_readable_after_same_day_contract_upgrade(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row()],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    legacy = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "snapshot_sha256",
            "canonical_economic_action_count",
            "combined_same_day_economic_action_count",
        }
    }
    legacy["schema_version"] = LEGACY_SNAPSHOT_SCHEMA
    legacy["source_query"] = {
        key: value
        for key, value in legacy["source_query"].items()
        if key != "same_day_canonicalization"
    }
    legacy["snapshot_sha256"] = canonical_sha256(legacy)
    path = tmp_path / "legacy-v1.json"
    path.write_bytes(canonical_json_bytes(legacy))

    action = CorporateActionBook.open(path).actions[0]

    assert action.quantity_multiplier == Decimal("1.3")
    assert action.cashflow_yuan_per_share == Decimal("0.5")
    assert action.source_economic_action_count == 1


def test_snapshot_uses_earliest_availability_for_equivalent_revisions() -> None:
    payload = _snapshot_payload(
        [
            _row(imp_ann_date=date(2026, 5, 21)),
            _row(ann_date=date(2026, 5, 2), imp_ann_date=date(2026, 5, 20)),
        ],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["canonicalized_equivalent_revision_count"] == 1
    assert payload["actions"][0]["source_available_at"] == cutoff_on(
        date(2026, 5, 20)
    ).isoformat()


def test_snapshot_uses_typed_record_date_proxy_when_implementation_date_missing() -> None:
    payload = _snapshot_payload(
        [_row(imp_ann_date=None)],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )

    assert payload["record_date_availability_proxy_count"] == 1
    assert payload["actions"][0]["source_record_date_proxy_count"] == 1
    assert payload["actions"][0]["source_available_at"] == cutoff_on(
        date(2026, 5, 26)
    ).isoformat()

    with pytest.raises(ActionValueError, match="CORPORATE_ACTION_AVAILABILITY_UNVERIFIABLE"):
        _snapshot_payload(
            [_row(imp_ann_date=None, record_date=date(2026, 5, 27))],
            symbols=("000001.SZ",),
            start=date(2026, 1, 1),
            end=date(2026, 12, 31),
        )


def test_v2_snapshot_without_record_date_proxy_fields_remains_readable(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row()],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    legacy = {
        key: value
        for key, value in payload.items()
        if key not in {"snapshot_sha256", "record_date_availability_proxy_count"}
    }
    legacy["schema_version"] = SAME_DAY_SNAPSHOT_SCHEMA
    legacy["source_query"] = {
        key: value for key, value in legacy["source_query"].items() if key != "availability_policy"
    }
    for action in legacy["actions"]:
        action.pop("source_record_date_proxy_count")
    legacy["snapshot_sha256"] = canonical_sha256(legacy)
    path = tmp_path / "legacy-v2.json"
    path.write_bytes(canonical_json_bytes(legacy))

    assert CorporateActionBook.open(path).actions[0].source_record_date_proxy_count == 0


def test_v3_snapshot_with_old_distribution_identity_remains_readable(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row()],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    legacy = {key: value for key, value in payload.items() if key != "snapshot_sha256"}
    legacy["schema_version"] = AVAILABILITY_SNAPSHOT_SCHEMA
    legacy["source_query"]["same_day_canonicalization"] = (
        "COLLAPSE_EQUIVALENT_REVISIONS_WITHIN_END_BASE_RECORD_IDENTITY_"
        "THEN_SUM_DISTINCT_PRE_ACTION_PER_SHARE_DISTRIBUTIONS"
    )
    legacy["snapshot_sha256"] = canonical_sha256(legacy)
    path = tmp_path / "legacy-v3.json"
    path.write_bytes(canonical_json_bytes(legacy))

    assert CorporateActionBook.open(path).actions[0].quantity_multiplier == Decimal("1.3")


def test_v4_snapshot_with_end_record_identity_remains_readable(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row()],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    legacy = {key: value for key, value in payload.items() if key != "snapshot_sha256"}
    legacy["schema_version"] = IDENTITY_SNAPSHOT_SCHEMA
    legacy["source_query"]["same_day_canonicalization"] = (
        "COLLAPSE_EQUIVALENT_REVISIONS_WITHIN_END_RECORD_IDENTITY_"
        "THEN_SUM_DISTINCT_PRE_ACTION_PER_SHARE_DISTRIBUTIONS"
    )
    legacy["snapshot_sha256"] = canonical_sha256(legacy)
    path = tmp_path / "legacy-v4.json"
    path.write_bytes(canonical_json_bytes(legacy))

    assert CorporateActionBook.open(path).actions[0].quantity_multiplier == Decimal("1.3")


def test_snapshot_separates_account_cash_from_reference_price_cash(tmp_path: Path) -> None:
    payload = _snapshot_payload(
        [_row(cash_div=Decimal("0.4"), cash_div_tax=Decimal("0.5"))],
        symbols=("000001.SZ",),
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
    )
    path = tmp_path / "actions.json"
    path.write_bytes(canonical_json_bytes(payload))
    action = CorporateActionBook.open(path).actions[0]

    application = apply_corporate_action_with_audit(
        PositionState(100, 100, Decimal("1000"), Decimal("10000")),
        action,
        next_trade_date=date(2026, 5, 28),
    )

    assert action.reference_price_cash_yuan_per_share == Decimal("0.5")
    assert action.cashflow_yuan_per_share == Decimal("0.4")
    assert application.state.cash == Decimal("1040")


def test_snapshot_rejects_conflicting_economics_and_missing_tax_cash() -> None:
    with pytest.raises(ActionValueError, match="CORPORATE_ACTION_ECONOMIC_CONFLICT"):
        _snapshot_payload(
            [_row(), _row(cash_div_tax=Decimal("0.4"))],
            symbols=("000001.SZ",),
            start=date(2026, 1, 1),
            end=date(2026, 12, 31),
        )
    with pytest.raises(ActionValueError, match="CORPORATE_ACTION_CASH_COMPONENT_UNAVAILABLE"):
        _snapshot_payload(
            [_row(cash_div_tax=None)],
            symbols=("000001.SZ",),
            start=date(2026, 1, 1),
            end=date(2026, 12, 31),
        )


def test_action_applies_pre_action_cash_and_locks_new_shares_until_next_session() -> None:
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=date(2026, 5, 27),
        quantity_multiplier=Decimal("1.3"),
        cashflow_yuan_per_share=Decimal("0.5"),
        reference_price_cash_yuan_per_share=Decimal("0.5"),
        cash_pay_date=date(2026, 5, 27),
        share_listing_date=date(2026, 5, 28),
        source_available_at=datetime(2026, 5, 20, 20, tzinfo=TZ),
        source_row_count=1,
        source_rows_sha256="a" * 64,
    )
    state = PositionState(
        quantity=100,
        sellable=100,
        cash=Decimal("1000"),
        capital=Decimal("10000"),
        entry_cost=Decimal("10"),
        holding_age=20,
    )
    transformed = apply_corporate_action(
        state,
        action,
        next_trade_date=date(2026, 5, 28),
    )
    assert transformed.quantity == 130
    assert transformed.sellable == 100
    assert transformed.cash == Decimal("1050")
    assert transformed.entry_cost == Decimal("950") / Decimal("130")


def test_action_floors_unverifiable_fractional_entitlement_and_reports_it() -> None:
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=date(2026, 5, 27),
        quantity_multiplier=Decimal("1.45"),
        cashflow_yuan_per_share=Decimal("0"),
        reference_price_cash_yuan_per_share=Decimal("0"),
        cash_pay_date=None,
        share_listing_date=date(2026, 5, 28),
        source_available_at=datetime(2026, 5, 20, 20, tzinfo=TZ),
        source_row_count=1,
        source_rows_sha256="b" * 64,
    )
    state = PositionState(130, 130, Decimal("100"), Decimal("10000"))
    application = apply_corporate_action_with_audit(
        state,
        action,
        next_trade_date=date(2026, 5, 28),
    )
    assert application.state.quantity == 188
    assert application.state.sellable == 130
    assert application.fractional_share_discarded == Decimal("0.50")
    assert application.fractional_sellable_share_discarded == 0

    same_day = replace(action, share_listing_date=date(2026, 5, 27))
    same_day_application = apply_corporate_action_with_audit(
        PositionState(130, 129, Decimal("100"), Decimal("10000")),
        same_day,
        next_trade_date=date(2026, 5, 28),
    )
    assert same_day_application.state.sellable == 187
    assert same_day_application.fractional_sellable_share_discarded == Decimal("0.05")

    delayed_cash = replace(
        action,
        cashflow_yuan_per_share=Decimal("1"),
        cash_pay_date=date(2026, 5, 28),
    )
    with pytest.raises(ActionValueError, match="CORPORATE_ACTION_DEFERRED_CASH_UNAVAILABLE"):
        apply_corporate_action(state, delayed_cash, next_trade_date=date(2026, 5, 28))
