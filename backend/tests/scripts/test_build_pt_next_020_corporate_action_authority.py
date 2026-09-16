from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.action_value_corporate_actions import CorporateActionBook
from backend.services.position_timing.contracts import canonical_sha256
from scripts import build_pt_next_020_corporate_action_authority as build


def _row(signature: tuple[str | None, ...]) -> tuple[object, ...]:
    values: list[object] = []
    for name, value in zip(build.SOURCE_COLUMNS, signature):
        if value is None:
            values.append(None)
        elif name in {"end_date", "ann_date", "imp_ann_date", "ex_date", "record_date", "pay_date", "div_listdate", "base_date"}:
            values.append(dt.date.fromisoformat(value))
        elif name == "ts_code":
            values.append(value)
        else:
            values.append(Decimal(value))
    return tuple(values)


def _target_rows() -> tuple[tuple[object, ...], ...]:
    return tuple(_row(signature) for signature in sorted(build.EXPECTED_TARGET_ROWS))


def test_classification_expresses_aggregate_components_and_holder_classes() -> None:
    classified = build.classify_target_rows(_target_rows())
    by_key = {(entry["source_row"]["ts_code"], entry["source_row"]["cash_div"]): entry for entry in classified}
    assert by_key[("002352.SZ", "0.4")]["classification"] == "DISTINCT_SAME_DAY_ACTION"
    assert by_key[("002352.SZ", "1")]["accumulation_policy"] == "INCLUDE_COMPONENT"
    assert by_key[("002352.SZ", "1.4")]["classification"] == "AGGREGATE_OF_COMPONENTS"
    assert by_key[("600989.SH", "0.3158")]["holder_applicability"] == build.ACCOUNT_CLASS
    assert by_key[("600989.SH", "0.265")]["accumulation_policy"] == "EXCLUDE_MUTUALLY_EXCLUSIVE_HOLDER_CLASS"


def test_classification_fails_closed_on_source_drift() -> None:
    rows = list(_target_rows())
    changed = list(rows[0])
    changed[8] = Decimal("9.9")
    rows[0] = tuple(changed)
    with pytest.raises(build.AuthorityBuildError, match="source rows drifted"):
        build.classify_target_rows(rows)


def test_snapshot_is_v6_consumer_compatible_and_does_not_double_count(tmp_path: Path) -> None:
    rows = _target_rows()
    classified = build.classify_target_rows(rows)
    resolutions = build.resolution_payloads(classified)
    payload = build.build_snapshot(
        rows,
        symbols=("002352.SZ", "600989.SH"),
        start=dt.date(2018, 8, 1),
        end=dt.date(2026, 8, 31),
        authority_sha256="a" * 64,
        resolutions=resolutions,
    )
    path = tmp_path / "snapshot.json"
    path.write_bytes(build.canonical_json_bytes(payload) + b"\n")
    book = CorporateActionBook.open(path)
    sf = book.on("002352.SZ", dt.date(2024, 11, 7))
    bf = book.on("600989.SH", dt.date(2024, 7, 24))
    assert sf is not None and sf.cashflow_yuan_per_share == Decimal("1.4")
    assert sf.reference_price_cash_yuan_per_share == Decimal("1.3939620")
    assert sf.source_row_count == 3 and sf.source_economic_action_count == 2
    assert bf is not None and bf.cashflow_yuan_per_share == Decimal("0.3158")
    assert bf.reference_price_cash_yuan_per_share == Decimal("0.28")
    assert bf.source_row_count == 2 and bf.source_economic_action_count == 1
    assert payload["schema_version"] == build.SNAPSHOT_SCHEMA
    assert payload["raw_source_row_count"] == 5
    assert payload["snapshot_sha256"] == canonical_sha256({key: value for key, value in payload.items() if key != "snapshot_sha256"})


def test_document_download_rejects_bot_html(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def read(self): return b"<html>bot denied</html>"

    monkeypatch.setattr(build, "urlopen", lambda *_args, **_kwargs: Response())
    with pytest.raises(build.AuthorityBuildError, match="not PDF"):
        build.download_documents(tmp_path / "sources")


def test_database_config_keeps_dev_and_production_separate(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("\n".join((
        "TDX_DB_DEV_HOST=127.0.0.1", "TDX_DB_DEV_PORT=5433", "TDX_DB_DEV_NAME=aistock_dev", "TDX_DB_DEV_USER=u", "TDX_DB_DEV_PASSWORD=p",
        "TDX_DB_HOST=127.0.0.1", "TDX_DB_PORT=5432", "TDX_DB_NAME=aistock", "TDX_DB_USER=u", "TDX_DB_PASSWORD=p",
    )), encoding="utf-8")
    assert build.database_config("dev", env)["dbname"] == "aistock_dev"
    assert build.database_config("production", env)["dbname"] == "aistock"
