from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pandas as pd
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


def test_document_copy_reverifies_pinned_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"%PDF-1.4\npinned authority\n"
    spec = {
        "document_id": "PINNED",
        "document_type": "IMPLEMENTATION",
        "official_source_url": "https://example.invalid/pinned.pdf",
        "capture_url": "https://example.invalid/pinned.pdf",
        "filename": "pinned.pdf",
        "source_content_sha256": build.sha256_bytes(body),
        "source_file_size": len(body),
    }
    monkeypatch.setattr(build, "DOCUMENTS", (spec,))
    source = tmp_path / "input"
    source.mkdir()
    (source / "pinned.pdf").write_bytes(body)
    references = build.download_documents(tmp_path / "output", source_document_root=source)
    assert references[0]["source_content_sha256"] == build.sha256_bytes(body)
    assert (tmp_path / "output" / "pinned.pdf").read_bytes() == body

    (source / "pinned.pdf").write_bytes(b"%PDF-1.4\ndrifted\n")
    with pytest.raises(build.AuthorityBuildError, match="identity differs"):
        build.download_documents(tmp_path / "rejected", source_document_root=source)


def test_database_config_keeps_dev_and_production_separate(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("\n".join((
        "TDX_DB_DEV_HOST=127.0.0.1", "TDX_DB_DEV_PORT=5433", "TDX_DB_DEV_NAME=aistock_dev", "TDX_DB_DEV_USER=u", "TDX_DB_DEV_PASSWORD=p",
        "TDX_DB_HOST=127.0.0.1", "TDX_DB_PORT=5432", "TDX_DB_NAME=aistock", "TDX_DB_USER=u", "TDX_DB_PASSWORD=p",
    )), encoding="utf-8")
    assert build.database_config("dev", env)["dbname"] == "aistock_dev"
    assert build.database_config("production", env)["dbname"] == "aistock"


def test_full_scope_specs_cover_all_discovered_failure_classes() -> None:
    specs = build.FULL_SCOPE_RESOLUTION_SPECS
    counts: dict[str, int] = {}
    for spec in specs.values():
        counts[spec["classification"]] = counts.get(spec["classification"], 0) + 1
    assert len(specs) == 15
    assert sum(int(spec["source_row_count"]) for spec in specs.values()) == 26
    assert counts == {
        "DUPLICATE_SOURCE_RECORD": 1,
        "PREHISTORY_BOUNDARY_ACTION": 6,
        "SPECIAL_RESTRUCTURING_NON_PRO_RATA": 7,
        "TREASURY_SHARE_EXCLUDED_DUAL_BASIS_DISTRIBUTION": 1,
    }
    dual = specs[("300234.SZ", "2020-04-21")]
    assert dual["account_economics"]["quantity_multiplier"] == "1.8125392"
    assert dual["reference_price_economics"]["quantity_multiplier"] == "1.766366"


class _CandidateFixture:
    def __init__(self) -> None:
        self.calendar = pd.DatetimeIndex(pd.to_datetime(["2018-08-01", "2018-08-02", "2018-08-03"]))
        self.spans = pd.DataFrame(
            [
                {"symbol": "111111.SZ", "start": pd.Timestamp("2018-08-01"), "end": pd.Timestamp("2018-08-03")},
                {"symbol": "222222.SZ", "start": pd.Timestamp("2018-08-01"), "end": pd.Timestamp("2018-08-03")},
            ]
        )
        self.references: dict[str, dict[str, object]] = {}

    def bars(self, symbol: str) -> pd.DataFrame:
        values = [1.0, 1.2, 1.2] if symbol == "111111.SZ" else [1.5, 1.5, 1.5]
        self.references[f"{symbol}:factor"] = {
            "path": f"X:/fixture/{symbol.lower()}/factor.day.bin",
            "sha256": ("1" if symbol == "111111.SZ" else "2") * 64,
            "size_bytes": 16,
        }
        return pd.DataFrame({"factor": values}, index=self.calendar)


def _full_scope_fixture() -> tuple[tuple[object, ...], tuple[object, ...]]:
    return (
        (
            "111111.SZ", dt.date(2018, 6, 30), dt.date(2018, 7, 30),
            dt.date(2018, 7, 31), dt.date(2018, 8, 2), Decimal("1"), None,
            Decimal("1"), Decimal("0"), Decimal("0"), dt.date(2018, 8, 1),
            None, dt.date(2018, 8, 2), dt.date(2018, 7, 31), Decimal("100"),
        ),
        (
            "222222.SZ", dt.date(2017, 12, 31), dt.date(2018, 7, 20),
            dt.date(2018, 7, 25), dt.date(2018, 8, 1), Decimal("0.5"), None,
            Decimal("0.5"), Decimal("0.1"), Decimal("0.1"), dt.date(2018, 7, 31),
            dt.date(2018, 8, 1), dt.date(2018, 8, 1), dt.date(2018, 7, 31), Decimal("200"),
        ),
    )


def test_full_scope_authority_is_typed_content_addressed_and_fail_closed() -> None:
    rows = _full_scope_fixture()
    first_hash = canonical_sha256(build._stable_portable_rows((rows[0],)))
    second_hash = canonical_sha256(build._stable_portable_rows((rows[1],)))
    specs = {
        ("111111.SZ", "2018-08-02"): {
            "classification": "SPECIAL_RESTRUCTURING_NON_PRO_RATA",
            "source_row_count": 1,
            "source_rows_sha256": first_hash,
            "accumulation_policy": "DO_NOT_CREDIT_CONVERSION_SHARES_TO_ORDINARY_ACCOUNT",
            "replay_application": "REFERENCE_PRICE_ONLY",
            "account_economics": {"quantity_multiplier": "1", "cash_yuan_per_share": "0"},
            "reference_price_economics": {"factor_ratio": "1.2"},
            "factor_boundary": {"previous": "2018-08-01", "current": "2018-08-02"},
            "announcement_url": "https://example.invalid/restructuring.pdf",
        },
        ("222222.SZ", "2018-08-01"): {
            "classification": "PREHISTORY_BOUNDARY_ACTION",
            "source_row_count": 1,
            "source_rows_sha256": second_hash,
            "accumulation_policy": "RETAIN_SOURCE_DO_NOT_REPLAY",
            "replay_application": "EXCLUDE_BEFORE_FIRST_OBSERVABLE_POSITION",
            "account_economics": {"quantity_multiplier": "1.5", "cash_yuan_per_share": "0.1"},
            "reference_price_economics": None,
            "factor_boundary": {"first_valid": "2018-08-01", "first_pit_eligible": "2018-08-01"},
            "announcement_url": None,
        },
    }
    captured = dt.datetime(2026, 9, 16, tzinfo=dt.timezone.utc)
    payload = build.build_full_scope_authority(
        rows,
        candidate=_CandidateFixture(),
        candidate_manifest={"path": "X:/candidate/qe_dataset_manifest.json", "sha256": "a" * 64, "size_bytes": 1},
        captured_at=captured,
        specs=specs,
    )
    assert payload["schema_version"] == build.FULL_SCOPE_AUTHORITY_SCHEMA
    assert payload["classification_counts"] == {
        "PREHISTORY_BOUNDARY_ACTION": 1,
        "SPECIAL_RESTRUCTURING_NON_PRO_RATA": 1,
    }
    assert payload["consumer_contract"]["typed_authority_reader_required"] is True
    assert payload["safety"]["database_write_performed"] is False
    assert payload["canonical_sha256"] == canonical_sha256(
        {key: value for key, value in payload.items() if key != "canonical_sha256"}
    )

    changed = [list(row) for row in rows]
    changed[0][8] = Decimal("9")
    with pytest.raises(build.AuthorityBuildError, match="source rows drifted"):
        build.build_full_scope_authority(
            tuple(tuple(row) for row in changed),
            candidate=_CandidateFixture(),
            candidate_manifest={"path": "X:/candidate/qe_dataset_manifest.json", "sha256": "a" * 64, "size_bytes": 1},
            captured_at=captured,
            specs=specs,
        )
