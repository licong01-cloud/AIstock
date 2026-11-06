from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.hmm_risk import observed_industry_authority as subject
from backend.services.industry_pit.candidate_builder import FrozenDenominator, UniverseSpan
from backend.services.industry_pit.contracts import (
    ResolutionRequest,
    ResolvedIndustryIdentity,
    UnavailableIndustryIdentity,
)
from backend.services.industry_pit.resolver import IndustryPitResolver
from backend.tests.hmm_risk.test_industry_pit_adapter import _bundle


@pytest.fixture
def inputs(tmp_path):
    base = _bundle(tmp_path)
    symbol = base.classification_intervals[0].canonical_symbol
    dates = tuple(date(2026, 10, day) for day in (9, 12, 13, 14, 22))
    denominator = FrozenDenominator.build(
        window_start=dates[0],
        window_end=dates[-1],
        trading_dates=dates,
        universe_spans=(UniverseSpan(symbol, dates[0], None),),
    )
    identity = base.classification_intervals[1].identity
    observation = subject.Observation(
        captured_at=datetime(2026, 10, 12, 15, 4, tzinfo=timezone.utc),
        data_date=dates[-1],
        rows={
            symbol: {
                "taxonomy_l3_code": identity.l3_code,
                "source_reported_inclusion_at": "2026-09-24T10:53:00",
                "source_reported_update_at": "2026-09-24T10:53:00",
            }
        },
        source_hashes=("a" * 64, "b" * 64),
        conflicts=({"symbol": symbol, "database_l2_code": "801016.SI", "official_l2_code": "801151.SI"},),
    )
    catalog = {row.identity.l3_code: row.identity for row in base.classification_intervals}
    return base, denominator, observation, catalog


def resolve(receipt, rows, symbol, day):
    resolver = IndustryPitResolver(
        receipt=receipt,
        intervals=rows,
        known_taxonomy_versions={(receipt.taxonomy_contract_id, receipt.taxonomy_version)},
    )
    return resolver.resolve(
        ResolutionRequest(
            canonical_symbol=symbol,
            trade_date=day,
            authority_type=receipt.authority_type,
            taxonomy_contract_id=receipt.taxonomy_contract_id,
            taxonomy_version=receipt.taxonomy_version,
            authority_receipt_hash=receipt.receipt_hash,
            knowledge_time_policy=receipt.knowledge_time_policy,
            research_basis=receipt.research_basis,
        )
    )


def test_observed_snapshot_resolves_forward_but_never_backdates_to_inclusion(inputs):
    base, denominator, observation, catalog = inputs
    receipt, rows, index_receipt, index_rows = subject.build_intervals(base, denominator, observation, catalog)
    symbol = next(iter(observation.rows))
    assert receipt.receipt_hash != base.classification_receipt.receipt_hash
    for day in (date(2026, 10, 9), date(2026, 10, 12)):
        assert isinstance(resolve(receipt, rows, symbol, day), UnavailableIndustryIdentity)
    result = resolve(receipt, rows, symbol, date(2026, 10, 22))
    assert isinstance(result, ResolvedIndustryIdentity)
    assert result.identity == catalog[observation.rows[symbol]["taxonomy_l3_code"]]
    assert result.known_from == date(2026, 10, 13)  # capture was 23:04 Shanghai
    assert isinstance(resolve(index_receipt, index_rows, symbol, date(2026, 10, 22)), UnavailableIndustryIdentity)
    assert receipt.denominator_digest == index_receipt.denominator_digest == denominator.digest


def test_prior_authority_is_not_mutated_or_extended(inputs):
    base, denominator, observation, catalog = inputs
    before = tuple(row.as_dict() for row in base.classification_intervals)
    _, rows, _, _ = subject.build_intervals(base, denominator, observation, catalog)
    assert tuple(row.as_dict() for row in base.classification_intervals) == before
    symbol = next(iter(observation.rows))
    assert all(row.eligible_from >= date(2026, 10, 13) for row in rows if row.canonical_symbol == symbol)


def test_existing_causal_history_remains_and_observation_supersedes_only_from_known_day(inputs):
    base, denominator, observation, catalog = inputs
    prior = subject._rebind(
        base.classification_intervals[0], base.classification_receipt, start=date(2026, 10, 9), end=date(2026, 10, 23)
    )
    base = replace(base, classification_intervals=(prior,))
    receipt, rows, _, _ = subject.build_intervals(base, denominator, observation, catalog)
    symbol = prior.canonical_symbol
    assert resolve(receipt, rows, symbol, date(2026, 10, 12)).identity == prior.identity
    assert (
        resolve(receipt, rows, symbol, date(2026, 10, 13)).identity
        == catalog[observation.rows[symbol]["taxonomy_l3_code"]]
    )
    preserved = next(row for row in rows if prior.row_hash in row.lineage_hashes)
    assert preserved.known_from == prior.known_from
    assert preserved.eligible_to_exclusive == date(2026, 10, 13)


@pytest.mark.parametrize(
    "fault", ["future_capture", "wrong_window", "unknown_taxonomy", "empty_source", "missing_target"]
)
def test_invalid_observation_fails_closed(inputs, fault):
    base, denominator, observation, catalog = inputs
    if fault == "future_capture":
        observation = replace(observation, captured_at=datetime(2026, 10, 23, tzinfo=timezone.utc))
    elif fault == "wrong_window":
        observation = replace(observation, data_date=date(2026, 10, 21))
    elif fault == "unknown_taxonomy":
        observation = replace(
            observation,
            rows={
                next(iter(observation.rows)): {
                    "taxonomy_l3_code": "999999",
                    "source_reported_inclusion_at": "2026-09-24",
                    "source_reported_update_at": None,
                }
            },
        )
    elif fault == "empty_source":
        observation = replace(observation, source_hashes=())
    else:
        observation = replace(observation, rows={})
    with pytest.raises(ValueError):
        subject.build_intervals(base, denominator, observation, catalog)


def test_naive_capture_and_no_next_session_are_not_assumed(inputs):
    _, denominator, _, _ = inputs
    with pytest.raises(ValueError, match="timezone"):
        subject.known_day(datetime(2026, 10, 12), denominator.trading_dates)
    with pytest.raises(ValueError, match="session"):
        subject.known_day(datetime(2026, 10, 23, tzinfo=timezone.utc), denominator.trading_dates)


def test_file_pin_and_indirect_paths_are_checked_before_parse(tmp_path, monkeypatch):
    path = tmp_path / "source.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="hash"):
        subject.pinned_bytes(path, "a" * 64)
    real = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda p: p == tmp_path or real(p))
    with pytest.raises(ValueError, match="indirect"):
        subject.pinned_bytes(path, "a" * 64)


@pytest.mark.parametrize("fault", [None, "row_reference", "text_projection", "publication", "duplicate", "ambiguity"])
def test_source_mapping_is_verified_against_the_pinned_workbook(inputs, tmp_path, fault):
    base, _, observation, _ = inputs
    old, new = (row.identity for row in base.classification_intervals[:2])
    source_rows = [
        ["000001", pd.Timestamp("2021-07-30"), old.l3_code, pd.NaT],
        ["000001", pd.Timestamp("2026-09-24T10:53:00"), new.l3_code, pd.NaT],
    ]
    if fault == "ambiguity":
        source_rows.append(["000001", source_rows[1][1], old.l3_code, pd.NaT])
    workbook = tmp_path / "observed.xlsx"
    pd.DataFrame(source_rows, columns=["股票代码", "计入日期", "行业代码", "更新日期"]).to_excel(workbook, index=False)

    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def save(name, body):
        path = tmp_path / name
        path.write_text(json.dumps(body), encoding="utf-8")
        return path, sha(path)

    observed_at = observation.captured_at.isoformat()
    capture, capture_sha = save(
        "capture.json",
        {
            "status": 200,
            "tls_verification_disabled": False,
            "source_url": subject.OFFICIAL_URL,
            "final_url": subject.OFFICIAL_URL,
            "sha256": sha(workbook),
            "captured_at": observed_at,
            "path": str(workbook),
            "bytes": workbook.stat().st_size,
            "row_count": len(source_rows),
        },
    )
    row = {
        "symbol": "000001.SZ",
        "canonical_l2_code": "801783.SI",
        "taxonomy_l2_code": new.l2_code,
        "taxonomy_l3_code": new.l3_code,
        "source_excel_row": 3,
        "source_reported_inclusion_at": "2026-09-24T10:53:00",
        "source_reported_update_at": None,
        "publication_at": None,
        "publication_time_quality": "FIRST_PUBLICATION_UNVERIFIED",
        "first_proven_observation_at": observed_at,
    }
    if fault == "row_reference":
        row["source_excel_row"] = 2
    elif fault == "text_projection":
        row["canonical_l2_code"] = "801016.SI"
    elif fault == "publication":
        row["publication_at"] = row["source_reported_inclusion_at"]
    mapping, mapping_sha = save(
        "mapping.json",
        {
            "artifact_role": "OFFICIAL_SOURCE_EFFECTIVE_MAPPING_READBACK_NOT_RUNTIME_AUTHORITY",
            "taxonomy_version": "SW2021",
            "data_date": "2026-10-22",
            "stock_count": 1,
            "code_projection": {"canonical_hash": "c" * 64},
            "official_source": {
                "path": str(workbook),
                "sha256": sha(workbook),
                "capture_receipt": str(capture),
                "captured_at": observed_at,
                "url": subject.OFFICIAL_URL,
            },
            "rows": [row, row] if fault == "duplicate" else [row],
        },
    )
    readback, readback_sha = save(
        "readback.json",
        {
            "status": "SOURCE_READBACK_COMPLETE_FORWARD_AUTHORITY_NOT_REBUILT",
            "data_date": "2026-10-22",
            "mapping_file": {"file_sha256": mapping_sha, "path": str(mapping)},
            "expected_symbols": 1,
            "source_file_identities": {str(capture): capture_sha},
            "official_database_l2_disagreements": [],
            "missing_mapping_symbols": 0,
            "unresolved_l2_projection_symbols": 0,
            "ambiguous_latest_assignment_symbols": [],
            "official_database_l2_match_symbols": 1,
        },
    )
    authority = {
        "l2_projection": {
            "canonical_hash": "c" * 64,
            "rows": [{"canonical_l2_code": "801783.SI", "taxonomy_l2_code": new.l2_code}],
        }
    }
    if fault is not None:
        with pytest.raises(ValueError):
            subject.load_observation(mapping, mapping_sha, readback, readback_sha, authority)
    else:
        loaded = subject.load_observation(mapping, mapping_sha, readback, readback_sha, authority)
        assert loaded.rows["000001.SZ"]["canonical_l2_code"] == "801783.SI"
        assert loaded.rows["000001.SZ"]["publication_at"] is None
        assert loaded.captured_at == observation.captured_at


def test_cli_unsafe_output_fails_before_any_database_access(tmp_path, monkeypatch):
    import runpy
    import sys
    from backend.services.hmm_risk import risk_l2_database_inference as database

    root = Path(__file__).resolve().parents[3]
    script = root / "scripts/hmm_risk/build_observed_industry_authority.py"
    monkeypatch.setattr(database, "connect", lambda *_: pytest.fail("database accessed"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--request",
            str(tmp_path / "missing.json"),
            "--mapping",
            str(tmp_path / "missing-map.json"),
            "--mapping-sha256",
            "a" * 64,
            "--readback",
            str(tmp_path / "missing-receipt.json"),
            "--readback-sha256",
            "b" * 64,
            "--output-root",
            str(root / "tmp/invalid-authority-output"),
        ],
    )
    with pytest.raises(ValueError):
        runpy.run_path(str(script))["main"]()


def test_refresh_reuses_bytes_and_first_observation_but_records_new_check(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    raw = _official_workbook_bytes()
    now = [datetime(2026, 11, 5, 9, tzinfo=timezone.utc)]
    monkeypatch.setattr(source, "_download", lambda: raw)
    monkeypatch.setattr(source, "_now", lambda: now[0])
    first = source.refresh(tmp_path / "sources")
    now[0] = datetime(2026, 11, 6, 9, tzinfo=timezone.utc)
    second = source.refresh(tmp_path / "sources")
    a, b = (source.load_capture(Path(p["path"]), p["sha256"])[0] for p in (first, second))
    assert a["first_observed_at"] == b["first_observed_at"] == "2026-11-05T09:00:00+00:00"
    assert b["captured_at"] == "2026-11-06T09:00:00+00:00"
    assert a["path"] == b["path"]
    assert len(list((tmp_path / "sources" / "workbooks").iterdir())) == 1
    assert source.read_current(tmp_path / "sources") == second


def test_failed_refresh_keeps_last_observation_without_claiming_success(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    previous = source.refresh(tmp_path / "sources")
    monkeypatch.setattr(source, "_download", lambda: b"<html>blocked</html>")
    with pytest.raises(ValueError):
        source.refresh(tmp_path / "sources")
    assert source.read_current(tmp_path / "sources") == previous


def _official_workbook_bytes(code="480301", included="2021-07-30"):
    import io

    output = io.BytesIO()
    pd.DataFrame(
        [["000001", pd.Timestamp(included), code, pd.NaT]], columns=["股票代码", "计入日期", "行业代码", "更新日期"]
    ).to_excel(output, index=False)
    return output.getvalue()


def test_returning_old_content_is_a_new_observation_segment(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    a, b = _official_workbook_bytes(), _official_workbook_bytes(code="370102")
    current = [a, datetime(2026, 11, 3, 9, tzinfo=timezone.utc)]
    monkeypatch.setattr(source, "_download", lambda: current[0])
    monkeypatch.setattr(source, "_now", lambda: current[1])
    source.refresh(tmp_path / "sources")
    current[:] = [b, datetime(2026, 11, 4, 9, tzinfo=timezone.utc)]
    source.refresh(tmp_path / "sources")
    current[:] = [a, datetime(2026, 11, 5, 9, tzinfo=timezone.utc)]
    pin = source.refresh(tmp_path / "sources")
    capture, _ = source.load_capture(Path(pin["path"]), pin["sha256"])
    assert capture["first_observed_at"] == "2026-11-05T09:00:00+00:00"
    assert capture["first_observation"] is None
    assert len(list((tmp_path / "sources" / "workbooks").iterdir())) == 2


@pytest.mark.parametrize("fault", ["network", "hash_drift", "clock_rollback", "locked"])
def test_refresh_errors_do_not_advance_observation_pointer(tmp_path, monkeypatch, fault):
    from scripts.hmm_risk import observed_industry_source as source

    now = datetime(2026, 11, 5, 9, tzinfo=timezone.utc)
    monkeypatch.setattr(source, "_now", lambda: now)
    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    root = tmp_path / "sources"
    pin = source.refresh(root)
    pointer = (root / "current.json").read_bytes()
    if fault == "network":

        def fail():
            raise OSError("injected network failure")

        monkeypatch.setattr(source, "_download", fail)
    elif fault == "hash_drift":
        capture = json.loads(Path(pin["path"]).read_text())
        Path(capture["path"]).write_bytes(b"changed")
    elif fault == "clock_rollback":
        monkeypatch.setattr(source, "_now", lambda: datetime(2026, 11, 4, tzinfo=timezone.utc))
    if fault == "locked":
        with source._locked(root), pytest.raises(ValueError, match="in progress"):
            source.refresh(root)
    else:
        with pytest.raises((ValueError, OSError)):
            source.refresh(root)
    assert (root / "current.json").read_bytes() == pointer


@pytest.mark.parametrize("fault", [None, "missing_symbol", "projection_hash", "unknown_l2", "ambiguous"])
def test_direct_source_mapping_preserves_official_text_identity_and_time(tmp_path, monkeypatch, fault):
    import io
    from scripts.hmm_risk import observed_industry_source as source
    from backend.services.dataset_release.canonical import digest_named_fields

    monkeypatch.setattr(source, "_now", lambda: datetime(2026, 11, 5, 9, tzinfo=timezone.utc))
    raw = _official_workbook_bytes()
    if fault == "ambiguous":
        frame = pd.read_excel(io.BytesIO(raw), dtype=object)
        frame.loc[1] = ["000001", pd.Timestamp("2021-07-30"), "370102", pd.NaT]
        stream = io.BytesIO()
        frame.to_excel(stream, index=False)
        raw = stream.getvalue()
    monkeypatch.setattr(source, "_download", lambda: raw)
    pin = source.refresh(tmp_path / "sources")
    projection = {"rows": [{"taxonomy_l2_code": "480300", "canonical_l2_code": "801783.SI"}]}
    if fault == "unknown_l2":
        projection["rows"][0]["taxonomy_l2_code"] = "370100"
    projection["canonical_hash"] = digest_named_fields(source.HMM_L2_CODE_PROJECTION_SCHEMA, projection)
    if fault == "projection_hash":
        projection["canonical_hash"] = "0" * 64
    symbols = {"000001.SZ", "000002.SZ"} if fault == "missing_symbol" else {"000001.SZ"}
    args = (Path(pin["path"]), pin["sha256"], {"l2_projection": projection})
    if fault:
        with pytest.raises(ValueError):
            source.observation_from_capture(*args, data_date=date(2026, 11, 6), symbols=symbols)
    else:
        observed = source.observation_from_capture(*args, data_date=date(2026, 11, 6), symbols=symbols)
        row = observed.rows["000001.SZ"]
        assert row["canonical_l2_code"] == "801783.SI"
        assert row["publication_at"] is None
        assert row["publication_time_quality"] == "FIRST_PUBLICATION_UNVERIFIED"
        assert subject.known_day(observed.captured_at, (date(2026, 11, 5), date(2026, 11, 6))) == date(2026, 11, 6)
        with pytest.raises(ValueError, match="no next session"):
            subject.known_day(observed.captured_at, (date(2026, 10, 29),))


def test_copied_earlier_timestamp_without_source_evidence_is_rejected(tmp_path, monkeypatch):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    pin = source.refresh(tmp_path / "sources")
    body = json.loads(Path(pin["path"]).read_bytes())
    body["first_observed_at"] = "2021-01-01T00:00:00+00:00"
    forged = tmp_path / "forged.json"
    forged.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="no evidence"):
        source.load_capture(forged, hashlib.sha256(forged.read_bytes()).hexdigest())


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", "unknown"),
        ("tls_verification_disabled", True),
        ("final_url", "http://invalid.example/file.xls"),
        ("bytes", 1),
        ("row_count", 999),
        ("first_publication_verified", True),
        ("business_preflight_passed", True),
    ],
)
def test_capture_metadata_drift_is_not_accepted(tmp_path, monkeypatch, field, value):
    from scripts.hmm_risk import observed_industry_source as source

    monkeypatch.setattr(source, "_download", _official_workbook_bytes)
    pin = source.refresh(tmp_path / "sources")
    body = json.loads(Path(pin["path"]).read_bytes())
    body[field] = value
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(body))
    with pytest.raises(ValueError):
        source.load_capture(changed, hashlib.sha256(changed.read_bytes()).hexdigest())


@pytest.mark.parametrize("pending", [False, True])
def test_cli_refresh_uses_formal_readonly_denominator_and_never_builds_future_authority(
    inputs, tmp_path, monkeypatch, capsys, pending
):
    from contextlib import contextmanager
    import runpy
    import sys
    from types import SimpleNamespace
    from scripts.hmm_risk import observed_industry_source as source
    from backend.services.hmm_risk import risk_l2_database_inference as database
    from backend.services.hmm_risk import rotation_l1_input_bundle as rotation
    from backend.services.hmm_risk import stock_fact_repository as facts

    _, denominator, observation, _ = inputs
    if pending:
        observation = replace(observation, captured_at=datetime(2026, 10, 23, tzinfo=timezone.utc))
    observation = replace(observation, checked_at=observation.captured_at)
    output, store = tmp_path / "authority", tmp_path / "sources"
    request = {
        "source": {
            "knowledge_mode": "forward_current_read_snapshot",
            "industry_authority": {"artifact_root": str(tmp_path / "baseline")},
            "universe_key": "pit",
            "universe_rule_version": "v2",
            "security_identity_manifest": str(tmp_path / "security"),
            "security_identity_sha256": "a" * 64,
            "provider_absence_manifest": str(tmp_path / "provider"),
            "provider_absence_sha256": "b" * 64,
        }
    }
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(request))
    monkeypatch.setattr(database, "validate_request", lambda _: None)
    monkeypatch.setattr(rotation, "_industry_adapter", lambda *_a, **_k: None)
    monkeypatch.setattr(database, "load_security_source_identity_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(database, "load_provider_absence_manifest", lambda *_a, **_k: None)
    connection_token = object()

    @contextmanager
    def connect(_):
        yield connection_token

    monkeypatch.setattr(database, "connect", connect)
    monkeypatch.setattr(
        database,
        "_read_plan",
        lambda req, conn: {"start": denominator.window_start.isoformat(), "end": denominator.window_end.isoformat()},
    )

    class Reader:
        def __init__(self, connection, *_args, **_kwargs):
            assert connection is connection_token

        def validate_source(self):
            pass

        def load_industry_pit_denominator(self, **kwargs):
            assert kwargs == {"window_start": denominator.window_start, "window_end": denominator.window_end}
            return denominator

    monkeypatch.setattr(facts, "PostgresStockFactReader", Reader)
    pin = {"path": str(store / "capture.json"), "sha256": "c" * 64}
    monkeypatch.setattr(source, "refresh", lambda root: pin if root == store else pytest.fail("wrong store"))
    monkeypatch.setattr(source, "observation_from_capture", lambda *_a, **_k: observation)

    def build(**kwargs):
        assert not pending
        assert kwargs["denominator"] is denominator and kwargs["observation"] is observation
        output.mkdir()
        (output / "authority.json").write_text("{}")
        return dict.fromkeys(
            (
                "schema_version",
                "authority_path",
                "identity",
                "data_date",
                "target_symbols",
                "resolved_symbols",
                "target_unavailable",
                "causal_daily_from",
                "database_write",
                "dataset_write",
            ),
            None,
        )

    monkeypatch.setattr(subject, "build_authority", build)
    root = Path(__file__).resolve().parents[3]
    script = root / "scripts/hmm_risk/build_observed_industry_authority.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [str(script), "--request", str(request_path), "--source-store", str(store), "--output-root", str(output)],
    )
    module = runpy.run_path(str(script))
    monkeypatch.setattr(
        module["subprocess"], "run", lambda args, **_: SimpleNamespace(stdout="" if args[1] == "status" else "a" * 40)
    )
    assert module["main"]() == (2 if pending else 0)
    result = json.loads(capsys.readouterr().out)
    if pending:
        assert result["status"] == "PENDING_OBSERVATION_NOT_YET_CAUSAL"
        assert result["full_hmm_preflight_passed"] is False and not output.exists()
    else:
        assert (output / "database_forward_request.json").is_file()
