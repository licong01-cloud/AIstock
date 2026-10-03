from __future__ import annotations

from datetime import date
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
from pathlib import Path

import pytest

from backend.data_service.moneyflow_contract import MONEYFLOW_FIELD_MAP
from backend.data_service.security_source_identity import load_default_security_source_identity_manifest
from scripts import audit_qe_moneyflow_alias_coverage as audit_module
from scripts import repair_qe_moneyflow_alias_candidate as repair

audit = audit_module


def test_provider_absence_keys_reject_duplicate_identity(tmp_path: Path) -> None:
    row = {
        "canonical_ts_code": "302132.SZ",
        "source_dataset": "market.moneyflow_ts",
        "source_ts_code": "300114.SZ",
        "trade_date": "2024-08-13",
    }
    path = tmp_path / "provider_absence.json"
    path.write_text(json.dumps({"schema_version": "test_v1", "rows": [row, row]}), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate provider absence key"):
        audit_module._provider_absence_keys(path)


def test_main_writes_receipt_create_exclusive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = tmp_path / "receipt.json"
    receipt = {
        "schema_version": audit_module.SCHEMA_VERSION,
        "status": "PASS",
        "database_read": True,
        "database_write": False,
    }
    monkeypatch.setattr(audit_module, "audit", lambda **_: receipt)
    args = [
        "--candidate-root",
        str(tmp_path),
        "--security-identity-manifest",
        str(tmp_path / "identity.json"),
        "--provider-absence-manifest",
        str(tmp_path / "absence.json"),
        "--start-date",
        "2024-08-13",
        "--end-date",
        "2025-02-14",
        "--output",
        str(output),
    ]

    assert audit_module.main(args) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == receipt
    with pytest.raises(FileExistsError):
        audit_module.main(args)


def facts(code, dates, value=100.0):
    frame = pd.DataFrame({"ts_code": code, "trade_date": pd.to_datetime(dates)})
    for column in MONEYFLOW_FIELD_MAP.values():
        frame[column] = value
    return frame


def classify(source, frozen, *, prices=(), suspended=(), absence=()):
    return audit.classify_sessions(
        identity=load_default_security_source_identity_manifest(),
        symbol="302132.SZ",
        sessions=[date(2023, 1, 12), date(2025, 2, 14), date(2025, 2, 17)],
        price_dates=set(prices), suspension_dates=set(suspended),
        source=source, frozen=frozen, absence_keys=set(absence),
    )


def test_full_calendar_includes_dates_missing_from_both_source_and_file():
    result = classify(facts("300114.SZ", ["2025-02-14"]), facts("300114.SZ", []))
    assert [r["classification"] for r in result] == [
        "SOURCE_AND_FROZEN_MISSING", "EXPORT_MISSING", "SOURCE_AND_FROZEN_MISSING"
    ]
    assert [r["effective_source_code"] for r in result] == ["300114.SZ", "300114.SZ", "302132.SZ"]


def test_suspension_only_explains_absence_without_price_or_moneyflow_fact():
    suspended = [date(2023, 1, 12)]
    result = classify(facts("300114.SZ", []), facts("300114.SZ", []), suspended=suspended)
    assert result[0]["classification"] == "SUSPENDED"
    result = classify(facts("300114.SZ", []), facts("300114.SZ", []), suspended=suspended, prices=suspended)
    assert result[0]["classification"] == "SOURCE_AND_FROZEN_MISSING"


def test_all_fields_finite_units_and_source_identity_are_checked():
    source = facts("300114.SZ", ["2025-02-14"])
    frozen = source.copy()
    assert classify(source, frozen)[1]["classification"] == "MATCHED"
    frozen.loc[0, "mf_sm_buy_vol"] = np.nan
    assert classify(source, frozen)[1]["classification"] == "NONFINITE_FROZEN"
    frozen = source.copy()
    frozen.loc[0, "mf_net_amt"] *= 10000
    assert classify(source, frozen)[1]["classification"] == "VALUE_MISMATCH"
    frozen = facts("302132.SZ", ["2025-02-14"])
    assert classify(source, frozen)[1]["classification"] == "SOURCE_IDENTITY_CONFLICT"


def test_duplicate_source_rows_and_absence_contradictions_fail_closed():
    source = facts("300114.SZ", ["2025-02-14"])
    with pytest.raises(ValueError, match="duplicate"):
        classify(pd.concat([source, source]), source)
    absence = [("302132.SZ", "market.moneyflow_ts", "300114.SZ", date(2025, 2, 14))]
    with pytest.raises(ValueError, match="contradict"):
        classify(source, source, absence=absence)


def test_repair_preserves_finite_values_and_only_adds_missing_source_dates():
    source = facts("300114.SZ", ["2023-01-12", "2025-02-14"])
    existing = source.iloc[1:].rename(columns={"trade_date": "datetime", "ts_code": "instrument"})
    existing = existing.set_index(["datetime", "instrument"])
    patch = repair.missing_rows(source, existing)
    assert len(patch) == 1
    assert patch.index[0] == (pd.Timestamp("2023-01-12"), "300114.SZ")
    existing.loc[:, "mf_net_amt"] *= 10
    with pytest.raises(ValueError, match="disagree"):
        repair.missing_rows(source, existing)
    source.loc[0, "mf_net_amt"] = np.nan
    with pytest.raises(ValueError, match="nonfinite"):
        repair.missing_rows(source, existing)


def test_copy_on_write_omits_moneyflow_and_never_overwrites_candidate(tmp_path):
    baseline = tmp_path / "baseline"
    staging = tmp_path / "staging"
    factor = baseline / repair.FACTOR
    factor.mkdir(parents=True)
    (factor / "moneyflow.h5").write_bytes(b"original")
    (factor / "daily_pv.h5").write_bytes(b"unchanged")
    reused = repair.clone_reusing_bytes(baseline, staging)
    assert len(reused) == 1
    assert not (staging / repair.MONEYFLOW).exists()
    assert (staging / repair.FACTOR / "daily_pv.h5").samefile(factor / "daily_pv.h5")
    with pytest.raises(FileExistsError):
        repair.clone_reusing_bytes(baseline, staging)
    with pytest.raises(ValueError, match="distinct sibling"):
        repair.clone_reusing_bytes(baseline, baseline)


def test_complete_successor_build_updates_pins_and_keeps_source_private(tmp_path, monkeypatch):
    baseline, target = tmp_path / "baseline", tmp_path / "successor"
    factor = baseline / repair.FACTOR
    factor.mkdir(parents=True)
    identity = load_default_security_source_identity_manifest()
    (factor / "security_source_identity.json").write_bytes(identity.source_path.read_bytes())
    source = pd.concat([facts("300114.SZ", ["2023-01-12", "2025-02-14"]),
                        facts("302132.SZ", ["2025-02-17"])], ignore_index=True)
    source["_canonical_ts_code"] = "302132.SZ"
    frame = source.rename(columns={"trade_date": "datetime", "ts_code": "instrument"})
    frame = frame.set_index(["datetime", "instrument"])[list(MONEYFLOW_FIELD_MAP.values())]
    frame.iloc[1:].to_hdf(factor / "moneyflow.h5", key="data", format="table", data_columns=True)
    prices = pd.DataFrame({"close": 10.0}, index=pd.MultiIndex.from_arrays(
        [pd.to_datetime(source.trade_date), ["302132.SZ"] * 3], names=["datetime", "instrument"]))
    prices.to_hdf(factor / "daily_pv.h5", key="data", format="table", data_columns=True)
    repair.write_json(factor / "meta.json", {"rows_by_file": {"moneyflow.h5": 2}})
    calendar = baseline / "components/daily_bin_candidate/calendars/day.txt"
    calendar.parent.mkdir(parents=True)
    calendar.write_text("2023-01-12\n2025-02-14\n2025-02-17\n")
    pools = baseline / "stock_pools/stock_universe.txt"
    pools.parent.mkdir()
    pools.write_text("302132.SZ\t2020-07-30\t2026-08-31\n")
    suspend = baseline / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
    suspend.parent.mkdir()
    pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"]).to_parquet(suspend)
    absence = tmp_path / "absence.json"
    repair.write_json(absence, {"rows": []})

    def pin(path):
        return {"path": path.relative_to(baseline).as_posix(), "sha256": repair._sha256(path), "size": path.stat().st_size}

    files = [pin(p) for p in factor.iterdir()]
    inventory = baseline / "reports/inventory.json"
    repair.write_json(inventory, {"files": files, "file_count": len(files)})
    old_audit, old_repair = baseline / "reports/audit.json", baseline / "reports/repair.json"
    repair.write_json(old_audit, {})
    repair.write_json(old_repair, {})
    manifest = {"release_id": "test", "cutoff_trade_date": "2026-08-31", "source_contract": {},
        "components": {"moneyflow_h5": pin(factor / "moneyflow.h5"), "factor_meta": pin(factor / "meta.json"),
            "factor_content_manifest": pin(inventory), "moneyflow_alias_coverage": pin(old_audit),
            "moneyflow_alias_repair": pin(old_repair), "security_source_identity": pin(factor / "security_source_identity.json")}}
    manifest["dataset_manifest_sha256"] = repair._manifest_identity(manifest)
    repair.write_json(baseline / "qe_dataset_manifest.json", manifest)
    repair.write_json(baseline / "direct_monthly_state.json", {"components": {}})
    profile = tmp_path / "active.json"
    repair.write_json(profile, {"components": {"dataset_manifest_sha256": manifest["dataset_manifest_sha256"]}})
    profile_hash = repair._sha256(profile)
    cursor = SimpleNamespace(execute=lambda *_: None, fetchone=lambda: ("test", "on", "readonly-snapshot"))
    from contextlib import nullcontext
    conn = SimpleNamespace(set_session=lambda **_: None, cursor=lambda: nullcontext(cursor),
                           rollback=lambda: None, close=lambda: None)
    monkeypatch.setattr(repair.psycopg2, "connect", lambda **_: conn)
    monkeypatch.setattr(repair, "dotenv_values", lambda _: {k: "test" for k in
                        ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]})
    monkeypatch.setattr(repair, "_load_authoritative_source_facts", lambda *_a, **_k: source)
    args = SimpleNamespace(baseline_root=baseline, candidate_root=target, active_profile=profile,
        provider_absence_manifest=absence, expected_manifest=manifest["dataset_manifest_sha256"],
        start_date=date(2023, 1, 12), end_date=date(2025, 2, 17), env_file=tmp_path / "unused",
        generation="next", revision="next", bug_id="BUG-1646", dry_run=False)
    result = repair.repair(args)
    assert result["status"] == "CANDIDATE_READY" and result["appended"] == 1
    assert result["after"]["resolved"] == 3 and result["after"]["unknown"] == 0
    assert repair._sha256(profile) == profile_hash
    assert not (target / repair.MONEYFLOW).samefile(baseline / repair.MONEYFLOW)
    assert (target / repair.FACTOR / "daily_pv.h5").samefile(factor / "daily_pv.h5")
    assert len(pd.read_hdf(factor / "moneyflow.h5", "data")) == 2
    assert json.loads((target / repair.META).read_text())["rows_by_file"]["moneyflow.h5"] == 3
    updated = json.loads((target / "qe_dataset_manifest.json").read_text())
    assert repair._manifest_identity(updated) == result["manifest"]
    for item in updated["components"].values():
        assert repair._sha256(target / item["path"]) == item["sha256"]
    with pytest.raises(ValueError, match="unused sibling"):
        repair.repair(args)
