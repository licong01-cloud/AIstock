from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import formal_state_input as subject
from backend.services.hmm_risk import formal_state_executor as executor
from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES
from backend.services.hmm_risk.formal_state_executor import frozen_release_binding
from backend.services.hmm_risk.formal_state_model import FormalStateError


def test_effect_source_and_parquet_reads_are_bounded_before_file_access(monkeypatch, tmp_path):
    from backend.services.hmm_risk import formal_state_effect as effect

    monkeypatch.setattr(effect, "validate_models", lambda _: None)
    monkeypatch.setattr(
        reader, "load_rotation_l1_direct_v2_source_assets", lambda *_a, **_kw: pytest.fail("source access")
    )
    with pytest.raises(FormalStateError, match="boundary"):
        subject._effect_stock_facts({}, {}, start=date(2025, 4, 1), end=date(2026, 4, 1))
    path = tmp_path / "facts.parquet"
    pd.DataFrame({"trade_date": pd.to_datetime(["2026-03-30", "2026-04-01"]), "value": [1.0, float("nan")]}).to_parquet(
        path
    )
    original = pd.read_parquet

    def bounded(*args, **kwargs):
        assert kwargs["filters"] == [
            ("trade_date", ">=", pd.Timestamp("2026-03-30")),
            ("trade_date", "<=", pd.Timestamp("2026-03-31")),
        ]
        return original(*args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", bounded)
    frame = reader._read_parquet_date_window(
        path, start=date(2026, 3, 30), end=date(2026, 3, 31), columns=["trade_date", "value"]
    )
    assert len(frame) == 1 and frame["value"].tolist() == [1.0]


def test_effect_explicit_catalog_retains_structurally_absent_sector_as_na():
    from dataclasses import replace
    from backend.tests.hmm_risk.test_stock_fact_observation import _feature_domain_aggregate
    from backend.services.hmm_risk.stock_fact_observation import build_c010_feature_domain_panel
    from backend.services.hmm_risk.contracts import StateModelSetError

    days = [date(2025, 4, 1), date(2025, 4, 2)]
    codes = [f"801{i:03d}.SI" for i in range(131)]
    aggregates = [
        replace(_feature_domain_aggregate(day, i, j), l1_code=codes[i])
        for j, day in enumerate(days)
        for i in range(130)
    ]
    kwargs = {
        "trading_dates": days,
        "csi300_returns": {day: 0.01 for day in days},
        "expected_sector_count": 131,
        "direct_sector_level": "L2",
    }
    with pytest.raises(StateModelSetError, match="131"):
        build_c010_feature_domain_panel(aggregates, **kwargs)
    panel, _, _ = build_c010_feature_domain_panel(aggregates, **kwargs, canonical_sector_codes=codes)
    assert len(panel) == 262
    assert panel.xs(codes[-1], level="l1_code")[list(ALL_CORE_FEATURES)].isna().all().all()
    empty_panel, _, _ = build_c010_feature_domain_panel([], **kwargs, canonical_sector_codes=codes)
    assert len(empty_panel) == 262 and empty_panel[list(ALL_CORE_FEATURES)].isna().all().all()


def test_effect_strictly_prior_circ_context_is_used_without_same_day_substitution(tmp_path, monkeypatch):
    from types import SimpleNamespace

    symbol = "000001.SZ"
    prior, day = date(2025, 3, 31), date(2025, 4, 1)
    rows = np.ones(1, dtype=reader._QLIB_SOURCE_DTYPE)
    rows["symbol"] = symbol.encode("ascii")
    rows["trade_date"] = 20250401
    path = tmp_path / "202504.bin"
    rows.tofile(path)
    index = pd.MultiIndex.from_arrays([pd.to_datetime([day]), [symbol]], names=["datetime", "instrument"])
    basic = pd.DataFrame(1.0, index=index, columns=reader._DAILY_BASIC_COLUMNS, dtype=np.float32)
    flow = pd.DataFrame(1.0, index=index, columns=reader._MONEYFLOW_COLUMNS, dtype=np.float32)
    monkeypatch.setattr(
        reader,
        "_load_fixed_h5_window",
        lambda _p, **kw: basic if tuple(kw["expected_columns"]) == reader._DAILY_BASIC_COLUMNS else flow,
    )
    adapter = SimpleNamespace(
        resolve=lambda *_: SimpleNamespace(
            status="resolved", reason_code=None, l1_code="801010.SI", l1_name="L1", l2_code="801011.SI", l2_name="L2"
        )
    )
    security = SimpleNamespace(
        resolve=lambda *_: SimpleNamespace(source_ts_code=symbol, evidence=lambda: {"source_ts_code": symbol})
    )
    captured = []
    context = {symbol: (prior, 500.0, "available", None)}
    kwargs = {
        "month_paths": [path],
        "assets": {"files": {"daily_basic": tmp_path / "basic", "moneyflow": tmp_path / "flow"}},
        "calendar": [prior, day],
        "spans": {symbol: ((day, day),)},
        "adapter": adapter,
        "security": security,
        "provider_absence": SimpleNamespace(rows=()),
        "suspension_keys": frozenset(),
        "contributor_eligibility": {symbol: True},
        "window_start": day,
        "window_end": day,
        "build_feature_domain_aggregates": False,
        "day_rows_callback": lambda _d, values: captured.extend(values),
    }
    reader._build_stock_fact_aggregates(**kwargs, initial_circ_state=context)
    assert captured[0]["prev_circ_mv_cny"] == 500.0 and captured[0]["circ_mv_source_date"] == prior
    assert context == {symbol: (prior, 500.0, "available", None)}
    with pytest.raises(reader.RotationL1InputBundleError, match="strictly prior"):
        reader._build_stock_fact_aggregates(**kwargs, initial_circ_state={symbol: (day, 500.0, "available", None)})


def test_bounded_suspend_empty_window_still_validates_declared_counts(tmp_path):
    import json

    path = tmp_path / "suspend.parquet"
    meta_path = tmp_path / "meta.json"
    day = date(2026, 3, 31)
    pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": pd.to_datetime(["2026-08-31"]),
            "suspend_type": ["S"],
            "suspend_timing": [""],
        }
    ).to_parquet(path)
    meta = {
        "schema_version": reader.DIRECT_V2_SUSPEND_SCHEMA_VERSION,
        "component": "suspend_d",
        "start": reader.DIRECT_V2_RELEASE_START.isoformat(),
        "end": "2026-08-31",
        "universe_key": "test",
        "source_table": "market.suspend_d",
        "suspend_type": "S",
        "daily_row_counts": {day.isoformat(): 1},
    }
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    kwargs = {
        "calendar": [day],
        "expected_release_cutoff": date(2026, 8, 31),
        "expected_universe_key": "test",
        "bounded": True,
    }
    with pytest.raises(reader.RotationL1InputBundleError, match="readback"):
        reader._load_suspend_keys(path, meta_path, **kwargs)
    meta["daily_row_counts"][day.isoformat()] = 0
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    assert reader._load_suspend_keys(path, meta_path, **kwargs) == frozenset()


def test_frozen_binding_pins_final_v17_without_changing_model_contract():
    assert frozen_release_binding() == {
        "generation": "20261002-v17-unified-basic-history1",
        "release_id": "qe_hmm_full_v2_20260831",
        "revision": "20261002-r8-unified-basic-history1",
        "cutoff": "2026-08-31",
        "manifest_sha256": "97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c",
        "manifest_file_sha256": "eb193a03fedeb0aa4c825daed492f50331d4f1581b59b750203501b95694ad76",
    }
    assert subject.SOURCE_START == date(2020, 7, 30)
    assert subject.SOURCE_END == date(2025, 4, 30)


@pytest.mark.parametrize(
    "generation,manifest",
    [
        ("20261001-v16-unified-moneyflow2", "fcc90ff0df761511c7e4d431da8a9bddf04e1c66c70c8780c6359e876f247098"),
        ("20261002-v17-unified-basic-history1", "eef96dd6c90617be5b0d01ab64acab4e5e885c925757e14f75242c4eaedb48b7"),
    ],
)
def test_constructor_rejects_old_or_prefinal_release_before_building(monkeypatch, tmp_path, generation, manifest):
    monkeypatch.setattr(
        reader,
        "load_rotation_l1_direct_v2_source_assets",
        lambda *_, **__: {
            "release_identity": {"frozen_release_generation": generation, "dataset_manifest_sha256": manifest}
        },
    )
    monkeypatch.setattr(reader, "load_active_hmm_dataset_identity", lambda *_: pytest.fail("active fallback accessed"))
    with pytest.raises(FormalStateError) as error:
        subject.prepare_file_request(
            candidate_root=tmp_path,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "absence.json",
            industry_authority={},
            work_parent=tmp_path / "work",
            producer_commit="a" * 40,
        )
    assert error.value.reason_code == "hmm_risk_formal_identity_mismatch"
    assert not (tmp_path / "work").exists()
    request = executor.receipt(
        {
            "schema_version": executor.VERSION,
            "contracts": executor.CONTRACTS,
            "source_identity": {"generation": generation, "manifest_sha256": manifest, "cutoff": "2026-08-31"},
            "industry_authority": {},
            "policy": {},
            "train_calendar": [],
            "validation_calendar": [],
            "sector_codes": {},
            "series": {},
        }
    )
    monkeypatch.setattr(executor, "read_json", lambda _: request)
    with pytest.raises(FormalStateError) as error:
        executor.load_request(tmp_path / "old-request.json")
    assert error.value.reason_code == "hmm_risk_formal_identity_mismatch"


def test_file_constructor_supplies_approved_frozen_binding_without_active_or_fit(monkeypatch, tmp_path):
    def first_read(root, **kwargs):
        assert root == tmp_path
        assert kwargs["frozen_release_binding"] == frozen_release_binding()
        assert kwargs["data_window_end"] == date(2025, 4, 30)
        assert kwargs["security_identity_manifest"] == tmp_path / "security.json"
        assert kwargs["provider_absence_manifest"] == tmp_path / "absence.json"
        raise FormalStateError("hmm_risk_formal_input_invalid", "explicit first file read reached")

    monkeypatch.setattr(reader, "load_rotation_l1_direct_v2_source_assets", first_read)
    monkeypatch.setattr(reader, "load_active_hmm_dataset_identity", lambda *_: pytest.fail("active fallback accessed"))
    with pytest.raises(FormalStateError, match="explicit first file read"):
        subject.prepare_file_request(
            candidate_root=tmp_path,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "absence.json",
            industry_authority={},
            work_parent=tmp_path / "work",
            producer_commit="a" * 40,
        )
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize("fault", [None, "duplicate", "sector_missing", "calendar_missing", "nine_dimensions"])
def test_full_feature_frame_closes_calendar_and_sector_denominator(fault):
    calendar = ["2024-07-01", "2024-07-02"]
    codes = ["801001.SI", "801002.SI"]
    index = pd.MultiIndex.from_product([pd.to_datetime(calendar), codes], names=["date", "sector"])
    columns = [*ALL_CORE_FEATURES, "benchmark_return"]
    panel = pd.DataFrame(np.arange(4 * len(columns)).reshape(4, len(columns)), index=index, columns=columns)
    if fault == "duplicate":
        panel = pd.concat([panel, panel.iloc[:1]])
    elif fault == "sector_missing":
        panel = panel.loc[panel.index.get_level_values(1) != codes[1]]
    elif fault == "calendar_missing":
        panel = panel.iloc[:2]
    elif fault == "nine_dimensions":
        panel = panel.iloc[:, :9]
    if fault is not None:
        with pytest.raises(FormalStateError):
            subject._frame(panel, codes=codes, calendar=calendar)
    else:
        restored = subject._frame(panel.iloc[::-1], codes=codes, calendar=calendar)
        assert restored.equals(panel) and len(restored.columns) == 21
