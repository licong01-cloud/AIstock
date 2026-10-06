from __future__ import annotations

from datetime import date
import hashlib
import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.data_service.security_source_identity import DEFAULT_MANIFEST_PATH
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_bridge import CompiledMonthlyBuild
from backend.services.dataset_release.monthly_shared_components import (
    FrozenMonthlySharedComponentBuilder,
    MonthlySharedComponentsError,
    _build_sector_membership,
    _index_pool_intervals,
    _pit_intervals,
    _append_sector_month_sidecars,
    _native_sector_summary,
    _build_suspend,
)
from backend.services.dataset_release.monthly_worker import ProducerContext
from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.sector_enrichment import FrozenSectorEnricher
from backend.services.dataset_release.shared_sector_context import (
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
)


def _snapshot():  # type: ignore[no-untyped-def]
    return freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": "2024-07-01",
                "eligible_end": "2024-07-05",
                "entry_reason": None,
                "exit_reason": None,
            }
        ],
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="fixture",
        scope_start=date(2024, 7, 1),
        cutoff=date(2024, 7, 5),
        state_identity="fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def test_native_suspend_keeps_pinned_prefix_and_checks_only_new_month(tmp_path):
    import json

    old_root = tmp_path / "august"
    old_root.mkdir()
    old = pd.DataFrame([{"trade_date": pd.Timestamp("2026-08-31"), "ts_code": "000005.SZ",
                         "suspend_type": "S", "suspend_timing": None}])
    old.to_parquet(old_root / "suspend.parquet", index=False)
    (old_root / "meta.json").write_text(json.dumps({"end": "2026-08-31", "row_count": 1,
        "daily_row_counts": {"2026-08-31": 1}}), encoding="utf-8")
    def pin(name):
        path = old_root / name
        return {"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size}
    prefix = SimpleNamespace(root=old_root, cutoff=date(2026, 8, 31), manifest_sha256="a" * 64,
        manifest={"components": {"suspend_data": pin("suspend.parquet"), "suspend_meta": pin("meta.json")}})
    new_root = tmp_path / "september"
    new_root.mkdir()
    day = date(2026, 9, 1)
    parquet, meta, keys, count = _build_suspend(root=new_root, rows=[{
        "trade_date": day, "ts_code": "000001.SZ", "suspend_type": "S", "suspend_timing": None,
    }], pit_rows=[("000001.SZ", day, day)], calendar=(date(2026, 8, 31), day),
        profile=SimpleNamespace(start_date=date(2018, 8, 1), universe_key="pit"), cutoff=day, prefix=prefix)
    actual = pd.read_parquet(parquet)
    pd.testing.assert_frame_equal(actual.iloc[:1].reset_index(drop=True), old)
    assert keys == {("000001.SZ", day)}
    assert count == 1
    readback = json.loads(meta.read_text(encoding="utf-8"))
    assert readback["validation_scope"] == "month_delta"
    assert readback["historical_values_validated"] == 0
    assert readback["row_count"] == 2
    assert readback["daily_row_counts"] == {"2026-08-31": 1, "2026-09-01": 1}
    assert hashlib.sha256((old_root / "suspend.parquet").read_bytes()).hexdigest() == prefix.manifest["components"]["suspend_data"]["sha256"]


def test_sector_month_reads_only_physical_tail(tmp_path, monkeypatch):
    from backend.services.dataset_release.monthly_preparation_shared import summarize_sector_context

    index = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-08-30"), "000001.SZ"), (pd.Timestamp("2024-09-02"), "000001.SZ")],
        names=["datetime", "instrument"],
    )
    path = tmp_path / "sector.h5"
    pd.DataFrame({"l2_code_id": [-1, 0], "sw2_pct_change": [float("inf"), 1.0],
                  "sw2_vol": [float("inf"), 2.0], "sw2_amount": [float("inf"), 3.0]}, index=index).to_hdf(
        path, "data", format="table", data_columns=["datetime", "instrument"])
    selected = []
    original = pd.HDFStore.select

    def bounded(store, key, **kwargs):
        selected.append(kwargs)
        assert kwargs.get("start") == 1 and kwargs.get("stop") == 2
        return original(store, key, **kwargs)

    monkeypatch.setattr(pd.HDFStore, "select", bounded)
    day = date(2024, 9, 2)
    member, market, counts = summarize_sector_context(
        sector_h5=path, profile=SimpleNamespace(start_date=date(2018, 8, 1)),
        enricher=SimpleNamespace(), code_map=SimpleNamespace(id_to_code={0: "801011.SI"}),
        quote=SimpleNamespace(entries={"801011.SI": ((day, day),)}),
        calendar=(date(2024, 8, 30), day), pit_rows=(("000001.SZ", day, day),),
        start=day, cutoff=day, bound=1, checkpoint=lambda: None,
        market_start=day, source_start_row=1, source_month_rows=1,
    )
    assert selected and market.sw_daily_total_vol.tolist() == [2.0]
    assert len(member) == 1 and counts["frozen_stock_trading_day_count"] == 1


def test_sector_month_inherits_prefix_and_keeps_boundary_transition():
    old = pd.DataFrame([
        ["000001.SZ", date(2024, 7, 1), date(2024, 8, 30), 0],
        ["000002.SZ", date(2024, 7, 1), date(2024, 8, 30), 0],
    ], columns=["instrument", "start_date", "end_date", "l2_code_id"])
    new = pd.DataFrame([
        ["000001.SZ", date(2024, 9, 2), date(2024, 9, 3), 0],
        ["000002.SZ", date(2024, 9, 2), date(2024, 9, 3), 1],
    ], columns=old.columns)
    prior = pd.DataFrame({"trade_date": [date(2024, 8, 30)], "sw_daily_total_vol": [5.0]})
    delta = pd.DataFrame({"trade_date": [date(2024, 9, 2), date(2024, 9, 3)], "sw_daily_total_vol": [6.0, 7.0]})
    merged, market = _append_sector_month_sidecars(
        old_membership=old, old_market=prior, new_membership=new, new_market=delta,
        calendar=tuple(prior.trade_date) + tuple(delta.trade_date), predecessor_cutoff=date(2024, 8, 31),
        cutoff=date(2024, 9, 3),
    )
    assert merged[merged.instrument == "000001.SZ"].end_date.tolist() == [date(2024, 9, 3)]
    assert merged[merged.instrument == "000002.SZ"].l2_code_id.tolist() == [0, 1]
    assert market.sw_daily_total_vol.tolist() == [5.0, 6.0, 7.0]
    assert old.end_date.tolist() == [date(2024, 8, 30)] * 2
    with pytest.raises(MonthlySharedComponentsError, match="month calendar"):
        _append_sector_month_sidecars(
            old_membership=old, old_market=prior, new_membership=new, new_market=delta.iloc[:1],
            calendar=tuple(prior.trade_date) + tuple(delta.trade_date), predecessor_cutoff=date(2024, 8, 31),
            cutoff=date(2024, 9, 3),
        )


def test_native_sector_summary_uses_pinned_metadata_and_real_month_rows(tmp_path, monkeypatch):
    from backend.services.dataset_release.shared_sector_context import (
        build_release_sw_l2_code_map_payload, validate_release_sw_l2_code_map,
    )
    from backend.services.dataset_release.monthly_legacy_prefix import _signature
    from backend.services.dataset_release.canonical import canonical_json_bytes

    predecessor = tmp_path / "old"
    predecessor.mkdir()
    payload = build_release_sw_l2_code_map_payload(
        code_to_id={f"801{number:03d}.SI": number for number in range(131)},
        member_backed_codes=tuple(f"801{number:03d}.SI" for number in range(131)),
        authority_id="fixture", authority_sha256="a" * 64,
    )
    calendar = (date(2024, 8, 30), date(2024, 9, 2))
    old_member = pd.DataFrame([["000001.SZ", calendar[0], calendar[0], 0]],
                             columns=["instrument", "start_date", "end_date", "l2_code_id"])
    old_market = pd.DataFrame({"trade_date": [calendar[0]], "sw_daily_total_vol": [4.0]})
    old_member.to_parquet(predecessor / "member.parquet", index=False)
    old_market.to_parquet(predecessor / "market.parquet", index=False)
    (predecessor / "map.json").write_bytes(canonical_json_bytes(payload) + b"\n")
    pins = {}
    for name, filename in (("sector_membership_spans", "member.parquet"), ("market_context", "market.parquet"),
                           ("sector_code_map", "map.json")):
        path = predecessor / filename
        pins[name] = {"path": filename, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size}
    pins["sector_data"] = {"row_count": 1}
    prefix = SimpleNamespace(root=predecessor, manifest={"components": pins}, cutoff=date(2024, 8, 31),
                             manifest_sha256="b" * 64)
    data = tmp_path / "sector.h5"
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(day), "000001.SZ") for day in calendar],
                                     names=["datetime", "instrument"])
    pd.DataFrame({"l2_code_id": [0, 0], "sw2_pct_change": [float("inf"), 0.1],
                  "sw2_vol": [float("inf"), 5.0], "sw2_amount": [float("inf"), 9.0]}, index=index).to_hdf(
        data, "data", format="table", data_columns=["datetime", "instrument"])
    authority = {"verified_output_files": {"factor_bundle/sector_data.h5": {
        "signature": list(_signature(data)), "size": data.stat().st_size,
        "sha256": hashlib.sha256(data.read_bytes()).hexdigest()}},
        "physical_month_ranges": {"sector_data": {"start_row": 1, "row_count": 1}}}
    arguments = dict(prefix=prefix, sector_h5=data, physical_authority=authority,
        profile=SimpleNamespace(start_date=calendar[0], resource_policy=SimpleNamespace(validation_read_chunk_rows=1)),
        enricher=SimpleNamespace(), code_map=validate_release_sw_l2_code_map(payload),
        quote=SimpleNamespace(entries={"801000.SI": ((calendar[0], calendar[-1]),)}), calendar=calendar,
        pit_rows=(("000001.SZ", calendar[0], calendar[-1]),), cutoff=calendar[-1])
    member, market, counts = _native_sector_summary(**arguments)
    assert member.end_date.tolist() == [calendar[-1]]
    assert market.sw_daily_total_vol.tolist() == [4.0, 5.0]
    assert counts["historical_sector_values_read"] == 0 and counts["validation_scope"] == "month_delta"
    codes = {f"801{number:03d}.SI": number for number in range(131)}
    codes["801000.SI"], codes["801001.SI"] = 1, 0
    drifted = build_release_sw_l2_code_map_payload(
        code_to_id=codes, member_backed_codes=tuple(sorted(codes)), authority_id="new", authority_sha256="c" * 64)
    # An independently valid reordered mapping must still be rejected before
    # reading sector history; changing code IDs is not a monthly append.
    with pytest.raises(MonthlySharedComponentsError, match="mapping differs"):
        _native_sector_summary(**{**arguments, "code_map": validate_release_sw_l2_code_map(drifted)})
    pins["sector_data"]["row_count"] = 2
    with pytest.raises(MonthlySharedComponentsError, match="inherited row count"):
        _native_sector_summary(**arguments)


@pytest.mark.parametrize("unexpected_quote", [False, True])
def test_sector_month_keeps_stopped_quote_na_without_losing_membership(tmp_path, unexpected_quote):
    from backend.services.dataset_release.monthly_preparation_shared import summarize_sector_context
    from backend.services.dataset_release.monthly_component_preparation import ComponentPreparationError
    day = date(2024, 9, 2)
    index = pd.MultiIndex.from_tuples([(pd.Timestamp(day), symbol) for symbol in ("000001.SZ", "000002.SZ")],
                                     names=["datetime", "instrument"])
    value = 0.0 if unexpected_quote else float("nan")
    source = pd.DataFrame({"l2_code_id": [0, 1], "sw2_pct_change": [value, 1.0],
                           "sw2_vol": [value, 2.0], "sw2_amount": [value, 3.0],
                           "sw2_mf_net_amt": [100.0, 200.0]}, index=index)
    path = tmp_path / "sector.h5"
    source.to_hdf(path, "data", format="table", data_columns=["datetime", "instrument"])
    before = path.read_bytes()
    arguments = dict(sector_h5=path, profile=SimpleNamespace(start_date=day), enricher=SimpleNamespace(),
        code_map=SimpleNamespace(id_to_code={0: "801019.SI", 1: "801011.SI"}),
        quote=SimpleNamespace(entries={"801019.SI": (), "801011.SI": ((day, day),)}),
        calendar=(day,), pit_rows=tuple((symbol, day, day) for symbol in ("000001.SZ", "000002.SZ")),
        start=day, cutoff=day, bound=1, checkpoint=lambda: None, market_start=day,
        source_start_row=0, source_month_rows=2)
    if unexpected_quote:
        with pytest.raises(ComponentPreparationError, match="stopped sector"):
            summarize_sector_context(**arguments)
    else:
        member, market, counts = summarize_sector_context(**arguments)
        assert member.instrument.tolist() == ["000001.SZ", "000002.SZ"]
        assert market.sw_daily_total_vol.tolist() == [2.0] and counts["quote_gap_count"] == 0
    assert path.read_bytes() == before


def test_index_pool_sidecars_intersect_membership_with_frozen_pit() -> None:
    calendar = tuple(date(2024, 7, day) for day in range(1, 6))
    pit = _pit_intervals(_snapshot(), calendar)
    rows = []
    definitions = {
        "csi300": ("000300.SH", "CSI"),
        "csi500": ("000905.SH", "CSI"),
        "csi1000": ("000852.SH", "CSI"),
        "star50": ("000688.SH", "SSE"),
        "star100": ("000698.SH", "SSE"),
    }
    for pool_id, (index_code, provider) in definitions.items():
        rows.append(
            {
                "pool_id": pool_id,
                "index_code": index_code,
                "ts_code": "000001.SZ",
                "effective_from": "2024-06-01",
                "effective_to_exclusive": "2024-07-04",
                "source_provider": provider,
                "source_reference": "fixture",
                "updated_at": "2026-09-22T00:00:00+08:00",
            }
        )

    result = _index_pool_intervals(
        membership_rows=rows,
        pit_rows=pit,
        calendar=calendar,
        cutoff=date(2024, 7, 5),
    )

    assert result["csi300"] == (
        ("000001.SZ", date(2024, 7, 1), date(2024, 7, 3)),
    )
    assert set(result) == set(definitions)


def test_sector_membership_fails_closed_on_unknown_active_stock_date() -> None:
    calendar = (date(2024, 7, 1), date(2024, 7, 2))
    enricher = FrozenSectorEnricher.build(
        [{"index_code": "801011.SI", "level": "L2"}],
        [],
    )

    with pytest.raises(MonthlySharedComponentsError, match="cannot resolve"):
        _build_sector_membership(
            enricher=enricher,
            pit_rows=(("000001.SZ", calendar[0], calendar[-1]),),
            calendar=calendar,
            start=calendar[0],
            end=calendar[-1],
        )


def test_sector_membership_compresses_causal_code_transitions() -> None:
    calendar = tuple(date(2024, 7, day) for day in range(1, 6))
    enricher = FrozenSectorEnricher.build(
        [
            {"index_code": "801011.SI", "level": "L2"},
            {"index_code": "801012.SI", "level": "L2"},
        ],
        [
            {
                "ts_code": "000001.SZ",
                "in_date": "2024-07-01",
                "out_date": "2024-07-02",
                "l2_code": "801011.SI",
            },
            {
                "ts_code": "000001.SZ",
                "in_date": "2024-07-03",
                "out_date": None,
                "l2_code": "801012.SI",
            },
        ],
    )

    frame, resolved, frozen_days, gap_fill_days = _build_sector_membership(
        enricher=enricher,
        pit_rows=(("000001.SZ", calendar[0], calendar[-1]),),
        calendar=calendar,
        start=calendar[0],
        end=calendar[-1],
    )

    assert resolved == 5
    assert frozen_days == 0
    assert gap_fill_days == 5
    assert frame.to_dict("records") == [
        {
            "instrument": "000001.SZ",
            "start_date": date(2024, 7, 1),
            "end_date": date(2024, 7, 2),
            "l2_code_id": 0,
        },
        {
            "instrument": "000001.SZ",
            "start_date": date(2024, 7, 3),
            "end_date": date(2024, 7, 5),
            "l2_code_id": 1,
        },
    ]


def test_shared_builder_seals_all_sidecars_from_one_frozen_source(
    tmp_path: Path, monkeypatch
) -> None:
    cutoff = date(2024, 7, 5)
    staging = tmp_path / "release" / ".staging" / "candidate"
    calendar_path = staging / "daily_bin/qlib/calendars/day.txt"
    instruments_path = staging / "daily_bin/qlib/instruments/all.txt"
    calendar_path.parent.mkdir(parents=True)
    instruments_path.parent.mkdir(parents=True)
    calendar_path.write_text(
        "2024-07-01\n2024-07-02\n2024-07-03\n2024-07-04\n2024-07-05\n",
        encoding="utf-8",
    )
    instruments_path.write_text(
        "SZ000001\t2024-07-01\t2024-07-05\n", encoding="utf-8"
    )
    (calendar_path.parent.parent / "features" / "sz000001").mkdir(parents=True)
    (calendar_path.parent.parent / "features" / "sz000001" / "close.day.bin").write_bytes(
        b"daily"
    )
    (instruments_path.parent / "index.txt").write_text(
        "".join(
            f"{item.daily_code}\t{item.required_from.isoformat()}\t{cutoff.isoformat()}\n"
            for item in DOMESTIC_INDEX_DEFINITIONS
        ),
        encoding="utf-8",
    )
    minute_root = staging / "minute_bin/qlib"
    (minute_root / "calendars").mkdir(parents=True)
    (minute_root / "instruments").mkdir()
    (minute_root / "features" / "sz000001").mkdir(parents=True)
    (minute_root / "calendars" / "1min.txt").write_text(
        "2024-07-01\n2024-07-02\n2024-07-03\n2024-07-04\n2024-07-05\n",
        encoding="utf-8",
    )
    (minute_root / "instruments" / "all.txt").write_text(
        "SZ000001\t2024-07-01\t2024-07-05\n", encoding="utf-8"
    )
    (minute_root / "features" / "sz000001" / "close.1min.bin").write_bytes(
        b"minute"
    )
    factor_root = staging / "factor_bundle"
    factor_root.mkdir(parents=True)
    index = pd.MultiIndex.from_product(
        [pd.to_datetime([f"2024-07-0{day}" for day in range(1, 6)]), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame(
        {
            "l2_code_id": [0] * 5,
            "sw2_pct_change": [0.1] * 5,
            "sw2_vol": [100.0] * 5,
            "sw2_amount": [1000.0] * 5,
        },
        index=index,
    ).to_hdf(factor_root / "sector_data.h5", key="data", format="table")
    for dataset in FACTOR_H5_DATASETS:
        path = factor_root / f"{dataset}.h5"
        if not path.exists():
            path.write_bytes(dataset.encode("ascii"))
    (factor_root / "static_factors.parquet").write_bytes(b"static")
    (factor_root / "security_source_identity.json").write_bytes(
        DEFAULT_MANIFEST_PATH.read_bytes()
    )
    (factor_root / "moneyflow_alias_coverage_v1.json").write_text(
        '{"schema_version":"qe_moneyflow_alias_coverage_receipt_v1","status":"PASS"}\n',
        encoding="utf-8",
    )
    index_root = staging / "index_context"
    index_root.mkdir()
    (index_root / "index_daily.h5").write_bytes(b"index")

    snapshot = _snapshot()
    frozen = SimpleNamespace(
        source_content_root="a" * 64,
        artifact_ready_content_root="b" * 64,
        artifact_ready_contract_ref=SimpleNamespace(),
        pit_snapshot=snapshot,
        pit_snapshot_digest=snapshot.spans_sha256,
    )
    monkeypatch.setattr(
        "backend.services.dataset_release.monthly_shared_components.load_source_stage_receipt",
        lambda *_args, **_kwargs: frozen,
    )
    codes = [f"801{index:03d}.SI" for index in range(131)]
    source = {
        "index_membership_pit": [
            {
                "pool_id": pool_id,
                "index_code": index_code,
                "ts_code": "000001.SZ",
                "effective_from": "2024-07-01",
                "effective_to_exclusive": None,
                "source_provider": provider,
                "source_reference": "fixture",
                "updated_at": "2026-09-22T00:00:00+08:00",
            }
            for pool_id, index_code, provider in (
                ("csi300", "000300.SH", "CSI"),
                ("csi500", "000905.SH", "CSI"),
                ("csi1000", "000852.SH", "CSI"),
                ("star50", "000688.SH", "SSE"),
                ("star100", "000698.SH", "SSE"),
            )
        ],
        "suspend_d": [],
        "sw_index_classify": [
            {"index_code": code, "level": "L2"} for code in codes
        ],
        "sw_index_member": [
            {
                "ts_code": "000001.SZ",
                "in_date": "2024-07-01",
                "out_date": None,
                "l2_code": codes[0],
            }
        ],
    }
    monkeypatch.setattr(
        "backend.services.dataset_release.monthly_shared_components._source_rows",
        lambda _cas, _frozen, dataset: tuple(source[dataset]),
    )

    def quote_payload(*, code_map, required_start, cutoff):  # type: ignore[no-untyped-def]
        entries = [
            {
                "canonical_l2_code": code,
                "availability_spans": [
                    {
                        "start_date": required_start.isoformat(),
                        "end_date": cutoff.isoformat(),
                    }
                ],
            }
            for code in sorted(code_map.code_to_id)
        ]
        authority = dict(code_map.mapping_authority)
        return {
            "schema_version": SECTOR_QUOTE_AVAILABILITY_SCHEMA,
            "mapping_authority": authority,
            "entries": entries,
            "quote_availability_digest": digest_named_fields(
                SECTOR_QUOTE_AVAILABILITY_SCHEMA,
                {"mapping_authority": authority, "entries": entries},
            ),
        }

    monkeypatch.setattr(
        "backend.services.dataset_release.monthly_shared_components.build_quote_availability_payload",
        quote_payload,
    )
    control = ControlStore.initialize(tmp_path / "control")
    cas = CASStore(control.root)
    source_ref = cas.put_json({"source": "fixture"})
    frozen.source_manifest_ref = source_ref
    bundle = tmp_path / "bundle.json"
    bundle.write_text("{}\n", encoding="utf-8")
    compiled = CompiledMonthlyBuild(
        source_bundle_path=bundle,
        source_bundle_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(),
        source_stage_receipt_ref=source_ref,
        monthly_actions={},
        physical_plan={},
    )
    context = ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={
            "target_cutoff": cutoff.isoformat(),
            "release_id": "qe_hmm_full_v2_20240705",
        },
        prior_receipts={},
    )
    profile = SimpleNamespace(
        profile="fixture",
        start_date=date(2024, 7, 1),
        resource_policy=SimpleNamespace(validation_read_chunk_rows=2),
        universe_key="aistock_equity_pit_canonical_v2",
    )

    monkeypatch.setattr(
        "backend.services.dataset_release.monthly_shared_components._read_sector_frame",
        lambda *_args: pytest.fail("whole-history stock-sector DataFrame must not be used"),
    )
    result = FrozenMonthlySharedComponentBuilder(
        profile=profile,  # type: ignore[arg-type]
        cas=cas,
        sector_membership_start=date(2024, 7, 1),
    ).execute(
        context=context,
        staging_root=staging,
        compiled=compiled,
        validation_result={
            "validation_ref": source_ref.as_dict(),
            "component_artifact_manifest_ref": source_ref.as_dict(),
        },
    )

    assert result.source_contract["database_fallback"] is False
    assert result.source_contract["provider_fallback_after_source_seal"] is False
    assert result.st_pit_manifest["cutoff_trade_date"] == cutoff.isoformat()
    assert set(result.st_pit_manifest["index_membership_sidecars"]) == {
        "stock_universe",
        "csi300",
        "csi500",
        "csi1000",
        "star50",
        "star100",
    }
    assert all(path.is_file() for path in result.required_files)
    assert (
        staging
        / "components/sector_context_candidate_v1/sector_membership_spans.parquet"
    ).is_file()

    # Builder-integration boundary: use independently copied, actually hashed
    # fixture outputs. Strict full-SOURCE lookup/receipt rejection/recovery is
    # tested separately with the real typed reader, not faked by this lookup.
    from backend.services.dataset_release.monthly_preparation_executor import VerifiedPreparationCheckpoint
    from backend.services.dataset_release.monthly_component_preparation import _file_ref
    from backend.services.dataset_release import monthly_preparation_shared as private

    catalog = staging.parent.parent
    profile.candidate_root = catalog
    definitions = {
        "stock_pools": {path.name: path for path in (staging / "stock_pools").glob("*.txt")
                        if path.name != "benchmark.txt"},
        "benchmark": {"benchmark.txt": staging / "stock_pools/benchmark.txt"},
        "suspend": {f"components/suspend_d_daily_candidate_v2/{name}":
                    staging / f"components/suspend_d_daily_candidate_v2/{name}"
                    for name in ("suspend_d.parquet", "meta.json")},
        "sector_context": {name: staging / f"components/sector_context_candidate_v1/{name}"
                           for name in ("market_context.parquet", "sector_membership_spans.parquet")},
    }
    proofs = {}
    for domain, files in definitions.items():
        prior = catalog / ".staging" / "unit-prepared-fixture" / domain
        for relative, original in files.items():
            target = prior / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(original, target)
        proofs[domain] = VerifiedPreparationCheckpoint(
            {"component_root": prior.relative_to(catalog).as_posix()},
            {"output_refs": [_file_ref(prior, relative) for relative in files]},
        )
    monkeypatch.setattr(private, "recover_prepared_shared_components", lambda **_kwargs: proofs)
    successor = catalog / ".staging" / "adoption-fixture"
    successor.mkdir()
    for name in ("daily_bin", "minute_bin", "factor_bundle", "index_context"):
        if (staging / name).is_dir():
            shutil.copytree(staging / name, successor / name)
    adopted = FrozenMonthlySharedComponentBuilder(profile, cas, date(2024, 7, 1)).execute(
        context=context, staging_root=successor, compiled=compiled,
        validation_result={"validation_ref": source_ref.as_dict(),
                           "component_artifact_manifest_ref": source_ref.as_dict()},
    )
    assert all(path.is_file() for path in adopted.required_files)
    for domain, files in definitions.items():
        for relative, original in files.items():
            target = (successor / "stock_pools" / relative if domain in {"stock_pools", "benchmark"}
                      else successor / relative if domain == "suspend"
                      else successor / "components/sector_context_candidate_v1" / relative)
            assert target.read_bytes() == original.read_bytes()
            assert target.stat().st_ino != (catalog / proofs[domain].record["component_root"] / relative).stat().st_ino
    assert not list(successor.rglob("prepared-component.json"))
