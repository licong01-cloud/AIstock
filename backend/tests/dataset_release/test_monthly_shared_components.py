from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

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
)
from backend.services.dataset_release.monthly_worker import ProducerContext
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
        plan={"target_cutoff": cutoff.isoformat()},
        prior_receipts={},
    )
    profile = SimpleNamespace(
        profile="fixture",
        start_date=date(2024, 7, 1),
        universe_key="aistock_equity_pit_canonical_v2",
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
