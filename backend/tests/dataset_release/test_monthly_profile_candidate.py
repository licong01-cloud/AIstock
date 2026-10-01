from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_profile_candidate import (
    MonthlyProfileCandidateError,
    SealedMonthlyProfileCandidateBuilder,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


HEX = "a" * 64
POOLS = {
    "stock_universe": "stock_universe.txt",
    "csi300": "index_pool__csi300.txt",
    "csi500": "index_pool__csi500.txt",
    "csi1000": "index_pool__csi1000.txt",
    "star50": "index_pool__star50.txt",
    "star100": "index_pool__star100.txt",
}


def _json(path: Path, value: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _bytes(path: Path, value: bytes = b"data") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[ProducerContext, Path, Path]:
    candidate = tmp_path / "candidate"
    day = candidate / "components" / "daily_bin_candidate"
    minute = candidate / "components" / "minute_bin_candidate"
    factor = candidate / "components" / "factor_h5_static_candidate_v2"
    index = candidate / "components" / "index_context"
    suspend = candidate / "components" / "suspend_d_daily_candidate_v2"
    sector = candidate / "components" / "sector_context_candidate_v1"
    files = [
        _bytes(day / "calendars" / "day.txt", b"2018-08-01\n2026-09-29\n2026-09-30\n"),
        _bytes(day / "instruments" / "all.txt"),
        _json(
            day / "meta_export.json",
            {
                "snapshot_id": "daily_bin_candidate",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "rule_version": "shsz_a_252td_st_delist_asof_v2",
            },
        ),
        _bytes(minute / "calendars" / "1min.txt"),
        _bytes(minute / "instruments" / "all.txt"),
        _json(
            minute / "meta_export.json",
            {
                "snapshot_id": "minute_bin_candidate",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "rule_version": "shsz_a_252td_st_delist_asof_v2",
            },
        ),
        _json(
            factor / "meta.json",
            {
                "schema_version": "qe_direct_factor_h5_static_v2",
                "start": "2018-08-01",
                "end": "2026-09-30",
                "universe_key": "aistock_equity_pit_canonical_v2",
            },
        ),
        _bytes(factor / "sector_data.h5"),
        _bytes(index / "index_daily.h5"),
        _json(
            index / "meta.json",
            {"end": "2026-09-30", "codes": ["000300.SH"]},
        ),
        _bytes(suspend / "suspend_d.parquet"),
        _json(
            suspend / "meta.json",
            {
                "schema_version": "qe_direct_suspend_d_v1",
                "component": "suspend_d",
                "start": "2018-08-01",
                "end": "2026-09-30",
                "universe_key": "aistock_equity_pit_canonical_v2",
                "source_table": "market.suspend_d",
                "suspend_type": "S",
            },
        ),
        _bytes(day / "instruments" / "benchmark.txt"),
    ]
    sidecars: dict[str, dict[str, Any]] = {}
    for pool_id, filename in POOLS.items():
        path = _bytes(candidate / "stock_pools" / filename, b"000001.SZ\t2018-08-01\t2026-09-30\n")
        files.append(path)
        sidecars[pool_id] = {
            "path": f"stock_pools/{filename}",
            "sha256": _sha(path),
            "size": path.stat().st_size,
        }
    day_stock = _bytes(
        day / "instruments" / "stock_universe.txt",
        (candidate / "stock_pools" / "stock_universe.txt").read_bytes(),
    )
    files.append(day_stock)
    coverage = _json(
        candidate / "reports" / "qe_index_pool_coverage_receipt.json",
        {
            "schema_version": "qe_index_pool_coverage_receipt_v1",
            "release_id": "qe_hmm_full_v2_20260930",
            "cutoff": "2026-09-30",
            "pools": {
                pool_id: {
                    "available_start": "2018-08-01",
                    "available_end": "2026-09-30",
                    "gaps": [],
                }
                for pool_id in POOLS
            },
        },
    )
    files.append(coverage)
    code_map = _bytes(sector / "sector_code_map.json")
    market = _bytes(sector / "market_context.parquet")
    membership = _bytes(sector / "sector_membership_spans.parquet")
    quote = _bytes(sector / "sector_quote_availability.json")
    files.extend((code_map, market, membership, quote))
    sector_receipt = _json(
        sector / "component_receipt.json",
        {
            "schema_version": "aistock_sector_context_receipt_v1",
            "source_dataset_manifest_sha256": HEX,
            "sector_code_map": {
                "sha256": _sha(code_map),
                "code_map_digest": HEX,
                "authority": {"authority_id": "monthly:test", "authority_sha256": HEX},
            },
            "market_context": {
                "sha256": _sha(market),
                "definition": "sum_market_sw_daily_vol_all_rows_v1",
                "start": "2018-08-01",
                "end": "2026-09-30",
            },
            "membership": {
                "sha256": _sha(membership),
                "start": "2024-07-01",
                "end": "2026-09-30",
            },
            "quote_availability": {
                "sha256": _sha(quote),
                "canonical_digest": HEX,
                "schema_version": "aistock_release_sw_l2_quote_availability_v1",
            },
            "sector_data": {"sha256": _sha(factor / "sector_data.h5")},
        },
    )
    files.append(sector_receipt)
    components = {
        path.relative_to(candidate).as_posix().replace("/", "__"): {
            "path": path.relative_to(candidate).as_posix(),
            "sha256": _sha(path),
            "size": path.stat().st_size,
        }
        for path in files
    }
    manifest: dict[str, Any] = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "cutoff_trade_date": "2026-09-30",
        "components": components,
        "st_pit_manifest": {
            "cutoff_trade_date": "2026-09-30",
            "snapshot_id": "pit-20260930",
            "index_membership_sidecars": sidecars,
        },
    }
    manifest["dataset_manifest_sha256"] = hashlib.sha256(
        canonical_json_bytes(manifest)
    ).hexdigest()
    _json(candidate / "qe_dataset_manifest.json", manifest)
    registry = _json(candidate / "derived" / "derived_asset_registry.json", {"assets": []})
    closure = _json(candidate / "release_closure_receipt.json", {"status": "PASS"})
    predecessor = _json(
        tmp_path / "active.json",
        {
            "consumers": {
                "qe": {
                    "defaults": {
                        "train_start": "2018-08-01",
                        "train_end": "2022-12-30",
                        "valid_start": "2023-01-03",
                        "valid_end": "2024-06-28",
                        "test_start": "2024-07-01",
                        "test_end": "2026-08-31",
                        "signal_end": "2026-08-31",
                        "backtest_end": "2026-08-28",
                    }
                }
            }
        },
    )
    plan = {
        "generation": "20261001-monthly-v2-unified",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "target_cutoff": "2026-09-30",
        "candidate_root": str(candidate.resolve()),
        "profile_candidate": str((tmp_path / "profile.json").resolve()),
        "node_roots": {
            "wsl2-5080": "/mnt/wsl/releases/candidate",
            "rdagent-node1": "/home/data/releases/candidate",
        },
        "predecessor": {"profile_path": str(predecessor.resolve())},
        "predecessor_profile_ref": {
            "id": predecessor.name,
            "sha256": _sha(predecessor),
            "size": predecessor.stat().st_size,
        },
    }
    context = ProducerContext(
        stage="LOCAL_VALIDATE",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan=plan,
        prior_receipts={
            "DERIVE": {
                "scope": {"derived_asset_registry_sha256": _sha(registry)}
            }
        },
    )
    return context, closure, registry


def test_profile_builder_derives_v4_from_candidate_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, closure, registry = _fixture(tmp_path)
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_dataset_profile.load_qe_profile",
        lambda path: {"path": path},
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_dataset_profile.validate_controller_snapshot",
        lambda _profile: None,
    )

    output = SealedMonthlyProfileCandidateBuilder().execute(
        context,
        release_closure_path=closure,
        derived_asset_registry_path=registry,
    )

    raw = output.read_bytes()
    profile = json.loads(raw)
    assert raw == canonical_json_bytes(profile) + b"\n"
    assert profile["schema_version"] == "aistock_active_dataset_profile_v4"
    assert profile["cutoff"] == "2026-09-30"
    assert profile["consumers"]["qe"]["defaults"]["backtest_end"] == "2026-09-29"
    assert profile["components"]["release_closure_sha256"] == _sha(closure)
    assert set(profile["consumers"]) == {
        "qe",
        "hmm",
        "selection",
        "advisory",
        "qe_single",
        "qe_custom",
        "qe_multi_alpha",
        "qe_p10",
        "qe_p11",
        "hmm_file_only",
        "factor_research",
        "position_timing",
        "unified_backtest",
    }


def test_profile_builder_rejects_manifest_unpinned_component(
    tmp_path: Path,
) -> None:
    context, closure, registry = _fixture(tmp_path)
    candidate = Path(str(context.plan["candidate_root"]))
    (candidate / "components" / "daily_bin_candidate" / "calendars" / "day.txt").write_text(
        "2018-08-01\n2026-09-30\n",
        encoding="utf-8",
    )

    with pytest.raises(MonthlyProfileCandidateError, match="manifest-pinned"):
        SealedMonthlyProfileCandidateBuilder().execute(
            context,
            release_closure_path=closure,
            derived_asset_registry_path=registry,
        )


def test_profile_builder_rejects_registry_outside_candidate(tmp_path: Path) -> None:
    context, closure, _registry = _fixture(tmp_path)
    external_registry = _json(tmp_path / "external-registry.json", {"assets": []})
    context.prior_receipts["DERIVE"]["scope"]["derived_asset_registry_sha256"] = _sha(
        external_registry
    )

    with pytest.raises(MonthlyProfileCandidateError, match="regular candidate files"):
        SealedMonthlyProfileCandidateBuilder().execute(
            context,
            release_closure_path=closure,
            derived_asset_registry_path=external_registry,
        )


def test_profile_builder_rejects_predecessor_drift(tmp_path: Path) -> None:
    context, closure, registry = _fixture(tmp_path)
    predecessor = Path(str(context.plan["predecessor"]["profile_path"]))
    predecessor.write_bytes(predecessor.read_bytes() + b" ")

    with pytest.raises(MonthlyProfileCandidateError, match="predecessor profile bytes differ"):
        SealedMonthlyProfileCandidateBuilder().execute(
            context,
            release_closure_path=closure,
            derived_asset_registry_path=registry,
        )


def test_profile_builder_never_overwrites_existing_target(tmp_path: Path) -> None:
    context, closure, registry = _fixture(tmp_path)
    output = Path(str(context.plan["profile_candidate"]))
    output.write_bytes(b"owned")

    with pytest.raises(MonthlyProfileCandidateError, match="profile target"):
        SealedMonthlyProfileCandidateBuilder().execute(
            context,
            release_closure_path=closure,
            derived_asset_registry_path=registry,
        )
    assert output.read_bytes() == b"owned"
