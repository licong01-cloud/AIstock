from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

import pytest

import backend.services.dataset_release.monthly_node_probe as node_probe
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_node_probe import (
    NODE_PROBE_REQUEST_SCHEMA,
    NODE_PROBE_RESULT_SCHEMA,
    MonthlyNodeProbeError,
    run_node_probe,
)


MANIFEST = "a" * 64


def _file(path: Path, content: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "path": path.as_posix(),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
    }


def _request(tmp_path: Path, consumer_id: str = "factor_research") -> tuple[dict[str, Any], Path]:
    root = tmp_path / "candidate"
    manifest_content = canonical_json_bytes({"dataset_manifest_sha256": MANIFEST}) + b"\n"
    manifest = _file(root / "qe_dataset_manifest.json", manifest_content)
    text = _file(root / "stock_pools/stock_universe.txt", b"000001.SZ\t2018-08-01\t2026-09-30\n")
    request = {
        "schema_version": NODE_PROBE_REQUEST_SCHEMA,
        "consumer_id": consumer_id,
        "node_id": "wsl2-5080",
        "candidate_root": root.as_posix(),
        "dataset_manifest_sha256": MANIFEST,
        "required_window": {
            "test_start": "2024-07-01",
            "backtest_end": "2026-09-29",
            "test_end": "2026-09-30",
        },
        "file_refs": [
            {"role": "dataset_manifest", **manifest},
            {"role": "stock_universe", **text},
        ],
    }
    for ref in request["file_refs"]:
        ref["path"] = Path(ref["path"]).relative_to(root).as_posix()
    return request, root


def test_node_probe_reads_files_inside_node_root(tmp_path: Path) -> None:
    request, _root = _request(tmp_path)

    result = run_node_probe(request)

    assert result["schema_version"] == NODE_PROBE_RESULT_SCHEMA
    assert result["status"] == "PASS"
    assert result["coverage_counts"] == {
        "unresolved_count": 0,
        "verified_file_count": 2,
        "sentinel_row_count": 1,
    }
    assert result["request_sha256"] == hashlib.sha256(
        canonical_json_bytes(request)
    ).hexdigest()
    assert all(value is False for value in result["side_effect_flags"].values())


def test_node_probe_rejects_file_drift(tmp_path: Path) -> None:
    request, root = _request(tmp_path)
    (root / "stock_pools/stock_universe.txt").write_text("drift\n", encoding="utf-8")

    with pytest.raises(MonthlyNodeProbeError, match="bytes differ"):
        run_node_probe(request)


def test_qe_node_probe_calls_real_data_plane_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, root = _request(tmp_path, "qe_single")
    observed: dict[str, Any] = {}

    def qlib_counts(candidate: Path, window: Mapping[str, Any]) -> dict[str, int]:
        observed.update(candidate=candidate, window=window)
        return {"qlib_daily_row_count": 1, "qlib_minute_row_count": 240}

    monkeypatch.setattr(node_probe, "_qlib_counts", qlib_counts)
    result = run_node_probe(request)

    assert observed["candidate"] == root.resolve()
    assert result["coverage_counts"]["qlib_minute_row_count"] == 240


def test_hmm_node_probe_calls_formal_frozen_loader_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, root = _request(tmp_path, "hmm_file_only")
    for role in sorted(node_probe._HMM_ROLES):
        relative = {
            "sector_data_h5": "components/factor_h5_static_candidate_v2/sector_data.h5",
            "index_daily_h5": "components/index_context/index_daily.h5",
            "sector_code_map_json": "components/sector_context_candidate_v1/sector_code_map.json",
            "market_context_parquet": "components/sector_context_candidate_v1/market_context.parquet",
            "sector_membership_spans_parquet": "components/sector_context_candidate_v1/sector_membership_spans.parquet",
            "sector_quote_availability_json": "components/sector_context_candidate_v1/sector_quote_availability.json",
        }[role]
        ref = _file(root / relative, role.encode())
        request["file_refs"].append(
            {"role": role, **ref, "path": relative}
        )
    observed: dict[str, Any] = {}

    def hmm_counts(
        candidate: Path,
        files: Mapping[str, Any],
        window: Mapping[str, Any],
        manifest: str,
    ) -> dict[str, int]:
        observed.update(candidate=candidate, files=files, window=window, manifest=manifest)
        return {
            "hmm_trade_date_count": 527,
            "hmm_active_sector_count": 119,
            "hmm_coefficient_sector_count": 113,
        }

    monkeypatch.setattr(node_probe, "_hmm_counts", hmm_counts)
    result = run_node_probe(request)

    assert observed["manifest"] == MANIFEST
    assert set(observed["files"]).issuperset(node_probe._HMM_ROLES)
    assert result["coverage_counts"]["hmm_trade_date_count"] == 527
