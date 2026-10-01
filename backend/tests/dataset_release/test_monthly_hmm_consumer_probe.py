from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
)
from backend.services.dataset_release.monthly_hmm_consumer_probe import (
    MonthlyHMMConsumerProbe,
)
from backend.services.dataset_release.monthly_hmm_derive import (
    FROZEN_HMM_INPUT_SCHEMA,
    MARKET_VOLUME_DEFINITION,
)


MANIFEST = "a" * 64


def _json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _fixture(tmp_path: Path):
    candidate = tmp_path / "candidate"
    relatives = (
        "components/factor_h5_static_candidate_v2/sector_data.h5",
        "components/index_context/index_daily.h5",
        "components/sector_context_candidate_v1/sector_code_map.json",
        "components/sector_context_candidate_v1/market_context.parquet",
        "components/sector_context_candidate_v1/sector_membership_spans.parquet",
        "components/sector_context_candidate_v1/sector_quote_availability.json",
    )
    paths = []
    for index, relative in enumerate(relatives):
        path = candidate / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"file-{index}".encode())
        paths.append(path)
    raw = {
        "generation": "20260930-monthly-v2-unified",
        "release_id": "qe_hmm_full_v2_20260930",
        "cutoff": "2026-09-30",
        "consumers": {
            "qe": {
                "defaults": {
                    "test_start": "2024-07-01",
                    "backtest_end": "2026-09-29",
                }
            }
        },
    }
    profile = SimpleNamespace(raw=raw)
    profile_path = _json(tmp_path / "profile.json", raw)
    binding_path = _json(tmp_path / "attempt/hmm-binding.json", {"manifest": MANIFEST})
    request = ConsumerProbeRequest(
        consumer_id="hmm_file_only",
        node_id="wsl2-5080",
        dataset_manifest_sha256=MANIFEST,
        binding_path=binding_path,
        profile_path=profile_path,
        controller_candidate_root=candidate,
        node_candidate_root="/mnt/wsl/releases/candidate",
        resolved_component_paths=tuple(paths),
        derived_asset_paths=(),
    )
    return request, profile


def test_hmm_probe_runs_formal_frozen_input_loader(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path)
    observed: dict[str, Any] = {}

    def binding_resolver(**kwargs: Any) -> Mapping[str, Any]:
        assert kwargs == {
            "consumer_id": "hmm_file_only",
            "node_id": "wsl2-5080",
            "profile": profile,
        }
        return {
            "consumer_id": "hmm_file_only",
            "candidate_root": request.node_candidate_root,
            "dataset_manifest_sha256": MANIFEST,
            "legacy_fallback": False,
        }

    def frozen_loader(bundle: dict[str, Any], **kwargs: Any) -> Mapping[str, Any]:
        observed["bundle"] = bundle
        observed.update(kwargs)
        return {
            "dataset_identity": bundle["dataset_identity"],
            "stock_sector_maps_by_date": {
                "2024-07-01": {"000001.SZ": "801011.SI"},
                "2026-09-29": {"000001.SZ": "801011.SI"},
            },
            "active_sector_codes": ["801011.SI", "801012.SI"],
            "coefficient_sector_codes": ["801011.SI"],
        }

    result_path = MonthlyHMMConsumerProbe(
        profile_loader=lambda _path: profile,
        binding_resolver=binding_resolver,
        frozen_loader=frozen_loader,
    ).run(request)
    result = json.loads(result_path.read_text(encoding="utf-8"))

    assert result["schema_version"] == CONSUMER_PROBE_RESULT_SCHEMA
    assert result["status"] == "PASS"
    assert result["coverage_counts"] == {
        "unresolved_count": 0,
        "input_file_count": 6,
        "trade_date_count": 2,
        "active_sector_count": 2,
        "coefficient_sector_count": 1,
    }
    assert result["side_effect_flags"] == {
        "outcomes_read": False,
        "training_started": False,
        "experiment_started": False,
        "runtime_action_performed": False,
    }
    assert observed["bundle"]["schema_version"] == FROZEN_HMM_INPUT_SCHEMA
    assert observed["bundle"]["market_volume_definition"] == MARKET_VOLUME_DEFINITION
    assert observed["test_start"] == date(2024, 7, 1)
    assert observed["backtest_end"] == date(2026, 9, 29)
    assert observed["history_start"] == date(2021, 6, 2)
    assert set(observed["bundle"]["files"]) == {
        "sector_data_h5",
        "index_daily_h5",
        "sector_code_map_json",
        "market_context_parquet",
        "sector_membership_spans_parquet",
        "sector_quote_availability_json",
    }


def test_hmm_probe_rejects_cross_release_binding(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path)
    probe = MonthlyHMMConsumerProbe(
        profile_loader=lambda _path: profile,
        binding_resolver=lambda **_kwargs: {
            "consumer_id": "hmm_file_only",
            "candidate_root": request.node_candidate_root,
            "dataset_manifest_sha256": "f" * 64,
            "legacy_fallback": False,
        },
        frozen_loader=lambda *_args, **_kwargs: {},
    )

    with pytest.raises(MonthlyConsumerValidationError, match="binding differs"):
        probe.run(request)


def test_hmm_probe_rejects_unpinned_input_file(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path)
    request = replace(
        request,
        resolved_component_paths=request.resolved_component_paths[:-1],
    )
    probe = MonthlyHMMConsumerProbe(
        profile_loader=lambda _path: profile,
        binding_resolver=lambda **_kwargs: {
            "consumer_id": "hmm_file_only",
            "candidate_root": request.node_candidate_root,
            "dataset_manifest_sha256": MANIFEST,
            "legacy_fallback": False,
        },
        frozen_loader=lambda *_args, **_kwargs: {},
    )

    with pytest.raises(MonthlyConsumerValidationError, match="not consumer-pinned"):
        probe.run(request)
