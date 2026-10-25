from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
)
from backend.services.dataset_release.monthly_shared_consumer_probe import (
    SHARED_DATA_CONSUMERS,
    MonthlySharedDatasetConsumerProbe,
    _read_sentinel,
    monthly_shared_dataset_consumer_probes,
)
from backend.services.dataset_release.profile_contract import (
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)


MANIFEST = "a" * 64


SENTINELS = {
    "manifest": ("qe_dataset_manifest.json",),
    "day": (
        "components/daily_bin_candidate/calendars/day.txt",
        "components/daily_bin_candidate/instruments/all.txt",
        "components/daily_bin_candidate/meta_export.json",
    ),
    "minute": (
        "components/minute_bin_candidate/calendars/1min.txt",
        "components/minute_bin_candidate/instruments/all.txt",
        "components/minute_bin_candidate/meta_export.json",
    ),
    "factor": (
        "components/factor_h5_static_candidate_v2/meta.json",
        "components/factor_h5_static_candidate_v2/static_factors.parquet",
    ),
    "index": (
        "components/index_context/meta.json",
        "components/index_context/index_daily.h5",
    ),
    "suspend": (
        "components/suspend_d_daily_candidate_v2/meta.json",
        "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
    ),
    "benchmark": ("components/daily_bin_candidate/instruments/benchmark.txt",),
    "coverage": ("reports/qe_index_pool_coverage_receipt.json",),
    "stock_pools": ("stock_pools/stock_universe.txt",),
}


def _json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _fixture(tmp_path: Path, consumer_id: str):
    candidate = tmp_path / "candidate"
    required = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer_id])
    paths: list[Path] = []
    for component in required:
        for relative in SENTINELS[component]:
            path = candidate / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"sentinel")
            paths.append(path)
    raw = {
        "release_id": "qe_hmm_full_v2_20260930",
        "consumers": {
            "qe": {
                "defaults": {
                    "train_start": "2018-08-01",
                    "train_end": "2023-06-30",
                    "valid_start": "2023-07-03",
                    "valid_end": "2024-06-28",
                    "test_start": "2024-07-01",
                    "backtest_end": "2026-09-29",
                    "test_end": "2026-09-30",
                }
            }
        },
    }
    profile = SimpleNamespace(raw=raw)
    profile_path = _json(tmp_path / "profile.json", raw)
    binding_path = _json(tmp_path / "attempt/binding.json", {"manifest": MANIFEST})
    request = ConsumerProbeRequest(
        consumer_id=consumer_id,
        node_id="rdagent-node1",
        dataset_manifest_sha256=MANIFEST,
        binding_path=binding_path,
        profile_path=profile_path,
        controller_candidate_root=candidate,
        node_candidate_root="/home/data/releases/candidate",
        resolved_component_paths=tuple(paths),
        derived_asset_paths=(),
    )
    return request, profile, required


@pytest.mark.parametrize("consumer_id", SHARED_DATA_CONSUMERS)
def test_shared_consumer_probe_reads_exact_registered_sentinels(
    tmp_path: Path,
    consumer_id: str,
) -> None:
    request, profile, required = _fixture(tmp_path, consumer_id)
    reads: list[Path] = []

    def resolver(**kwargs: Any) -> Mapping[str, Any]:
        assert kwargs["profile"] is profile
        return {
            "consumer_id": consumer_id,
            "candidate_root": request.node_candidate_root,
            "dataset_manifest_sha256": MANIFEST,
            "required_components": required,
            "resolved_once": True,
            "legacy_fallback": False,
        }

    def reader(path: Path) -> int:
        reads.append(path)
        return 2

    result_path = MonthlySharedDatasetConsumerProbe(
        profile_loader=lambda _path: profile,
        binding_resolver=resolver,
        sentinel_reader=reader,
    ).run(request)
    result = json.loads(result_path.read_text(encoding="utf-8"))

    assert result["schema_version"] == CONSUMER_PROBE_RESULT_SCHEMA
    assert result["status"] == "PASS"
    assert result["coverage_counts"]["unresolved_count"] == 0
    assert result["coverage_counts"]["required_component_count"] == len(required)
    assert result["coverage_counts"]["sentinel_file_count"] == len(reads)
    assert result["coverage_counts"]["readable_row_count"] == len(reads) * 2
    assert result["side_effect_flags"] == {
        "outcomes_read": False,
        "training_started": False,
        "experiment_started": False,
        "runtime_action_performed": False,
    }


def test_shared_consumer_probe_rejects_legacy_binding(tmp_path: Path) -> None:
    request, profile, required = _fixture(tmp_path, "factor_research")
    probe = MonthlySharedDatasetConsumerProbe(
        profile_loader=lambda _path: profile,
        binding_resolver=lambda **_kwargs: {
            "consumer_id": request.consumer_id,
            "candidate_root": request.node_candidate_root,
            "dataset_manifest_sha256": MANIFEST,
            "required_components": required,
            "resolved_once": True,
            "legacy_fallback": True,
        },
        sentinel_reader=lambda _path: 1,
    )

    with pytest.raises(MonthlyConsumerValidationError, match="binding differs"):
        probe.run(request)


def test_shared_sentinel_reader_uses_real_file_formats(tmp_path: Path) -> None:
    json_path = _json(tmp_path / "value.json", {"value": 1})
    text_path = tmp_path / "value.txt"
    text_path.write_text("one\ntwo\n", encoding="utf-8")
    parquet_path = tmp_path / "value.parquet"
    pd.DataFrame({"value": [1.0, 2.0]}).to_parquet(parquet_path, index=False)
    h5_path = tmp_path / "value.h5"
    pd.DataFrame({"value": [1.0, 2.0]}).to_hdf(h5_path, key="data", format="table")

    assert _read_sentinel(json_path) == 1
    assert _read_sentinel(text_path) == 2
    assert _read_sentinel(parquet_path) == 2
    assert _read_sentinel(h5_path) == 2


def test_shared_consumer_registry_is_exact() -> None:
    probes = monthly_shared_dataset_consumer_probes()

    assert tuple(probes) == SHARED_DATA_CONSUMERS
    assert {probe.probe_id for probe in probes.values()} == {
        "aistock.monthly.shared_dataset.file_binding"
    }
