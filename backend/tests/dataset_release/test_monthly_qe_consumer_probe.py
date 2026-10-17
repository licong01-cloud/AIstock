from __future__ import annotations

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
from backend.services.dataset_release.monthly_hmm_derive import SHARED_HMM_COEFFICIENT_SCHEMA
from backend.services.dataset_release.monthly_qe_consumer_probe import (
    MonthlyQEConsumerProbe,
    QE_CONSUMERS,
    monthly_qe_consumer_probes,
)
from backend.services.hmm_risk.qe_assistance_transport import BINDING_PARAM


MANIFEST = "a" * 64
MEMBERSHIP = "b" * 64
UNIVERSE = "c" * 64


def _json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _fixture(tmp_path: Path, consumer_id: str, *, asset_manifest: str = MANIFEST):
    candidate = tmp_path / "candidate"
    sector = candidate / "components/sector_context_candidate_v1"
    _json(
        sector / "sector_code_map.json",
        {
            "schema_version": "aistock_release_sw_l2_code_map_v1",
            "member_backed_codes": ["801011.SI", "801012.SI"],
            "entries": [
                {"l2_code_id": 1, "canonical_l2_code": "801011.SI"},
                {"l2_code_id": 2, "canonical_l2_code": "801012.SI"},
            ],
        },
    )
    profile_raw = {
        "release_id": "qe_hmm_full_v2_20260930",
        "components": {
            "dataset_manifest_sha256": MANIFEST,
            "sector_context_pins": {
                "component_root": "components/sector_context_candidate_v1",
                "code_map_file": "sector_code_map.json",
                "membership_sha256": MEMBERSHIP,
            },
        },
        "consumers": {
            "qe": {"universes": {"stock_universe": {"sha256": UNIVERSE}}}
        },
    }
    profile = SimpleNamespace(raw=profile_raw)
    profile_path = _json(tmp_path / "profile.json", profile_raw)
    binding = _json(tmp_path / "attempt/qe-binding.json", {"manifest": MANIFEST})
    derived: tuple[Path, ...] = ()
    if consumer_id in {"qe_p10", "qe_p11"}:
        derived = (
            _json(
                candidate / "derived/coefficients_preset_A.json",
                {
                    "schema_version": SHARED_HMM_COEFFICIENT_SCHEMA,
                    "preset_key": "preset_A",
                    "dataset_manifest_sha256": asset_manifest,
                    "dataset_identity": {"dataset_manifest_sha256": asset_manifest},
                    "input_file_sha256": {
                        "sector_membership_spans_parquet": MEMBERSHIP,
                    },
                    "daily_coefficients": {"2026-09-29": {"801011.SI": 1.0}},
                    "stock_sector_membership_spans": {
                        "000001.SZ": [
                            {
                                "start_date": "2024-07-01",
                                "end_date": "2026-09-29",
                                "sector_code": "801011.SI",
                            }
                        ]
                    },
                },
            ),
        )
    request = ConsumerProbeRequest(
        consumer_id=consumer_id,
        node_id="wsl2-5080",
        dataset_manifest_sha256=MANIFEST,
        binding_path=binding,
        profile_path=profile_path,
        controller_candidate_root=candidate,
        node_candidate_root="/mnt/wsl/releases/candidate",
        resolved_component_paths=(sector / "sector_code_map.json",),
        derived_asset_paths=derived,
    )
    return request, profile


@pytest.mark.parametrize(
    ("consumer_id", "mode", "pools"),
    [
        ("qe_single", "stock_universe", []),
        ("qe_custom", "single_index", ["csi300"]),
        ("qe_multi_alpha", "union", ["csi500", "csi1000"]),
        ("qe_p10", "stock_universe", []),
        ("qe_p11", "stock_universe", []),
    ],
)
def test_qe_probe_uses_real_resolver_contract(
    tmp_path: Path,
    consumer_id: str,
    mode: str,
    pools: list[str],
) -> None:
    request, profile = _fixture(tmp_path, consumer_id)
    observed: dict[str, Any] = {}
    hmm_reads: list[str] = []

    def resolver(**kwargs: Any) -> Any:
        observed.update(kwargs)
        is_p11 = consumer_id == "qe_p11"
        policy = (
            {
                "blacklist_excluded_count": 2,
                "requested_sector_codes": ["801011.SI"],
            }
            if is_p11
            else None
        )
        return SimpleNamespace(
            data_split={
                "train_start": "2018-08-01",
                "train_end": "2023-06-30",
                "valid_start": "2023-07-03",
                "valid_end": "2024-06-28",
                "test_start": "2024-07-01",
                "backtest_end": "2026-09-29",
                "test_end": "2026-09-30",
            },
            binding=SimpleNamespace(
                candidate_root=request.node_candidate_root,
                release_id=profile.raw["release_id"],
                selection_pins={"mode": mode, "pool_ids": pools},
            ),
            stock_pool_content=(
                "000001.SZ\t2018-08-01\t2026-09-30\n" if is_p11 else None
            ),
            sector_blacklist_policy=policy,
        )

    probe = MonthlyQEConsumerProbe(
        profile_loader=lambda _path: profile,
        dataset_resolver=resolver,
        hmm_reader=hmm_reads.append,
    )
    result_path = probe.run(request)
    result = json.loads(result_path.read_text(encoding="utf-8"))

    assert result["schema_version"] == CONSUMER_PROBE_RESULT_SCHEMA
    assert result["status"] == "PASS"
    assert result["coverage_counts"]["unresolved_count"] == 0
    assert result["side_effect_flags"] == {
        "outcomes_read": False,
        "training_started": False,
        "experiment_started": False,
        "runtime_action_performed": False,
    }
    assert observed["profile"] is profile
    assert observed["universe_selection"] == {"mode": mode, "pool_ids": pools}
    if consumer_id in {"qe_p10", "qe_p11"}:
        binding = observed["custom_params"][BINDING_PARAM]["dataset_binding"]
        assert binding["dataset_manifest_sha256"] == MANIFEST
        assert binding["sector_membership_sha256"] == MEMBERSHIP
        assert binding["source_universe_sha256"] == UNIVERSE
        assert len(hmm_reads) == 1
    else:
        assert observed["custom_params"] == {}
        assert hmm_reads == []
    if consumer_id == "qe_p11":
        assert observed["custom_params"]["sector_blacklist"] == ["801011.SI"]
        assert result["coverage_counts"]["materialized_interval_count"] == 1


def test_qe_probe_rejects_hmm_asset_from_another_manifest(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path, "qe_p10", asset_manifest="d" * 64)
    probe = MonthlyQEConsumerProbe(
        profile_loader=lambda _path: profile,
        dataset_resolver=lambda **_kwargs: None,
        hmm_reader=lambda _content: None,
    )

    with pytest.raises(MonthlyConsumerValidationError, match="identity differs"):
        probe.run(request)


def test_qe_p10_uses_actual_composer_file_reader(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path, "qe_p10")

    def resolver(**_kwargs: Any) -> Any:
        return SimpleNamespace(
            data_split={"test_start": "2024-07-01", "test_end": "2026-09-30"},
            binding=SimpleNamespace(
                candidate_root=request.node_candidate_root,
                release_id=profile.raw["release_id"],
                selection_pins={"mode": "stock_universe", "pool_ids": []},
            ),
            stock_pool_content=None,
            sector_blacklist_policy=None,
        )

    result = MonthlyQEConsumerProbe(
        profile_loader=lambda _path: profile,
        dataset_resolver=resolver,
    ).run(request)

    assert json.loads(result.read_text(encoding="utf-8"))["status"] == "PASS"


def test_qe_p11_rejects_metadata_only_blacklist(tmp_path: Path) -> None:
    request, profile = _fixture(tmp_path, "qe_p11")

    def resolver(**_kwargs: Any) -> Any:
        return SimpleNamespace(
            data_split={"test_start": "2024-07-01", "test_end": "2026-09-30"},
            binding=SimpleNamespace(
                candidate_root=request.node_candidate_root,
                release_id=profile.raw["release_id"],
                selection_pins={"mode": "stock_universe", "pool_ids": []},
            ),
            stock_pool_content="000001.SZ\t2018-08-01\t2026-09-30\n",
            sector_blacklist_policy={"blacklist_excluded_count": 0},
        )

    probe = MonthlyQEConsumerProbe(
        profile_loader=lambda _path: profile,
        dataset_resolver=resolver,
        hmm_reader=lambda _content: None,
    )
    with pytest.raises(MonthlyConsumerValidationError, match="filtered PIT universe"):
        probe.run(request)


def test_monthly_qe_probe_registry_is_exact() -> None:
    probes = monthly_qe_consumer_probes()

    assert tuple(probes) == QE_CONSUMERS
    assert {probe.probe_id for probe in probes.values()} == {
        "aistock.monthly.qe.file_only"
    }
