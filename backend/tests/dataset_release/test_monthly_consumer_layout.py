from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_consumer_layout import (
    MonthlyConsumerLayoutError,
    publish_consumer_layout,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path):  # type: ignore[no-untyped-def]
    root = tmp_path / "candidate"
    day = root / "daily_bin" / "qlib"
    minute = root / "minute_bin" / "qlib"
    for provider, frequency in ((day, "day"), (minute, "1min")):
        (provider / "calendars").mkdir(parents=True)
        (provider / "instruments").mkdir()
        (provider / "features" / "sz000001").mkdir(parents=True)
        (provider / "calendars" / f"{frequency}.txt").write_text(
            "2024-07-01\n2024-07-05\n", encoding="utf-8"
        )
        (provider / "instruments" / "all.txt").write_text(
            "SZ000001\t2024-07-01\t2024-07-05\n", encoding="utf-8"
        )
        (provider / "features" / "sz000001" / f"close.{frequency}.bin").write_bytes(
            b"bin"
        )
    (day / "instruments" / "index.txt").write_text(
        "".join(
            f"{item.daily_code}\t{item.required_from.isoformat()}\t2024-07-05\n"
            for item in DOMESTIC_INDEX_DEFINITIONS
        ),
        encoding="utf-8",
    )
    factor = root / "factor_bundle"
    factor.mkdir()
    for dataset in FACTOR_H5_DATASETS:
        (factor / f"{dataset}.h5").write_bytes(dataset.encode("ascii"))
    (factor / "static_factors.parquet").write_bytes(b"static")
    index = root / "index_context"
    index.mkdir()
    (index / "index_daily.h5").write_bytes(b"index")
    pools = root / "stock_pools"
    pools.mkdir()
    sidecars = {}
    filenames = {
        "stock_universe": "stock_universe.txt",
        "csi300": "index_pool__csi300.txt",
        "csi500": "index_pool__csi500.txt",
        "csi1000": "index_pool__csi1000.txt",
        "star50": "index_pool__star50.txt",
        "star100": "index_pool__star100.txt",
    }
    for pool_id, filename in filenames.items():
        path = pools / filename
        path.write_text("000001.SZ\t2024-07-01\t2024-07-05\n", encoding="utf-8")
        sidecars[pool_id] = {
            "path": path.relative_to(root).as_posix(),
            "sha256": _sha(path),
            "size": path.stat().st_size,
        }
    profile = SimpleNamespace(
        start_date=date(2024, 7, 1),
        universe_key="aistock_equity_pit_canonical_v2",
    )
    st_pit = {
        "cutoff_trade_date": "2024-07-05",
        "universe_key": profile.universe_key,
        "rule_version": "fixture-v1",
        "index_membership_sidecars": sidecars,
    }
    validation = {
        "validation_ref": {
            "sha256": "a" * 64,
            "size": 12,
            "relative_path": f"cas/sha256/aa/{'a' * 64}",
        },
        "component_artifact_manifest_ref": {
            "sha256": "b" * 64,
            "size": 34,
            "relative_path": f"cas/sha256/bb/{'b' * 64}",
        },
    }
    return root, profile, st_pit, validation


def test_publish_consumer_layout_creates_shared_hardlink_release(tmp_path: Path) -> None:
    root, profile, st_pit, validation = _fixture(tmp_path)

    result = publish_consumer_layout(
        root=root,
        profile=profile,
        cutoff=date(2024, 7, 5),
        release_id="qe_hmm_full_v2_20240705",
        st_pit_manifest=st_pit,
        validation_authority=validation,
    )

    day_close = root / "components/daily_bin_candidate/features/sz000001/close.day.bin"
    source_close = root / "daily_bin/qlib/features/sz000001/close.day.bin"
    factor = root / "components/factor_h5_static_candidate_v2/sector_data.h5"
    assert os.path.samefile(day_close, source_close)
    assert os.path.samefile(factor, root / "factor_bundle/sector_data.h5")
    assert (
        root / "components/daily_bin_candidate/instruments/benchmark.txt"
    ).read_text(encoding="utf-8") == "000300.SH\t2018-08-01\t2024-07-05\n"
    coverage = json.loads(result.coverage_receipt_path.read_bytes())
    assert set(coverage["pools"]) == set(st_pit["index_membership_sidecars"])
    assert all(value["gaps"] == [] for value in coverage["pools"].values())
    assert all(path.is_file() and not path.is_symlink() for path in result.required_files)


def test_publish_consumer_layout_is_create_exclusive(tmp_path: Path) -> None:
    root, profile, st_pit, validation = _fixture(tmp_path)
    arguments = {
        "root": root,
        "profile": profile,
        "cutoff": date(2024, 7, 5),
        "release_id": "qe_hmm_full_v2_20240705",
        "st_pit_manifest": st_pit,
        "validation_authority": validation,
    }
    publish_consumer_layout(**arguments)

    with pytest.raises(MonthlyConsumerLayoutError, match="source/target"):
        publish_consumer_layout(**arguments)


def test_publish_consumer_layout_rejects_cross_release_pit(tmp_path: Path) -> None:
    root, profile, st_pit, validation = _fixture(tmp_path)
    st_pit["cutoff_trade_date"] = "2024-07-04"

    with pytest.raises(MonthlyConsumerLayoutError, match="ST-PIT identity"):
        publish_consumer_layout(
            root=root,
            profile=profile,
            cutoff=date(2024, 7, 5),
            release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit,
            validation_authority=validation,
        )


def test_publish_consumer_layout_rejects_incomplete_validation_authority(
    tmp_path: Path,
) -> None:
    root, profile, st_pit, validation = _fixture(tmp_path)
    validation["validation_ref"].pop("relative_path")

    with pytest.raises(MonthlyConsumerLayoutError, match="complete content reference"):
        publish_consumer_layout(
            root=root,
            profile=profile,
            cutoff=date(2024, 7, 5),
            release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit,
            validation_authority=validation,
        )
