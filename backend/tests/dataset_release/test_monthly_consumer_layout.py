from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.data_service.security_source_identity import DEFAULT_MANIFEST_PATH
from backend.services.dataset_release.factor_materializer import FACTOR_H5_DATASETS
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.monthly_consumer_layout import (
    MonthlyConsumerLayoutError,
    publish_consumer_layout,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path, *, native_writer: bool = False):  # type: ignore[no-untyped-def]
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
    if native_writer:
        # Real incremental writers retain predecessor metadata, but no index.txt.
        (day / "instruments" / "stock_universe.txt").write_text(
            "000001.SZ\t2024-07-01\t2024-07-04\n", encoding="utf-8"
        )
        (day / "instruments" / "benchmark.txt").write_text(
            "000300.SH\t2018-08-01\t2024-07-04\n", encoding="utf-8"
        )
        for provider in (day, minute):
            (provider / "meta_export.json").write_text(
                '{"end":"2024-07-04","writer_metadata":true}\n', encoding="utf-8"
            )
    else:
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
    (factor / "security_source_identity.json").write_bytes(DEFAULT_MANIFEST_PATH.read_bytes())
    (factor / "moneyflow_alias_coverage_v1.json").write_text(
        '{"schema_version":"qe_moneyflow_alias_coverage_receipt_v1","status":"PASS"}\n',
        encoding="utf-8",
    )
    index = root / "index_context"
    index.mkdir()
    (index / "index_daily.h5").write_bytes(b"index")
    pools = root / "stock_pools"
    pools.mkdir()
    (pools / "benchmark.txt").write_text(
        "000300.SH\t2024-07-01\t2024-07-05\n", encoding="utf-8"
    )
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


@pytest.mark.parametrize("native_writer", [False, True], ids=["legacy", "native"])
def test_publish_consumer_layout_creates_shared_hardlink_release(tmp_path: Path, native_writer: bool) -> None:
    root, profile, st_pit, validation = _fixture(tmp_path, native_writer=native_writer)
    writer_files = {
        path: (path.read_bytes(), path.stat().st_ino)
        for provider in ("daily_bin", "minute_bin")
        for path in (root / provider).rglob("*") if path.is_file()
    }
    pins = {}
    for path in writer_files:
        state = path.stat()
        pins[path.relative_to(root).as_posix()] = {
            "sha256": _sha(path), "size": state.st_size,
            "signature": [state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns],
        }

    result = publish_consumer_layout(
        root=root,
        profile=profile,
        cutoff=date(2024, 7, 5),
        release_id="qe_hmm_full_v2_20240705",
        st_pit_manifest=st_pit,
        validation_authority=validation,
        verified_output_files=pins,
    )

    day_close = root / "components/daily_bin_candidate/features/sz000001/close.day.bin"
    source_close = root / "daily_bin/qlib/features/sz000001/close.day.bin"
    factor = root / "components/factor_h5_static_candidate_v2/sector_data.h5"
    assert os.path.samefile(day_close, source_close)
    assert os.path.samefile(factor, root / "factor_bundle/sector_data.h5")
    assert os.path.samefile(
        root / "components/factor_h5_static_candidate_v2/security_source_identity.json",
        root / "factor_bundle/security_source_identity.json",
    )
    assert (
        root / "components/daily_bin_candidate/instruments/benchmark.txt"
    ).read_text(encoding="utf-8") == "000300.SH\t2024-07-01\t2024-07-05\n"
    coverage = json.loads(result.coverage_receipt_path.read_bytes())
    assert set(coverage["pools"]) == set(st_pit["index_membership_sidecars"])
    assert all(value["gaps"] == [] for value in coverage["pools"].values())
    assert all(path.is_file() and not path.is_symlink() for path in result.required_files)
    assert os.path.samefile(
        root / "components/daily_bin_candidate/instruments/stock_universe.txt",
        root / st_pit["index_membership_sidecars"]["stock_universe"]["path"],
    )
    for provider in ("daily_bin_candidate", "minute_bin_candidate"):
        metadata = json.loads((root / "components" / provider / "meta_export.json").read_bytes())
        assert metadata["end"] == "2024-07-05"
        assert "writer_metadata" not in metadata
    assert {path: (path.read_bytes(), path.stat().st_ino) for path in writer_files} == writer_files


@pytest.mark.parametrize("payload", [
    None,
    "000300.SH\t2024-07-01\t2024-07-04\n",
    "000300.SH\t2018-08-01\t2024-07-05\n",
    "000300.SH\t2024-07-01\t2024-07-05\n000001.SH\t2024-07-01\t2024-07-05\n",
    "",
])
def test_consumer_layout_rejects_invalid_frozen_benchmark(tmp_path, payload):
    root, profile, st_pit, validation = _fixture(tmp_path)
    source = root / "stock_pools/benchmark.txt"
    if payload is None:
        source.unlink()  # A valid legacy index.txt must not mask a missing authority.
    else:
        source.write_text(payload, encoding="utf-8")
    with pytest.raises(MonthlyConsumerLayoutError, match="benchmark authority"):
        publish_consumer_layout(
            root=root, profile=profile, cutoff=date(2024, 7, 5), release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit, validation_authority=validation,
        )


def test_native_consumer_layout_rejects_changed_frozen_stock_pool(tmp_path):
    root, profile, st_pit, validation = _fixture(tmp_path, native_writer=True)
    (root / "stock_pools/stock_universe.txt").write_text(
        "000002.SZ\t2024-07-01\t2024-07-05\n", encoding="utf-8"
    )
    with pytest.raises(MonthlyConsumerLayoutError, match="stock-universe bytes differ"):
        publish_consumer_layout(
            root=root, profile=profile, cutoff=date(2024, 7, 5), release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit, validation_authority=validation,
        )


def test_consumer_layout_does_not_skip_link_checks_on_owned_metadata(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_consumer_layout as module
    root, profile, st_pit, validation = _fixture(tmp_path, native_writer=True)
    unsafe = root / "daily_bin/qlib/meta_export.json"
    original = module._is_link
    monkeypatch.setattr(module, "_is_link", lambda path: path == unsafe or original(path))
    with pytest.raises(MonthlyConsumerLayoutError, match="link"):
        publish_consumer_layout(
            root=root, profile=profile, cutoff=date(2024, 7, 5), release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit, validation_authority=validation,
        )


def test_consumer_layout_reuses_actual_writer_hashes(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_consumer_layout as module
    root, profile, st_pit, validation = _fixture(tmp_path)
    source = root / "factor_bundle/sector_data.h5"
    state = source.stat()
    pin = {"sha256": _sha(source), "size": state.st_size,
           "signature": [state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns]}
    original = module._sha256

    def no_second_read(path):
        if os.path.samefile(path, source):
            pytest.fail("verified writer output must not be read again for a hardlink digest")
        return original(path)

    monkeypatch.setattr(module, "_sha256", no_second_read)
    result = publish_consumer_layout(
        root=root, profile=profile, cutoff=date(2024, 7, 5), release_id="qe_hmm_full_v2_20240705",
        st_pit_manifest=st_pit, validation_authority=validation,
        verified_output_files={"factor_bundle/sector_data.h5": pin},
    )
    assert result.verified_output_files["components/factor_h5_static_candidate_v2/sector_data.h5"] == pin
    receipt = json.loads(result.receipt_path.read_bytes())
    assert receipt["components"]["factor"]["file_count"] == len(FACTOR_H5_DATASETS) + 4
    assert len(receipt["components"]["factor"]["content_digest"]) == 64


def test_consumer_layout_rejects_stale_writer_hash(tmp_path):
    root, profile, st_pit, validation = _fixture(tmp_path)
    source = root / "factor_bundle/sector_data.h5"
    state = source.stat()
    pin = {"sha256": _sha(source), "size": state.st_size,
           "signature": [state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns]}
    source.write_bytes(b"changed")
    with pytest.raises(MonthlyConsumerLayoutError, match="writer output changed"):
        publish_consumer_layout(
            root=root, profile=profile, cutoff=date(2024, 7, 5), release_id="qe_hmm_full_v2_20240705",
            st_pit_manifest=st_pit, validation_authority=validation,
            verified_output_files={"factor_bundle/sector_data.h5": pin},
        )


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
