import hashlib
import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[3] / "scripts" / "sync_qe_models_to_aistock_cache.py"
SPEC = importlib.util.spec_from_file_location("sync_qe_models_to_aistock_cache", SCRIPT_PATH)
sync_mod = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = sync_mod
SPEC.loader.exec_module(sync_mod)


def _write_model(path: Path, payload: bytes = b"model-bytes") -> str:
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def test_dry_run_plans_per_algo_destination_without_writing(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()
    expected_hash = _write_model(source_dir / "early.pt")

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "V25_1_SMALL_CAP",
            "--model",
            "early.pt",
            "--expected-sha256",
            f"early.pt={expected_hash}",
            "--json",
        ]
    )

    assert rc == 0
    destination = tmp_path / "cache" / "V25_1_SMALL_CAP" / "early.pt"
    assert not destination.exists()


def test_apply_copies_model_and_writes_sidecar_metadata(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()
    expected_hash = _write_model(source_dir / "early.pt", b"early-model")
    cache_root = tmp_path / "cache"

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(cache_root),
            "--algo-code",
            "V25_TWO_STAGE",
            "--model",
            "early.pt",
            "--expected-sha256",
            f"early.pt={expected_hash}",
            "--apply",
        ]
    )

    destination = cache_root / "V25_TWO_STAGE" / "early.pt"
    sidecar = destination.with_name(destination.name + sync_mod.SIDECAR_SUFFIX)
    assert rc == 0
    assert destination.read_bytes() == b"early-model"
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    assert metadata["algo_code"] == "V25_TWO_STAGE"
    assert metadata["model"] == "early.pt"
    assert metadata["sha256"] == expected_hash
    assert metadata["destination"] == str(destination)


def test_apply_blocks_existing_different_hash_without_overwrite(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()
    _write_model(source_dir / "early.pt", b"new-model")
    destination_dir = tmp_path / "cache" / "V25_TWO_STAGE"
    destination_dir.mkdir(parents=True)
    destination = destination_dir / "early.pt"
    destination.write_bytes(b"old-model")

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "V25_TWO_STAGE",
            "--model",
            "early.pt",
            "--apply",
        ]
    )

    assert rc == 2
    assert destination.read_bytes() == b"old-model"
    assert not destination.with_name(destination.name + sync_mod.SIDECAR_SUFFIX).exists()


def test_hash_mismatch_fails_fast(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()
    _write_model(source_dir / "early.pt")

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "V25_TWO_STAGE",
            "--model",
            "early.pt",
            "--expected-sha256",
            f"early.pt={'0' * 64}",
        ]
    )

    assert rc == 2
    assert not (tmp_path / "cache").exists()


def test_model_name_must_not_escape_source_dir(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "V25_TWO_STAGE",
            "--model",
            "../early.pt",
        ]
    )

    assert rc == 2


def test_model_name_must_not_use_windows_drive_relative_segment(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "V25_TWO_STAGE",
            "--model",
            "C:early.pt",
        ]
    )

    assert rc == 2


def test_algo_code_must_not_use_windows_drive_relative_segment(tmp_path):
    source_dir = tmp_path / "qe_models"
    source_dir.mkdir()
    _write_model(source_dir / "early.pt")

    rc = sync_mod.main(
        [
            "--source-dir",
            str(source_dir),
            "--cache-root",
            str(tmp_path / "cache"),
            "--algo-code",
            "C:V25_TWO_STAGE",
            "--model",
            "early.pt",
        ]
    )

    assert rc == 2


def test_windows_wsl_mount_path_translation():
    translated = sync_mod.translate_wsl_path("/mnt/f/Dev/AIstock/rdagent_assets")

    assert translated == "F:\\Dev\\AIstock\\rdagent_assets"
