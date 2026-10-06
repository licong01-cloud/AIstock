from __future__ import annotations

import pytest
from datetime import date
import json
from pathlib import Path
import subprocess
import sys

from backend.routers import hmm_risk
from backend.services.hmm_risk import product_validation_store as store
from backend.services.hmm_risk.contracts import canonical_sha256


def _record(product, *, identity="a" * 64, row_hash="b" * 64):
    body = {"schema_version": store.SCHEMAS[product]}
    if product.endswith("l1"):
        body.update(
            model_hash=identity,
            trade_date="2026-03-31",
            row_count=31,
            input_row_sha256=row_hash,
            repository_readback_passed=True,
            api_readback_passed=True,
            ui_readback_passed=True,
            mock_used=False,
        )
    else:
        body.update(run_id=identity, model_hash="c" * 64, writer_readback=True, api_readback=True, browser_no_mock=True)
        body["canonical_row_sha256" if product == "rotation_l2" else "row_hash"] = row_hash
        if product == "risk_l2":
            body.update(deployment_commit="d" * 40, target="backend-main", day_row_hashes={"2026-03-31": "e" * 64})
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _binding(product, record):
    binding = {
        "identity": record["model_hash" if product.endswith("l1") else "run_id"],
        "row_hash": record[
            "input_row_sha256"
            if product.endswith("l1")
            else "canonical_row_sha256"
            if product == "rotation_l2"
            else "row_hash"
        ],
    }
    if product.endswith("l1"):
        binding["trade_date"] = record["trade_date"]
    if product == "risk_l2":
        binding["deployment_commit"] = record["deployment_commit"]
    return binding


@pytest.mark.parametrize(
    ("provider", "environment_key"),
    [
        (hmm_risk.get_rotation_l1_repository, "AISTOCK_HMM_ROTATION_L1_PRODUCT_VALIDATION_RECEIPT"),
        (hmm_risk.get_rotation_l2_repository, "AISTOCK_HMM_ROTATION_L2_PRODUCT_VALIDATION_RECEIPT"),
        (hmm_risk.get_risk_l1_repository, "AISTOCK_HMM_RISK_L1_PRODUCT_VALIDATION_RECEIPT"),
        (hmm_risk.get_risk_l2_repository, "AISTOCK_HMM_RISK_L2_PRODUCT_VALIDATION_RECEIPT"),
    ],
)
def test_providers_ignore_per_experiment_environment_binding(provider, environment_key, monkeypatch):
    monkeypatch.setenv(environment_key, "/stale/other-experiment.json")
    assert provider().surface_validation_receipt_path is None


@pytest.mark.parametrize("product", store.SCHEMAS)
def test_register_is_atomic_idempotent_identity_bound_and_visible_without_restart(product, tmp_path):
    record = _record(product)
    binding = _binding(product, record)
    assert store.find_receipt(product, root=tmp_path, **binding) is None
    first = store.register_receipt(record, root=tmp_path)
    assert first == store.register_receipt(record, root=tmp_path)
    path = store.find_receipt(product, root=tmp_path, **binding)
    assert store.read_receipt(path) == record
    assert not list(path.parent.glob("tmp*"))
    assert store.find_receipt(product, root=tmp_path, **{**binding, "identity": "f" * 64}) is None
    assert store.find_receipt(product, root=tmp_path, **{**binding, "row_hash": "f" * 64}) is None
    changed = {**record, "extra_note": "different"}
    changed["receipt_sha256"] = canonical_sha256({k: v for k, v in changed.items() if k != "receipt_sha256"})
    with pytest.raises(store.ProductValidationStoreError, match="conflicts"):
        store.register_receipt(changed, root=tmp_path)
    assert store.read_receipt(path) == record


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": "unknown"},
        {"receipt_sha256": "f" * 64},
        {"writer_readback": False},
        {"browser_no_mock": 1},
        {"run_id": "../escape"},
    ],
)
def test_bad_registration_is_rejected_without_publishing(change, tmp_path):
    record = {**_record("rotation_l2"), **change}
    if "receipt_sha256" not in change:
        record["receipt_sha256"] = canonical_sha256({k: v for k, v in record.items() if k != "receipt_sha256"})
    with pytest.raises(store.ProductValidationStoreError):
        store.register_receipt(record, root=tmp_path)
    assert not list(tmp_path.rglob("validation.json"))


def test_paths_reject_symlink_junction_relative_root_and_corrupt_content(tmp_path, monkeypatch):
    with pytest.raises(store.ProductValidationStoreError, match="absolute"):
        store.register_receipt(_record("rotation_l2"), root=Path("relative"))
    record = _record("rotation_l2")
    result = store.register_receipt(record, root=tmp_path)
    path = Path(result["path"])
    original = Path.lstat
    from types import SimpleNamespace

    def junction(entry):
        info = original(entry)
        if entry == path.parent:
            return SimpleNamespace(st_mode=info.st_mode, st_file_attributes=0x400)
        return info

    monkeypatch.setattr(Path, "lstat", junction)
    with pytest.raises(store.ProductValidationStoreError, match="junction"):
        store.find_receipt("rotation_l2", root=tmp_path, **_binding("rotation_l2", record))
    monkeypatch.setattr(Path, "lstat", original)
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(store.ProductValidationStoreError, match="object"):
        store.read_receipt(path)


def test_rotation_l2_same_repository_observes_new_record_without_restart_and_rejects_tampering(tmp_path, monkeypatch):
    from backend.tests.hmm_risk import test_rotation_l2_prediction as fixture
    from backend.services.hmm_risk import rotation_l2_prediction as product

    rows = product.rows_from_acceptance(fixture._acceptance())
    repo = product.RotationL2PredictionRepository(
        conn_factory=lambda: fixture._Connection(fixture._OverviewCursor([date(2026, 3, 31)])),
        surface_validation_store_root=tmp_path,
    )
    monkeypatch.setattr(repo, "read_date", lambda day, **_k: {"trade_date": str(day), "rows": rows})
    before = repo.overview(run_id="a" * 64)
    assert before["research_surface_status"] == "NOT_AVAILABLE"
    result = store.register_receipt(_record("rotation_l2", row_hash=before["canonical_row_sha256"]), root=tmp_path)
    after = repo.overview(run_id="a" * 64)
    assert after["research_surface_status"] == "AVAILABLE_EXPERIMENTAL"
    assert {k: v for k, v in before.items() if k != "research_surface_status"} == {
        k: v for k, v in after.items() if k != "research_surface_status"
    }
    Path(result["path"]).write_text("{}", encoding="utf-8")
    with pytest.raises(product.RotationL2PredictionError, match="identity/checksum"):
        repo.overview(run_id="a" * 64)


@pytest.mark.parametrize("product", ["rotation_l1", "risk_l1"])
def test_l1_same_repository_reads_new_result_without_environment_rebinding(product, tmp_path):
    if product == "rotation_l1":
        from backend.tests.hmm_risk import test_rotation_l1_prediction as fixture

        process, acceptance = fixture._process_and_acceptance()
        rows = fixture._build_rows(process, acceptance)
        conn = fixture._Connection()
        repo = fixture.subject.RotationL1PredictionRepository(
            conn_factory=fixture._factory(conn), surface_validation_store_root=tmp_path
        )
        record = fixture._surface_receipt(rows)
        key = "research_surface_status"
    else:
        from backend.tests.hmm_risk import test_risk_l1_prediction as fixture

        rows = fixture._rows()
        conn = fixture._Connection()
        repo = fixture.subject.RiskL1PredictionRepository(
            conn_factory=fixture._factory(conn), surface_validation_store_root=tmp_path
        )
        record = fixture._receipt(rows)
        key = "risk_l1_research_surface_status"
    repo.write_rows(rows)
    assert repo.overview(model_hash=rows[0]["model_hash"])[key] == "NOT_AVAILABLE"
    store.register_receipt(record, root=tmp_path)
    assert repo.overview(model_hash=rows[0]["model_hash"])[key] == "AVAILABLE_EXPERIMENTAL"


def test_risk_l2_same_repository_reads_new_result_and_preserves_deployment_binding(tmp_path):
    from backend.services.hmm_risk.risk_l2_prediction import RiskL2PredictionRepository

    record = _record("risk_l2")
    record.update(acceptance_hash="f" * 64, input_hash="a" * 64)
    record["receipt_sha256"] = canonical_sha256({k: v for k, v in record.items() if k != "receipt_sha256"})
    run = {
        "run_id": record["run_id"],
        "model_hash": record["model_hash"],
        "acceptance_hash": record["acceptance_hash"],
        "input_hash": record["input_hash"],
        "dates": ["2026-03-31"],
        "compact_summary": {
            "row_hash": record["row_hash"],
            "day_row_hashes": record["day_row_hashes"],
            "daily": [{"trade_date": "2026-03-31", "warning_count": 0}],
        },
    }
    repo = RiskL2PredictionRepository(surface_validation_store_root=tmp_path, deployment_commit="d" * 40)
    assert repo._surface(run) == "NOT_AVAILABLE"
    store.register_receipt(record, root=tmp_path)
    assert repo._surface(run) == "AVAILABLE_EXPERIMENTAL"
    repo.deployment_commit = "e" * 40
    assert repo._surface(run) == "NOT_AVAILABLE"


@pytest.mark.parametrize("duplicate_field", [False, True])
def test_cli_registers_an_existing_result_without_db_env_or_server(tmp_path, duplicate_field):
    record = _record("rotation_l2")
    source = tmp_path / "existing-result.json"
    payload = json.dumps(record)
    if duplicate_field:
        payload = payload.replace('"writer_readback": true', '"writer_readback": false, "writer_readback": true')
    source.write_text(payload, encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "scripts.hmm_risk.register_product_validation",
        "--receipt",
        str(source),
        "--store-root",
        str(tmp_path / "store"),
    ]
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    if duplicate_field:
        assert result.returncode != 0
        assert "duplicate fields" in result.stderr
        assert not (tmp_path / "store").exists()
        return
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["registration_readback"] is True
    assert store.read_receipt(Path(value["path"])) == record


@pytest.mark.parametrize("raw", ['{"x":NaN}', '{"x":1,"x":2}'])
def test_record_reader_rejects_nonfinite_or_duplicate_fields(raw, tmp_path):
    path = tmp_path / "record.json"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(store.ProductValidationStoreError):
        store.read_receipt(path)


def test_concurrent_identical_registration_is_complete_and_idempotent(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    record = _record("rotation_l2")
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: store.register_receipt(record, root=tmp_path), range(4)))
    assert all(value == results[0] for value in results)
    assert store.read_receipt(Path(results[0]["path"])) == record
