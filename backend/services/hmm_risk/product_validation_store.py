"""Small file store for HMM product-validation results, keyed by business identity.

No per-experiment environment variable, current/latest pointer, or process cache.
The operator registering a record uses the same service account as the backend.
"""

from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping

from backend.services.hmm_risk.contracts import canonical_json_bytes, canonical_sha256

SCHEMAS = {
    "rotation_l1": "hmm_risk_rotation_l1_product_validation_v1",
    "risk_l1": "hmm_risk_risk_l1_product_validation_v1",
    "rotation_l2": "hmm_risk_rotation_l2_product_validation_v1",
    "risk_l2": "hmm_risk_risk_l2_product_validation_v1",
}


class ProductValidationStoreError(RuntimeError):
    reason_code = "hmm_risk_product_validation_invalid"


def default_store_root() -> Path:
    return Path.home() / ".aistock" / "hmm" / "product_validation"


def _sha(value: Any, length: int = 64) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is None:
        raise ProductValidationStoreError("validation identity is not a lowercase digest")
    return value


def _safe_path(path: Path) -> None:
    if not path.is_absolute():
        raise ProductValidationStoreError("validation store path must be absolute")
    for entry in reversed((path, *path.parents)):
        try:
            info = entry.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ProductValidationStoreError("validation store path is a symlink or junction")
        if entry != path and not stat.S_ISDIR(info.st_mode):
            raise ProductValidationStoreError("validation store parent is not a directory")


def receipt_path(
    product: str,
    *,
    identity: str,
    row_hash: str,
    trade_date: str | None = None,
    deployment_commit: str | None = None,
    root: Path | None = None,
) -> Path:
    if product not in SCHEMAS:
        raise ProductValidationStoreError("unknown HMM validation product")
    base = Path(root) if root is not None else default_store_root()
    path = base / product / _sha(identity)
    if product.endswith("l1"):
        try:
            day = date.fromisoformat(str(trade_date)).isoformat()
        except ValueError as exc:
            raise ProductValidationStoreError("L1 validation date is invalid") from exc
        path /= day
    path /= _sha(row_hash)
    if product == "risk_l2":
        path /= _sha(deployment_commit, 40)
    path /= "validation.json"
    _safe_path(path)
    return path


def find_receipt(product: str, **identity: Any) -> Path | None:
    path = receipt_path(product, **identity)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise ProductValidationStoreError("validation record is not a regular file")
    return path


def read_receipt(path: Path) -> dict[str, Any]:
    _safe_path(path)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ProductValidationStoreError("validation record is not a regular file")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        payload = stream.read()
    after = path.lstat()
    stamps = [(v.st_dev, v.st_ino, v.st_size, v.st_mtime_ns) for v in (before, opened, after)]
    if len(set(stamps)) != 1:
        raise ProductValidationStoreError("validation record changed during read")
    _safe_path(path)

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, item in pairs:
            if key in result:
                raise ProductValidationStoreError("validation record contains duplicate fields")
            result[key] = item
        return result

    value = json.loads(payload.decode("utf-8"), object_pairs_hook=unique_object)
    if not isinstance(value, dict):
        raise ProductValidationStoreError("validation record must be an object")
    try:
        canonical_json_bytes(value)
    except ValueError as exc:
        raise ProductValidationStoreError("validation record contains non-finite values") from exc
    return value


def register_receipt(value: Mapping[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    """Register an already-produced validation result; never asserts a check for it."""
    record = dict(value)
    product = next((key for key, schema in SCHEMAS.items() if record.get("schema_version") == schema), None)
    if product is None:
        raise ProductValidationStoreError("unknown validation record schema")
    body = {key: item for key, item in record.items() if key != "receipt_sha256"}
    if record.get("receipt_sha256") != canonical_sha256(body):
        raise ProductValidationStoreError("validation record checksum differs")
    if product.endswith("l1"):
        flags = ("repository_readback_passed", "api_readback_passed", "ui_readback_passed")
        if record.get("mock_used") is not False or record.get("row_count") != 31:
            raise ProductValidationStoreError("L1 validation is mocked or incomplete")
        binding = {
            "identity": record.get("model_hash"),
            "row_hash": record.get("input_row_sha256"),
            "trade_date": record.get("trade_date"),
        }
    else:
        flags = ("writer_readback", "api_readback", "browser_no_mock")
        binding = {
            "identity": record.get("run_id"),
            "row_hash": record.get("canonical_row_sha256" if product == "rotation_l2" else "row_hash"),
        }
        if "model_hash" in record:
            _sha(record["model_hash"])
        if product == "risk_l2":
            binding["deployment_commit"] = record.get("deployment_commit")
            if record.get("target") != "backend-main" or not isinstance(record.get("day_row_hashes"), dict):
                raise ProductValidationStoreError("L2 risk deployment/date validation is incomplete")
    if any(record.get(key) is not True for key in flags):
        raise ProductValidationStoreError("required real validation result is missing")
    path = receipt_path(product, root=root, **binding)
    payload = canonical_json_bytes(record) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    _safe_path(path)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if read_receipt(path) != record:
                raise ProductValidationStoreError("existing validation record conflicts") from None
        if read_receipt(path) != record:
            raise ProductValidationStoreError("validation record readback differs")
    finally:
        if temporary is not None:
            temporary.unlink()
    return {
        "product": product,
        "path": str(path),
        "receipt_sha256": record["receipt_sha256"],
        "registration_readback": True,
    }
