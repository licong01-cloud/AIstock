"""Atomic external persistence for Phase 1F plans and release receipts."""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from backend.services.advisory_phase1.release_schema_contract import canonical_json_text


REASON_RECEIPT_STORE_FAILED = "PHASE1F_RECEIPT_STORE_FAILED"
REASON_RECEIPT_COLLISION = "PHASE1F_RECEIPT_COLLISION"
_IDENTITY_RE = re.compile(r"^[0-9a-f]{64}$")


class ReleaseSchemaReceiptStoreError(RuntimeError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        super().__init__(f"{reason_code}: {detail}")


@dataclass(frozen=True)
class StoredReleaseArtifact:
    kind: Literal["plans", "receipts"]
    identity: str
    path: Path
    idempotent: bool


class ReleaseSchemaReceiptStore:
    """Write only content-addressed artifacts outside the repository and WSL mounts."""

    def __init__(self, root: Path) -> None:
        self.root = self._validate_root(root)

    @staticmethod
    def _validate_root(root: Path) -> Path:
        resolved = root.expanduser().resolve()
        raw = str(resolved).replace("/", "\\").lower()
        if raw.startswith("\\\\wsl$") or raw.startswith("\\\\?\\unc\\wsl$") or raw.startswith("\\mnt\\"):
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "receipt root must not be a WSL filesystem path")
        repository_root = Path(__file__).resolve().parents[3]
        try:
            resolved.relative_to(repository_root)
        except ValueError:
            pass
        else:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "receipt root must be outside the repository")
        return resolved

    @staticmethod
    def _validate_identity(identity: str) -> str:
        if not _IDENTITY_RE.fullmatch(identity):
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "artifact identity must be a sha256")
        return identity

    @staticmethod
    def _publish_no_replace(source: Path, target: Path) -> bool:
        """Publish one fsynced temp file atomically without replacing a peer."""

        if os.name == "nt":
            import ctypes

            move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
            move_file_ex.restype = ctypes.c_int
            movefile_write_through = 0x00000008
            if move_file_ex(str(source), str(target), movefile_write_through):
                return True
            error_code = ctypes.get_last_error()
            if error_code in {80, 183}:
                return False
            raise ctypes.WinError(error_code)
        try:
            os.link(source, target)
        except FileExistsError:
            return False
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True

    def write_plan(self, *, identity: str, payload: dict[str, Any]) -> StoredReleaseArtifact:
        return self._write(kind="plans", identity=identity, payload=payload)

    def write_receipt(self, *, identity: str, payload: dict[str, Any]) -> StoredReleaseArtifact:
        return self._write(kind="receipts", identity=identity, payload=payload)

    def _write(
        self,
        *,
        kind: Literal["plans", "receipts"],
        identity: str,
        payload: dict[str, Any],
    ) -> StoredReleaseArtifact:
        identity = self._validate_identity(identity)
        identity_field = "plan_content_hash" if kind == "plans" else "receipt_content_hash"
        if payload.get(identity_field) != identity:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "artifact payload identity does not match requested path identity")
        content = (canonical_json_text(payload) + "\n").encode("utf-8")
        directory = self.root / kind
        target = directory / f"{identity}.json"
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, type(exc).__name__) from exc
        temp_path: Path | None = None
        try:
            descriptor, temp_name = tempfile.mkstemp(prefix=f".{identity}.", suffix=".tmp", dir=directory)
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            published = self._publish_no_replace(temp_path, target)
            if not published:
                existing = target.read_bytes()
                if existing != content:
                    raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_COLLISION, f"artifact identity collision for {kind}")
                return StoredReleaseArtifact(kind=kind, identity=identity, path=target, idempotent=True)
            persisted = target.read_bytes()
            if persisted != content:
                raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "post-write readback mismatch")
            try:
                parsed = json.loads(persisted.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "post-write JSON readback failed") from exc
            if canonical_json_text(parsed).encode("utf-8") != content.rstrip(b"\n"):
                raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "post-write canonical readback mismatch")
            return StoredReleaseArtifact(kind=kind, identity=identity, path=target, idempotent=False)
        except ReleaseSchemaReceiptStoreError:
            raise
        except OSError as exc:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, type(exc).__name__) from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def load(self, *, kind: Literal["plans", "receipts"], path: Path) -> dict[str, Any]:
        resolved = path.expanduser().resolve()
        expected_root = (self.root / kind).resolve()
        try:
            resolved.relative_to(expected_root)
        except ValueError as exc:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "artifact path escapes receipt root") from exc
        try:
            payload = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "unable to load stored artifact") from exc
        if not isinstance(payload, dict):
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "stored artifact must be a JSON object")
        identity_field = "plan_content_hash" if kind == "plans" else "receipt_content_hash"
        identity = payload.get(identity_field)
        if not isinstance(identity, str) or resolved.stem != identity or not _IDENTITY_RE.fullmatch(identity):
            raise ReleaseSchemaReceiptStoreError(REASON_RECEIPT_STORE_FAILED, "stored artifact path and content identity differ")
        return payload
