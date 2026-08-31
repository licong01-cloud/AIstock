"""Immutable, content-addressed storage for dataset-release control artifacts.

The store deliberately has no initialization side effect.  A control root must
first be created by :meth:`backend.services.dataset_release.control_store.ControlStore.initialize`.
Runtime callers may only add immutable blobs below the pre-created CAS root.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .control_store import IDENTITY_SCHEMA, volume_identity


_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_IDENTITY_FILE = "control_store_identity.json"


class CASStoreError(RuntimeError):
    """Base class for durable CAS failures."""


class CASStoreNotInitialized(CASStoreError):
    """Raised when runtime code points at a root that was not explicitly initialized."""


class CASCorruptionError(CASStoreError):
    """Raised when persisted bytes do not match their content identity."""


class CASHashOnlyMismatch(CASStoreError):
    """Raised when a freshly consumed stream differs from sealed CAS identity."""


@dataclass(frozen=True, slots=True)
class CASRef:
    """Stable reference to one immutable blob."""

    sha256: str
    size: int
    relative_path: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "size": self.size,
            "relative_path": self.relative_path,
        }

    @classmethod
    def from_value(cls, value: "CASRef | Mapping[str, Any] | str") -> "CASRef":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            digest = _require_sha256(value)
            return cls(digest, -1, f"cas/sha256/{digest[:2]}/{digest}")
        try:
            digest = _require_sha256(str(value["sha256"]))
            size = int(value["size"])
            relative_path = str(value["relative_path"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CASStoreError("invalid CAS reference") from exc
        return cls(digest, size, relative_path)


@dataclass(frozen=True, slots=True)
class CASPutResult:
    reference: CASRef
    created: bool


class CASStore:
    """Create-if-absent CAS with flush, atomic publish, and readback verification."""

    def __init__(self, control_root: str | Path) -> None:
        self.root = _require_initialized_control_root(Path(control_root))
        self.cas_root = self.root / "cas" / "sha256"
        if not self.cas_root.is_dir():
            raise CASStoreNotInitialized("initialized control root is missing cas/sha256")
        _assert_no_reparse_path(self.cas_root, self.root)

    def put_bytes(self, payload: bytes | bytearray | memoryview) -> CASRef:
        raw = bytes(payload)
        digest = hashlib.sha256(raw).hexdigest()
        destination = self._path(digest)
        destination.parent.mkdir(parents=False, exist_ok=True)
        _assert_no_reparse_path(destination.parent, self.root)

        if destination.exists():
            return self._verify_existing(destination, digest=digest, expected=raw)

        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{digest}.", suffix=".partial", dir=destination.parent)
        temporary = Path(temporary_name)
        published = False
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            published = _publish_no_replace(temporary, destination)
            if published:
                _flush_directory(destination.parent)
            return self._verify_existing(destination, digest=digest, expected=raw)
        finally:
            # A real crash may leave an undiscoverable partial.  A handled
            # failure cleans only the exact temp file owned by this call.
            if temporary.exists() and (not published or temporary != destination):
                temporary.unlink(missing_ok=True)

    def put_stream(
        self,
        chunks: Iterable[bytes | bytearray | memoryview],
        *,
        max_chunk_bytes: int = 16 * 1024 * 1024,
    ) -> CASRef:
        return self.put_stream_observed(chunks, max_chunk_bytes=max_chunk_bytes).reference

    def put_stream_observed(
        self,
        chunks: Iterable[bytes | bytearray | memoryview],
        *,
        max_chunk_bytes: int = 16 * 1024 * 1024,
    ) -> CASPutResult:
        """Persist one bounded byte stream without materializing the full blob.

        The temporary file lives directly under the initialized CAS root, so
        the final create-if-absent publication never crosses a filesystem.
        Callers remain responsible for yielding deterministic canonical bytes.
        """

        if type(max_chunk_bytes) is not int or max_chunk_bytes <= 0:
            raise ValueError("max_chunk_bytes must be a positive integer")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".stream.", suffix=".partial", dir=self.cas_root)
        temporary = Path(temporary_name)
        digest = hashlib.sha256()
        size = 0
        destination: Path | None = None
        published = False
        try:
            with os.fdopen(descriptor, "wb") as handle:
                for raw_chunk in chunks:
                    if not isinstance(raw_chunk, (bytes, bytearray, memoryview)):
                        raise CASStoreError("CAS stream chunks must be bytes-like")
                    chunk = bytes(raw_chunk)
                    if len(chunk) > max_chunk_bytes:
                        raise CASStoreError("CAS stream chunk exceeds the configured memory bound")
                    if not chunk:
                        continue
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            hexdigest = digest.hexdigest()
            destination = self._path(hexdigest)
            destination.parent.mkdir(parents=False, exist_ok=True)
            _assert_no_reparse_path(destination.parent, self.root)
            assert destination is not None
            published = _publish_no_replace(temporary, destination)
            if published:
                _flush_directory(destination.parent)
            return CASPutResult(
                self._verify_existing_digest(
                    destination,
                    digest=hexdigest,
                    expected_size=size,
                ),
                created=published,
            )
        finally:
            if temporary.exists() and (destination is None or temporary != destination):
                temporary.unlink(missing_ok=True)

    def verify_stream_hash_only(
        self,
        chunks: Iterable[bytes | bytearray | memoryview],
        *,
        expected_digest: str,
        expected_size: int,
        expected_relative_path: str,
        expected_codec_identity: str,
        observed_codec_identity: str,
        max_chunk_bytes: int = 16 * 1024 * 1024,
    ) -> CASRef:
        """Consume and hash a stream without CAS payload reads or writes.

        This is deliberately separate from :meth:`put_stream`: it never creates
        a temporary file, never publishes a blob, and never opens the existing
        blob for readback.  The caller must supply the complete sealed digest,
        size, canonical path, and codec identity.  Metadata-only existence and
        size checks prevent returning a reference to a missing/torn blob while
        preserving zero payload reads.
        """

        digest_text = _require_sha256(expected_digest)
        if type(expected_size) is not int or expected_size < 0:
            raise CASStoreError("expected stream size must be a non-negative integer")
        if type(max_chunk_bytes) is not int or max_chunk_bytes <= 0:
            raise ValueError("max_chunk_bytes must be a positive integer")
        if (
            not isinstance(expected_codec_identity, str)
            or not expected_codec_identity
            or len(expected_codec_identity) > 512
            or not isinstance(observed_codec_identity, str)
            or not observed_codec_identity
            or len(observed_codec_identity) > 512
        ):
            raise CASStoreError("stream codec identity is invalid")
        if observed_codec_identity != expected_codec_identity:
            raise CASHashOnlyMismatch("fresh stream codec identity differs")

        destination = self._path(digest_text)
        canonical_relative_path = destination.relative_to(self.root).as_posix()
        if expected_relative_path != canonical_relative_path:
            raise CASStoreError("expected stream CAS path is non-canonical")
        _assert_no_reparse_path(destination, self.root)
        try:
            metadata = destination.stat()
        except FileNotFoundError as exc:
            raise CASCorruptionError(f"CAS blob is missing: {digest_text}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise CASCorruptionError("CAS blob path is not a regular file")
        persisted_size = metadata.st_size
        if persisted_size != expected_size:
            raise CASCorruptionError(f"CAS metadata size mismatch: expected={expected_size} actual={persisted_size}")

        observed_digest = hashlib.sha256()
        observed_size = 0
        for raw_chunk in chunks:
            if not isinstance(raw_chunk, (bytes, bytearray, memoryview)):
                raise CASStoreError("CAS stream chunks must be bytes-like")
            chunk = bytes(raw_chunk)
            if len(chunk) > max_chunk_bytes:
                raise CASStoreError("CAS stream chunk exceeds the configured memory bound")
            if not chunk:
                continue
            observed_digest.update(chunk)
            observed_size += len(chunk)
        actual_digest = observed_digest.hexdigest()
        if observed_size != expected_size or actual_digest != digest_text:
            raise CASHashOnlyMismatch(
                "fresh stream differs from sealed CAS identity: "
                f"expected={digest_text}/{expected_size} "
                f"actual={actual_digest}/{observed_size}"
            )
        return CASRef(digest_text, expected_size, canonical_relative_path)

    def put_json(self, payload: Mapping[str, Any] | list[Any]) -> CASRef:
        return self.put_bytes(canonical_json_bytes(payload))

    def get_bytes(self, reference: CASRef | Mapping[str, Any] | str) -> bytes:
        ref = CASRef.from_value(reference)
        path = self._path(ref.sha256)
        _assert_no_reparse_path(path, self.root)
        try:
            raw = path.read_bytes()
        except FileNotFoundError as exc:
            raise CASCorruptionError(f"CAS blob is missing: {ref.sha256}") from exc
        actual = hashlib.sha256(raw).hexdigest()
        if actual != ref.sha256 or (ref.size >= 0 and len(raw) != ref.size):
            raise CASCorruptionError(
                f"CAS readback mismatch: expected={ref.sha256}/{ref.size} actual={actual}/{len(raw)}"
            )
        return raw

    def get_json(self, reference: CASRef | Mapping[str, Any] | str) -> Any:
        try:
            return json.loads(self.get_bytes(reference).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CASCorruptionError("CAS blob is not valid UTF-8 JSON") from exc

    def get_json_bounded(
        self,
        reference: CASRef | Mapping[str, Any] | str,
        *,
        max_bytes: int,
    ) -> Any:
        """Read a JSON control artifact without permitting an unbounded allocation."""

        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        ref = CASRef.from_value(reference)
        path = self._path(ref.sha256)
        _assert_no_reparse_path(path, self.root)
        try:
            size = path.stat().st_size
        except FileNotFoundError as exc:
            raise CASCorruptionError(f"CAS blob is missing: {ref.sha256}") from exc
        if size > max_bytes or (ref.size >= 0 and ref.size > max_bytes):
            raise CASCorruptionError(f"CAS blob exceeds bounded read limit: size={size} max={max_bytes}")
        with path.open("rb") as handle:
            raw = handle.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise CASCorruptionError(f"CAS blob exceeds bounded read limit: size>{max_bytes} max={max_bytes}")
        actual = hashlib.sha256(raw).hexdigest()
        if actual != ref.sha256 or (ref.size >= 0 and len(raw) != ref.size):
            raise CASCorruptionError(
                f"CAS readback mismatch: expected={ref.sha256}/{ref.size} actual={actual}/{len(raw)}"
            )
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CASCorruptionError("CAS blob is not valid UTF-8 JSON") from exc

    def verify(self, reference: CASRef | Mapping[str, Any] | str) -> CASRef:
        ref = CASRef.from_value(reference)
        path = self._path(ref.sha256)
        _assert_no_reparse_path(path, self.root)
        try:
            metadata = path.stat()
        except FileNotFoundError as exc:
            raise CASCorruptionError(f"CAS blob is missing: {ref.sha256}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise CASCorruptionError("CAS blob path is not a regular file")
        persisted_size = metadata.st_size
        if ref.size >= 0 and persisted_size != ref.size:
            raise CASCorruptionError(f"CAS readback size mismatch: expected={ref.size} actual={persisted_size}")
        digest = hashlib.sha256()
        observed_size = 0
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                observed_size += len(block)
                digest.update(block)
        actual_digest = digest.hexdigest()
        if observed_size != persisted_size or actual_digest != ref.sha256:
            raise CASCorruptionError(
                f"CAS readback mismatch: expected={ref.sha256}/{ref.size} actual={actual_digest}/{observed_size}"
            )
        return CASRef(
            ref.sha256,
            observed_size,
            path.relative_to(self.root).as_posix(),
        )

    def validate_reference_metadata(self, reference: CASRef | Mapping[str, Any] | str) -> CASRef:
        """Validate a complete canonical reference without reading blob bytes."""

        ref = CASRef.from_value(reference)
        if ref.size < 0:
            raise CASStoreError("CAS reference size is incomplete")
        path = self._path(ref.sha256)
        canonical_relative_path = path.relative_to(self.root).as_posix()
        if ref.relative_path != canonical_relative_path:
            raise CASStoreError("CAS reference path is non-canonical")
        _assert_no_reparse_path(path, self.root)
        try:
            persisted_size = path.stat().st_size
        except FileNotFoundError as exc:
            raise CASCorruptionError(f"CAS blob is missing: {ref.sha256}") from exc
        if persisted_size != ref.size:
            raise CASCorruptionError(f"CAS metadata size mismatch: expected={ref.size} actual={persisted_size}")
        return CASRef(ref.sha256, ref.size, canonical_relative_path)

    def _path(self, digest: str) -> Path:
        normalized = _require_sha256(digest)
        path = self.cas_root / normalized[:2] / normalized
        try:
            path.relative_to(self.cas_root)
        except ValueError as exc:  # defensive; digest validation already excludes separators
            raise CASStoreError("CAS path escapes configured root") from exc
        return path

    def _verify_existing(self, path: Path, *, digest: str, expected: bytes) -> CASRef:
        _assert_no_reparse_path(path, self.root)
        try:
            persisted = path.read_bytes()
        except OSError as exc:
            raise CASCorruptionError(f"CAS blob cannot be read: {digest}") from exc
        actual = hashlib.sha256(persisted).hexdigest()
        if actual != digest or persisted != expected:
            raise CASCorruptionError(f"CAS identity collision or corruption: expected={digest} actual={actual}")
        return CASRef(digest, len(persisted), path.relative_to(self.root).as_posix())

    def _verify_existing_digest(
        self,
        path: Path,
        *,
        digest: str,
        expected_size: int,
    ) -> CASRef:
        _assert_no_reparse_path(path, self.root)
        actual = hashlib.sha256()
        size = 0
        try:
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    actual.update(chunk)
                    size += len(chunk)
        except OSError as exc:
            raise CASCorruptionError(f"CAS blob cannot be read: {digest}") from exc
        if actual.hexdigest() != digest or size != expected_size:
            raise CASCorruptionError(
                f"CAS identity collision or corruption: expected={digest}/{expected_size} "
                f"actual={actual.hexdigest()}/{size}"
            )
        return CASRef(digest, size, path.relative_to(self.root).as_posix())


def canonical_json_bytes(payload: Any) -> bytes:
    """Return the single canonical JSON representation used by control CAS."""

    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _require_initialized_control_root(root: Path) -> Path:
    expanded = root.expanduser()
    normalized = str(expanded).replace("\\", "/").lower()
    if not expanded.is_absolute() or normalized.startswith("//") or normalized.startswith("/mnt/"):
        raise CASStoreNotInitialized("control root must be an absolute local path")
    try:
        resolved = expanded.resolve(strict=True)
    except OSError as exc:
        raise CASStoreNotInitialized("control root does not exist") from exc
    if not resolved.is_dir() or not (resolved / _IDENTITY_FILE).is_file():
        raise CASStoreNotInitialized("control root was not explicitly initialized")
    _assert_existing_chain_no_reparse(resolved)
    try:
        identity = json.loads((resolved / _IDENTITY_FILE).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CASStoreNotInitialized("control root identity is unreadable") from exc
    expected_path = str(resolved).replace("\\", "/").casefold()
    if (
        identity.get("schema_version") != IDENTITY_SCHEMA
        or identity.get("normalized_root") != expected_path
        or identity.get("volume_identity") != volume_identity(resolved)
    ):
        raise CASStoreNotInitialized("control root identity drifted")
    return resolved


def _assert_no_reparse_path(path: Path, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise CASStoreError("CAS path escapes configured root") from exc
    current = root
    _assert_plain_node(current)
    for part in relative.parts:
        current = current / part
        if current.exists():
            _assert_plain_node(current)


def _assert_existing_chain_no_reparse(path: Path) -> None:
    current = Path(path.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in path.parts[1:]:
        current = current / part
        _assert_plain_node(current)


def _assert_plain_node(path: Path) -> None:
    try:
        attributes = os.lstat(path)
    except OSError as exc:
        raise CASStoreError(f"CAS path component is unavailable: {path}") from exc
    if stat.S_ISLNK(attributes.st_mode) or (int(getattr(attributes, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise CASStoreError(f"CAS path traverses a symlink or reparse point: {path}")


def _publish_no_replace(source: Path, target: Path) -> bool:
    if os.name == "nt":
        move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        move_file_ex.restype = ctypes.c_int
        if move_file_ex(str(source), str(target), 0x00000008):  # MOVEFILE_WRITE_THROUGH
            return True
        error = ctypes.get_last_error()
        if error in {80, 183}:  # ERROR_FILE_EXISTS / ERROR_ALREADY_EXISTS
            return False
        raise ctypes.WinError(error)
    try:
        os.link(source, target)
    except FileExistsError:
        return False
    return True


def _flush_directory(path: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    )
    create_file.restype = ctypes.c_void_p
    handle = create_file(
        str(path),
        0x80000000,  # GENERIC_READ
        0x00000001 | 0x00000002 | 0x00000004,
        None,
        3,  # OPEN_EXISTING
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle == invalid:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if not kernel32.FlushFileBuffers(ctypes.c_void_p(handle)):
            error = ctypes.get_last_error()
            # Some local filesystems persist MoveFileExW(WRITE_THROUGH) but do
            # not support flushing a directory handle.  The write-through move
            # remains the documented Windows durability primitive.
            if error not in {1, 5, 87}:
                raise ctypes.WinError(error)
    finally:
        kernel32.CloseHandle(ctypes.c_void_p(handle))


def _require_sha256(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise CASStoreError("CAS digest must be lowercase SHA-256")
    return normalized
