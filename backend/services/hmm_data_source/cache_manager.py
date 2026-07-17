"""Trust-boundary cache for read-only QE HMM artifacts."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import re
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import ValidationError

from .artifact_manifest import ArtifactManifest, ArtifactProvenance
from .exceptions import CacheError

logger = logging.getLogger(__name__)

_ARTIFACT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_LOOP_REF_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")


class ArtifactCacheManager:
    """Cache artifacts only after path, provenance, size, and digest validation."""

    DEFAULT_MAX_ARTIFACT_BYTES = 2 * 1024**3
    DEFAULT_MAX_CACHE_BYTES = 8 * 1024**3
    DEFAULT_TTL_SECONDS = 30 * 24 * 60 * 60
    DEFAULT_LOCK_TIMEOUT_SECONDS = 10.0
    STALE_LOCK_SECONDS = 3600.0
    MAX_MANIFEST_BYTES = 64 * 1024

    _locks_guard = threading.Lock()
    _entry_locks: dict[str, threading.RLock] = {}

    def __init__(
        self,
        cache_dir: str = "tmp/hmm_evolution_cache/",
        *,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
        max_cache_bytes: int = DEFAULT_MAX_CACHE_BYTES,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
        allow_test_fixtures: bool = False,
    ) -> None:
        if (
            max_artifact_bytes <= 0
            or max_cache_bytes <= 0
            or ttl_seconds <= 0
            or lock_timeout_seconds <= 0
        ):
            raise CacheError("cache size and TTL limits must be positive")
        if max_artifact_bytes > max_cache_bytes:
            raise CacheError("max_artifact_bytes cannot exceed max_cache_bytes")

        self.cache_dir = Path(cache_dir)
        self.max_artifact_bytes = int(max_artifact_bytes)
        self.max_cache_bytes = int(max_cache_bytes)
        self.ttl_seconds = int(ttl_seconds)
        self.lock_timeout_seconds = float(lock_timeout_seconds)
        self.allow_test_fixtures = bool(allow_test_fixtures)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._assert_not_reparse(self.cache_dir, label="cache root")
        if not self.cache_dir.is_dir():
            raise CacheError(f"cache root is not a directory: {self.cache_dir}")
        self._root = self.cache_dir.resolve(strict=True)

    @staticmethod
    def _validate_loop_ref(loop_ref: str) -> str:
        normalized = str(loop_ref).strip()
        if not _LOOP_REF_RE.fullmatch(normalized):
            raise CacheError(
                "loop_ref must contain exactly two safe path segments: task_id/loop_name"
            )
        return normalized

    @staticmethod
    def _validate_artifact_name(artifact_name: str) -> str:
        normalized = str(artifact_name).strip()
        if not _ARTIFACT_NAME_RE.fullmatch(normalized):
            raise CacheError("artifact_name must be a safe basename")
        return normalized

    @classmethod
    def _cache_key(cls, loop_ref: str) -> str:
        return hashlib.sha256(loop_ref.encode("utf-8")).hexdigest()

    @classmethod
    def _lock_for(cls, entry_dir: Path) -> threading.RLock:
        key = os.path.normcase(str(entry_dir.resolve(strict=False)))
        with cls._locks_guard:
            return cls._entry_locks.setdefault(key, threading.RLock())

    @staticmethod
    def _is_reparse(path: Path) -> bool:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if callable(is_junction) and is_junction():
            return True
        try:
            attrs = getattr(path.lstat(), "st_file_attributes", 0)
        except FileNotFoundError:
            return False
        return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))

    @classmethod
    def _assert_not_reparse(cls, path: Path, *, label: str) -> None:
        if path.exists() and cls._is_reparse(path):
            raise CacheError(f"refusing {label} reparse point: {path}")

    def _assert_contained(self, path: Path) -> None:
        # Identifiers are already reduced to a digest and safe basename.  Use
        # lexical absolute paths here because concurrent Windows publication can
        # make Path.resolve() alternate between normal and extended path forms.
        root = os.path.normcase(os.path.abspath(self.cache_dir))
        candidate = os.path.normcase(os.path.abspath(path))
        try:
            common = os.path.normcase(os.path.commonpath((root, candidate)))
        except ValueError as exc:
            raise CacheError(f"cache path is outside cache root: {path}") from exc
        if common != root:
            raise CacheError(f"cache path is outside cache root: {path}")

    def _entry_dir(self, loop_ref: str) -> Path:
        normalized = self._validate_loop_ref(loop_ref)
        entry_dir = self.cache_dir / self._cache_key(normalized)
        self._assert_contained(entry_dir)
        self._assert_not_reparse(entry_dir, label="cache entry")
        return entry_dir

    def get_artifact_path(self, loop_ref: str, artifact_name: str) -> Path:
        """Return the contained artifact path without creating directories."""
        artifact = self._entry_dir(loop_ref) / self._validate_artifact_name(artifact_name)
        self._assert_contained(artifact)
        self._assert_not_reparse(artifact, label="artifact")
        return artifact

    def _manifest_path(self, loop_ref: str, artifact_name: str) -> Path:
        artifact = self.get_artifact_path(loop_ref, artifact_name)
        manifest = artifact.with_name(f"{artifact.name}.manifest.json")
        self._assert_contained(manifest)
        self._assert_not_reparse(manifest, label="artifact manifest")
        return manifest

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temp_path.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    @contextmanager
    def _process_lock(self, entry_dir: Path):
        """Acquire an inter-process lock using atomic exclusive creation."""
        entry_dir.mkdir(parents=True, exist_ok=True)
        self._assert_not_reparse(entry_dir, label="cache entry")
        lock_path = entry_dir / ".cache.lock"
        self._assert_contained(lock_path)
        deadline = time.monotonic() + self.lock_timeout_seconds
        acquired = False
        while not acquired:
            try:
                descriptor = os.open(
                    lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
            except FileExistsError:
                self._assert_not_reparse(lock_path, label="cache lock")
                try:
                    age = time.time() - lock_path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > self.STALE_LOCK_SECONDS:
                    try:
                        lock_path.unlink()
                    except FileNotFoundError:
                        pass
                    continue
                if time.monotonic() >= deadline:
                    raise CacheError(f"timed out acquiring cache lock: {lock_path}")
                time.sleep(0.05)
                continue
            try:
                os.write(
                    descriptor,
                    f"pid={os.getpid()} acquired_at={datetime.now(timezone.utc).isoformat()}\n".encode(),
                )
            finally:
                os.close(descriptor)
            acquired = True
        try:
            yield
        finally:
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    def _cache_size(self) -> int:
        total = 0
        for root, directories, files in os.walk(self.cache_dir, followlinks=False):
            root_path = Path(root)
            self._assert_not_reparse(root_path, label="cache directory")
            for name in directories:
                self._assert_not_reparse(root_path / name, label="cache directory")
            for name in files:
                if name == ".cache.lock" or (name.startswith(".") and name.endswith(".tmp")):
                    continue
                path = root_path / name
                self._assert_not_reparse(path, label="cache file")
                total += path.stat().st_size
        return total

    def _tree_size(self, root: Path) -> int:
        total = 0
        for current, directories, files in os.walk(root, followlinks=False):
            current_path = Path(current)
            self._assert_not_reparse(current_path, label="cache directory")
            for name in directories:
                self._assert_not_reparse(current_path / name, label="cache directory")
            for name in files:
                if name == ".cache.lock" or (name.startswith(".") and name.endswith(".tmp")):
                    continue
                path = current_path / name
                self._assert_not_reparse(path, label="cache file")
                total += path.stat().st_size
        return total

    def _entry_age_key(self, entry_dir: Path) -> float:
        cached_times: list[float] = []
        for manifest_path in entry_dir.glob("*.manifest.json"):
            self._assert_not_reparse(manifest_path, label="artifact manifest")
            try:
                manifest = ArtifactManifest.model_validate_json(
                    manifest_path.read_text(encoding="utf-8")
                )
            except (OSError, ValidationError):
                continue
            cached_times.append(manifest.cached_at.timestamp())
        return max(cached_times, default=entry_dir.stat().st_mtime)

    def _ensure_capacity(
        self,
        *,
        artifact_size: int,
        manifest_size: int,
        artifact_path: Path,
        manifest_path: Path,
        protected_entry: Path,
    ) -> None:
        if artifact_size > self.max_artifact_bytes:
            raise CacheError(
                f"artifact size {artifact_size} exceeds limit {self.max_artifact_bytes}"
            )
        replaced_size = sum(
            path.stat().st_size for path in (artifact_path, manifest_path) if path.exists()
        )
        projected = (
            self._cache_size()
            - replaced_size
            + artifact_size
            + manifest_size
        )
        if projected <= self.max_cache_bytes:
            return

        candidates: list[Path] = []
        for entry in self.cache_dir.iterdir():
            if entry.name == ".cache.lock" or entry == protected_entry:
                continue
            self._assert_not_reparse(entry, label="cache eviction candidate")
            if entry.is_dir():
                candidates.append(entry)
        candidates.sort(key=self._entry_age_key)
        for candidate in candidates:
            self._assert_not_reparse(candidate, label="cache eviction candidate")
            candidate_size = self._tree_size(candidate)
            with self._lock_for(candidate):
                with self._process_lock(candidate):
                    self._safe_remove_tree(candidate)
            projected -= candidate_size
            if projected <= self.max_cache_bytes:
                return
        if projected > self.max_cache_bytes:
            raise CacheError(
                f"cache size would exceed limit: projected={projected}, "
                f"limit={self.max_cache_bytes}"
            )

    def save_artifact(
        self,
        loop_ref: str,
        artifact_name: str,
        artifact_bytes: bytes,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Path:
        """Atomically publish an artifact and its mandatory provenance manifest."""
        normalized_loop_ref = self._validate_loop_ref(loop_ref)
        normalized_name = self._validate_artifact_name(artifact_name)
        if not isinstance(artifact_bytes, bytes):
            raise CacheError("artifact_bytes must be bytes")
        try:
            provenance = ArtifactProvenance.model_validate(metadata or {})
        except ValidationError as exc:
            raise CacheError(f"invalid artifact provenance: {exc}") from exc
        if provenance.source == "test_fixture" and not self.allow_test_fixtures:
            raise CacheError("test_fixture provenance is disabled for this cache manager")

        artifact_path = self.get_artifact_path(normalized_loop_ref, normalized_name)
        manifest_path = self._manifest_path(normalized_loop_ref, normalized_name)
        entry_dir = artifact_path.parent
        lock = self._lock_for(entry_dir)
        root_lock = self._lock_for(self.cache_dir)
        with root_lock:
            with self._process_lock(self.cache_dir):
                with lock:
                    with self._process_lock(entry_dir):
                        sha256 = hashlib.sha256(artifact_bytes).hexdigest()
                        if provenance.source == "qe_workspace" and (
                            provenance.remote_sha256 != sha256
                            or provenance.remote_size_bytes != len(artifact_bytes)
                        ):
                            raise CacheError(
                                "downloaded artifact does not match trusted remote manifest"
                            )
                        now = datetime.now(timezone.utc)
                        manifest = ArtifactManifest(
                            loop_ref=normalized_loop_ref,
                            cache_key=self._cache_key(normalized_loop_ref),
                            artifact_name=normalized_name,
                            file_size=len(artifact_bytes),
                            sha256=sha256,
                            cached_at=now,
                            expires_at=now + timedelta(seconds=self.ttl_seconds),
                            provenance=provenance,
                        )
                        manifest_bytes = (
                            json.dumps(
                                manifest.model_dump(mode="json"),
                                indent=2,
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                            + "\n"
                        ).encode("utf-8")
                        if len(manifest_bytes) > self.MAX_MANIFEST_BYTES:
                            raise CacheError("generated artifact manifest exceeds size limit")
                        self._ensure_capacity(
                            artifact_size=len(artifact_bytes),
                            manifest_size=len(manifest_bytes),
                            artifact_path=artifact_path,
                            manifest_path=manifest_path,
                            protected_entry=entry_dir,
                        )
                        try:
                            self._atomic_write(artifact_path, artifact_bytes)
                            self._atomic_write(manifest_path, manifest_bytes)
                        except Exception as exc:
                            raise CacheError(
                                f"failed to save artifact {normalized_name}: {exc}"
                            ) from exc
        return artifact_path

    def _load_manifest(self, loop_ref: str, artifact_name: str) -> ArtifactManifest:
        manifest_path = self._manifest_path(loop_ref, artifact_name)
        if not manifest_path.is_file():
            raise CacheError(f"trusted manifest not found: {artifact_name} for {loop_ref}")
        if manifest_path.stat().st_size > self.MAX_MANIFEST_BYTES:
            raise CacheError(f"artifact manifest exceeds size limit: {manifest_path}")
        try:
            manifest = ArtifactManifest.model_validate_json(
                manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, ValidationError) as exc:
            raise CacheError(f"invalid artifact manifest: {manifest_path}: {exc}") from exc
        expected_key = self._cache_key(loop_ref)
        if (
            manifest.loop_ref != loop_ref
            or manifest.artifact_name != artifact_name
            or manifest.cache_key != expected_key
        ):
            raise CacheError("artifact manifest identity does not match cache request")
        if manifest.provenance.source == "qe_workspace" and (
            manifest.provenance.remote_sha256 != manifest.sha256
            or manifest.provenance.remote_size_bytes != manifest.file_size
        ):
            raise CacheError("local manifest does not match trusted remote manifest")
        if (
            manifest.provenance.source == "test_fixture"
            and not self.allow_test_fixtures
        ):
            raise CacheError("test_fixture provenance is not trusted by this cache manager")
        if manifest.expires_at <= datetime.now(timezone.utc):
            raise CacheError(f"artifact cache entry expired: {artifact_name} for {loop_ref}")
        return manifest

    def load_artifact(
        self,
        loop_ref: str,
        artifact_name: str,
        verify_checksum: bool = True,
    ) -> bytes:
        """Load only a complete, non-expired artifact with valid provenance and digest."""
        if not verify_checksum:
            raise CacheError("checksum verification cannot be disabled")
        normalized_loop_ref = self._validate_loop_ref(loop_ref)
        normalized_name = self._validate_artifact_name(artifact_name)
        artifact_path = self.get_artifact_path(normalized_loop_ref, normalized_name)
        if not artifact_path.is_file():
            raise CacheError(
                f"artifact not found in cache: {normalized_name} for {normalized_loop_ref}"
            )
        lock = self._lock_for(artifact_path.parent)
        with lock:
            with self._process_lock(artifact_path.parent):
                manifest = self._load_manifest(normalized_loop_ref, normalized_name)
                stat_result = artifact_path.stat()
                if stat_result.st_size > self.max_artifact_bytes:
                    raise CacheError("cached artifact exceeds configured size limit")
                if stat_result.st_size != manifest.file_size:
                    raise CacheError("cached artifact size does not match manifest")
                try:
                    artifact_bytes = artifact_path.read_bytes()
                except OSError as exc:
                    raise CacheError(f"failed to read cached artifact: {exc}") from exc
                actual_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
                if actual_sha256 != manifest.sha256:
                    raise CacheError(
                        f"checksum mismatch for {normalized_name}: "
                        f"expected {manifest.sha256}, got {actual_sha256}"
                    )
        return artifact_bytes

    def load_pickle(self, loop_ref: str, artifact_name: str) -> Any:
        """Deserialize a checksum- and provenance-verified artifact."""
        try:
            payload = pickle.loads(self.load_artifact(loop_ref, artifact_name))
            manifest = self._load_manifest(loop_ref, artifact_name)
            expected_rows = manifest.provenance.remote_row_count
            if expected_rows is not None:
                try:
                    actual_rows = len(payload)
                except TypeError as exc:
                    raise CacheError(
                        "artifact payload has no row count required by remote manifest"
                    ) from exc
                if actual_rows != expected_rows:
                    self.clear_cache(loop_ref)
                    raise CacheError(
                        f"artifact row_count mismatch: expected {expected_rows}, "
                        f"got {actual_rows}"
                    )
            return payload
        except CacheError:
            raise
        except Exception as exc:
            raise CacheError(f"failed to load pickle {artifact_name}: {exc}") from exc

    def is_cached(self, loop_ref: str, artifact_name: str) -> bool:
        """Return true only for an entry that is currently safe to load."""
        try:
            self.load_artifact(loop_ref, artifact_name)
        except CacheError:
            return False
        return True

    def is_fresh(self, loop_ref: str, artifact_name: str) -> bool:
        """Check manifest identity and TTL without re-reading a verified payload."""
        try:
            artifact_path = self.get_artifact_path(loop_ref, artifact_name)
            manifest = self._load_manifest(loop_ref, artifact_name)
            return artifact_path.is_file() and artifact_path.stat().st_size == manifest.file_size
        except (CacheError, OSError):
            return False

    def _safe_remove_tree(self, root: Path) -> None:
        self._assert_contained(root)
        self._assert_not_reparse(root, label="cache removal root")
        for current, directories, files in os.walk(root, topdown=True, followlinks=False):
            current_path = Path(current)
            self._assert_not_reparse(current_path, label="cache directory")
            for name in directories:
                self._assert_not_reparse(current_path / name, label="cache directory")
            for name in files:
                self._assert_not_reparse(current_path / name, label="cache file")
        for current, directories, files in os.walk(root, topdown=False, followlinks=False):
            current_path = Path(current)
            for name in files:
                (current_path / name).unlink()
            for name in directories:
                (current_path / name).rmdir()
        root.rmdir()

    def clear_cache(self, loop_ref: Optional[str] = None) -> None:
        """Safely clear one entry or all entries without following reparse points."""
        with self._lock_for(self.cache_dir):
            with self._process_lock(self.cache_dir):
                if loop_ref is not None:
                    entry_dir = self._entry_dir(loop_ref)
                    if entry_dir.exists():
                        with self._lock_for(entry_dir):
                            with self._process_lock(entry_dir):
                                self._safe_remove_tree(entry_dir)
                    return
                for child in list(self.cache_dir.iterdir()):
                    if child.name == ".cache.lock":
                        continue
                    self._assert_not_reparse(child, label="cache child")
                    if child.is_dir():
                        with self._lock_for(child):
                            with self._process_lock(child):
                                self._safe_remove_tree(child)
                    elif child.is_file():
                        child.unlink()
                    else:
                        raise CacheError(f"unsupported cache entry type: {child}")

    def get_cache_info(self, loop_ref: Optional[str] = None) -> dict[str, Any]:
        """Return manifest-backed cache inventory without guessing lossy identities."""
        if loop_ref is not None:
            normalized_loop_ref = self._validate_loop_ref(loop_ref)
            entry_dir = self._entry_dir(normalized_loop_ref)
            if not entry_dir.exists():
                return {"loop_ref": normalized_loop_ref, "cached": False}
            artifacts: dict[str, dict[str, Any]] = {}
            total_size = 0
            for manifest_path in entry_dir.glob("*.manifest.json"):
                artifact_name = manifest_path.name.removesuffix(".manifest.json")
                try:
                    manifest = self._load_manifest(normalized_loop_ref, artifact_name)
                    self.load_artifact(normalized_loop_ref, artifact_name)
                except CacheError as exc:
                    artifacts[artifact_name] = {"valid": False, "error": str(exc)}
                    continue
                total_size += manifest.file_size
                artifacts[artifact_name] = {
                    "valid": True,
                    "size": manifest.file_size,
                    "cached_at": manifest.cached_at.isoformat(),
                    "expires_at": manifest.expires_at.isoformat(),
                    "source": manifest.provenance.source,
                }
            return {
                "loop_ref": normalized_loop_ref,
                "cached": any(item.get("valid") for item in artifacts.values()),
                "artifacts": artifacts,
                "total_size": total_size,
            }

        all_info: dict[str, Any] = {}
        for entry_dir in self.cache_dir.iterdir():
            self._assert_not_reparse(entry_dir, label="cache entry")
            if not entry_dir.is_dir():
                continue
            original_loop_ref = self._recover_loop_ref(entry_dir)
            if original_loop_ref is None:
                logger.warning("cannot recover loop_ref for cache dir %s", entry_dir.name)
                continue
            all_info[original_loop_ref] = self.get_cache_info(original_loop_ref)
        return all_info

    def _recover_loop_ref(self, loop_dir: Path) -> Optional[str]:
        for manifest_path in loop_dir.glob("*.manifest.json"):
            self._assert_not_reparse(manifest_path, label="artifact manifest")
            if manifest_path.stat().st_size > self.MAX_MANIFEST_BYTES:
                continue
            try:
                manifest = ArtifactManifest.model_validate_json(
                    manifest_path.read_text(encoding="utf-8")
                )
            except (OSError, ValidationError):
                continue
            return manifest.loop_ref
        return None

    def _load_metadata(self, loop_ref: str, artifact_name: str) -> Optional[dict[str, Any]]:
        """Compatibility reader returning the validated manifest as a mapping."""
        try:
            manifest = self._load_manifest(loop_ref, artifact_name)
        except CacheError:
            return None
        return manifest.model_dump(mode="json", exclude_none=True)
