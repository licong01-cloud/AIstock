"""Freeze the exact source closure used by one historical-range request."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_json_sha256


@dataclass(frozen=True)
class HistoricalRangeCodeReleaseV1:
    code_release_id: str
    code_release_hash: str
    git_commit: str
    file_content_hashes: tuple[tuple[str, str], ...]

    def semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": "advisory_historical_range_code_release_v1",
            "git_commit": self.git_commit,
            "file_content_hashes": [
                {"relative_path": relative, "content_sha256": digest}
                for relative, digest in self.file_content_hashes
            ],
        }


class HistoricalRangeCodeReleaseResolver:
    """Hash referenced source bytes without imposing a clean-worktree gate."""

    def __init__(self, *, repository_root: Path, closure_paths: tuple[Path | str, ...]) -> None:
        self._repository_root = repository_root.resolve(strict=True)
        if not closure_paths:
            raise ValueError("historical code release requires an explicit source closure")
        self._closure_paths = closure_paths

    def resolve(self) -> HistoricalRangeCodeReleaseV1:
        commit = self._git_commit()
        members: list[tuple[str, str]] = []
        for configured in self._closure_paths:
            candidate = Path(configured)
            path = candidate if candidate.is_absolute() else self._repository_root / candidate
            resolved = path.resolve(strict=True)
            try:
                relative = resolved.relative_to(self._repository_root).as_posix()
            except ValueError as exc:
                raise ValueError("historical code closure path escapes the repository") from exc
            if not resolved.is_file():
                raise ValueError(f"historical code closure member is not a file: {relative}")
            members.append((relative, hashlib.sha256(resolved.read_bytes()).hexdigest()))
        ordered = tuple(sorted(members))
        if len(ordered) != len({item[0] for item in ordered}):
            raise ValueError("historical code closure contains duplicate files")
        release = HistoricalRangeCodeReleaseV1(
            code_release_id=f"git_{commit[:16]}",
            code_release_hash="",
            git_commit=commit,
            file_content_hashes=ordered,
        )
        return HistoricalRangeCodeReleaseV1(
            code_release_id=release.code_release_id,
            code_release_hash=canonical_json_sha256(release.semantic_payload()),
            git_commit=release.git_commit,
            file_content_hashes=release.file_content_hashes,
        )

    def _git_commit(self) -> str:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self._repository_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        commit = completed.stdout.strip().lower()
        if completed.returncode != 0 or len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
            raise RuntimeError(
                "historical code release could not resolve git HEAD: "
                f"{completed.stderr.strip()[-500:]}"
            )
        return commit
