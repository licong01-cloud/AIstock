"""Shared builder-identity fixture retained after removing the DB E2E duplicate."""

from __future__ import annotations

import hashlib
from pathlib import Path


_BRIDGE_BUILDER_SOURCE_FILES = (
    "backend/services/advisory_historical_range/dataset_bridge.py",
    "backend/services/advisory_historical_range/dataset_bridge_postgres.py",
    "backend/services/advisory_historical_range/retrospective_projection.py",
    "backend/services/advisory_phase1/capture_foundation.py",
    "backend/services/advisory_phase1/observation_capture.py",
    "backend/services/advisory_phase1/observation_capture_postgres.py",
    "backend/services/advisory_phase1/label_capture.py",
    "backend/services/advisory_phase1/label_builder_postgres.py",
    "backend/services/advisory_phase1/retrospective_selector.py",
    "backend/services/advisory_phase1/retrospective_selector_postgres.py",
    "backend/services/advisory_phase1/dataset_build.py",
    "backend/services/advisory_phase1/dataset_build_postgres.py",
)


def _bridge_builder_hash(repository_root: Path) -> str:
    digest = hashlib.sha256()
    for relative_path in _BRIDGE_BUILDER_SOURCE_FILES:
        digest.update(relative_path.encode())
        digest.update(b"\0")
        digest.update((repository_root / relative_path).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()

