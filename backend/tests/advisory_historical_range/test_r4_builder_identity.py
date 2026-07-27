"""R4 bridge builder-identity regression.

The durable bridge/capture identity must cover every source file that
participates in the business output (bridge, capture adapters, selectors,
dataset build).  If any participating file is added, removed, or modified
without recomputing the identity, the durable request hash changes and new
capture batches must be created instead of reusing stale ones.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from backend.tests.advisory_historical_range.test_r4_historical_e2e import (
    _BRIDGE_BUILDER_SOURCE_FILES,
    _bridge_builder_hash,
)


_REQUIRED_SOURCE_FILES = (
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


def test_bridge_builder_identity_covers_business_source_files(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[3]
    # Exact ordered coverage: no participating adapter may be dropped and no
    # unrelated file may silently enter the identity.
    assert tuple(_BRIDGE_BUILDER_SOURCE_FILES) == _REQUIRED_SOURCE_FILES
    for relative in _REQUIRED_SOURCE_FILES:
        assert (repository_root / relative).is_file(), relative

    digest = _bridge_builder_hash(repository_root)
    assert len(digest) == 64

    # The identity is content-derived: the same bytes under the same
    # relative paths reproduce it, and any single-byte business change
    # invalidates it.
    mirror = tmp_path / "mirror"
    for relative in _REQUIRED_SOURCE_FILES:
        target = mirror / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repository_root / relative, target)
    assert _bridge_builder_hash(mirror) == digest

    changed = mirror / _REQUIRED_SOURCE_FILES[0]
    changed.write_bytes(changed.read_bytes() + b"\n")
    assert _bridge_builder_hash(mirror) != digest
