"""Minimal Batch-D transaction and fail-closed recovery contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_phase1.dataset_build import (
    BuildCheckpoint,
    InMemoryDatasetBuildRepository,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.snapshot_writer import (
    DatasetSnapshotMaterializer,
    DatasetSnapshotPipeline,
    DeterministicParquetWriter,
)
from backend.tests.advisory_phase1.test_phase1c3_batch_d_writer import (
    UTC_TS,
    _fixture_rows,
    _identity,
    _request,
)


def _pipeline(repository, store):  # type: ignore[no-untyped-def]
    class Source:
        @staticmethod
        def read(_build):  # type: ignore[no-untyped-def]
            return {role: list(rows) for role, rows in _fixture_rows().items()}

    return DatasetSnapshotPipeline(
        repository=repository,
        materializer=DatasetSnapshotMaterializer(
            source_reader=Source(),
            writer=DeterministicParquetWriter(),
        ),
        store=store,
    )


@pytest.mark.parametrize(
    ("method_name", "expected_checkpoint"),
    (
        ("complete_materialize", BuildCheckpoint.REQUESTED),
        ("complete_full_verify", BuildCheckpoint.MATERIALIZED),
        ("complete_promote", BuildCheckpoint.VERIFIED),
        ("save_sealed_snapshot", BuildCheckpoint.PROMOTED),
    ),
)
def test_checkpoint_failure_never_fakes_success_and_exact_resume_seals(
    method_name: str,
    expected_checkpoint: BuildCheckpoint,
    monkeypatch,
    tmp_path: Path,
) -> None:
    repository = InMemoryDatasetBuildRepository(now_provider=lambda: UTC_TS)
    build = repository.create_or_get(_request(), actor="test")
    store = LocalContentAddressedStore(
        root=(tmp_path / method_name / "store").resolve(),
        repository_root=(tmp_path / method_name / "repo").resolve(),
        store_identity=_identity(),
    )
    pipeline = _pipeline(repository, store)
    original = getattr(repository, method_name)

    def fail_once(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        monkeypatch.setattr(repository, method_name, original)
        raise RuntimeError(f"crash before {method_name} checkpoint")

    monkeypatch.setattr(repository, method_name, fail_once)
    with pytest.raises(RuntimeError, match="crash before"):
        pipeline.run(build_id=build.build_id, actor="test")
    failed = repository.get_build(build.build_id)
    assert failed.checkpoint is expected_checkpoint
    assert failed.current_attempt_id is None
    assert pipeline.run(build_id=build.build_id, actor="test").checkpoint is BuildCheckpoint.SEALED

