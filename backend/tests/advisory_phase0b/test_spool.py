from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_phase0b.errors import Phase0BAuditError
from backend.services.advisory_phase0b.spool import Phase0BBoundedSpool


_FILE_HASH = "a" * 64


def _roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    repository_root = (tmp_path / "repository").resolve()
    dataset_root = (tmp_path / "dataset").resolve()
    output_root = (tmp_path / "reports").resolve()
    repository_root.mkdir()
    dataset_root.mkdir()
    output_root.mkdir()
    return repository_root, dataset_root, output_root


def _spool(tmp_path: Path, *, operation_id: str) -> Phase0BBoundedSpool:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    return Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id=operation_id,
    )


def test_spool_is_date_ordered_and_cleans_only_its_operation_path(tmp_path: Path) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    unrelated = output_root / "keep.txt"
    unrelated.write_text("keep", encoding="utf-8")

    with Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id="operation-1",
    ) as spool:
        assert spool.append_rows(
            snapshot_id="snapshot-1",
            logical_role="outcome_labels",
            source_file_sha256=_FILE_HASH,
            rows=(
                {
                    "label_key_hash": "b",
                    "decision_as_of_trade_date": "2026-07-02",
                    "value": Decimal("0.2"),
                },
                {
                    "label_key_hash": "a",
                    "decision_as_of_trade_date": "2026-07-01",
                    "value": Decimal("0.1"),
                },
            ),
            identity_fields=("label_key_hash",),
            decision_date_field="decision_as_of_trade_date",
        ) == 2
        assert spool.decision_dates(snapshot_id="snapshot-1") == (
            "2026-07-01",
            "2026-07-02",
        )
        assert list(
            spool.iter_rows(
                snapshot_id="snapshot-1",
                logical_role="outcome_labels",
                decision_date="2026-07-01",
            )
        )[0]["value"] == "0.100000000000"
        path = spool.path
        assert path.exists()

    assert not path.exists()
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_spool_rejects_duplicate_frozen_identity(tmp_path: Path) -> None:
    with _spool(tmp_path, operation_id="operation-2") as spool:
        with pytest.raises(Phase0BAuditError, match="conflict with frozen identities"):
            spool.append_rows(
                snapshot_id="snapshot-1",
                logical_role="stage_candidates",
                source_file_sha256=_FILE_HASH,
                rows=(
                    {"stage_evidence_id": "stage", "symbol": "000001.SZ"},
                    {"stage_evidence_id": "stage", "symbol": "000001.SZ"},
                ),
                identity_fields=("stage_evidence_id", "symbol"),
                decision_date_field=None,
            )


def test_spool_rejects_missing_decision_date_without_inserting_hidden_row(tmp_path: Path) -> None:
    with _spool(tmp_path, operation_id="operation-3") as spool:
        with pytest.raises(Phase0BAuditError, match="lacks its decision date"):
            spool.append_rows(
                snapshot_id="snapshot-1",
                logical_role="outcome_labels",
                source_file_sha256=_FILE_HASH,
                rows=({"label_key_hash": "x"},),
                identity_fields=("label_key_hash",),
                decision_date_field="decision_as_of_trade_date",
            )


def test_spool_rejects_relative_or_protected_output_root(tmp_path: Path) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    with pytest.raises(ValueError, match="explicit absolute"):
        Phase0BBoundedSpool(
            output_root=Path("relative-output"),
            repository_root=repository_root,
            dataset_root=dataset_root,
            operation_id="relative",
        )
    with pytest.raises(ValueError, match="must not overlap the repository root"):
        Phase0BBoundedSpool(
            output_root=repository_root / "reports",
            repository_root=repository_root,
            dataset_root=dataset_root,
            operation_id="inside-repository",
        )
    assert not (output_root / ".phase0b-tmp").exists()


def test_spool_initialization_failure_cleans_exact_operation_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)

    def _fail_connect(_path: Path) -> None:
        raise OSError("injected sqlite open failure")

    monkeypatch.setattr("backend.services.advisory_phase0b.spool.sqlite3.connect", _fail_connect)
    with pytest.raises(OSError, match="injected sqlite open failure"):
        Phase0BBoundedSpool(
            output_root=output_root,
            repository_root=repository_root,
            dataset_root=dataset_root,
            operation_id="init-failure",
        )
    assert not (output_root / ".phase0b-tmp" / "init-failure").exists()


def _append_role(
    spool: Phase0BBoundedSpool,
    *,
    role: str,
    row: dict[str, object],
    identity_fields: tuple[str, ...],
    file_hash_char: str,
    decision_date_field: str | None = None,
) -> None:
    spool.append_rows(
        snapshot_id="snapshot-1",
        logical_role=role,
        source_file_sha256=file_hash_char * 64,
        rows=(row,),
        identity_fields=identity_fields,
        decision_date_field=decision_date_field,
    )


def test_spool_closes_relations_and_propagates_decision_date(tmp_path: Path) -> None:
    with _spool(tmp_path, operation_id="relations") as spool:
        _append_role(
            spool,
            role="canonical_signals",
            row={"canonical_signal_id": "signal-1", "decision_as_of_trade_date": "2026-07-01"},
            identity_fields=("canonical_signal_id",),
            file_hash_char="a",
            decision_date_field="decision_as_of_trade_date",
        )
        _append_role(
            spool,
            role="observation_versions",
            row={
                "observation_version_id": "obs-1",
                "canonical_signal_id": "signal-1",
                "observation_content_hash": "4" * 64,
                "observation_revision_no": 1,
                "observation_status": "COMPLETE",
            },
            identity_fields=("observation_version_id",),
            file_hash_char="b",
        )
        _append_role(
            spool,
            role="selected_observations",
            row={
                "selected_mapping_id": "mapping-1",
                "canonical_signal_id": "signal-1",
                "terminal_observation_version_id": "obs-1",
                "terminal_observation_content_hash": "4" * 64,
                "terminal_revision_no": 1,
            },
            identity_fields=("selected_mapping_id",),
            file_hash_char="c",
        )
        _append_role(
            spool,
            role="lineage",
            row={
                "lineage_id": "lineage-1",
                "canonical_signal_id": "signal-1",
                "observation_version_id": "obs-1",
            },
            identity_fields=("lineage_id",),
            file_hash_char="d",
        )
        _append_role(
            spool,
            role="stage_summaries",
            row={"stage_evidence_id": "stage-1", "observation_version_id": "obs-1"},
            identity_fields=("stage_evidence_id",),
            file_hash_char="e",
        )
        _append_role(
            spool,
            role="stage_candidates",
            row={"stage_evidence_id": "stage-1", "symbol": "000001.SZ"},
            identity_fields=("stage_evidence_id", "symbol"),
            file_hash_char="f",
        )
        _append_role(
            spool,
            role="source_revisions",
            row={"source_revision_set_hash": "1" * 64, "member_key": "daily:20260701"},
            identity_fields=("source_revision_set_hash", "member_key"),
            file_hash_char="1",
        )
        _append_role(
            spool,
            role="outcome_labels",
            row={
                "label_version_id": "label-1",
                "label_key_hash": "2" * 64,
                "label_content_hash": "5" * 64,
                "label_revision_no": 1,
                "canonical_signal_id": "signal-1",
                "observation_version_id": "obs-1",
                "candidate_stage_evidence_id": "stage-1",
                "symbol": "000001.SZ",
                "owner_type": "CANDIDATE",
                "maturity_status": "MATURED",
                "outcome_event_status": "TERMINAL",
                "horizon_trading_days": 5,
                "projection": "RETURN_NET_EXCESS",
                "calculation_evidence_sha256": "6" * 64,
                "calculation_evidence_size_bytes": 100,
                "calculation_evidence_store_backend_hash": "7" * 64,
                "label_source_revision_set_hash": "9" * 64,
                "decision_as_of_trade_date": "2026-07-01",
            },
            identity_fields=("label_version_id",),
            file_hash_char="2",
            decision_date_field="decision_as_of_trade_date",
        )
        _append_role(
            spool,
            role="selected_labels",
            row={
                "selected_label_mapping_id": "selected-label-1",
                "label_key_hash": "2" * 64,
                "terminal_label_version_id": "label-1",
                "terminal_label_content_hash": "5" * 64,
                "terminal_label_revision_no": 1,
                "terminal_maturity_status": "MATURED",
                "terminal_outcome_event_status": "TERMINAL",
                "selection_status": "SELECTED",
            },
            identity_fields=("selected_label_mapping_id",),
            file_hash_char="3",
        )
        _append_role(
            spool,
            role="outcome_source_evidence",
            row={
                "owner_type": "CANDIDATE",
                "label_version_id": "label-1",
                "label_key_hash": "2" * 64,
                "canonical_signal_id": "signal-1",
                "symbol": "000001.SZ",
                "horizon_trading_days": 5,
                "projection": "RETURN_NET_EXCESS",
                "calculation_evidence_sha256": "6" * 64,
                "calculation_evidence_size_bytes": 100,
                "calculation_evidence_store_backend_hash": "7" * 64,
            },
            identity_fields=("owner_type", "label_version_id"),
            file_hash_char="8",
        )

        spool.close_relations(snapshot_id="snapshot-1")

        assert spool.decision_dates(snapshot_id="snapshot-1") == ("2026-07-01",)
        assert [
            row["symbol"]
            for row in spool.iter_rows(
                snapshot_id="snapshot-1",
                logical_role="stage_candidates",
                decision_date="2026-07-01",
            )
        ] == ["000001.SZ"]


def test_spool_relation_orphan_fails_visibly(tmp_path: Path) -> None:
    with _spool(tmp_path, operation_id="orphan") as spool:
        _append_role(
            spool,
            role="selected_observations",
            row={"selected_mapping_id": "mapping-1", "canonical_signal_id": "missing-signal"},
            identity_fields=("selected_mapping_id",),
            file_hash_char="a",
        )
        with pytest.raises(Phase0BAuditError, match="exactly one frozen parent"):
            spool.close_relations(snapshot_id="snapshot-1")


def test_target_dates_are_program_isolated_and_duplicate_lineage_does_not_duplicate_signal(
    tmp_path: Path,
) -> None:
    with _spool(tmp_path, operation_id="target-dates") as spool:
        spool.append_rows(
            snapshot_id="snapshot-1",
            logical_role="canonical_signals",
            source_file_sha256="a" * 64,
            rows=(
                {
                    "canonical_signal_id": "signal-a",
                    "package_id": "package-a",
                    "manifest_sha256": "1" * 64,
                    "alpha_mode": "single_alpha",
                    "decision_as_of_trade_date": "2026-07-01",
                },
                {
                    "canonical_signal_id": "signal-b",
                    "package_id": "package-b",
                    "manifest_sha256": "2" * 64,
                    "alpha_mode": "multi_alpha",
                    "decision_as_of_trade_date": "2026-07-02",
                },
            ),
            identity_fields=("canonical_signal_id",),
            decision_date_field="decision_as_of_trade_date",
        )
        spool.append_rows(
            snapshot_id="snapshot-1",
            logical_role="lineage",
            source_file_sha256="b" * 64,
            rows=(
                {
                    "lineage_id": "lineage-a1",
                    "canonical_signal_id": "signal-a",
                    "historical_range_frozen_program_hash": "3" * 64,
                },
                {
                    "lineage_id": "lineage-a2",
                    "canonical_signal_id": "signal-a",
                    "historical_range_frozen_program_hash": "3" * 64,
                },
                {
                    "lineage_id": "lineage-b",
                    "canonical_signal_id": "signal-b",
                    "historical_range_frozen_program_hash": "4" * 64,
                },
            ),
            identity_fields=("lineage_id",),
            decision_date_field=None,
        )

        assert spool.target_decision_dates(
            snapshot_id="snapshot-1",
            package_id="package-a",
            manifest_sha256="1" * 64,
            alpha_mode="single_alpha",
            program_id=None,
            range_program_hash="3" * 64,
        ) == ("2026-07-01",)
        assert len(
            tuple(
                spool.iter_target_signals(
                    snapshot_id="snapshot-1",
                    package_id="package-a",
                    manifest_sha256="1" * 64,
                    alpha_mode="single_alpha",
                    program_id=None,
                    range_program_hash="3" * 64,
                    decision_date="2026-07-01",
                )
            )
        ) == 1
