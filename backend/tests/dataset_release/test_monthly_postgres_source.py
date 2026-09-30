from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from backend.services.dataset_release.cas_store import CASRef
from backend.services.dataset_release.monthly_postgres_source import (
    FROZEN_SOURCE_BUNDLE_SCHEMA,
    PostgresMonthlySourceAdapter,
    _source_diffs,
)
from backend.services.dataset_release.monthly_snapshot import MonthlySnapshotIdentity


def _partition(
    dataset: str,
    key: str,
    *,
    content: str,
    schema: str = "schema-v1",
    rows: int = 1,
) -> dict[str, object]:
    return {
        "dataset": dataset,
        "partition_key": key,
        "schema_digest": schema,
        "content_digest": content,
        "row_count": rows,
    }


def test_source_diff_classifies_tail_history_schema_removal_and_pit() -> None:
    baseline = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="old"),
            _partition("adj_factor", "2026-08-01_2026-08-31", content="same"),
            _partition("daily_basic", "2026-08-01_2026-08-31", content="gone"),
        ]
    }
    current = {
        "partitions": [
            _partition("kline_daily_raw", "2026-08-01_2026-08-31", content="repaired"),
            _partition("kline_daily_raw", "2026-09-01_2026-09-30", content="tail"),
            _partition(
                "adj_factor",
                "2026-08-01_2026-08-31",
                content="same",
                schema="schema-v2",
            ),
        ]
    }

    observed = _source_diffs(
        baseline=baseline,
        current=current,
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    identities = {(item.dataset, item.kind, item.start, item.end) for item in observed}
    assert (
        "kline_daily_raw",
        "HISTORICAL_REPAIR",
        date(2026, 8, 1),
        date(2026, 8, 31),
    ) in identities
    assert (
        "kline_daily_raw",
        "TAIL_APPEND",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities
    assert ("adj_factor", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert ("daily_basic", "SCHEMA_CHANGE", date(2026, 8, 1), date(2026, 8, 31)) in identities
    assert (
        "stock_universe_pit",
        "PIT_REVISION",
        date(2026, 9, 1),
        date(2026, 9, 30),
    ) in identities


def test_first_unified_source_forces_complete_evidenced_migration() -> None:
    observed = _source_diffs(
        baseline=None,
        current={
            "partitions": [
                _partition("kline_daily_raw", "2026-09-01_2026-09-30", content="tail")
            ]
        },
        predecessor_cutoff=date(2026, 8, 31),
        target_cutoff=date(2026, 9, 30),
        pit_changed=True,
    )
    forced = {item.dataset for item in observed if item.kind == "SCHEMA_CHANGE"}
    assert {
        "kline_daily_raw",
        "kline_minute_raw",
        "adj_factor",
        "daily_basic",
        "moneyflow",
        "suspend_d",
        "stk_limit",
        "index_daily",
        "stock_universe_pit",
        "index_membership_pit",
        "industry_classification",
    }.issubset(forced)


def test_frozen_bundle_pins_formal_source_stage_receipt() -> None:
    digest = "a" * 64
    reference = CASRef(digest, 7, f"cas/sha256/aa/{digest}")
    frozen = SimpleNamespace(
        artifact_ready_contract_ref=reference,
        official_cutoff=date(2026, 9, 30),
        source_content_root=digest,
        source_provenance_root=digest,
        stable_source_provenance_root=digest,
        pit_snapshot_digest=digest,
        source_manifest_ref=reference,
        source_reuse_manifest_ref=reference,
        source_audit_ref=reference,
        source_provenance_ref=reference,
        pit_snapshot_ref=reference,
        artifact_ready_content_root=digest,
        artifact_ready_provenance_root=digest,
        provider_receipt_refs=(),
        derived_source_receipt_refs=(),
        artifact_ready_derived_source_receipt_refs=(),
        source_cas_usage={"predicted_remaining_new_bytes": 1},
    )
    identity = MonthlySnapshotIdentity(
        snapshot_id="1-ABC-1",
        source_as_of="2026-10-01T00:00:00+00:00",
        initial_repair_watermark="repair-1",
    )
    adapter = SimpleNamespace(profile=SimpleNamespace(profile="qe_hmm_full_v2"))

    bundle = PostgresMonthlySourceAdapter._bundle(
        adapter,
        frozen,
        identity=identity,
        predecessor_cutoff=date(2026, 8, 31),
        baseline_row=None,
        source_stage_ref=reference,
    )

    assert bundle["schema_version"] == FROZEN_SOURCE_BUNDLE_SCHEMA
    assert bundle["source_stage_receipt_ref"] == reference.as_dict()
