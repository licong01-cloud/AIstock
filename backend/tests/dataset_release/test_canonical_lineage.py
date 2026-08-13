from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.services.dataset_release import canonical_lineage as lineage_module
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.canonical_lineage import (
    CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA,
    CanonicalLineageError,
    LineageRef,
    MAX_LINEAGE_DESCRIPTOR_BYTES,
    MAX_LINEAGE_OBJECT_BYTES,
    active_segments,
    event_inventory,
    instrument_summaries,
    migrate_legacy_and_write_transition,
    namespace_manifest,
    validate_lineage_descriptor,
    write_genesis,
    write_transition,
    write_transition_updates,
)


FIELDS = ["date", "symbol", "open", "close"]


def _code(number: int) -> str:
    return f"{number:06d}.SZ"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _segment(number: int, month: int, *, root: str | None = None) -> dict:
    code = _code(number)
    year = 2026 + (month - 1) // 12
    calendar_month = (month - 1) % 12 + 1
    timestamp = f"{year:04d}-{calendar_month:02d}-28 00:00:00"
    return {
        "instrument": code,
        "root_relative_path": root
        or ("daily_bin/csv" if month == 1 else f"daily_bin/csv_deltas/{year:04d}{calendar_month:02d}"),
        "relative_path": f"{code.casefold()}.csv",
        "rows": 20,
        "sha256": _sha(f"{code}:{month}"),
        "size_bytes": 1024 + month,
        "start": timestamp,
        "end": timestamp,
    }


def _inventory(item: dict, *, active: bool = True) -> dict:
    return {**item, "active": active}


def test_lineage_genesis_three_months_namespace_and_idempotent_readback(
    tmp_path: Path,
) -> None:
    root = tmp_path / "daily_bin"
    root.mkdir()
    codes = tuple(_code(value) for value in range(1, 9))
    current = [_segment(value, 1) for value in range(1, 9)]
    genesis = write_genesis(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        segments=current,
        cutoff="2026-01-31",
        mutation_identity="a" * 64,
    )

    month_two = [_segment(value, 2) for value in range(1, 9)]
    current_two = [*current, *month_two]
    second = write_transition(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        baseline_descriptor=genesis.descriptor,
        current_segments=current_two,
        prior_active_segments=current,
        cutoff="2026-02-28",
        action="INCREMENTAL",
        mutation_identity="b" * 64,
        scopes=[{"kind": "monthly_tail_extension", "new_months": ["2026-02"]}],
        inventory=[_inventory(item) for item in month_two],
        planned_instruments=codes,
    )
    month_three = [_segment(value, 3) for value in range(1, 9)]
    current_three = [*current_two, *month_three]
    third = write_transition(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        baseline_descriptor=second.descriptor,
        current_segments=current_three,
        prior_active_segments=current_two,
        cutoff="2026-03-31",
        action="INCREMENTAL",
        mutation_identity="c" * 64,
        scopes=[{"kind": "monthly_tail_extension", "new_months": ["2026-03"]}],
        inventory=[_inventory(item) for item in month_three],
        planned_instruments=codes,
    )

    validated = validate_lineage_descriptor(root, third.descriptor)
    observed = active_segments(root, validated)
    summaries = instrument_summaries(root, validated)

    assert len(canonical_json_bytes(third.descriptor)) < MAX_LINEAGE_DESCRIPTOR_BYTES
    assert len(observed) == 8 * 3
    assert len(summaries) == 8
    assert {item["segments"] for item in summaries} == {3}
    assert {item["rows"] for item in summaries} == {60}
    assert max(path.stat().st_size for path in (root / "csv_lineage").rglob("*.json")) <= MAX_LINEAGE_OBJECT_BYTES

    namespace_root = "daily_bin/csv_deltas/202603"
    manifest = namespace_manifest(
        dataset="daily_bin",
        component_action="INCREMENTAL",
        phase="tail",
        segment_key="202603",
        namespace_root_relative_path=namespace_root,
        lineage=third,
        patch_actual_work={"stock_rows_transformed": 160},
    )
    assert manifest["schema_version"] == CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA
    assert len(canonical_json_bytes(manifest)) < 128 * 1024
    assert event_inventory(
        root,
        third.event_ref.as_dict(),
        namespace_root_relative_path=namespace_root,
    ) == tuple(
        sorted(
            (
                _inventory(item)
                | {
                    "inventory_identity": digest_named_fields(
                        lineage_module.CANONICAL_LINEAGE_INVENTORY_ROOT_SCHEMA,
                        _inventory(item),
                    )
                }
                for item in month_three
            ),
            key=lambda item: (item["instrument"], item["relative_path"]),
        )
    )

    replay = write_transition(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        baseline_descriptor=second.descriptor,
        current_segments=current_three,
        prior_active_segments=current_two,
        cutoff="2026-03-31",
        action="INCREMENTAL",
        mutation_identity="c" * 64,
        scopes=[{"kind": "monthly_tail_extension", "new_months": ["2026-03"]}],
        inventory=[_inventory(item) for item in month_three],
        planned_instruments=codes,
    )
    assert replay.descriptor == third.descriptor
    assert replay.created_paths == third.created_paths


def test_legacy_v1_anchor_migration_is_equivalent_and_non_destructive(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute_bin"
    root.mkdir()
    legacy_files = [
        {
            **_segment(value, 1, root="minute_bin/csv"),
            "relative_path": f"{_code(value).casefold()}.csv",
        }
        for value in range(1, 9)
    ]
    legacy = {
        "schema_version": "dataset_release_sealed_qlib_csv_rows_v1",
        "dataset": "minute_bin",
        "root_relative_path": "minute_bin/csv",
        "ordered_fields": FIELDS,
        "rows": 160,
        "files": [{key: value for key, value in item.items() if key != "root_relative_path"} for item in legacy_files],
    }
    month_two = [_segment(value, 2, root="minute_bin/csv_deltas/202602") for value in range(1, 9)]
    result = migrate_legacy_and_write_transition(
        root,
        dataset="minute_bin",
        ordered_fields=FIELDS,
        legacy_source=legacy,
        current_segments=[*legacy_files, *month_two],
        baseline_cutoff="2026-01-31",
        cutoff="2026-02-28",
        baseline_identity="d" * 64,
        baseline_binding={
            "release_id": "legacy-release",
            "release_digest": "e" * 64,
            "component_file_identity": "f" * 64,
        },
        action="INCREMENTAL",
        mutation_identity="1" * 64,
        scopes=[{"kind": "monthly_tail_extension", "new_months": ["2026-02"]}],
        inventory=[_inventory(item) for item in month_two],
        planned_instruments=tuple(_code(value) for value in range(1, 9)),
    )

    assert "legacy_anchor" in result.descriptor
    assert len(active_segments(root, result.descriptor)) == 16
    assert legacy["schema_version"] == "dataset_release_sealed_qlib_csv_rows_v1"
    assert b'"segments"' not in canonical_json_bytes(result.descriptor)


@pytest.mark.parametrize("mutation", ["missing", "tamper"])
def test_lineage_missing_or_tampered_object_fails_closed(tmp_path: Path, mutation: str) -> None:
    root = tmp_path / "daily_bin"
    root.mkdir()
    result = write_genesis(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        segments=[_segment(value, 1) for value in range(1, 9)],
        cutoff="2026-01-31",
        mutation_identity="2" * 64,
    )
    target = root / result.event_ref.relative_path
    if mutation == "missing":
        target.unlink()
    else:
        target.write_bytes(target.read_bytes() + b" ")

    with pytest.raises(CanonicalLineageError):
        validate_lineage_descriptor(root, result.descriptor)


def test_lineage_6000_by_36_metadata_growth_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "daily_bin"
    root.mkdir()
    objects: dict[str, bytes] = {}

    def memory_seal(_root: Path, relative: str, value: dict) -> LineageRef:
        payload = canonical_json_bytes(value)
        assert len(payload) <= MAX_LINEAGE_OBJECT_BYTES
        existing = objects.setdefault(relative, payload)
        if existing != payload:
            raise CanonicalLineageError("memory lineage object conflict")
        sha = hashlib.sha256(payload).hexdigest()
        return LineageRef(
            relative,
            len(payload),
            sha,
            digest_named_fields(
                lineage_module.CANONICAL_LINEAGE_REF_SCHEMA,
                {"relative_path": relative, "size_bytes": len(payload), "sha256": sha},
            ),
        )

    def memory_read_ref(_root: Path, ref: LineageRef, *, expected_schema: str):
        payload = objects[ref.relative_path]
        assert hashlib.sha256(payload).hexdigest() == ref.sha256
        value = json.loads(payload)
        assert value["schema_version"] == expected_schema
        return value

    def memory_read_path(_root: Path, relative: str, *, expected_schema: str):
        value = json.loads(objects[relative])
        assert value["schema_version"] == expected_schema
        return value

    monkeypatch.setattr(lineage_module, "_seal_json", memory_seal)
    monkeypatch.setattr(lineage_module, "_read_json_ref", memory_read_ref)
    monkeypatch.setattr(lineage_module, "_read_json_path", memory_read_path)

    codes = tuple(_code(value) for value in range(1, 6001))
    current = [_segment(value, 1) for value in range(1, 6001)]
    result = write_genesis(
        root,
        dataset="daily_bin",
        ordered_fields=FIELDS,
        segments=current,
        cutoff="2026-01-31",
        mutation_identity=_sha("genesis"),
    )
    monthly_bytes = [sum(len(value) for value in objects.values())]
    prior_total = monthly_bytes[0]
    for month in range(2, 37):
        result = write_transition_updates(
            root,
            dataset="daily_bin",
            ordered_fields=FIELDS,
            baseline_descriptor=result.descriptor,
            updates=[
                {
                    "instrument": _code(value),
                    "mode": "APPEND",
                    "segments": [_segment(value, month)],
                }
                for value in range(1, 6001)
            ],
            cutoff=_segment(1, month)["end"][:10],
            action="INCREMENTAL",
            mutation_identity=_sha(f"month:{month}"),
            scopes=[{"kind": "monthly_tail_extension", "new_months": [str(month)]}],
            planned_instruments=codes,
        )
        total = sum(len(value) for value in objects.values())
        monthly_bytes.append(total - prior_total)
        prior_total = total

    assert len(canonical_json_bytes(result.descriptor)) <= MAX_LINEAGE_DESCRIPTOR_BYTES
    assert max(len(value) for value in objects.values()) <= MAX_LINEAGE_OBJECT_BYTES
    assert max(monthly_bytes[-6:]) <= int(min(monthly_bytes[1:7]) * 1.10)
    assert sum(monthly_bytes) < 6000 * 36 * 1800
    assert len(instrument_summaries(root, result.descriptor)) == 6000
