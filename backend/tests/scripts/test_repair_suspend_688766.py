from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import repair_suspend_688766 as repair


def test_derive_plan_for_sparse_production_preimage() -> None:
    plan = repair.derive_plan(
        [
            ("2025-11-25", "S", None),
            ("2025-11-26", "R", None),
            ("2025-11-26", "S", "09:30-09:30"),
            ("2025-12-08", "S", None),
        ]
    )

    assert len(plan["insert_rows"]) == 8
    assert ("2025-12-09", "R", None) in plan["insert_rows"]
    assert plan["update_2025_11_26_s_to_full_day"] is True
    assert plan["delete_superseded_2025_11_26_r"] is True


def test_derive_plan_rejects_unexpected_preimage() -> None:
    with pytest.raises(repair.RepairError, match="unexpected suspend_d preimage"):
        repair.derive_plan([("2025-12-09", "S", None)])


def test_build_and_load_authority_is_content_bound_and_create_exclusive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    source = source_dir / "official.pdf"
    source.write_bytes(b"official-disclosure")
    spec = repair.DocumentSpec(
        filename=source.name,
        url="https://example.invalid/official.pdf",
        sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        size=source.stat().st_size,
        role="test_source",
    )
    output = tmp_path / "authority.json"

    payload = repair.build_authority(source_dir, output, documents=(spec,))
    loaded, observed_hash = repair.load_authority(output)

    assert loaded == payload
    assert observed_hash == hashlib.sha256(output.read_bytes()).hexdigest()
    assert output.read_bytes() == repair.canonical_json_bytes(json.loads(output.read_bytes()))
    with pytest.raises(FileExistsError):
        repair.build_authority(source_dir, output, documents=(spec,))


def test_load_authority_fails_when_frozen_source_changes(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    source = source_dir / "official.pdf"
    source.write_bytes(b"official-disclosure")
    spec = repair.DocumentSpec(
        filename=source.name,
        url="https://example.invalid/official.pdf",
        sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        size=source.stat().st_size,
        role="test_source",
    )
    output = tmp_path / "authority.json"
    repair.build_authority(source_dir, output, documents=(spec,))
    source.write_bytes(b"changed")

    with pytest.raises(repair.RepairError, match="frozen source document changed"):
        repair.load_authority(output)
