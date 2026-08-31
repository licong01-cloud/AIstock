from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_phase0b import producer_closure


def test_producer_closure_is_ordered_and_rotates_with_member_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path.resolve()
    first = root / "a.py"
    second = root / "b.py"
    first.write_text("a = 1\n", encoding="utf-8")
    second.write_text("b = 1\n", encoding="utf-8")
    monkeypatch.setattr(producer_closure, "PRODUCER_CLOSURE_PATHS", ("a.py", "b.py"))

    initial = producer_closure.phase0b_producer_code_closure_hash(repository_root=root)
    second.write_text("b = 2\n", encoding="utf-8")
    changed = producer_closure.phase0b_producer_code_closure_hash(repository_root=root)

    assert initial != changed
