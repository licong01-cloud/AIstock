from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "aistock_legacy_inventory.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("aistock_legacy_inventory", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_root_python_is_review_candidate_not_safe_delete(tmp_path: Path) -> None:
    inventory = _load_module()
    root_module = tmp_path / "legacy_service.py"
    root_module.write_text("def run():\n    return 'ok'\n", encoding="utf-8")

    items = inventory.collect_inventory(tmp_path, ["legacy_service.py"])

    assert len(items) == 1
    assert items[0].category == "root_python_review"
    assert items[0].risk == "high"
    assert items[0].lifecycle_status == "delete_candidate"
    assert items[0].recommended_action == "confirm_imports_then_move_or_remove"


def test_referenced_root_python_has_low_confidence(tmp_path: Path) -> None:
    inventory = _load_module()
    root_module = tmp_path / "legacy_service.py"
    consumer = tmp_path / "backend" / "main.py"
    consumer.parent.mkdir(parents=True)
    root_module.write_text("def run():\n    return 'ok'\n", encoding="utf-8")
    consumer.write_text("import legacy_service\n\nlegacy_service.run()\n", encoding="utf-8")

    items = inventory.collect_inventory(tmp_path, ["legacy_service.py", "backend/main.py"])

    assert len(items) == 1
    assert items[0].references_found == 1
    assert items[0].reference_examples == ("backend/main.py",)
    assert items[0].confidence == "low"
    assert items[0].lifecycle_status == "deprecated"


def test_docs_root_markdown_is_legacy_doc_review(tmp_path: Path) -> None:
    inventory = _load_module()
    doc = tmp_path / "docs" / "old_design.md"
    doc.parent.mkdir()
    doc.write_text("# Old design\n", encoding="utf-8")

    items = inventory.collect_inventory(tmp_path, ["docs/old_design.md"])

    assert len(items) == 1
    assert items[0].category == "legacy_doc_review"
    assert "needs_doc_taxonomy" in items[0].signals


def test_verify_script_is_script_lifecycle_review(tmp_path: Path) -> None:
    inventory = _load_module()
    script = tmp_path / "scripts" / "verify_old_pipeline.py"
    script.parent.mkdir()
    script.write_text("print('verify')\n", encoding="utf-8")

    items = inventory.collect_inventory(tmp_path, ["scripts/verify_old_pipeline.py"])

    assert len(items) == 1
    assert items[0].category == "script_lifecycle_review"
    assert items[0].recommended_action == "move_to_debug_tools_or_remove_after_review"


def test_protected_paths_are_not_cleanup_candidates(tmp_path: Path) -> None:
    inventory = _load_module()
    protected = tmp_path / "qe_archive" / "artifacts" / "model.pkl"
    protected.parent.mkdir(parents=True)
    protected.write_bytes(b"not a real pickle")

    items = inventory.collect_inventory(tmp_path, ["qe_archive/artifacts/model.pkl"])

    assert items == []


def test_missing_worktree_file_is_marked_low_confidence(tmp_path: Path) -> None:
    inventory = _load_module()

    items = inventory.collect_inventory(tmp_path, ["docs/old_missing_doc.md"])

    assert len(items) == 1
    assert "missing_in_worktree" in items[0].signals
    assert items[0].confidence == "low"


def test_inventory_writes_json_and_markdown(tmp_path: Path) -> None:
    inventory = _load_module()
    script = tmp_path / "scripts" / "smoke_old_flow.py"
    script.parent.mkdir()
    script.write_text("print('smoke')\n", encoding="utf-8")
    items = inventory.collect_inventory(tmp_path, ["scripts/smoke_old_flow.py"])
    json_path = tmp_path / "inventory.json"
    md_path = tmp_path / "inventory.md"

    inventory.write_json(json_path, items=items, files_scanned=1, mode="unit_test")
    inventory.write_summary_md(md_path, items=items, files_scanned=1, mode="unit_test", max_items=10)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    summary = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == inventory.SCHEMA_VERSION
    assert payload["summary"]["total_items"] == 1
    assert "AIstock Legacy Inventory Baseline" in summary
    assert "not a deletion list" in summary
