import ast
from pathlib import Path


def test_position_timing_has_no_order_scheduler_or_shared_registry_write_path() -> None:
    package = Path("backend/services/position_timing")
    text = "\n".join(path.read_text(encoding="utf-8") for path in package.glob("*.py"))
    forbidden = (
        "ALTER TABLE",
        "CREATE TABLE",
        "DELETE FROM",
        "DROP TABLE",
        "INSERT INTO",
        "UPDATE market.",
        "create_order",
        "place_order",
        "submit_order",
        "portfolio_scheduler",
        "smart_monitor",
        "miniqmt_execution_runtime",
        "AdvisoryResearchTrialRegistryV1",
        "generate_current_route(",
    )
    for token in forbidden:
        assert token not in text


def test_existing_business_modules_do_not_reverse_import_position_timing() -> None:
    allowed = {
        Path("backend/main.py"),
        Path("backend/routers/position_timing.py"),
    }
    reverse_imports = []
    for root in (Path("backend/services"), Path("backend/routers")):
        for path in root.rglob("*.py"):
            if path in allowed or "position_timing" in path.parts:
                continue
            source = path.read_text(encoding="utf-8-sig")
            if "position_timing" not in source:
                continue
            tree = ast.parse(source, filename=str(path))
            imported_names = []
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    imported_names.append(node.module or "")
                elif isinstance(node, ast.Import):
                    imported_names.extend(alias.name for alias in node.names)
            if any("position_timing" in name for name in imported_names):
                reverse_imports.append(path.as_posix())
    assert reverse_imports == []


def test_composition_root_has_only_thin_position_timing_router_wiring() -> None:
    source = Path("backend/main.py").read_text(encoding="utf-8-sig")
    assert "position_timing," in source
    assert 'app.include_router(position_timing.router, prefix="/api/v1")' in source
    assert source.count("position_timing") == 2
