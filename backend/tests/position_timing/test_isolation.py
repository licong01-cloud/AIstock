import ast
from pathlib import Path


def _approved_localsim_sdk_import(path: Path, module: str, names=()) -> bool:
    # Intraday blueprint §2 explicitly permits LocalSIM -> pure SDK. Daily
    # timing internals and every other business consumer remain isolated.
    if not path.is_relative_to(Path("backend/services/simulation_execution/localsim")):
        return False
    sdk = "backend.services.position_timing.intraday"
    if module == sdk:
        return bool(names) and set(names) <= {"evaluate", "research"}
    return module in {sdk + "." + suffix for suffix in (
        "contracts", "inputs", "replay", "research", "research_inputs", "research_results", "price_plans",
    )}


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
        "generate_current_route(",
    )
    for token in forbidden:
        assert token not in text

    pipeline = (package / "learnability_pipeline.py").read_text(encoding="utf-8")
    assert "AdvisoryResearchTrialRegistryV1(expected_registry).append_batch" in pipeline
    assert '"research_registry" / "timing_trial_registry_v1.jsonl"' in pipeline
    assert "AdvisoryResearchTrialRegistryV1(historical_registry).append" not in pipeline
    assert "generate_current_route(" not in pipeline


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
            imports = []
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    imports.append((module, [alias.name for alias in node.names]))
                    if module == "backend.services":
                        imports.extend((module + "." + alias.name, []) for alias in node.names)
                elif isinstance(node, ast.Import):
                    imports.extend((alias.name, []) for alias in node.names)
            if any("position_timing" in module and not _approved_localsim_sdk_import(path, module, names)
                   for module, names in imports):
                reverse_imports.append(path.as_posix())
    assert reverse_imports == []


def test_sdk_consumer_exception_does_not_open_other_consumers_or_internal_modules():
    consumer = Path("backend/services/simulation_execution/localsim/timing.py")
    sdk = "backend.services.position_timing.intraday"
    assert _approved_localsim_sdk_import(consumer, sdk, ["evaluate"])
    assert _approved_localsim_sdk_import(consumer, sdk + ".research_inputs", ["CandidateReader"])
    for path in (Path("backend/services/quantevolver/runner.py"), Path("backend/services/advisory/service.py")):
        assert not _approved_localsim_sdk_import(path, sdk + ".research_inputs", ["CandidateReader"])
    assert not _approved_localsim_sdk_import(consumer, "backend.services.position_timing.service", ["Service"])
    assert not _approved_localsim_sdk_import(consumer, sdk + ".store", ["IntradayStore"])
    assert not _approved_localsim_sdk_import(consumer, sdk, ["store"])


def test_composition_root_has_only_thin_position_timing_router_wiring() -> None:
    source = Path("backend/main.py").read_text(encoding="utf-8-sig")
    assert "position_timing," in source
    assert 'app.include_router(position_timing.router, prefix="/api/v1")' in source
    assert source.count("position_timing") == 2
