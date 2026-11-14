import ast
from pathlib import Path

NATURAL_READONLY_MODULES = {
    "adverse_wait_diagnostics": {"DiagnosticSpec", "source_preflight"},
    "continuous_results": {"PeriodAnalysisSpec", "period_source_preflight"},
}


def _approved_localsim_sdk_import(path: Path, module: str, names=()) -> bool:
    # Intraday blueprint §2 explicitly permits LocalSIM -> pure SDK. Daily
    # timing internals and every other business consumer remain isolated.
    if not path.is_relative_to(Path("backend/services/simulation_execution/localsim")):
        return False
    sdk = "backend.services.position_timing.intraday"
    research_symbols = {
        sdk + ".wait_control": {"CONTROL_ID", "WaitControlSpec", "WaitControlState", "WaitControlDecision", "evaluate_wait_control"},
        sdk + ".wait_control_results": {"inspect_plan", "economic_opening", "WaitBudgetAnalysisSpec", "WaitBudgetFacts", "WaitBudgetAccount"},
        sdk + ".wait_control_history": {"inspect_history_plan", "HistoryAnalysisSpec"},
        sdk + ".adverse_wait_control": {"CONTROL_ID", "AdverseWaitSpec", "AdverseWaitState", "AdverseWaitDecision", "ReleaseEvidence", "evaluate_adverse_wait"},
        sdk + ".adverse_wait_research": {"inspect_adverse_plan"},
    }
    if module in research_symbols:
        return bool(names) and set(names) <= research_symbols[module]
    # Approved period continuation seams are file-and-symbol specific. In
    # particular, permitting the typed error does not expose IntradayStore.
    period_seams = {
        ("period_continuation.py", sdk + ".store"): {"IntradayError"},
        ("period_research.py", sdk + ".continuous_results"): {"PeriodAnalysisSpec", "period_source_preflight"},
        ("period_research.py", sdk + ".period_context"): {"load_period_context"},
        ("period_research.py", sdk + ".price_plan_campaign"): {"inspect"},
        ("timing_campaign.py", sdk + ".period_context"): {"load_period_context"},
        ("natural_period_preparation.py", sdk): set(NATURAL_READONLY_MODULES),
        ("natural_period_research.py", sdk + ".natural_period_context"): {"load_context"},
        ("natural_period_research.py", sdk + ".natural_period_prices"): {"inspect"},
        ("dispatch_capacity_research.py", sdk + ".dispatch_coupling_research"): {"inspect_dispatch_capacity_plan"},
    }
    relative = path.relative_to(Path("backend/services/simulation_execution/localsim")).as_posix()
    if (relative, module) in period_seams:
        return bool(names) and set(names) <= period_seams[(relative, module)]
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
    assert _approved_localsim_sdk_import(consumer, sdk + ".wait_control", ["evaluate_wait_control", "WaitControlState"])
    assert _approved_localsim_sdk_import(consumer, sdk + ".wait_control_results", ["inspect_plan", "WaitBudgetFacts"])
    for denied in ("prepare", "analyze", "source_preflight", "*"):
        assert not _approved_localsim_sdk_import(consumer, sdk + ".wait_control_results", [denied])
        assert not _approved_localsim_sdk_import(consumer, sdk + ".wait_control_history", [denied])
    assert _approved_localsim_sdk_import(consumer, sdk + ".wait_control_history", ["inspect_history_plan", "HistoryAnalysisSpec"])
    assert _approved_localsim_sdk_import(consumer, sdk + ".adverse_wait_control", ["evaluate_adverse_wait", "AdverseWaitState"])
    assert _approved_localsim_sdk_import(consumer, sdk + ".adverse_wait_research", ["inspect_adverse_plan"])
    assert not _approved_localsim_sdk_import(consumer, sdk + ".adverse_wait_research", ["prepare"])
    assert not _approved_localsim_sdk_import(Path("backend/services/advisory/service.py"), sdk + ".adverse_wait_control", ["evaluate_adverse_wait"])
    assert not _approved_localsim_sdk_import(Path("backend/services/advisory/service.py"), sdk + ".wait_control_history", ["inspect_history_plan"])
    assert not _approved_localsim_sdk_import(Path("backend/services/advisory/service.py"), sdk + ".wait_control", ["evaluate_wait_control"])
    period = consumer.with_name("period_research.py")
    assert _approved_localsim_sdk_import(period, sdk + ".continuous_results", ["period_source_preflight"])
    assert not _approved_localsim_sdk_import(period, sdk + ".continuous_results", ["analyze_period"])
    assert not _approved_localsim_sdk_import(consumer, sdk + ".continuous_results", ["period_source_preflight"])
    continuation = consumer.with_name("period_continuation.py")
    assert _approved_localsim_sdk_import(continuation, sdk + ".store", ["IntradayError"])
    assert not _approved_localsim_sdk_import(continuation, sdk + ".store", ["IntradayStore"])
    assert not _approved_localsim_sdk_import(continuation, sdk + ".store", ["*"])
    assert not _approved_localsim_sdk_import(period.parent/"other"/period.name, sdk + ".period_context", ["load_period_context"])
    assert _approved_localsim_sdk_import(consumer.with_name("timing_campaign.py"), sdk + ".period_context", ["load_period_context"])


def test_composition_root_has_only_thin_position_timing_router_wiring() -> None:
    source = Path("backend/main.py").read_text(encoding="utf-8-sig")
    assert "position_timing," in source
    assert 'app.include_router(position_timing.router, prefix="/api/v1")' in source
    assert source.count("position_timing") == 2


def test_natural_preparation_exception_is_exact_and_readonly():
    sdk = "backend.services.position_timing.intraday"
    consumer = Path("backend/services/simulation_execution/localsim/natural_period_preparation.py")
    modules = set(NATURAL_READONLY_MODULES)
    assert _approved_localsim_sdk_import(consumer, sdk, modules)
    for path in (consumer.with_name("timing.py"), consumer.parent/"nested"/consumer.name,
                 Path("backend/services/advisory/natural_period_preparation.py")):
        assert not _approved_localsim_sdk_import(path, sdk, modules)
    for denied in ("*", "analyze", "prepare", "store", "natural_period_results"):
        assert not _approved_localsim_sdk_import(consumer, sdk, [denied])
    tree = ast.parse(consumer.read_text(encoding="utf-8-sig"))
    aliases = {alias.asname or alias.name: alias.name for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == sdk for alias in node.names}
    assert set(aliases.values()) == modules
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in aliases:
            assert node.attr in NATURAL_READONLY_MODULES[aliases[node.value.id]]


def test_natural_replay_exception_is_exact_and_readonly():
    sdk = "backend.services.position_timing.intraday"
    consumer = Path("backend/services/simulation_execution/localsim/natural_period_research.py")
    for suffix, symbol in (("natural_period_context", "load_context"), ("natural_period_prices", "inspect")):
        module = sdk + "." + suffix
        assert _approved_localsim_sdk_import(consumer, module, [symbol])
        for path in (consumer.with_name("timing.py"), consumer.with_name("natural_period_preparation.py"),
                     consumer.parent/"nested"/consumer.name,
                     Path("backend/services/advisory/natural_period_research.py")):
            assert not _approved_localsim_sdk_import(path, module, [symbol])
        for names in ((), ["*"], ["_" + symbol], ["build"], ["analyze"], [symbol, "pins"],
                      ["inspect" if symbol == "load_context" else "load_context"]):
            assert not _approved_localsim_sdk_import(consumer, module, names)
        assert not _approved_localsim_sdk_import(consumer, sdk, [suffix])
    assert not _approved_localsim_sdk_import(consumer, sdk + ".natural_period_results", ["analyze"])


def test_dispatch_capacity_exception_is_exact_and_readonly():
    root = Path("backend/services/simulation_execution/localsim")
    consumer = root / "dispatch_capacity_research.py"
    sdk = "backend.services.position_timing.intraday"
    module, symbol = sdk + ".dispatch_coupling_research", "inspect_dispatch_capacity_plan"
    assert _approved_localsim_sdk_import(consumer, module, [symbol])
    for path in (root / "timing.py", root / "other" / consumer.name,
                 Path("backend/services/advisory") / consumer.name):
        assert not _approved_localsim_sdk_import(path, module, [symbol])
    for names in ((), ["*"], ["prepare"], ["funding_boundary"], [symbol, "main"]):
        assert not _approved_localsim_sdk_import(consumer, module, names)
    assert not _approved_localsim_sdk_import(consumer, sdk, ["dispatch_coupling_research"])
    assert not _approved_localsim_sdk_import(consumer, sdk + ".dispatch_coupling_results", ["analyze"])
