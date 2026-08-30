from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SERVICES = ROOT / "services"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            result.add(node.module or "")
        elif isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
    return result


def test_new_layers_never_import_legacy_paper_product() -> None:
    for package in ("simulation_data", "simulation_signal", "simulation_execution"):
        for path in (SERVICES / package).glob("*.py"):
            assert not any(module.startswith("backend.services.paper_trading_v2") for module in _imports(path)), path


def test_layer_direction_is_one_way() -> None:
    forbidden = {
        "simulation_data": (
            "backend.services.simulation_signal",
            "backend.services.simulation_execution",
            "backend.services.simulation_runtime",
        ),
        "simulation_signal": ("backend.services.simulation_execution", "backend.services.simulation_runtime"),
        "simulation_execution": ("backend.services.simulation_runtime",),
    }
    for package, prefixes in forbidden.items():
        for path in (SERVICES / package).glob("*.py"):
            imports = _imports(path)
            assert not any(module.startswith(prefixes) for module in imports), path


def test_stage_a_retired_modules_are_physically_absent() -> None:
    assert not (SERVICES / "paper_trading_v2" / "broker" / "base.py").exists()
    assert not (SERVICES / "paper_trading_v2" / "day_features.py").exists()
    assert not (SERVICES / "strategy_package" / "multi_alpha_paper_admission.py").exists()
    assert not (SERVICES / "strategy_package" / "multi_alpha_paper_dry_run.py").exists()


def test_non_paper_modules_do_not_import_retired_shared_contract_owners() -> None:
    forbidden = {
        "backend.services.paper_trading_v2.broker.base",
        "backend.services.paper_trading_v2.market_data",
    }
    offenders: list[str] = []
    for path in SERVICES.rglob("*.py"):
        if "paper_trading_v2" in path.parts:
            continue
        if forbidden.intersection(_imports(path)):
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []


def test_stage_a_layers_open_no_product_route_or_economic_writer() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for package in ("simulation_data", "simulation_signal", "simulation_execution")
        for path in (SERVICES / package).glob("*.py")
    )
    assert "APIRouter(" not in source
    for fragment in (
        "INSERT INTO paper_v2.order",
        "INSERT INTO paper_v2.fill",
        "INSERT INTO paper_v2.cash",
        "INSERT INTO paper_v2.position",
        "UPDATE paper_v2.order",
    ):
        assert fragment not in source


def test_signal_owner_has_no_portfolio_or_session_creation_method() -> None:
    source = (SERVICES / "simulation_signal" / "strategy_package_selection.py").read_text(encoding="utf-8")
    assert "create_portfolio" not in source
    assert "create_session" not in source
    assert "paper_portfolio_service" not in source
