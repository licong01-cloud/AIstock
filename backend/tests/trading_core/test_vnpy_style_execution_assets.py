from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.execution_algos import ALGO_REGISTRY
from backend.execution_algos.vnpy_style import (
    VNPY_STYLE_ASSETS,
    VnpyActionType,
    VnpyAlgoConfig,
    VnpyDirection,
    VnpyOrderUpdate,
    VnpyTick,
    VnpyTradeUpdate,
    create_vnpy_style_core,
)
from backend.execution_algos.vnpy_style.best_limit_core import BestLimitMiniQMTCore
from backend.execution_algos.vnpy_style.sniper_core import SniperMiniQMTCore
from backend.execution_algos.vnpy_style.twap_lite_core import TwapLiteMiniQMTCore
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.execution_algo_capabilities import get_execution_algo_capability
from backend.services.trading_core.errors import RuntimeConfigInvalidError


def _tick(*, bid=9.9, bid_vol=500, ask=10.0, ask_vol=400) -> VnpyTick:
    return VnpyTick(
        symbol="000001.SZ",
        datetime=datetime(2026, 5, 29, 9, 30, tzinfo=UTC),
        bid_price_1=bid,
        bid_volume_1=bid_vol,
        ask_price_1=ask,
        ask_volume_1=ask_vol,
    )


def _config(algo_code: str, *, direction=VnpyDirection.LONG, price=10.0, volume=1000, setting=None) -> VnpyAlgoConfig:
    return VnpyAlgoConfig(
        algo_code=algo_code,
        symbol="000001.SZ",
        direction=direction,
        price=price,
        volume=volume,
        setting=setting or {},
        min_volume=100,
        volume_increment=100,
    )


def test_sniper_long_submits_only_when_ask_crosses_limit_and_caps_at_ask_volume() -> None:
    algo = SniperMiniQMTCore(_config("SNIPER_MINIQMT", price=10.0, volume=1000))
    algo.start()
    assert not [a for a in algo.update_tick(_tick(ask=10.01)) if a.action_type == VnpyActionType.SUBMIT]

    actions = algo.update_tick(_tick(ask=9.99, ask_vol=250))

    submit = next(a for a in actions if a.action_type == VnpyActionType.SUBMIT)
    assert submit.price == 10.0
    assert submit.volume == 200
    assert algo.vt_orderid == submit.vt_orderid


def test_sniper_active_order_cancels_before_new_submit() -> None:
    algo = SniperMiniQMTCore(_config("SNIPER_MINIQMT", price=10.0, volume=1000))
    algo.start()
    submit = next(a for a in algo.update_tick(_tick(ask=9.99)) if a.action_type == VnpyActionType.SUBMIT)
    algo.update_order(VnpyOrderUpdate(vt_orderid=submit.vt_orderid or "", active=True))

    actions = algo.update_tick(_tick(ask=9.98))

    assert any(a.action_type == VnpyActionType.CANCEL for a in actions)
    assert not any(a.action_type == VnpyActionType.SUBMIT for a in actions)


def test_sniper_short_uses_bid_crossing_condition() -> None:
    algo = SniperMiniQMTCore(_config("SNIPER_MINIQMT", direction=VnpyDirection.SHORT, price=10.0, volume=1000))
    algo.start()
    assert not [a for a in algo.update_tick(_tick(bid=9.99)) if a.action_type == VnpyActionType.SUBMIT]

    submit = next(a for a in algo.update_tick(_tick(bid=10.01, bid_vol=360)) if a.action_type == VnpyActionType.SUBMIT)

    assert submit.price == 10.0
    assert submit.volume == 300


def test_best_limit_long_submits_at_bid_and_cancels_when_bid_changes() -> None:
    algo = BestLimitMiniQMTCore(
        _config("BEST_LIMIT_MINIQMT", setting={"min_volume": 100, "max_volume": 500}),
        random_volume_provider=lambda _min, _max: 350,
    )
    algo.start()
    submit = next(a for a in algo.update_tick(_tick(bid=9.88)) if a.action_type == VnpyActionType.SUBMIT)
    assert submit.price == 9.88
    assert submit.volume == 300
    algo.update_order(VnpyOrderUpdate(vt_orderid=submit.vt_orderid or "", active=True, price=submit.price))

    actions = algo.update_tick(_tick(bid=9.89))

    assert any(a.action_type == VnpyActionType.CANCEL for a in actions)


def test_best_limit_short_submits_at_ask_and_validates_volume_window() -> None:
    algo = BestLimitMiniQMTCore(
        _config("BEST_LIMIT_MINIQMT", direction=VnpyDirection.SHORT, setting={"min_volume": 100, "max_volume": 500}),
        random_volume_provider=lambda _min, _max: 260,
    )
    algo.start()

    submit = next(a for a in algo.update_tick(_tick(ask=10.12)) if a.action_type == VnpyActionType.SUBMIT)

    assert submit.price == 10.12
    assert submit.volume == 200
    with pytest.raises(Exception):
        BestLimitMiniQMTCore(_config("BEST_LIMIT_MINIQMT", setting={"min_volume": 500, "max_volume": 100}))


def test_twap_lite_timer_waits_interval_cancels_before_slice_and_finishes_on_time() -> None:
    algo = TwapLiteMiniQMTCore(_config("TWAP_LITE_MINIQMT", price=10.0, volume=1000, setting={"time": 4, "interval": 2}))
    algo.start()
    algo.update_tick(_tick(ask=9.99))
    assert not [a for a in algo.update_timer() if a.action_type == VnpyActionType.SUBMIT]
    actions = algo.update_timer()
    submit = next(a for a in actions if a.action_type == VnpyActionType.SUBMIT)
    assert submit.volume == 500
    algo.update_order(VnpyOrderUpdate(vt_orderid=submit.vt_orderid or "", active=True))
    assert not [a for a in algo.update_timer() if a.action_type == VnpyActionType.SUBMIT]
    actions = algo.update_timer()
    assert any(a.action_type == VnpyActionType.FINISH for a in actions)


def test_template_update_order_trade_and_finish_match_vnpy_lifecycle() -> None:
    algo = SniperMiniQMTCore(_config("SNIPER_MINIQMT", price=10.0, volume=1000))
    algo.start()
    submit = next(a for a in algo.update_tick(_tick(ask=9.99)) if a.action_type == VnpyActionType.SUBMIT)
    algo.update_order(VnpyOrderUpdate(vt_orderid=submit.vt_orderid or "", active=True))
    assert submit.vt_orderid in algo.active_orders
    algo.update_order(VnpyOrderUpdate(vt_orderid=submit.vt_orderid or "", active=False))
    assert submit.vt_orderid not in algo.active_orders
    algo.update_trade(VnpyTradeUpdate(vt_orderid=submit.vt_orderid or "", volume=1000, price=9.99))
    assert algo.traded == 1000
    assert algo.status.value == "finished"


def test_vnpy_style_assets_are_registered_and_declared_live_supported() -> None:
    for code in ("SNIPER_MINIQMT", "BEST_LIMIT_MINIQMT", "TWAP_LITE_MINIQMT"):
        assert code in VNPY_STYLE_ASSETS
        assert code in ALGO_REGISTRY
        cap = get_execution_algo_capability(code)
        assert cap.live_supported is True
        assert cap.live_step_mode.startswith("miniqmt_event_driven")


def test_strategy_package_validator_fails_invalid_vnpy_style_config() -> None:
    validator = StrategyPackageValidator()
    validator.validate_execution_policy_for_paper(
        package_id="pkg",
        policy_json={"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 100, "max_volume": 300}},
        instantiate_runtime=False,
        require_runtime_assets=False,
    )
    with pytest.raises(RuntimeConfigInvalidError):
        validator.validate_execution_policy_for_paper(
            package_id="pkg",
            policy_json={"algo_code": "BEST_LIMIT_MINIQMT", "algo_config": {"min_volume": 500, "max_volume": 100}},
            instantiate_runtime=False,
            require_runtime_assets=False,
        )


def test_vnpy_style_core_import_boundary_has_no_runtime_coupling() -> None:
    forbidden = (
        "fastapi",
        "backend.db",
        "backend.infra.qmt_client",
        "backend.services.paper_trading_v2",
        "vnpy",
        "xtquant",
    )
    for path in Path("backend/execution_algos/vnpy_style").glob("*.py"):
        if path.name == "legacy_adapter.py":
            continue
        source = path.read_text(encoding="utf-8")
        import_lines = "\n".join(line for line in source.splitlines() if line.startswith(("import ", "from ")))
        for token in forbidden:
            assert token not in import_lines, f"{path} imports forbidden runtime token {token}"


def test_create_vnpy_style_core_returns_expected_core_classes() -> None:
    assert isinstance(
        create_vnpy_style_core(algo_code="SNIPER_MINIQMT", symbol="000001.SZ", side="BUY", price=10, volume=100),
        SniperMiniQMTCore,
    )
    assert isinstance(
        create_vnpy_style_core(
            algo_code="BEST_LIMIT_MINIQMT",
            symbol="000001.SZ",
            side="BUY",
            price=10,
            volume=100,
            algo_config={"min_volume": 100, "max_volume": 100},
        ),
        BestLimitMiniQMTCore,
    )
    assert isinstance(
        create_vnpy_style_core(
            algo_code="TWAP_LITE_MINIQMT",
            symbol="000001.SZ",
            side="BUY",
            price=10,
            volume=100,
            algo_config={"time": 60, "interval": 30},
        ),
        TwapLiteMiniQMTCore,
    )
