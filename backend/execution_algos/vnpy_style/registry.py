"""Registry for vn.py-style execution strategy assets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .attribution import AISTOCK_ASSET_VERSION, source_attribution
from .base import VnpyAlgoTemplate, VnpyStyleConfigError
from .best_limit_core import BestLimitMiniQMTCore
from .models import VnpyAlgoConfig, VnpyDirection
from .sniper_core import SniperMiniQMTCore
from .twap_lite_core import TwapLiteMiniQMTCore


@dataclass(frozen=True)
class VnpyStyleAssetSpec:
    algo_code: str
    core_class: type[VnpyAlgoTemplate]
    version: str
    source_file: str
    default_config: dict[str, Any]
    required_config_keys: tuple[str, ...] = ()
    optional_config_keys: tuple[str, ...] = ()
    live_supported: bool = True
    qe_ready: bool = True

    def metadata(self) -> dict[str, Any]:
        return {
            "algo_code": self.algo_code,
            "asset_version": self.version,
            "source_file": self.source_file,
            "default_config": dict(self.default_config),
            "required_config_keys": list(self.required_config_keys),
            "optional_config_keys": list(self.optional_config_keys),
            "live_supported": self.live_supported,
            "qe_ready": self.qe_ready,
            "source_attribution": source_attribution(self.algo_code),
        }


VNPY_STYLE_ASSETS: dict[str, VnpyStyleAssetSpec] = {
    "SNIPER_MINIQMT": VnpyStyleAssetSpec(
        algo_code="SNIPER_MINIQMT",
        core_class=SniperMiniQMTCore,
        version=AISTOCK_ASSET_VERSION,
        source_file="vnpy_algotrading/algos/sniper_algo.py",
        default_config={"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
        optional_config_keys=("time_in_force_seconds", "max_cancel_replace", "timer_iterations"),
    ),
    "BEST_LIMIT_MINIQMT": VnpyStyleAssetSpec(
        algo_code="BEST_LIMIT_MINIQMT",
        core_class=BestLimitMiniQMTCore,
        version=AISTOCK_ASSET_VERSION,
        source_file="vnpy_algotrading/algos/best_limit_algo.py",
        default_config={"min_volume": 100, "max_volume": 1000},
        required_config_keys=("min_volume", "max_volume"),
        optional_config_keys=("time_in_force_seconds", "max_cancel_replace", "timer_iterations"),
    ),
    "TWAP_LITE_MINIQMT": VnpyStyleAssetSpec(
        algo_code="TWAP_LITE_MINIQMT",
        core_class=TwapLiteMiniQMTCore,
        version=AISTOCK_ASSET_VERSION,
        source_file="vnpy_algotrading/algos/twap_algo.py",
        default_config={"time": 600, "interval": 60},
        required_config_keys=("time", "interval"),
        optional_config_keys=("duration_seconds", "interval_seconds", "timer_iterations"),
    ),
}


def is_vnpy_style_algo(algo_code: Any) -> bool:
    return str(algo_code or "").strip().upper() in VNPY_STYLE_ASSETS


def get_vnpy_style_asset(algo_code: Any) -> VnpyStyleAssetSpec:
    normalized = str(algo_code or "").strip().upper()
    try:
        return VNPY_STYLE_ASSETS[normalized]
    except KeyError as exc:
        raise VnpyStyleConfigError(f"unsupported vn.py-style algo_code: {normalized}") from exc


def create_vnpy_style_core(
    *,
    algo_code: Any,
    symbol: str,
    side: str,
    price: float,
    volume: int,
    algo_config: dict[str, Any] | None = None,
    algo_name: str | None = None,
    min_volume: int = 100,
    volume_increment: int = 100,
    random_volume_provider: Callable[[int, int], float] | None = None,
) -> VnpyAlgoTemplate:
    spec = get_vnpy_style_asset(algo_code)
    setting = _normalized_setting(spec, algo_config or {})
    direction = _direction_from_side(side)
    config = VnpyAlgoConfig(
        algo_code=spec.algo_code,
        symbol=str(symbol),
        direction=direction,
        price=float(price),
        volume=int(volume),
        setting=setting,
        algo_name=algo_name,
        min_volume=int(min_volume),
        volume_increment=int(volume_increment),
    )
    if spec.algo_code == "BEST_LIMIT_MINIQMT":
        return spec.core_class(config, random_volume_provider=random_volume_provider)  # type: ignore[misc]
    return spec.core_class(config)


def validate_vnpy_style_config(algo_code: Any, algo_config: dict[str, Any] | None) -> dict[str, Any]:
    spec = get_vnpy_style_asset(algo_code)
    return _normalized_setting(spec, algo_config or {})


def _normalized_setting(spec: VnpyStyleAssetSpec, algo_config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(algo_config, dict):
        raise VnpyStyleConfigError(f"{spec.algo_code} algo_config must be an object")
    setting = dict(spec.default_config)
    setting.update(algo_config)
    if spec.algo_code == "SNIPER_MINIQMT":
        return setting
    if spec.algo_code == "BEST_LIMIT_MINIQMT":
        min_volume = _positive_int(setting.get("min_volume"), "min_volume", spec.algo_code)
        max_volume = _positive_int(setting.get("max_volume"), "max_volume", spec.algo_code)
        if max_volume < min_volume:
            raise VnpyStyleConfigError("BEST_LIMIT_MINIQMT requires max_volume >= min_volume")
        setting["min_volume"] = min_volume
        setting["max_volume"] = max_volume
        return setting
    if spec.algo_code == "TWAP_LITE_MINIQMT":
        if "time" not in setting and "duration_seconds" in setting:
            setting["time"] = setting["duration_seconds"]
        if "interval" not in setting and "interval_seconds" in setting:
            setting["interval"] = setting["interval_seconds"]
        time = _positive_int(setting.get("time"), "time", spec.algo_code)
        interval = _positive_int(setting.get("interval"), "interval", spec.algo_code)
        if time < interval:
            raise VnpyStyleConfigError("TWAP_LITE_MINIQMT requires time >= interval")
        setting["time"] = time
        setting["interval"] = interval
        return setting
    return setting


def _direction_from_side(side: str) -> VnpyDirection:
    normalized = str(side or "").strip().upper()
    if normalized == "BUY":
        return VnpyDirection.LONG
    if normalized == "SELL":
        return VnpyDirection.SHORT
    raise VnpyStyleConfigError(f"unsupported side for vn.py-style algo: {side}")


def _positive_int(value: Any, name: str, algo_code: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise VnpyStyleConfigError(f"{algo_code} requires integer {name}") from exc
    if parsed <= 0:
        raise VnpyStyleConfigError(f"{algo_code} requires {name} > 0")
    return parsed
