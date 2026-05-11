"""trading_core SimGateway facade.

Phase 2 T5 (Lead 2026-05-09): Engine §6.2 names "vnpy SimGateway 撮合" as the
Paper Adapter's matching layer. Rather than pulling in the vnpy app layer
(handoff §8 禁区), this package exposes a vnpy-style facade over the
``paper_trading_v2.broker.LocalSimBackend`` already validated under Task #20.

The facade exists so that:

  * the daemon-side runner / future Paper Adapter can call a stable
    ``send_order(intent) -> handle`` / ``cancel(handle)`` / ``query_status(handle)``
    surface that is naming-compatible with vnpy ``BaseGateway`` (lower-case
    method names, gateway_name attribute) without forcing a vnpy import;
  * a future drop-in vnpy_xt gateway can replace this facade with no
    Engine-level code change;
  * tests can target the facade directly without reaching into broker
    internals.

This package is **not** a vnpy gateway. It does not subclass ``BaseGateway``,
does not run an ``EventEngine``, and does not import ``vnpy`` at module load.
"""

from .gateway import (
    SimGateway,
    SimGatewayConnectError,
    SimGatewayConnectionState,
)

__all__ = [
    "SimGateway",
    "SimGatewayConnectError",
    "SimGatewayConnectionState",
]
