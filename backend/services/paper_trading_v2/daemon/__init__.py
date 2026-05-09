"""Paper Trading v2 daemon-side: event log + sim runner.

Phase 2 T5 deliverable. Lives below paper_trading_v2 because it consumes
``broker.LocalSimBackend`` and ``StrategyPackageManifest``; the trading_core
package stays Engine-agnostic.

The SimGateway facade is in ``trading_core.sim_gateway`` and is reused here
without modification.
"""

from .event_log import (
    PAPER_DAEMON_EVENT_TYPE_NAMES,
    PAPER_DAEMON_SOURCE_SYSTEM,
    DaemonEventLog,
    DaemonEventRecord,
    DaemonEventType,
)
from .sim_runner import PaperV2SimRunner, SimRunResult

__all__ = [
    "PAPER_DAEMON_EVENT_TYPE_NAMES",
    "PAPER_DAEMON_SOURCE_SYSTEM",
    "DaemonEventLog",
    "DaemonEventRecord",
    "DaemonEventType",
    "PaperV2SimRunner",
    "SimRunResult",
]
