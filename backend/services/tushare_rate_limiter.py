"""Token-bucket rate limiter for Tushare API calls.

Thread-safe, per-API instances via get_limiter() factory.
Each API gets its own independent rate limit based on DatasetSpec.rate_per_minute.
"""
from __future__ import annotations

import threading
import time
from typing import Dict


class TushareRateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, rate_per_minute: int = 180):
        self._rate = max(1, rate_per_minute)
        self._interval = 60.0 / self._rate  # seconds per token
        self._tokens = float(self._rate)
        self._max_tokens = float(self._rate)
        self._last_refill = time.monotonic()
        self._cond = threading.Condition(threading.Lock())

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed / self._interval
        if new_tokens > 0:
            self._tokens = min(self._max_tokens, self._tokens + new_tokens)
            self._last_refill = now

    def acquire(self, cost: int = 1) -> None:
        """Block until *cost* tokens are available, then consume them."""
        with self._cond:
            while True:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                # Wait for enough time to accumulate the needed tokens
                deficit = cost - self._tokens
                wait = deficit * self._interval
                self._cond.wait(timeout=max(0.01, wait))

    @property
    def rate_per_minute(self) -> int:
        return self._rate


# ---------------------------------------------------------------------------
# Per-API limiter factory
# ---------------------------------------------------------------------------

_per_api_limiters: Dict[str, TushareRateLimiter] = {}
_per_api_lock = threading.Lock()


def get_limiter(api_name: str, rate_per_minute: int = 500) -> TushareRateLimiter:
    """获取或创建指定 API 的独立限流器."""
    with _per_api_lock:
        if api_name not in _per_api_limiters:
            _per_api_limiters[api_name] = TushareRateLimiter(rate_per_minute=rate_per_minute)
        return _per_api_limiters[api_name]
