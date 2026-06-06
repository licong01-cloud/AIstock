from __future__ import annotations

import pytest

from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader


def test_realtime_loader_rejects_malformed_ts_code_before_db(monkeypatch) -> None:
    loader = RealtimeFactorDataLoader()
    called = {"fetch": False}

    def fail_if_fetch(*_args, **_kwargs):
        called["fetch"] = True
        raise AssertionError("DB fetch should not run for malformed ts_code")

    monkeypatch.setattr(loader, "_fetch_from_db", fail_if_fetch)

    with pytest.raises(ValueError) as exc_info:
        loader.load(
            ["603819.S2026-06-01T01:59:30.734977444Z"],
            "2026-06-01",
            "2026-06-01",
        )

    message = str(exc_info.value)
    assert "invalid ts_code values before SQL execution" in message
    assert "source=RealtimeFactorDataLoader.load" in message
    assert "603819.S2026-06-01T01:59:30.734977444Z" in message
    assert called["fetch"] is False


def test_realtime_loader_accepts_prefixed_ts_code() -> None:
    loader = RealtimeFactorDataLoader()

    assert loader._to_ts_code("SH600000") == "600000.SH"
