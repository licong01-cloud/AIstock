from __future__ import annotations

from types import SimpleNamespace

from backend.services.advisory_historical_range.service import ResponseBoundHistoricalRangeDispatcher


class _Background:
    def __init__(self) -> None:
        self.task = None

    def add_task(self, func, *args, **kwargs):
        self.task = (func, args, kwargs)


def test_dispatcher_captures_only_json_round_tripped_identity() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    dispatcher.schedule(
        background,
        command="CATALOG_EXECUTE",
        payload={"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ("run_1",)},
    )
    assert background.task is not None
    _, args, kwargs = background.task
    assert kwargs == {}
    assert args[0] == "CATALOG_EXECUTE"
    assert args[1] == {"batch_id": "ahrb_1", "operation_id": "ahrop_1", "range_run_ids": ["run_1"]}


def test_dispatcher_rejects_request_scoped_non_json_objects() -> None:
    background = _Background()
    dispatcher = ResponseBoundHistoricalRangeDispatcher(runtime_factory=lambda: SimpleNamespace())
    try:
        dispatcher.schedule(background, command="CATALOG_EXECUTE", payload={"connection": object()})
    except TypeError:
        pass
    else:
        raise AssertionError("request-scoped object must not be captured")
    assert background.task is None
