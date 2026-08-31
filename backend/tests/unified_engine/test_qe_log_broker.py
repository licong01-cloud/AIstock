from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import httpx

from backend.services.quantevolver.qe_log_broker import (
    BROKER_CURSOR_CONFLICT,
    QELogBroker,
    QELogBrokerCursorError,
    QELogBrokerSource,
    resolve_broker_cursor,
)
from backend.services.quantevolver.qe_log_store import QELiveLogStore
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceLogCursorExpired,
    QEWorkspaceLogEvent,
)


def test_two_subscribers_share_one_upstream_and_last_close_stops_it(tmp_path: Path) -> None:
    async def scenario() -> None:
        opens: list[str | None] = []
        closed = asyncio.Event()
        hold = asyncio.Event()

        async def open_stream(cursor: str | None):
            opens.append(cursor)
            try:
                yield QEWorkspaceLogEvent(
                    data=json.dumps({"status": "running", "logs": ["line-1"]}),
                    cursor="rd-1",
                    event_type=None,
                    terminal=False,
                )
                await hold.wait()
            finally:
                closed.set()

        broker = QELogBroker(QELiveLogStore(tmp_path, max_file_bytes=4096))
        source = QELogBrokerSource("node-1", "node-1", open_stream)
        first = broker.stream("task-1", [source])
        first_event = await anext(first)
        second = broker.stream("task-1", [source])
        second_event = await anext(second)

        assert opens == [None]
        assert first_event.data == second_event.data
        assert broker.subscriber_count("task-1") == 2

        await first.aclose()
        assert broker.subscriber_count("task-1") == 1
        assert not closed.is_set()
        await second.aclose()
        await asyncio.wait_for(closed.wait(), timeout=1)
        assert broker.subscriber_count("task-1") == 0

    asyncio.run(scenario())


def test_resume_reuses_remote_cursor_without_zero_replay(tmp_path: Path) -> None:
    async def scenario() -> None:
        opens: list[str | None] = []

        async def open_stream(cursor: str | None):
            opens.append(cursor)
            sequence = len(opens)
            yield QEWorkspaceLogEvent(
                data=json.dumps({"status": "running", "logs": [f"line-{sequence}"]}),
                cursor=f"rd-{sequence}",
                event_type=None,
                terminal=False,
            )
            await asyncio.Event().wait()

        broker = QELogBroker(QELiveLogStore(tmp_path, max_file_bytes=4096))
        source = QELogBrokerSource("node-1", "node-1", open_stream)
        first = broker.stream("task-1", [source])
        first_event = await anext(first)
        await first.aclose()

        resumed = broker.stream("task-1", [source], after_cursor=first_event.cursor)
        resumed_event = await anext(resumed)
        await resumed.aclose()

        assert opens == [None, "rd-1"]
        assert "line-2" in resumed_event.data

    asyncio.run(scenario())


def test_upstream_stale_cursor_is_loud_and_never_replays_from_zero(tmp_path: Path) -> None:
    async def scenario() -> None:
        calls: list[str | None] = []

        async def open_stream(cursor: str | None):
            calls.append(cursor)
            raise QEWorkspaceLogCursorExpired("source replaced")
            yield  # pragma: no cover

        broker = QELogBroker(QELiveLogStore(tmp_path, max_file_bytes=4096))
        stream = broker.stream(
            "task-1",
            [QELogBrokerSource("node-1", "node-1", open_stream)],
        )
        event = await anext(stream)
        await stream.aclose()
        payload = json.loads(event.data)

        assert calls == [None]
        assert payload["reason_code"] == "qe_log_cursor_expired"
        assert payload["event"] == "qe_log_cursor_expired"

    asyncio.run(scenario())


def test_broker_cursor_conflict_and_process_restart_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(QELogBrokerCursorError) as conflict:
        resolve_broker_cursor("query", "header")
    assert conflict.value.reason_code == BROKER_CURSOR_CONFLICT

    broker = QELogBroker(QELiveLogStore(tmp_path, max_file_bytes=4096))
    with pytest.raises(QELogBrokerCursorError, match="process restart"):
        broker.validate_cursor("task-1", "opaque-old-process-cursor")


def test_evolution_service_has_no_live_legacy_append_and_client_discards_heartbeats() -> None:
    from backend.services.quantevolver import qe_evolution_service, qe_workspace_client

    service_source = Path(qe_evolution_service.__file__).read_text(encoding="utf-8")
    client_source = Path(qe_workspace_client.__file__).read_text(encoding="utf-8")

    assert "aiofiles.open(log_path" not in service_source
    assert "_legacy_stream_task_logs" not in service_source
    assert 'if line.startswith(":")' in client_source
    assert "QEWorkspaceLogCursorExpired" in client_source


def test_workspace_client_parses_cursor_and_discards_heartbeat(monkeypatch) -> None:
    async def scenario() -> None:
        observed_headers: list[dict[str, str] | None] = []

        class Response:
            status_code = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            def raise_for_status(self) -> None:
                return None

            async def aiter_lines(self):
                yield ": heartbeat"
                yield ""
                yield "id: rd-cursor-2"
                yield 'data: {"status":"running","logs":["line"]}'
                yield ""

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            def stream(self, _method, _url, **kwargs):
                observed_headers.append(kwargs.get("headers"))
                return Response()

        monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: Client())
        client = QEWorkspaceClient.__new__(QEWorkspaceClient)
        client.base_url = "http://node/api/v1/qe_workspace"
        events = [
            event
            async for event in client.stream_task_log_events(
                "task-1",
                after_cursor="rd-cursor-1",
            )
        ]

        assert observed_headers == [{"Last-Event-ID": "rd-cursor-1"}]
        assert len(events) == 1
        assert events[0].cursor == "rd-cursor-2"
        assert json.loads(events[0].data)["logs"] == ["line"]

    asyncio.run(scenario())


def test_store_failure_is_broadcast_without_stopping_live_delivery() -> None:
    async def scenario() -> None:
        class BrokenStore:
            def append(self, _record):
                raise OSError("disk full")

        async def open_stream(_cursor):
            yield QEWorkspaceLogEvent(
                data=json.dumps({"status": "completed", "logs": ["terminal-line"]}),
                cursor="rd-terminal",
                event_type="terminal",
                terminal=True,
            )

        broker = QELogBroker(BrokenStore())
        stream = broker.stream(
            "task-1",
            [QELogBrokerSource("node-1", "node-1", open_stream)],
        )
        events = [event async for event in stream]
        payloads = [json.loads(event.data) for event in events]

        assert payloads[0]["logs"] == ["terminal-line"]
        assert payloads[1]["reason_code"] == "qe_live_log_store_write_failed"

    asyncio.run(scenario())


def test_task_lifecycle_close_wakes_idle_subscribers(tmp_path: Path) -> None:
    async def scenario() -> None:
        async def open_stream(_cursor):
            await asyncio.Event().wait()
            yield  # pragma: no cover

        broker = QELogBroker(QELiveLogStore(tmp_path, max_file_bytes=4096))
        stream = broker.stream(
            "task-1",
            [QELogBrokerSource("node-1", "node-1", open_stream)],
        )
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        broker.request_close("task-1")
        event = await asyncio.wait_for(pending, timeout=1)
        payload = json.loads(event.data)

        assert payload["event"] == "task_deleted"
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert broker.subscriber_count("task-1") == 0

    asyncio.run(scenario())
