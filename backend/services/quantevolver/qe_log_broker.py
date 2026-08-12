"""Single-upstream, bounded fan-out broker for QE live logs."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

from .qe_log_store import QELiveLogStore, get_qe_live_log_store
from .qe_workspace_client import QEWorkspaceLogCursorExpired, QEWorkspaceLogEvent

logger = logging.getLogger(__name__)

BROKER_CURSOR_EXPIRED = "qe_log_broker_cursor_expired"
BROKER_CURSOR_CONFLICT = "qe_log_broker_cursor_conflict"
_CURSOR_CONTEXT = b"aistock-qe-log-broker-v1\0"
_HISTORY_LIMIT = 500
_HISTORY_BYTES_LIMIT = 8 * 1024 * 1024
_INITIAL_REPLAY_LIMIT = 200
_SUBSCRIBER_QUEUE_LIMIT = 64
_SUBSCRIBER_QUEUE_BYTES_LIMIT = 2 * 1024 * 1024
_SESSION_CACHE_LIMIT = 32
_RECONNECT_DELAY_SECONDS = 3.0


class QELogBrokerCursorError(ValueError):
    def __init__(self, message: str, *, reason_code: str = BROKER_CURSOR_EXPIRED) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class QELogBrokerSource:
    node_key: str
    node_label: str
    open_stream: Callable[[str | None], AsyncIterator[QEWorkspaceLogEvent]]


@dataclass(frozen=True)
class QELogBrokerEvent:
    seq: int
    cursor: str
    data: str
    event_type: str | None = None
    node_key: str | None = None

    def as_sse(self) -> str:
        parts = [f"id: {self.cursor}"]
        parts.append(f"data: {self.data}")
        return "\n".join(parts) + "\n\n"

    @property
    def size_bytes(self) -> int:
        return len(self.data.encode("utf-8")) + len(self.cursor) + 32


@dataclass
class _Subscriber:
    queue: asyncio.Queue[QELogBrokerEvent]
    node_keys: frozenset[str]
    dropped: int = 0
    queued_bytes: int = 0


def resolve_broker_cursor(after_cursor: str | None, last_event_id: str | None) -> str | None:
    query = str(after_cursor or "").strip() or None
    header = str(last_event_id or "").strip() or None
    if query and header and query != header:
        raise QELogBrokerCursorError(
            "after_cursor and Last-Event-ID disagree",
            reason_code=BROKER_CURSOR_CONFLICT,
        )
    return query or header


def _encode_cursor(stream_uuid: str, seq: int) -> str:
    body = {"v": 1, "stream_uuid": stream_uuid, "seq": int(seq)}
    raw_body = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    envelope = {
        "body": body,
        "sha256_16": hashlib.sha256(_CURSOR_CONTEXT + raw_body).hexdigest()[:16],
    }
    raw = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _decode_cursor(token: str) -> tuple[str, int]:
    value = str(token or "").strip()
    if not value or len(value) > 4096:
        raise QELogBrokerCursorError("QE log broker cursor is empty or oversized")
    try:
        padding = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        envelope = json.loads(raw.decode("utf-8"))
        body = envelope["body"]
        raw_body = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        actual = hashlib.sha256(_CURSOR_CONTEXT + raw_body).hexdigest()[:16]
        if str(envelope["sha256_16"]) != actual or int(body["v"]) != 1:
            raise QELogBrokerCursorError("QE log broker cursor checksum or version is invalid")
        stream_uuid = str(body["stream_uuid"])
        seq = int(body["seq"])
    except QELogBrokerCursorError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise QELogBrokerCursorError("QE log broker cursor is malformed") from exc
    if seq < 0:
        raise QELogBrokerCursorError("QE log broker cursor sequence is negative")
    return stream_uuid, seq


class _TaskSession:
    def __init__(
        self,
        task_id: str,
        sources: list[QELogBrokerSource],
        store: QELiveLogStore,
    ) -> None:
        self.task_id = task_id
        self.stream_uuid = uuid.uuid4().hex
        self.sources = {source.node_key: source for source in sources}
        self.store = store
        self.seq = 0
        self.history: deque[QELogBrokerEvent] = deque(maxlen=_HISTORY_LIMIT)
        self.history_bytes = 0
        self.subscribers: dict[str, _Subscriber] = {}
        self.source_cursors: dict[str, str] = {}
        self.source_tasks: dict[str, asyncio.Task[None]] = {}
        self.source_terminal: set[str] = set()
        self.last_error_fingerprint: dict[str, str] = {}
        self.closed = False
        self.lock = asyncio.Lock()

    def _validate_sources(self, sources: list[QELogBrokerSource]) -> None:
        for source in sources:
            self.sources.setdefault(source.node_key, source)

    def _event(
        self,
        payload: dict[str, Any],
        *,
        event_type: str | None = None,
        node_key: str | None = None,
    ) -> QELogBrokerEvent:
        self.seq += 1
        cursor = _encode_cursor(self.stream_uuid, self.seq)
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        event = QELogBrokerEvent(
            seq=self.seq,
            cursor=cursor,
            data=data,
            event_type=event_type,
            node_key=node_key,
        )
        if len(self.history) == self.history.maxlen and self.history:
            self.history_bytes -= self.history[0].size_bytes
        self.history.append(event)
        self.history_bytes += event.size_bytes
        while self.history and self.history_bytes > _HISTORY_BYTES_LIMIT:
            removed = self.history.popleft()
            self.history_bytes -= removed.size_bytes
        return event

    def _offer(self, subscriber: _Subscriber, event: QELogBrokerEvent) -> None:
        if event.size_bytes > _SUBSCRIBER_QUEUE_BYTES_LIMIT:
            payload = {
                "status": "warning",
                "event": "qe_log_subscriber_backpressure",
                "reason_code": "qe_log_subscriber_event_too_large",
                "dropped_events": 1,
                "logs": ["One oversized live log event was omitted from this subscriber."],
            }
            event = QELogBrokerEvent(
                seq=event.seq,
                cursor=event.cursor,
                data=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                event_type="error",
            )
        while (
            subscriber.queue.full()
            or subscriber.queued_bytes + event.size_bytes > _SUBSCRIBER_QUEUE_BYTES_LIMIT
        ) and not subscriber.queue.empty():
            removed = subscriber.queue.get_nowait()
            subscriber.queued_bytes -= removed.size_bytes
            subscriber.dropped += 1
        subscriber.queue.put_nowait(event)
        subscriber.queued_bytes += event.size_bytes

    def _broadcast(self, event: QELogBrokerEvent) -> None:
        for subscriber in tuple(self.subscribers.values()):
            if event.node_key is not None and event.node_key not in subscriber.node_keys:
                continue
            self._offer(subscriber, event)

    def _persist(
        self,
        source: QELogBrokerSource,
        payload: dict[str, Any],
        source_cursor: str | None,
    ) -> str | None:
        try:
            self.store.append(
                {
                    "task_id": self.task_id,
                    "node_id": source.node_label,
                    "source_cursor": source_cursor,
                    "broker_seq": self.seq,
                    "payload": payload,
                }
            )
        except Exception as exc:
            logger.exception(
                "QE live log mirror write failed task=%s node=%s",
                self.task_id,
                source.node_label,
            )
            return f"{type(exc).__name__}: {exc}"
        self.last_error_fingerprint.pop(f"store:{source.node_key}", None)
        return None

    def _broadcast_store_error(self, source: QELogBrokerSource, error: str | None) -> None:
        if not error:
            return
        key = f"store:{source.node_key}"
        if self.last_error_fingerprint.get(key) == error:
            return
        self.last_error_fingerprint[key] = error
        event = self._event(
            {
                "status": "error",
                "event": "qe_live_log_store_error",
                "reason_code": "qe_live_log_store_write_failed",
                "node_id": source.node_label,
                "logs": [f"QE live log mirror write failed: {error}"],
            },
            event_type="error",
            node_key=source.node_key,
        )
        self._broadcast(event)

    def _normalize_payload(self, source: QELogBrokerSource, data: str) -> dict[str, Any]:
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            parsed = {"status": "running", "event": "node_log", "logs": [data]}
        if not isinstance(parsed, dict):
            parsed = {"status": "running", "event": "node_log", "logs": [str(parsed)]}
        payload = dict(parsed)
        if len(self.sources) > 1:
            payload["node_id"] = source.node_label
            payload["source_node_id"] = source.node_label
            payload.setdefault("event", "node_log")
            logs = payload.get("logs")
            if logs is not None:
                values = logs if isinstance(logs, list) else [logs]
                prefix = f"[{source.node_label}]"
                payload["logs"] = [
                    str(value) if str(value).startswith(prefix) else f"{prefix} {value}"
                    for value in values
                ]
        return payload

    async def _run_source(self, source: QELogBrokerSource) -> None:
        while self.subscribers and source.node_key not in self.source_terminal:
            cursor = self.source_cursors.get(source.node_key)
            try:
                async for upstream in source.open_stream(cursor):
                    if not self.subscribers:
                        return
                    if upstream.cursor:
                        self.source_cursors[source.node_key] = upstream.cursor
                    payload = self._normalize_payload(source, upstream.data)
                    event = self._event(
                        payload,
                        event_type=upstream.event_type,
                        node_key=source.node_key,
                    )
                    store_error = self._persist(source, payload, upstream.cursor)
                    self._broadcast(event)
                    self._broadcast_store_error(source, store_error)
                    self.last_error_fingerprint.pop(source.node_key, None)
                    if upstream.terminal:
                        self.source_terminal.add(source.node_key)
                        return
                if source.node_key not in self.source_terminal and self.subscribers:
                    raise RuntimeError("QE log upstream ended before a terminal event")
            except asyncio.CancelledError:
                raise
            except QEWorkspaceLogCursorExpired as exc:
                payload = {
                    "status": "error",
                    "event": "qe_log_cursor_expired",
                    "reason_code": exc.reason_code,
                    "node_id": source.node_label,
                    "logs": [str(exc)],
                }
                event = self._event(payload, event_type="error", node_key=source.node_key)
                store_error = self._persist(source, payload, cursor)
                self._broadcast(event)
                self._broadcast_store_error(source, store_error)
                self.source_terminal.add(source.node_key)
                return
            except Exception as exc:
                fingerprint = f"{type(exc).__name__}:{exc}"
                if self.last_error_fingerprint.get(source.node_key) != fingerprint:
                    self.last_error_fingerprint[source.node_key] = fingerprint
                    payload = {
                        "status": "warning",
                        "event": "node_log_stream_error",
                        "node_id": source.node_label,
                        "logs": [f"[{source.node_label}] log stream failed: {exc}"],
                    }
                    event = self._event(payload, event_type="error", node_key=source.node_key)
                    store_error = self._persist(source, payload, cursor)
                    self._broadcast(event)
                    self._broadcast_store_error(source, store_error)
                    logger.warning(
                        "QE log upstream failed task=%s node=%s: %s",
                        self.task_id,
                        source.node_label,
                        exc,
                    )
                if cursor is None:
                    self.source_terminal.add(source.node_key)
                    return
                await asyncio.sleep(_RECONNECT_DELAY_SECONDS)

    def _ensure_workers(self) -> None:
        if not self.subscribers:
            return
        for source in self.sources.values():
            if source.node_key in self.source_terminal:
                continue
            task = self.source_tasks.get(source.node_key)
            if task is None or task.done():
                self.source_tasks[source.node_key] = asyncio.create_task(self._run_source(source))

    async def subscribe(
        self,
        sources: list[QELogBrokerSource],
        after_cursor: str | None,
    ) -> AsyncIterator[QELogBrokerEvent]:
        self._validate_sources(sources)
        replay: list[QELogBrokerEvent]
        requested_keys = {source.node_key for source in sources}
        if after_cursor:
            stream_uuid, after_seq = _decode_cursor(after_cursor)
            if stream_uuid != self.stream_uuid:
                raise QELogBrokerCursorError("QE log broker cursor belongs to another process stream")
            oldest = self.history[0].seq if self.history else self.seq + 1
            if after_seq < oldest - 1 or after_seq > self.seq:
                raise QELogBrokerCursorError("QE log broker cursor is outside retained replay history")
            replay = [
                event
                for event in self.history
                if event.seq > after_seq and (event.node_key is None or event.node_key in requested_keys)
            ]
            if (
                len(replay) > _SUBSCRIBER_QUEUE_LIMIT
                or sum(event.size_bytes for event in replay) > _SUBSCRIBER_QUEUE_BYTES_LIMIT
            ):
                raise QELogBrokerCursorError("QE log broker replay exceeds subscriber queue capacity")
        else:
            replay = [
                event
                for event in self.history
                if event.node_key is None or event.node_key in requested_keys
            ][-_INITIAL_REPLAY_LIMIT:]

        subscriber_id = uuid.uuid4().hex
        subscriber = _Subscriber(
            queue=asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_LIMIT),
            node_keys=frozenset(source.node_key for source in sources),
        )
        for event in replay:
            self._offer(subscriber, event)
        async with self.lock:
            self.subscribers[subscriber_id] = subscriber
            self._ensure_workers()

        try:
            while True:
                event = await subscriber.queue.get()
                subscriber.queued_bytes -= event.size_bytes
                if subscriber.dropped:
                    dropped = subscriber.dropped
                    subscriber.dropped = 0
                    payload = {
                        "status": "warning",
                        "event": "qe_log_subscriber_backpressure",
                        "reason_code": "qe_log_subscriber_events_dropped",
                        "dropped_events": dropped,
                        "logs": [f"Dropped {dropped} stale log events for this slow subscriber."],
                    }
                    yield QELogBrokerEvent(
                        seq=event.seq,
                        cursor=event.cursor,
                        data=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        event_type="error",
                    )
                yield event
                if (
                    self.closed
                    or requested_keys.issubset(self.source_terminal)
                ) and subscriber.queue.empty():
                    return
        finally:
            async with self.lock:
                self.subscribers.pop(subscriber_id, None)
                if not self.subscribers:
                    tasks = list(self.source_tasks.values())
                    self.source_tasks.clear()
                    for task in tasks:
                        task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)


class QELogBroker:
    def __init__(self, store: QELiveLogStore | None = None) -> None:
        self.store = store or get_qe_live_log_store()
        self._sessions: dict[str, _TaskSession] = {}
        self._lock = asyncio.Lock()

    async def stream(
        self,
        task_id: str,
        sources: list[QELogBrokerSource],
        *,
        after_cursor: str | None = None,
    ) -> AsyncIterator[QELogBrokerEvent]:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id or not sources:
            raise ValueError("QE log broker requires task_id and at least one source")
        async with self._lock:
            session = self._sessions.get(normalized_task_id)
            source_keys = {source.node_key for source in sources}
            if (
                session is not None
                and not session.subscribers
                and not after_cursor
                and (
                    source_keys != set(session.sources)
                    or session.closed
                    or len(session.source_terminal) == len(session.sources)
                )
            ):
                session = None
            if session is None:
                if after_cursor:
                    raise QELogBrokerCursorError("QE log broker cursor cannot resume after process restart")
                if len(self._sessions) >= _SESSION_CACHE_LIMIT:
                    inactive_key = next(
                        (key for key, cached in self._sessions.items() if not cached.subscribers),
                        None,
                    )
                    if inactive_key is None:
                        raise RuntimeError("QE log broker active-session capacity is exhausted")
                    self._sessions.pop(inactive_key, None)
                session = _TaskSession(normalized_task_id, sources, self.store)
                self._sessions[normalized_task_id] = session
        subscription = session.subscribe(sources, after_cursor)
        try:
            async for event in subscription:
                yield event
        finally:
            await subscription.aclose()

    def subscriber_count(self, task_id: str) -> int:
        session = self._sessions.get(str(task_id or "").strip())
        return len(session.subscribers) if session is not None else 0

    def request_close(self, task_id: str) -> None:
        session = self._sessions.get(str(task_id or "").strip())
        if session is None or session.closed:
            return
        session.closed = True
        for task in tuple(session.source_tasks.values()):
            task.cancel()
        event = session._event(
            {
                "status": "deleted",
                "event": "task_deleted",
                "logs": [f"Task {task_id} log broker was closed by task lifecycle control."],
            }
        )
        session._broadcast(event)

    def validate_cursor(self, task_id: str, cursor: str | None) -> None:
        if not cursor:
            return
        session = self._sessions.get(str(task_id or "").strip())
        if session is None:
            raise QELogBrokerCursorError("QE log broker cursor cannot resume after process restart")
        stream_uuid, after_seq = _decode_cursor(cursor)
        if stream_uuid != session.stream_uuid:
            raise QELogBrokerCursorError("QE log broker cursor belongs to another process stream")
        oldest = session.history[0].seq if session.history else session.seq + 1
        if after_seq < oldest - 1 or after_seq > session.seq:
            raise QELogBrokerCursorError("QE log broker cursor is outside retained replay history")


_PROCESS_BROKER: QELogBroker | None = None


def get_qe_log_broker() -> QELogBroker:
    global _PROCESS_BROKER
    if _PROCESS_BROKER is None:
        _PROCESS_BROKER = QELogBroker()
    return _PROCESS_BROKER
