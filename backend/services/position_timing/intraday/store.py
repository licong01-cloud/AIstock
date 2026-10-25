"""Independent minute namespace, bounded locks and hash-verified commit chains."""

from __future__ import annotations

import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path

from backend.services.position_timing.artifact_store import PositionTimingArtifactStore

from .contracts import canonical, digest, payload


class IntradayError(RuntimeError):
    def __init__(self, code: str, status: int = 409):
        super().__init__(code)
        self.code, self.status = code, status


class IntradayStore:
    def __init__(self, root: str | Path | None = None):
        self.root = PositionTimingArtifactStore(root).root / "intraday_v1"

    def path(self, group: str, name: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,200}", name):
            raise IntradayError("INVALID_ARTIFACT_ID", 422)
        return self.root / group / (name + ".json")

    @contextmanager
    def lock(self, name: str):
        path = self.path("locks", name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+b") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            deadline = time.monotonic() + 1
            while True:
                handle.seek(0)
                try:
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise IntradayError("BUSY_RETRYABLE", 503) from None
                    time.sleep(0.02)
            try:
                yield
            finally:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def read(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
            if set(envelope) != {"sha256", "payload"} or digest(envelope["payload"]) != envelope["sha256"]:
                raise ValueError("identity mismatch")
            return envelope["payload"]
        except (ValueError, TypeError, OSError) as exc:
            raise IntradayError("ARTIFACT_INTEGRITY_CONFLICT", 500) from exc

    def write(self, path: Path, value, *, mutable: bool = False) -> None:
        body = payload(value)
        content = canonical({"sha256": digest(body), "payload": body}) + b"\n"
        method = PositionTimingArtifactStore._atomic_replace if mutable else PositionTimingArtifactStore._publish_immutable
        method(path, content)

    def head(self, plan_id: str) -> dict | None:
        pointer = self.read(self.path("heads", plan_id))
        if pointer is None:
            return None
        transition = self.read(self.path("transitions", pointer["transition_sha256"]))
        if transition is None or digest(transition) != pointer["transition_sha256"] or transition["plan"]["plan_id"] != plan_id:
            raise IntradayError("STATE_HEAD_INTEGRITY_CONFLICT", 500)
        return transition

    def find_request(self, head: dict | None, key: str) -> dict | None:
        while head is not None:
            if head["request_key"] == key:
                return head
            parent = head["parent_sha256"]
            if parent is None:
                return None
            previous = self.read(self.path("transitions", parent))
            if previous is None or digest(previous) != parent:
                raise IntradayError("STATE_CHAIN_INTEGRITY_CONFLICT", 500)
            head = previous
        return None

    def reserve_request(self, plan_id: str, key: str, request_hash: str) -> bool:
        path = self.path("requests", digest({"plan_id": plan_id, "key": key}))
        existing = self.read(path)
        if existing is not None:
            if existing["request_sha256"] != request_hash:
                raise IntradayError("IDEMPOTENCY_CONTENT_CONFLICT")
            return True
        self.write(path, {"plan_id": plan_id, "key": key, "request_sha256": request_hash})
        return False

    def commit(self, *, plan, state, decision, input_value, request_key: str, request_sha256: str, response: dict) -> dict:
        current = self.head(plan.plan_id)
        input_hash = digest(input_value)
        self.write(self.path("inputs", input_hash), input_value)
        transition = payload({"plan": plan, "state": state, "decision": decision, "input_sha256": input_hash,
                              "request_key": request_key, "request_sha256": request_sha256, "response": response,
                              "parent_sha256": digest(current) if current else None})
        transition_hash = digest(transition)
        self.write(self.path("transitions", transition_hash), transition)
        self.write(self.path("heads", plan.plan_id), {"transition_sha256": transition_hash}, mutable=True)
        return transition
