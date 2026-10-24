"""File repositories for the existing LocalSIM economic coordinator.

Only persistence, CAS and readback live here. Account effects remain in the
normal LocalSIM ledger. A generation is visible only after one atomic publish.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
from uuid import uuid4

from backend.services.position_timing.intraday.contracts import canonical, digest

from .economic import LocalSimEconomicCommitRequest, LocalSimEconomicCoordinator, local_sim_fact_payload
from .models import LocalSimEconomicReceiptV1, LocalSimExecutionStateV1, LocalSimProjectionOutboxV1


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def publish(path: Path, value) -> str:
    """No replacement, including concurrent publication or corrupt old files."""
    body = canonical(value) + b"\n"
    if path.exists():
        if path.read_bytes() != body:
            raise ValueError("LOCALSIM_TIMING_IMMUTABLE_ARTIFACT_CONFLICT")
        return hashlib.sha256(body).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / (".pending-" + uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != body:
                raise ValueError("LOCALSIM_TIMING_IMMUTABLE_ARTIFACT_CONFLICT") from None
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(body).hexdigest()


class TimingFileRepository:
    """Implement both coordinator repository protocols in one file transaction."""

    def __init__(self, root: Path, run_id: str):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
            raise ValueError("LOCALSIM_TIMING_RUN_ID_INVALID")
        self.root = Path(root) / run_id
        self.run_id = run_id
        self._buffer = None
        self._prior = None
        self.coordinator = LocalSimEconomicCoordinator(
            runtime_repository=self, paper_repository=self, ensure_paper_run=lambda: None,
        )

    def latest(self) -> dict | None:
        previous, last = None, None
        directory = self.root / "generations"
        for number, path in enumerate(sorted(directory.glob("*.json")), 1):
            envelope = read_json(path)
            record = envelope["payload"]
            if (set(envelope) != {"sha256", "payload"} or digest(record) != envelope["sha256"]
                    or path.name != f"{number:08d}.json" or record["generation"] != number
                    or record["previous_sha256"] != previous or record["run_id"] != self.run_id):
                raise ValueError("LOCALSIM_TIMING_GENERATION_CHAIN_CONFLICT")
            receipt = LocalSimEconomicReceiptV1.model_validate(record["receipt"])
            outbox = LocalSimProjectionOutboxV1.model_validate(record["outbox"])
            if (receipt.run_id != self.run_id or receipt.generation != number
                    or outbox.receipt_id != receipt.receipt_id or outbox.economic_hash != receipt.economic_hash
                    or outbox.projection_payload != record["checkpoint"]
                    or receipt.economic_facts["account_sha256"] != digest(record["checkpoint"]["account"])):
                raise ValueError("LOCALSIM_TIMING_GENERATION_RECEIPT_CONFLICT")
            for state in record["states"].values():
                LocalSimExecutionStateV1.model_validate(state)
            previous, last = envelope["sha256"], record
        return last

    @contextmanager
    def local_sim_economic_transaction_scope(self):
        if self._buffer is not None:
            raise ValueError("LOCALSIM_TIMING_NESTED_WRITER")
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "writer.lock").open("a+b") as lock:
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                self._prior = self.latest()
                self._buffer = {"orders": {}, "fills": {}, "events": {}, "cash_entries": {}, "states": {}}
                if self._prior:
                    self._buffer.update(deepcopy(self._prior["facts"]))
                    self._buffer["states"] = deepcopy(self._prior["states"])
                self._staged = None
                yield
                if self._staged is None:
                    raise ValueError("LOCALSIM_TIMING_TRANSACTION_NOT_STAGED")
                if self._staged is not self._prior:
                    body = self._staged
                    publish(self.root / "generations" / f"{body['generation']:08d}.json",
                            {"sha256": digest(body), "payload": body})
            finally:
                self._buffer = self._prior = None
                lock.seek(0)
                if os.name == "nt":
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def local_sim_economic_transaction(self, run_id):
        if run_id != self.run_id or self._buffer is None:
            raise ValueError("LOCALSIM_TIMING_TRANSACTION_IDENTITY_MISMATCH")
        yield self

    def _save(self, run_id, item, group, key, *, mutable=False):
        if run_id != self.run_id or self._buffer is None:
            raise ValueError("LOCALSIM_TIMING_WRITER_SCOPE_REQUIRED")
        value = local_sim_fact_payload(item, fact_type=group)
        identity = str(getattr(item, key))
        prior = self._buffer[group].get(identity)
        if prior and prior != value and not mutable:
            raise ValueError("LOCALSIM_TIMING_DUPLICATE_FACT_CONFLICT")
        self._buffer[group][identity] = value

    def save_order(self, run_id, order):
        self._save(run_id, order, "orders", "order_id", mutable=True)

    def save_fill(self, run_id, fill):
        self._save(run_id, fill, "fills", "fill_id")

    def save_order_event(self, run_id, event):
        self._save(run_id, event, "events", "event_id")

    def save_cash_entry(self, run_id, entry):
        self._save(run_id, entry, "cash_entries", "fill_id")

    def save_run_event(self, **kwargs):
        if kwargs["run_id"] != self.run_id or self._staged is None:
            raise ValueError("LOCALSIM_TIMING_RUN_EVENT_SCOPE_REQUIRED")
        self._staged["run_event"] = kwargs

    def stage_local_sim_economic_commit(self, **kwargs):
        if kwargs["run_id"] != self.run_id or self._buffer is None:
            raise ValueError("LOCALSIM_TIMING_STAGE_SCOPE_REQUIRED")
        states = {s.state_id: s for s in kwargs["states"]}
        prior = self._prior
        if prior and (prior["receipt"]["economic_facts"] == kwargs["economic_facts"]
                      and prior["checkpoint"] == kwargs["projection_payload"]):
            self._staged = prior
            return (LocalSimEconomicReceiptV1.model_validate(prior["receipt"]),
                    LocalSimProjectionOutboxV1.model_validate(prior["outbox"]), False)
        if set(kwargs["expected_versions"]) != set(states):
            raise ValueError("LOCALSIM_TIMING_STATE_CAS_POPULATION_MISMATCH")
        for identity, state in states.items():
            old = self._buffer["states"].get(identity)
            actual = (old["sequence"], old["state_hash"]) if old else None
            if (actual != kwargs["expected_versions"][identity] or old and (
                    state.sequence < old["sequence"]
                    or state.sequence == old["sequence"] and state.state_hash != old["state_hash"])):
                raise ValueError("LOCALSIM_TIMING_STATE_CAS_CONFLICT")
            self._buffer["states"][identity] = state.model_dump(mode="json")
        generation = prior["generation"] + 1 if prior else 1
        receipt = LocalSimEconomicReceiptV1(
            run_id=self.run_id, binding_id=kwargs["binding_id"], trade_date=kwargs["trade_date"],
            plan_id=kwargs["plan_id"], generation=generation, economic_facts=kwargs["economic_facts"],
        )
        outbox = LocalSimProjectionOutboxV1(
            receipt_id=receipt.receipt_id, run_id=self.run_id, plan_id=kwargs["plan_id"],
            generation=generation, economic_hash=receipt.economic_hash,
            projection_payload=kwargs["projection_payload"],
        )
        self._staged = {"schema_version": "localsim_timing_file_generation_v1", "run_id": self.run_id,
                        "generation": generation, "previous_sha256": digest(prior) if prior else None,
                        "receipt": receipt.model_dump(mode="json"), "outbox": outbox.model_dump(mode="json"),
                        "states": deepcopy(self._buffer["states"]),
                        "facts": {k: deepcopy(self._buffer[k]) for k in ("orders", "fills", "events", "cash_entries")},
                        "checkpoint": kwargs["projection_payload"], "status": str(kwargs["status"])}
        return receipt, outbox, True

    def readback_local_sim_economic_commit(self, *, run_id, receipt, outbox):
        actual = self.latest()
        if (run_id != self.run_id or actual is None or actual["receipt"] != receipt.model_dump(mode="json")
                or actual["outbox"] != outbox.model_dump(mode="json")):
            raise ValueError("LOCALSIM_TIMING_COMMIT_READBACK_FAILED")

    def readback_local_sim_economic_facts(self, **kwargs):
        record = self.latest()
        if kwargs["run_id"] != self.run_id or record is None:
            raise ValueError("LOCALSIM_TIMING_FACT_READBACK_FAILED")
        counts = {}
        for group, expected in (("orders", "order_ids"), ("fills", "fill_ids"),
                                ("events", "order_event_ids"), ("cash_entries", "cash_fill_ids")):
            if not kwargs[expected].issubset(record["facts"][group]):
                raise ValueError("LOCALSIM_TIMING_FACT_READBACK_FAILED")
            counts[group] = len(kwargs[expected])
        return counts

    def checkpoint(self, session, extra: dict):
        prior = self.latest()
        state = session.execution_state
        old = prior["states"].get(state.state_id) if prior else None
        snapshot = session.snapshot()
        request = LocalSimEconomicCommitRequest(
            run_id=self.run_id, binding_id=session.binding_id, trade_date=state.trade_date,
            plan_id=state.plan_id, states=(state,),
            expected_versions={state.state_id: (old["sequence"], old["state_hash"]) if old else None},
            economic_facts={"account_sha256": digest(snapshot), "state_hash": state.state_hash},
            projection_payload={"account": snapshot, "extra": extra}, status="OFFLINE_RESEARCH_CHECKPOINT",
            payload_patch={}, payload_unset=(), orders=(session.order,), fills=tuple(session.fills),
            events=tuple(session.events), cash_entries=tuple(session.backend._ledger.cash_entries),
        )
        return self.coordinator.commit(request)
