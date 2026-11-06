"""Dependency-aware native outcome client. No SQL, Selection, training or service control.

Run only with an explicit manifest and a NEW X-drive state path. This does not
adopt or change the checkpoint/manifest of a running legacy client.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .api_models import HistoricalRangeRefreshOutcomesRequest
from .canonical import canonical_json_sha256
from .models import HistoricalRangeArtifactKind, HistoricalRangeArtifactRefV1, derive_prefixed_id


class OutcomeAPI:
    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/") + "/api/v1/advisory"

    def call(self, path: str, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(self._base + path, data=data, headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=60) as response:
            envelope = json.load(response)
        if envelope.get("ok") is not True or not isinstance(envelope.get("data"), dict):
            raise ValueError("invalid native Advisory response envelope")
        result = dict(envelope["data"])
        if envelope.get("page") is not None:
            result["next_cursor"] = envelope["page"].get("next_cursor")
        return result


class OutcomeContinuation:
    def __init__(self, *, manifest: dict, state: dict, api, save, max_active: int = 1):
        if manifest.get("schema_version") != "advisory_outcome_continuation_v1":
            raise ValueError("unsupported continuation manifest; legacy checkpoints cannot be adopted")
        if max_active < 1:
            raise ValueError("max_active must be positive")
        self.stages = manifest["stages"]
        names = [stage["name"] for stage in self.stages]
        if not names or len(set(names)) != len(names):
            raise ValueError("stages must be nonempty and unique")
        keys = set()
        for stage in self.stages:
            request = HistoricalRangeRefreshOutcomesRequest.model_validate(stage["request"])
            if request.range_run_ids != [stage["range_run_id"]] or request.expected_execution_identity is None:
                raise ValueError("each stage needs one explicit run and frozen execution identity")
            key = (stage["batch_id"], request.operation_idempotency_key)
            if key in keys or not set(stage.get("depends_on", ())).issubset(names):
                raise ValueError("duplicate command or unknown dependency")
            keys.add(key)
        pending = {stage["name"]: set(stage.get("depends_on", ())) for stage in self.stages}
        while pending:
            roots = {name for name, dependencies in pending.items() if not dependencies}
            if not roots:
                raise ValueError("continuation dependencies contain a cycle")
            pending = {name: dependencies - roots for name, dependencies in pending.items() if name not in roots}
        digest = canonical_json_sha256(manifest)
        if state.get("manifest_sha256", digest) != digest:
            raise ValueError("checkpoint belongs to another immutable manifest")
        self.state, self.api, self.save, self.max_active = state, api, save, max_active
        state["manifest_sha256"] = digest
        state.setdefault("stages", {})
        if not set(state["stages"]).issubset(names):
            raise ValueError("checkpoint contains an unknown stage")

    def _operations(self, batch_id, operation_type="REFRESH_OUTCOMES"):
        operations, cursor, seen = [], None, set()
        while True:
            query = {"operation_type": operation_type, "limit": 50}
            if cursor is not None:
                query["cursor"] = cursor
            page = self.api.call(f"/historical-range-batches/{batch_id}/operations?{urlencode(query)}")
            operations.extend(page["operations"])
            cursor = page.get("next_cursor")
            if cursor is None:
                return operations
            if cursor in seen:
                raise ValueError("native operation pagination repeated a cursor")
            seen.add(cursor)

    @staticmethod
    def _status(operation):
        if operation.get("error_json"):
            raise RuntimeError(f"native outcome error: {operation['error_json']}")
        status = operation["status"]
        if status == "COMPLETED":
            if not operation.get("result_ref"):
                raise ValueError("native completion has no result receipt")
            ref = HistoricalRangeArtifactRefV1.model_validate(operation["result_ref"])
            if ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT:
                raise ValueError("native completion does not reference an outcome receipt")
            return "COMPLETE"
        if status == "QUEUED" or (status == "RUNNING" and operation.get("lease_expired") is False):
            return "ACTIVE"
        raise RuntimeError(f"native outcome requires recovery: {status}")

    def _remember(self, stage, record, operation):
        body = record["request"]
        if (operation["operation_type"] != "REFRESH_OUTCOMES"
                or operation["operation_idempotency_key"] != body["operation_idempotency_key"]
                or operation["expected_row_version"] != body["expected_row_version"]
                or operation.get("batch_id", stage["batch_id"]) != stage["batch_id"]
                or record.get("operation_id", operation["operation_id"]) != operation["operation_id"]):
            raise ValueError("native operation differs from the persisted request")
        previous = record.get("request_payload_sha256")
        if previous is not None and previous != operation["request_payload_sha256"]:
            raise ValueError("native operation hash changed")
        if record.get("result_ref") is not None and record["result_ref"] != operation.get("result_ref"):
            raise ValueError("native completed result receipt changed")
        record.update(operation_id=operation["operation_id"],
                      request_payload_sha256=operation["request_payload_sha256"],
                      status=self._status(operation), result_ref=operation.get("result_ref"))
        self.state["current"] = stage["name"]
        self.save(self.state)

    def _check_child(self, stage):
        key = derive_prefixed_id("ahroutkey", {
            "parent_key": stage["request"]["operation_idempotency_key"],
            "range_run_id": stage["range_run_id"],
            "policy_bundle_hash": stage["request"]["expected_execution_identity"]["policy_bundle_hash"],
        })
        matches = [operation for operation in self._operations(stage["batch_id"], "REFRESH_OUTCOMES_RUN")
                   if operation["operation_idempotency_key"] == key]
        if len(matches) > 1:
            raise ValueError("duplicate native child identity")
        for operation in matches:
            self._status(operation)  # A healthy parent must not conceal an expired/failed child.

    def advance(self):
        # Query all pages once per batch/tick, not once per individual stage.
        batches = {stage["batch_id"] for stage in self.stages}
        by_batch = {batch: self._operations(batch) for batch in batches}
        active_batches, active_count = set(), 0
        for stage in self.stages:
            record = self.state["stages"].setdefault(stage["name"], {})
            if record.get("request") is not None and canonical_json_sha256(record["request"]) != record["api_request_sha256"]:
                raise ValueError("persisted request changed")
            if record.get("request") is not None:
                expected = HistoricalRangeRefreshOutcomesRequest.model_validate(stage["request"]).model_dump(
                    mode="json", exclude={"expected_row_version"})
                actual = HistoricalRangeRefreshOutcomesRequest.model_validate(record["request"]).model_dump(
                    mode="json", exclude={"expected_row_version"})
                if actual != expected:
                    raise ValueError("persisted request differs from the immutable stage")
            matches = [operation for operation in by_batch[stage["batch_id"]]
                       if operation["operation_idempotency_key"] == stage["request"]["operation_idempotency_key"]]
            if len(matches) > 1:
                raise ValueError("duplicate native operation identity")
            if matches:
                if "request" not in record:
                    raise ValueError("existing native command has no matching client checkpoint")
                if record.get("request_payload_sha256") is None:
                    # Lost POST response: native exact retry verifies the original
                    # frozen API payload/plan. Never construct a new body/key.
                    operation = self.api.call(f"/historical-range-batches/{stage['batch_id']}/refresh-outcomes", record["request"])["operation"]
                else:
                    operation = matches[0]
                self._remember(stage, record, operation)
            elif record.get("request") is not None:
                operation = self.api.call(f"/historical-range-batches/{stage['batch_id']}/refresh-outcomes", record["request"])["operation"]
                self._remember(stage, record, operation)
            if record.get("status") == "ACTIVE":
                self._check_child(stage)
                active_count += 1
                active_batches.add(stage["batch_id"])
        for stage in self.stages:
            record = self.state["stages"][stage["name"]]
            if record.get("status") in {"ACTIVE", "COMPLETE"}:
                continue
            if any(self.state["stages"][dependency].get("status") != "COMPLETE"
                   for dependency in stage.get("depends_on", ())):
                continue
            if active_count >= self.max_active or stage["batch_id"] in active_batches:
                continue
            if any(operation["status"] in {"QUEUED", "RUNNING"} for operation in by_batch[stage["batch_id"]]):
                continue  # Same-batch native inflight conflict, not a QE-wide gate.
            body = dict(stage["request"])
            body["expected_row_version"] = self.api.call(f"/historical-range-batches/{stage['batch_id']}")["batch"]["row_version"]
            body = HistoricalRangeRefreshOutcomesRequest.model_validate(body).model_dump(mode="json")
            record.update(request=body, api_request_sha256=canonical_json_sha256(body))
            self.save(self.state)  # checkpoint BEFORE the only new command POST
            operation = self.api.call(f"/historical-range-batches/{stage['batch_id']}/refresh-outcomes", body)["operation"]
            self._remember(stage, record, operation)
            active_count += record["status"] == "ACTIVE"
            active_batches.add(stage["batch_id"])
        complete = all(record.get("status") == "COMPLETE" for record in self.state["stages"].values())
        self.state["status"] = "COMPLETED" if complete else "RUNNING"
        self.save(self.state)
        return complete


def _x_path(path: Path) -> Path:
    path = path.resolve()
    valid = path.drive.upper() == "X:" if os.name == "nt" else path.is_relative_to(Path("/mnt/x/AIstock_temp"))
    if not valid:
        raise ValueError("temporary state must be on X drive (WSL: /mnt/x/AIstock_temp)")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--max-active", type=int, default=1)
    parser.add_argument("--poll-seconds", type=float, default=10)
    parser.add_argument("--report-seconds", type=float, default=1800)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if any(not math.isfinite(interval) or interval < 1 for interval in (args.poll_seconds, args.report_seconds)):
        raise ValueError("poll/report intervals must be positive")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    path = _x_path(args.state)
    path.parent.mkdir(parents=True, exist_ok=True)
    def save(state):
        descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".")
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(state, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    with path.with_suffix(path.suffix + ".lock").open("a+b") as lock:
        if os.name == "nt":
            import msvcrt
            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        runner = OutcomeContinuation(manifest=manifest, state=state, api=OutcomeAPI(args.base_url), save=save, max_active=args.max_active)
        if not args.execute:
            print(json.dumps({"status": "PREPARED_NOT_SUBMITTED", "stages": len(runner.stages)}))
            return
        next_report = 0.0
        while True:
            complete = runner.advance()
            current = time.monotonic()
            if complete or current >= next_report:
                print(json.dumps({"status": state["status"], "current": state.get("current")}), flush=True)
                next_report = current + args.report_seconds
            if complete:
                return
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
