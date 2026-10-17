"""Advisory-owned, read-only native-provider adapter to the shared node runner.

No selection, fusion, HMM, persistence or account algorithm is duplicated.
Native package preparation still dispatches single/multi-alpha legs. Only this
consumer's bounded prefetch uses one node session per frozen leg/date block.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import ast
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid
from typing import Any

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.strategy_package.live_inference import LiveInferenceResult, WslStrategyPackageInferenceProvider
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.canonical_pit_inference_boundary import manifest_inference_pit_identity

from .selection_signal_kernel import SelectionSignalDayV1


@dataclass
class _BatchScope:
    days: tuple[date, ...]
    run_id: str
    source_revision: str
    executions: dict[str, tuple[Path, dict[str, Any]]] = field(default_factory=dict)


class AdvisoryBatchInferenceProvider(WslStrategyPackageInferenceProvider):
    def __init__(self, *, temporary_root: Path, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.temporary_root = temporary_root.resolve()
        if self.temporary_root.drive.upper() != "X:":
            raise ValueError("Advisory node bridge temporary root must resolve to Windows X")
        self._scope: ContextVar[_BatchScope | None] = ContextVar(f"advisory_batch_{id(self)}", default=None)

    @contextmanager
    def batch_dates(self, *, trade_dates: tuple[date, ...], run_id: str, source_revision: str):
        if not trade_dates or any(left >= right for left, right in zip(trade_dates, trade_dates[1:])):
            raise ValueError("batch provider requires the exact increasing native date plan")
        if not run_id or not source_revision:
            raise ValueError("batch scope requires its own native run/source revision")
        token = self._scope.set(_BatchScope(trade_dates, run_id, source_revision))
        try:
            yield
        finally:
            self._scope.reset(token)  # No other consumer's task or process is controlled.

    def run(self, *, workspace: Any, trade_date: date, cutoff_date: date | None = None,
            historical_read_only: bool = False) -> LiveInferenceResult:
        if not historical_read_only:
            raise ValueError("Advisory historical bridge does not publish production signals")
        scope = self._scope.get()
        if scope is None:
            scope = _BatchScope((trade_date,), uuid.uuid4().hex, "standalone_read")
        if trade_date not in scope.days:
            raise ValueError("native provider requested a day outside its own batch scope")
        # The existing historical native preparation maps C=D. Other clocks
        # remain explicit standalone requests; never silently change them.
        cutoff = cutoff_date or trade_date
        if len(scope.days) > 1 and cutoff != trade_date:
            raise ValueError("native multi-day preparation requires its declared C=D mapping")
        manifest = json.loads((workspace.workspace_path / "manifest.json").read_text(encoding="utf-8"))
        pit = manifest_inference_pit_identity(manifest)
        date_bound = bool(pit and pit.get("universe_codes"))
        # An actual frozen member list belongs to its own as-of day. Never
        # apply it to the other days of a prefetch block or invent their PIT
        # receipts. Those native workspaces are consumed independently until
        # their owner supplies one explicit PIT identity per plan item.
        days = (trade_date,) if date_bound else scope.days
        identity = _workspace_identity(workspace.workspace_path)
        if date_bound or len(scope.days) == 1:
            identity += ":" + trade_date.isoformat() + ":" + cutoff.isoformat()
        if identity not in scope.executions:
            clocks = tuple(SelectionSignalDayV1(day, day if len(days) > 1 else cutoff, day) for day in days)
            scope.executions[identity] = self._execute(workspace.workspace_path, clocks, scope)
        root, index = scope.executions[identity]
        item = next((row for row in index.get("results", []) if row.get("trade_date") == trade_date.isoformat()), None)
        if item is None:
            raise DataUnavailableError("own native inference batch did not complete the requested day", context={
                "reason_code": index.get("reason_code", "advisory_batch_day_incomplete"),
                "failed_stage": index.get("failed_stage"), "failed_clocks": index.get("failed_clocks"),
                "batch_status": index.get("status"), "trade_date": trade_date.isoformat(),
                "own_batch_index": str(root / "batch.json"),
            })
        path = (root / "batch_dates" / trade_date.isoformat() / "scores.json").resolve()
        if (not path.is_relative_to(root) or item.get("output_path") != win_to_wsl_path(str(path))
                or hashlib.sha256(path.read_bytes()).hexdigest() != item.get("sha256")):
            raise ValueError("own batch daily output path/hash mismatch")
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata", {})
        expected_clock = SelectionSignalDayV1(trade_date, cutoff, trade_date).to_dict()
        if (payload.get("ok") is not True or not isinstance(payload.get("scores"), list)
                or metadata.get("consumer_scope") != "ADVISORY" or metadata.get("run_id") != scope.run_id
                or metadata.get("clocks") != expected_clock or metadata.get("binding") != index.get("binding")
                or not isinstance(payload.get("input_context"), dict) or not isinstance(payload.get("source_read_receipts"), list)
                or isinstance(payload.get("universe_count"), bool) or not isinstance(payload.get("universe_count"), int)
                or payload["universe_count"] <= 0):
            raise ValueError("own batch daily result consumer/run/clock/source identity mismatch")
        metadata = deepcopy(metadata)
        metadata.update(inference_backend="wsl", wsl_distro=self.distro, wsl_conda_env=self.conda_env,
                        inference_batch_status=index["status"], inference_batch_index=str(root / "batch.json"),
                        inference_batch_shape="DATE_BOUND_FROZEN_PIT" if date_bound else "NATIVE_DATE_BLOCK")
        return LiveInferenceResult(scores=payload["scores"], metadata=metadata, universe_count=payload["universe_count"],
                                   source_read_receipts=payload["source_read_receipts"], input_context=payload["input_context"])

    def _execute(self, workspace: Path, clocks: tuple[SelectionSignalDayV1, ...], scope: _BatchScope) -> tuple[Path, dict[str, Any]]:
        from scripts.strategy_package_live_inference import _atomic_json
        root = (self.temporary_root / "ADVISORY" / hashlib.sha256(scope.run_id.encode()).hexdigest() / uuid.uuid4().hex).resolve()
        if not root.is_relative_to(self.temporary_root):
            raise ValueError("batch node output escaped its owned X root")
        root.mkdir(parents=True)
        plan_path, output = root / "plan.json", root / "batch.json"
        _atomic_json(plan_path, [day.to_dict() for day in clocks])
        arguments = ["scripts/strategy_package_live_inference.py", "--runtime-workspace", win_to_wsl_path(str(workspace)),
                     "--historical-plan", win_to_wsl_path(str(plan_path)), "--historical-read-only",
                     "--consumer-scope", "ADVISORY", "--run-id", scope.run_id, "--input-revision", scope.source_revision,
                     "--temp-root", win_to_wsl_path(str(root)), "--output-path", win_to_wsl_path(str(output))]
        command = (f"source {self.conda_sh} && conda activate {self.conda_env} && cd {self._quote(win_to_wsl_path(str(self.repo_root)))} && "
                   + self._build_env_exports(historical_read_only=True) + " python " + " ".join(self._quote(arg) for arg in arguments))
        completed = subprocess.run(["wsl", "-d", self.distro, "bash", "-lc", command], capture_output=True,
                                   text=True, encoding="utf-8", errors="replace", check=False,
                                   timeout=self.timeout_seconds, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        # Do not expose a shell command or environment containing credentials.
        if not output.is_file():
            raise DataUnavailableError("own node inference wrote no batch index", context={
                "reason_code": "advisory_batch_index_missing", "returncode": completed.returncode, "own_output": str(output),
            })
        index = json.loads(output.read_text(encoding="utf-8"))
        if (index.get("consumer_scope") != "ADVISORY" or index.get("run_id") != scope.run_id
                or index.get("status") not in {"COMPLETE", "FAILED"} or not isinstance(index.get("results"), list)
                or not index.get("binding")):
            raise ValueError("node batch index identity/status is invalid")
        if index["status"] == "COMPLETE" and (completed.returncode != 0 or index.get("ok") is not True or len(index["results"]) != len(clocks)):
            raise ValueError("node batch claims complete without all dates and successful exit")
        return root, index


def _workspace_identity(workspace: Path) -> str:
    """Physical paths/nonces aren't algorithms; frozen code/assets ARE identity."""
    manifest = json.loads((workspace / "manifest.json").read_text(encoding="utf-8"))
    primary = manifest["primary_assets"]
    code = {}
    if primary.get("factor_entry_relpath"):
        tree = ast.parse((workspace / primary["factor_entry_relpath"]).read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "_FACTOR_FILES" for target in node.targets):
                paths = ast.literal_eval(node.value)
                hashes = {}
                for name, value in paths.items():
                    path = Path(value[5].upper() + ":/" + value[7:]) if value.startswith("/mnt/") and len(value) > 7 and value[6] == "/" else Path(value)
                    hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
                node.value = ast.parse(repr(hashes), mode="eval").body
        code["factor_entry"] = hashlib.sha256(ast.dump(tree).encode()).hexdigest()
    code["factor_order"] = hashlib.sha256((workspace / "factor_order.json").read_bytes()).hexdigest()
    for path in (workspace / "model").rglob("*.py"):
        code[path.relative_to(workspace).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    assets = {}
    for key in ("model_weight_relpath", "dataset_processor_relpath", "dataset_relpath"):
        if primary.get(key):
            stat = (workspace / primary[key]).stat()
            assets[key] = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
    # Include declared pool/PIT inputs. Do not deduplicate two different
    # populations merely because they use the same package or model file.
    manifest = {key: value for key, value in manifest.items() if key != "diagnostics"}
    manifest["package_manifest_sha256"] = json.loads((workspace / "manifest.json").read_text(encoding="utf-8")).get("diagnostics", {}).get("package_manifest_sha256")
    return hashlib.sha256(json.dumps([manifest, code, assets], sort_keys=True).encode()).hexdigest()
