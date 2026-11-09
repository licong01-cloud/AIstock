"""WSL execution envelope; the original model/trading command is immutable."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import shlex

GIB = 1024**3
TRAINING_MEMORY_BYTES = 40 * GIB
# Conservative whole-job reservations, not measured peaks. A full-market
# 565-day replay exceeded the old6GiB cap while the API remained isolated.
PEAK_BYTES = {"gpu": 32 * GIB, "cpu": 20 * GIB, "replay": 20 * GIB}
GUARD_NAME = "qe_wsl_workload_guard.py"
COMMAND_NAME = "qe_wsl_workload_command.sh"
POLICY_NAME = "qe_wsl_workload_policy.json"


def memory_slot_limit(
    compute_kind: str | None, *, backtest_only: bool, prediction_replay: bool = False,
) -> int:
    kind = "replay" if backtest_only and prediction_replay else ("cpu" if compute_kind == "cpu" else "gpu")
    return TRAINING_MEMORY_BYTES // PEAK_BYTES[kind]


def isolate_payload(payload, *, source, compute_kind: str | None):
    """Wrap only a not-yet-POSTed payload, never modify an existing worker."""
    # BACKTEST_ONLY reloads a model/dataset and predicts; it is not pure replay.
    # Unknown model inference retains the conservative GPU-sized memory budget.
    pure_replay = source.backtest_only and (
        payload.config.get("prediction_replay") is True
        or bool(re.search(r"(?:^|\s)--pred-backtest(?:\s|$)", payload.wsl_command))
    )
    kind = "replay" if pure_replay else ("cpu" if compute_kind == "cpu" else "gpu")
    lexer = shlex.shlex(payload.wsl_command, posix=True, punctuation_chars=";&|()<>")
    first = [lexer.get_token() for _ in range(3)]
    if first[0:2] == ["cd", "--"]:
        workspace = first[2]
        separator = lexer.get_token()
    else:
        workspace = first[1]
        separator = first[2]
    if first[0] != "cd" or separator != "&&":
        raise ValueError("qe_wsl_isolation_requires_canonical_workspace_cd")
    if not workspace.startswith("/") or ".." in Path(workspace).parts or "\x00" in workspace:
        raise ValueError("qe_wsl_isolation_invalid_workspace")
    guard = Path(__file__).with_name(GUARD_NAME)
    guard_text = guard.read_text(encoding="utf-8")
    command_sha = hashlib.sha256(payload.wsl_command.encode()).hexdigest()
    identity = hashlib.sha256(
        f"{payload.task_id}|{payload.loop_id}|{source.submission_intent_hash}".encode()
    ).hexdigest()
    policy = {
        "schema": "qe_wsl_workload_envelope_v1",
        "identity": identity,
        "task_id": payload.task_id,
        "loop_id": payload.loop_id,
        "kind": kind,
        "peak_bytes": PEAK_BYTES[kind],
        "budget_bytes": TRAINING_MEMORY_BYTES,
        "swap_bytes": 8 * GIB,
        "original_command_sha256": command_sha,
        "guard_sha256": hashlib.sha256(guard_text.encode()).hexdigest(),
        "original_files_sha256": {
            name: hashlib.sha256(text.encode()).hexdigest()
            for name, text in payload.experiment_files.items()
        },
        "interpretation": "Runtime isolation only; original formula, factors, model, data and account contract unchanged.",
    }
    files = dict(payload.experiment_files)
    if {GUARD_NAME, COMMAND_NAME, POLICY_NAME} & set(files):
        raise ValueError("qe_wsl_isolation_reserved_asset_collision")
    files.update(
        {GUARD_NAME: guard_text, COMMAND_NAME: payload.wsl_command, POLICY_NAME: json.dumps(policy, sort_keys=True)}
    )
    from ..multi_alpha.qe_subprocess_env import qe_subprocess_credential_scrub_command

    guarded = qe_subprocess_credential_scrub_command() + f" && exec /usr/bin/python3 {GUARD_NAME}"
    command = f"cd -- {shlex.quote(workspace)} && exec /bin/bash --noprofile --norc -c {shlex.quote(guarded)}"
    return replace(payload, experiment_files=files, wsl_command=command)
