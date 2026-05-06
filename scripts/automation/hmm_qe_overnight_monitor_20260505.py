"""Overnight monitor for Loop10 FPB_VALZ HMM QE experiments.

The script is idempotent and intended to be called by Windows Task Scheduler
every 40 minutes.  It checks the active QE task, saves progress snapshots, runs
diagnostics when a stage completes, and conditionally launches one focused
stage-2 QE task if a newly registered bottom-penalty candidate beats Loop10.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[2]
WORK_DIR = ROOT / ".codex_tmp" / "hmm_l10_bottom_penalty_qe_20260505"
STATE_PATH = WORK_DIR / "overnight_monitor_state.json"
LOCK_PATH = WORK_DIR / "overnight_monitor.lock"
LOG_PATH = WORK_DIR / "overnight_monitor.log"
BASE_PAYLOAD_PATH = ROOT / ".codex_tmp" / "hmm_utility_aggressive_custom_evo_payload_dev8011_retry1_20260504.json"
REG_SCRIPT = ROOT / "scripts" / "register_hmm_loop10_bottom_penalty_candidates_20260505.py"
DIAG_SCRIPT = ROOT / "scripts" / "qe_evolution_diagnostic.py"
API_BASE_8001 = "http://127.0.0.1:8001/api/v1"
API_BASE_8011 = "http://127.0.0.1:8011/api/v1"
NODE_ID = "rdagent-node1"
LOOP10_SNAPSHOT_ID = "6ea64754-003d-48d8-ad9e-d0e7857716c8"
TERMINAL_STATUSES = {"completed", "failed", "cancelled", "canceled", "paused"}


def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def log(message: str) -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    line = f"{datetime.now().isoformat(timespec='seconds')} {message}"
    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state(initial_task_id: str | None, task_name: str | None) -> dict[str, Any]:
    if STATE_PATH.is_file():
        return read_json(STATE_PATH)
    if not initial_task_id:
        raise RuntimeError("--initial-task-id is required for the first monitor run")
    state = {
        "created_at": datetime.now().isoformat(),
        "active_task_id": initial_task_id,
        "stage": 1,
        "finished": False,
        "task_scheduler_name": task_name,
        "tasks": [{"stage": 1, "task_id": initial_task_id}],
        "analysis": [],
    }
    write_json(STATE_PATH, state)
    return state


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = datetime.now().isoformat()
    write_json(STATE_PATH, state)


def acquire_lock() -> bool:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.close(fd)
        return True
    except FileExistsError:
        age = time.time() - LOCK_PATH.stat().st_mtime
        if age > 2 * 60 * 60:
            LOCK_PATH.unlink(missing_ok=True)
            return acquire_lock()
        return False


def release_lock() -> None:
    LOCK_PATH.unlink(missing_ok=True)


def api_get_task(task_id: str, api_base: str = API_BASE_8001) -> dict[str, Any]:
    resp = requests.get(f"{api_base}/quantevolver/evolution/tasks/{task_id}", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload


def post_custom_task(payload: dict[str, Any]) -> dict[str, Any]:
    resp = requests.post(f"{API_BASE_8011}/quantevolver/evolution/custom-tasks", json=payload, timeout=120)
    data = resp.json()
    resp.raise_for_status()
    return data


def loop_config(loop: dict[str, Any]) -> dict[str, Any]:
    cfg = loop.get("config_json") or {}
    if isinstance(cfg, str):
        cfg = json.loads(cfg)
    return cfg


def loop_snapshot_id(cfg: dict[str, Any]) -> str | None:
    snapshot_id = cfg.get("hmm_model_version_id")
    model_params = cfg.get("model_params") or {}
    if snapshot_id is None and isinstance(model_params, dict):
        snapshot_id = model_params.get("hmm_model_version_id")
    return snapshot_id


def extract_summary(task: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for loop in sorted(task.get("loops") or [], key=lambda item: int(item.get("loop_index") or 0)):
        cfg = loop_config(loop)
        metrics = loop.get("metrics_json") or {}
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        rows.append(
            {
                "loop_index": int(loop.get("loop_index") or 0),
                "status": loop.get("status"),
                "label": cfg.get("label"),
                "snapshot_id": loop_snapshot_id(cfg),
                "annualized_return": metrics.get("annualized_return")
                or metrics.get("1day.excess_return_with_cost.annualized_return"),
                "sharpe": metrics.get("sharpe")
                or metrics.get("1day.excess_return_with_cost.information_ratio"),
                "max_drawdown": metrics.get("max_drawdown")
                or metrics.get("1day.excess_return_with_cost.max_drawdown"),
                "final_nav": metrics.get("final_nav"),
            }
        )
    return pd.DataFrame(rows)


def run_qe_diagnostic(task_id: str, out_dir: Path) -> Path | None:
    if not DIAG_SCRIPT.is_file():
        return None
    out_path = out_dir / f"{task_id}_qe_diag.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(DIAG_SCRIPT),
            task_id,
            "--json",
            "--api-base",
            API_BASE_8001,
        ],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=300,
    )
    out_path.write_text(proc.stdout, encoding="utf-8")
    if proc.returncode != 0:
        (out_dir / f"{task_id}_qe_diag_error.txt").write_text(proc.stderr, encoding="utf-8")
        log(f"diagnostic returned {proc.returncode} for {task_id}; stderr saved")
    return out_path


def analyze_task(task_id: str, stage: int) -> dict[str, Any]:
    out_dir = WORK_DIR / f"stage{stage}_{task_id}_analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    task = api_get_task(task_id)
    write_json(out_dir / f"{task_id}_detail.json", {"status": "success", "data": task})
    summary = extract_summary(task)
    summary_path = out_dir / f"{task_id}_loop_summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    diag_path = run_qe_diagnostic(task_id, out_dir)

    completed = summary[summary["status"].eq("completed")].copy()
    loop10 = completed[
        completed["snapshot_id"].eq(LOOP10_SNAPSHOT_ID)
        | completed["label"].fillna("").str.contains("LOOP10_BASE", regex=False)
    ].head(1)
    candidates = completed[completed["label"].fillna("").str.contains("BOTTOM", regex=False)].copy()
    if loop10.empty:
        loop10_row: dict[str, Any] | None = None
    else:
        loop10_row = loop10.iloc[0].to_dict()
    valuable = pd.DataFrame()
    if loop10_row and not candidates.empty:
        ann0 = float(loop10_row["annualized_return"])
        sharpe0 = float(loop10_row["sharpe"])
        # Preserve candidates that beat Loop10, or are nearly flat with better Sharpe.
        valuable = candidates[
            (candidates["annualized_return"].astype(float) > ann0)
            | (
                (candidates["annualized_return"].astype(float) >= ann0 - 0.002)
                & (candidates["sharpe"].astype(float) > sharpe0)
            )
        ].copy()
    valuable_path = out_dir / "valuable_hmm_models.csv"
    valuable.to_csv(valuable_path, index=False, encoding="utf-8-sig")

    best_row: dict[str, Any] | None = None
    if not candidates.empty:
        best_row = candidates.sort_values(["annualized_return", "sharpe"], ascending=False).iloc[0].to_dict()
    report = {
        "task_id": task_id,
        "stage": stage,
        "task_status": task.get("status"),
        "summary_path": str(summary_path),
        "diagnostic_path": str(diag_path) if diag_path else None,
        "valuable_path": str(valuable_path),
        "loop10": loop10_row,
        "best_candidate": best_row,
        "valuable_count": int(len(valuable)),
    }
    write_json(out_dir / "analysis_report.json", report)
    log(f"analysis saved for {task_id}: {out_dir}")
    return report


def stage2_specs(best_label: str, *, fallback: bool = False) -> list[dict[str, Any]]:
    if fallback:
        return [
            {
                "display_name": "HMM_TEST_L10_FPBVALZ_BOTTOM15_PENALTY_0p985__qe20260505_stage2",
                "variant_name": "l10_fpbvalz_bottom15_penalty_0p985_stage2",
                "virtual_coeff_filename": "VIRT_L10_FPB_VALZ_BOTTOMP15_PENALTY_0p985.json",
                "hypothesis": "Fallback stage2: milder Loop10 plus bottom 15% FPB_VALZ sparse penalty at 0.985; no boost.",
            },
            {
                "display_name": "HMM_TEST_L10_FPBVALZ_BOTTOM20_PENALTY_0p985__qe20260505_stage2",
                "variant_name": "l10_fpbvalz_bottom20_penalty_0p985_stage2",
                "virtual_coeff_filename": "VIRT_L10_FPB_VALZ_BOTTOMP20_PENALTY_0p985.json",
                "hypothesis": "Fallback stage2: milder Loop10 plus bottom 20% FPB_VALZ sparse penalty at 0.985; no boost.",
            },
            {
                "display_name": "HMM_TEST_L10_VOLCOMP_BOTTOM15_PENALTY_0p99__qe20260505_stage2",
                "variant_name": "l10_volcomp_bottom15_penalty_0p99_stage2",
                "virtual_coeff_filename": "VIRT_L10_VOLCOMP_BOTTOMP15_PENALTY_0p99.json",
                "hypothesis": "Fallback stage2: different vol-compression source, bottom 15% sparse penalty at 0.99; no boost.",
            },
            {
                "display_name": "HMM_TEST_L10_VOLCOMP_RISKONLY_0p995__qe20260505_stage2",
                "variant_name": "l10_volcomp_riskonly_0p995_stage2",
                "virtual_coeff_filename": "VIRT_L10_VOLCOMP_RISKONLY_0p995.json",
                "hypothesis": "Fallback stage2: vol-compression risk-only overlay with very mild 0.995 penalty; no boost.",
            },
        ]
    match = re.search(r"BOTTOM(\d+)", best_label or "")
    best_pct = int(match.group(1)) if match else 20
    if best_pct <= 15:
        pcts = [10, 15, 20]
    elif best_pct >= 25:
        pcts = [15, 20, 25]
    else:
        pcts = [15, 20, 25]
    specs = []
    for pct in pcts:
        specs.append(
            {
                "display_name": f"HMM_TEST_L10_FPBVALZ_BOTTOM{pct}_PENALTY_0p985__qe20260505_stage2",
                "variant_name": f"l10_fpbvalz_bottom{pct}_penalty_0p985_stage2",
                "virtual_coeff_filename": f"VIRT_L10_FPB_VALZ_BOTTOMP{pct}_PENALTY_0p985.json",
                "hypothesis": f"Stage2 sensitivity: Loop10 plus bottom {pct}% FPB_VALZ sparse penalty at 0.985; no boost.",
            }
        )
    return specs


def register_stage2_candidates(specs: list[dict[str, Any]], stage_dir: Path) -> list[dict[str, Any]]:
    spec_path = stage_dir / f"stage2_candidate_specs_{now_tag()}.json"
    write_json(spec_path, {"candidates": specs})
    response_path = stage_dir / f"stage2_registry_response_{now_tag()}.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(REG_SCRIPT),
            "--candidates-json",
            str(spec_path),
        ],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=300,
    )
    response_path.write_text(proc.stdout, encoding="utf-8")
    if proc.returncode != 0:
        (stage_dir / "stage2_registry_error.txt").write_text(proc.stderr, encoding="utf-8")
        raise RuntimeError(f"stage2 candidate registration failed: {proc.returncode}")
    data = json.loads(proc.stdout)
    return data["registered"]


def base_loop_templates() -> tuple[dict[str, Any], dict[str, Any]]:
    payload = read_json(BASE_PAYLOAD_PATH)
    by_label = {loop["label"]: loop for loop in payload["loops"]}
    return (
        dict(by_label["NO_HMM__qe_20260502_131502_9b54_Loop1_replica"]),
        dict(by_label["LOOP10_BASE__penalty_only_f096"]),
    )


def make_hmm_loop(template: dict[str, Any], label: str, snapshot_id: str) -> dict[str, Any]:
    loop = dict(template)
    loop.pop("loop_index", None)
    loop["label"] = label
    loop["node_id"] = NODE_ID
    loop["enable_sector_hmm"] = True
    loop["hmm_signal_preset"] = "preset_A"
    loop["hmm_model_version_id"] = snapshot_id
    loop["backtest_only"] = False
    return loop


def launch_stage2(best: dict[str, Any], state: dict[str, Any], *, fallback: bool = False) -> str:
    stage_dir = WORK_DIR / f"stage2_launch_{now_tag()}"
    stage_dir.mkdir(parents=True, exist_ok=True)
    specs = stage2_specs(str(best.get("label") or ""), fallback=fallback)
    registered = register_stage2_candidates(specs, stage_dir)
    reg_by_variant = {row.get("variant_name"): row for row in registered}
    no_hmm, loop10_template = base_loop_templates()
    no_hmm.pop("loop_index", None)
    no_hmm["node_id"] = NODE_ID
    no_hmm["label"] = "NO_HMM__stage2_control"

    loops = [
        no_hmm,
        make_hmm_loop(loop10_template, "LOOP10_BASE__stage2_control", LOOP10_SNAPSHOT_ID),
    ]
    best_snapshot = best.get("snapshot_id") if isinstance(best, dict) else None
    if best_snapshot:
        loops.append(
            make_hmm_loop(
                loop10_template,
                f"STAGE1_BEST__{best.get('label')}",
                str(best_snapshot),
            )
        )
    for spec in specs:
        row = reg_by_variant[spec["variant_name"]]
        loops.append(make_hmm_loop(loop10_template, spec["display_name"].replace("HMM_TEST_", ""), row["snapshot_id"]))
    payload = {
        "task_name": (
            f"HMM_L10_FPBVALZ_stage2_fallback_remote_p2_{now_tag()}"
            if fallback
            else f"HMM_L10_FPBVALZ_bottom_penalty_stage2_remote_p2_{now_tag()}"
        ),
        "target_desc": (
            "Automatically launched fallback stage-2 QE because stage-1 did not beat Loop10. "
            "Tests other Loop10-centered optimization directions: milder FPB_VALZ penalties and "
            "VOLCOMP defensive overlays. Parallelism remains 2."
            if fallback
            else "Automatically launched stage-2 QE after stage-1 Loop10-centered HMM screen completed. "
            "Tests 0.985 sparse bottom-sector penalty sensitivity around the best stage-1 FPB_VALZ candidate. "
            "Parallelism remains 2."
        ),
        "loops": loops,
        "execution_mode": "parallel_2",
        "node_id": NODE_ID,
        "node_parallelism": {NODE_ID: 2},
        "engine_mode": "unified",
        "clone_from_task_id": state.get("active_task_id"),
    }
    payload_path = stage_dir / f"stage2_custom_evo_payload_{now_tag()}.json"
    write_json(payload_path, payload)
    response = post_custom_task(payload)
    response_path = stage_dir / f"stage2_submit_response_{now_tag()}.json"
    write_json(response_path, response)
    task_id = response["task_id"]
    log(f"stage2 task launched: {task_id}; fallback={fallback}")
    return task_id


def maybe_disable_task(task_name: str | None) -> None:
    if not task_name:
        return
    subprocess.run(["schtasks", "/Change", "/TN", task_name, "/DISABLE"], text=True, capture_output=True)
    log(f"requested scheduler disable for {task_name}")


def run_once(initial_task_id: str | None, task_name: str | None) -> None:
    if not acquire_lock():
        log("another monitor instance is active; exiting")
        return
    try:
        state = load_state(initial_task_id, task_name)
        if state.get("finished"):
            log("state is already finished; exiting")
            maybe_disable_task(state.get("task_scheduler_name"))
            return
        active_task_id = state["active_task_id"]
        task = api_get_task(active_task_id)
        status = str(task.get("status") or "").lower()
        progress = f"{task.get('current_loop')}/{task.get('max_loops')}"
        snapshot_path = WORK_DIR / f"progress_{active_task_id}_{now_tag()}.json"
        write_json(snapshot_path, {"status": "success", "data": task})
        log(f"checked {active_task_id}: status={status}, progress={progress}")
        state["last_check"] = {
            "task_id": active_task_id,
            "status": status,
            "progress": progress,
            "snapshot_path": str(snapshot_path),
            "checked_at": datetime.now().isoformat(),
        }
        if status not in TERMINAL_STATUSES:
            save_state(state)
            return

        stage = int(state.get("stage") or 1)
        analyzed_key = f"stage{stage}_analyzed"
        if not state.get(analyzed_key):
            report = analyze_task(active_task_id, stage)
            state["analysis"].append(report)
            state[analyzed_key] = True
            save_state(state)
        else:
            report = state["analysis"][-1]

        if status != "completed":
            state["finished"] = True
            state["finish_reason"] = f"active task ended with status={status}"
            save_state(state)
            maybe_disable_task(state.get("task_scheduler_name"))
            return

        if stage == 1:
            best = report.get("best_candidate")
            loop10 = report.get("loop10")
            valuable = int(report.get("valuable_count") or 0)
            should_launch = False
            if best and loop10 and valuable > 0:
                should_launch = float(best["annualized_return"]) > float(loop10["annualized_return"])
            if should_launch:
                next_task_id = launch_stage2(best, state, fallback=False)
                state["active_task_id"] = next_task_id
                state["stage"] = 2
                state["tasks"].append({"stage": 2, "task_id": next_task_id})
                state["stage2_launched_at"] = datetime.now().isoformat()
                state["stage2_reason"] = "stage1 candidate beat Loop10; launching focused 0.985 sensitivity stage"
                save_state(state)
                return
            if best:
                next_task_id = launch_stage2(best, state, fallback=True)
                state["active_task_id"] = next_task_id
                state["stage"] = 2
                state["tasks"].append({"stage": 2, "task_id": next_task_id})
                state["stage2_launched_at"] = datetime.now().isoformat()
                state["stage2_reason"] = "stage1 did not beat Loop10; launching fallback directions"
                save_state(state)
                return
            state["finished"] = True
            state["finish_reason"] = "stage1 completed but no candidate rows were available for fallback stage2"
            save_state(state)
            maybe_disable_task(state.get("task_scheduler_name"))
            return

        state["finished"] = True
        state["finish_reason"] = "stage2 completed and analyzed"
        save_state(state)
        maybe_disable_task(state.get("task_scheduler_name"))
    finally:
        release_lock()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initial-task-id", default=None)
    parser.add_argument("--task-scheduler-name", default=None)
    args = parser.parse_args()
    run_once(args.initial_task_id, args.task_scheduler_name)


if __name__ == "__main__":
    main()
