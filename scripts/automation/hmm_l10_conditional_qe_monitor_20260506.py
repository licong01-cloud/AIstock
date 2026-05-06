"""Monitor Loop10 conditional-sparse HMM QE runs and auto-continue once.

This script is designed for Windows Task Scheduler.  Each invocation checks the
active QE task, writes progress evidence, analyzes terminal tasks, and, if the
first round does not beat Loop10, starts one narrower follow-up QE round.
"""

from __future__ import annotations

import json
import os
import csv
import re
import subprocess
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / ".codex_tmp" / "hmm_l10_conditional_sparse_qe_20260506"
STATE_PATH = STATE_DIR / "monitor_state.json"
LOG_PATH = STATE_DIR / "monitor.log"
LOCK_PATH = STATE_DIR / "monitor.lock"
API_BASE_DEFAULT = "http://127.0.0.1:8001/api/v1"
SOURCE_MODEL_TASK_ID = "qe_20260505_123035_bf80"
SOURCE_MODEL_LOOP_INDEX = 1
NODE_ID = "rdagent-node1"
MAX_QE_ROUNDS = 2


def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def log(message: str) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {message}"
    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def read_json(path: Path, default: Any) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def acquire_lock() -> bool:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
        return True
    except FileExistsError:
        age = time.time() - LOCK_PATH.stat().st_mtime
        if age > 6 * 3600:
            LOCK_PATH.unlink(missing_ok=True)
            return acquire_lock()
        return False


def release_lock() -> None:
    LOCK_PATH.unlink(missing_ok=True)


def api_get(api_base: str, path: str, *, timeout: int = 180) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(api_base + path, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - monitor must persist and log transient API timeouts.
            last_exc = exc
            log(f"GET retry {attempt + 1}/3 failed for {path}: {exc}")
            time.sleep(10)
    raise RuntimeError(f"GET failed for {path}: {last_exc}")


def api_post(api_base: str, path: str, payload: dict[str, Any], *, timeout: int = 180) -> dict[str, Any]:
    resp = requests.post(api_base + path, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def unwrap_task(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def metric_value(metrics: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = metrics.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def loop_rows(task: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for loop in sorted(task.get("loops") or [], key=lambda x: int(x.get("loop_index") or 0)):
        cfg = loop.get("config_json") or {}
        metrics = loop.get("metrics_json") or {}
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        params = cfg.get("model_params") or {}
        snapshot = cfg.get("hmm_model_version_id") or params.get("hmm_model_version_id")
        rows.append(
            {
                "loop_index": int(loop.get("loop_index") or 0),
                "label": cfg.get("label") or f"Loop{loop.get('loop_index')}",
                "status": loop.get("status"),
                "snapshot_id": snapshot or "",
                "annualized_return": metric_value(metrics, "annualized_return"),
                "sharpe": metric_value(metrics, "sharpe"),
                "max_drawdown": metric_value(metrics, "max_drawdown"),
                "IC": metric_value(metrics, "IC"),
                "Rank_IC": metric_value(metrics, "Rank_IC"),
            }
        )
    return rows


def run_subprocess(args: list[str], *, output_path: Path | None = None, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    log("RUN " + " ".join(args))
    proc = subprocess.run(
        args,
        cwd=str(ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
    )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(proc.stdout + ("\nSTDERR:\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"command failed {proc.returncode}: {' '.join(args)}\n{proc.stderr[-2000:]}")
    return proc


def save_remote_backtest_only_proof(task_id: str, out_dir: Path) -> None:
    script = f"""
import json
from pathlib import Path
base=Path('/home/lc999/projects/RD-Agent-main/qe_workspace/{task_id}')
source=Path('/home/lc999/projects/RD-Agent-main/qe_workspace/{SOURCE_MODEL_TASK_ID}/Loop{SOURCE_MODEL_LOOP_INDEX}/mlruns')
rows=[]
for i in range(1, 20):
    loop=base/f'Loop{{i}}'
    if not loop.exists():
        continue
    log=loop/'run.log'
    text=log.read_text(errors='ignore') if log.exists() else ''
    ml=loop/'mlruns'
    resolved=str(ml.resolve()) if ml.exists() else None
    rows.append({{
        'loop': i,
        'status': (loop/'status.txt').read_text(errors='ignore').strip() if (loop/'status.txt').exists() else None,
        'has_backtest_only_arg': '--backtest-only' in text,
        'has_symlink_mlruns': 'Symlink mlruns' in text,
        'has_skip_training': 'skipping model training' in text.lower(),
        'mlruns_is_symlink': ml.is_symlink(),
        'mlruns_target': resolved,
        'mlruns_target_matches_source': resolved == str(source.resolve()) if ml.exists() and source.exists() else False,
    }})
print(json.dumps(rows,ensure_ascii=False,indent=2))
"""
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "lc999@192.168.50.215", "python3", "-"],
            input=script,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=120,
        )
        payload = {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
        if proc.returncode == 0:
            payload["rows"] = json.loads(proc.stdout)
        write_json(out_dir / "remote_backtest_only_proof.json", payload)
    except Exception as exc:  # noqa: BLE001 - evidence failure should not stop analysis.
        write_json(out_dir / "remote_backtest_only_proof.json", {"error": str(exc)})


def analyze_task(api_base: str, task_id: str, round_no: int) -> dict[str, Any]:
    out_dir = STATE_DIR / f"analysis_round{round_no}_{task_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    task_payload = api_get(api_base, f"/quantevolver/evolution/tasks/{task_id}", timeout=240)
    task = unwrap_task(task_payload)
    write_json(out_dir / "task_detail_final.json", task_payload)
    rows = loop_rows(task)
    pd.DataFrame(rows).to_csv(out_dir / "loop_summary.csv", index=False, encoding="utf-8-sig")

    diag_path = out_dir / "qe_evolution_diagnostic.json"
    try:
        proc = run_subprocess(
            [sys.executable, "scripts/qe_evolution_diagnostic.py", task_id, "--json", "--api-base", api_base],
            output_path=diag_path,
            timeout=600,
        )
        try:
            write_json(out_dir / "qe_evolution_diagnostic_parsed.json", json.loads(proc.stdout))
        except json.JSONDecodeError:
            pass
    except Exception as exc:  # noqa: BLE001
        write_json(diag_path, {"error": str(exc)})

    save_remote_backtest_only_proof(task_id, out_dir)

    loop10 = next((r for r in rows if "LOOP10_BASE" in r["label"]), None)
    candidate_rows = [
        r for r in rows
        if r["annualized_return"] is not None
        and ("L10_TIGHTEN" in r["label"] or "HMM_AUTO" in r["label"])
    ]
    best = max([r for r in rows if r["annualized_return"] is not None], key=lambda x: x["annualized_return"], default=None)
    best_candidate = max(candidate_rows, key=lambda x: x["annualized_return"], default=None)
    loop10_ann = loop10["annualized_return"] if loop10 else None
    candidate_delta = (
        best_candidate["annualized_return"] - loop10_ann
        if best_candidate and loop10_ann is not None
        else None
    )
    result = {
        "task_id": task_id,
        "round": round_no,
        "status": task.get("status"),
        "best": best,
        "loop10": loop10,
        "best_candidate": best_candidate,
        "best_candidate_delta_vs_loop10": candidate_delta,
        "candidate_beats_loop10": bool(candidate_delta is not None and candidate_delta > 0),
        "analysis_dir": str(out_dir),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(out_dir / "analysis_summary.json", result)

    lines = [
        f"# HMM L10 Conditional QE Round {round_no} Analysis - {task_id}",
        "",
        f"- Task status: `{task.get('status')}`",
        f"- Best loop: `{best['label'] if best else 'NA'}` / ann={best['annualized_return'] if best else None}",
        f"- Loop10 ann: `{loop10_ann}`",
        f"- Best new candidate: `{best_candidate['label'] if best_candidate else 'NA'}` / delta_vs_loop10={candidate_delta}",
        "",
        "## Loop Summary",
        "",
        "```text",
        pd.DataFrame(rows).to_string(index=False),
        "```",
    ]
    (out_dir / "analysis_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result


def build_loop(template: dict[str, Any], idx: int, label: str, snapshot: str | None, desc: str) -> dict[str, Any]:
    loop = deepcopy(template)
    loop["loop_index"] = idx
    loop["label"] = label
    loop["candidate_description"] = desc
    loop["node_id"] = NODE_ID
    loop["backtest_only"] = True
    loop["model_source_task_id"] = SOURCE_MODEL_TASK_ID
    loop["model_source_loop_index"] = SOURCE_MODEL_LOOP_INDEX
    if snapshot:
        loop["enable_sector_hmm"] = True
        loop["hmm_signal_preset"] = "preset_A"
        loop["hmm_model_version_id"] = snapshot
    else:
        loop["enable_sector_hmm"] = False
        loop.pop("hmm_signal_preset", None)
        loop.pop("hmm_model_version_id", None)
    return loop


def create_followup_payload(
    base_payload_path: Path,
    candidate_specs: list[dict[str, Any]],
    registered_rows: list[dict[str, Any]],
    clone_from: str,
) -> dict[str, Any]:
    base_payload = read_json(base_payload_path, {})
    old = base_payload["loops"]
    rows_by_variant = {r["variant_name"]: r for r in registered_rows}
    loops = [
        build_loop(old[0], 1, "NO_HMM__bt_source_loop1_control", None, "No-HMM backtest-only control."),
        build_loop(old[1], 2, "LOOP10_BASE__penalty_only_f096__current_best", "6ea64754-003d-48d8-ad9e-d0e7857716c8", "Current best Loop10 baseline."),
        build_loop(old[2], 3, "LOOP2_BASE__old_covfix_w3_raw__drawdown_control", "bbec3863-fb67-445f-938e-66f092d18696", "Old Loop2 baseline."),
        build_loop(old[6], 4, "STAGE3_SPARSE_TL_B15_PEN_0p995__near_best_prev", "db001359-2ef4-4db3-8cab-c68bc1ea18b2", "Previous near-best sparse candidate."),
    ]
    for spec in candidate_specs:
        snap = rows_by_variant[spec["variant_name"]]["snapshot_id"]
        loops.append(
            build_loop(
                old[1],
                len(loops) + 1,
                "HMM_AUTO_" + spec["variant_name"].upper(),
                snap,
                spec["hypothesis"],
            )
        )
    return {
        "task_name": "HMM_l10_conditional_auto_next_remote_p4_" + now_tag(),
        "target_desc": "Auto follow-up backtest-only QE for Loop10 conditional-sparse HMM candidates.",
        "loops": loops,
        "execution_mode": "parallel_4",
        "node_id": NODE_ID,
        "node_parallelism": {NODE_ID: 4},
        "engine_mode": "unified",
        "clone_from_task_id": clone_from,
    }


def launch_followup(api_base: str, state: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any] | None:
    round_next = int(state.get("round", 1)) + 1
    if round_next > MAX_QE_ROUNDS:
        return None

    screen_root = STATE_DIR / "auto_next_screen"
    run_subprocess(
        [
            sys.executable,
            "scripts/hmm_loop10_conditional_sparse_screen_20260506.py",
            "--output-dir",
            str(screen_root),
            "--task-id",
            state["active_task_id"],
            "--pcts",
            "0.12",
            "0.15",
            "0.18",
            "--penalties",
            "0.998",
            "0.9975",
            "0.995",
            "--tighten-penalties",
            "0.9525",
            "0.955",
            "0.9575",
            "0.96",
            "--persist-days",
            "2",
            "--vote-thresholds",
            "2",
        ],
        output_path=STATE_DIR / f"auto_next_screen_round{round_next}.log",
        timeout=3600,
    )
    coeff_root = screen_root / state["active_task_id"] / "candidate_coefficients"
    # The screen is intentionally bounded; read only the ranked head needed to
    # choose the next QE candidates instead of loading every diagnostic row.
    ranked_rows: list[dict[str, Any]] = []
    with (screen_root / state["active_task_id"] / "conditional_sparse_holdout_ranked.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as fh:
        for idx, row in enumerate(csv.DictReader(fh)):
            if idx >= 200:
                break
            ranked_rows.append(dict(row))
    used = set(state.get("tested_candidate_names") or [])
    candidates: list[dict[str, Any]] = []
    for row in ranked_rows:
        name = str(row["candidate"])
        if row.get("family") != "tighten_existing_loop10" or name in used:
            continue
        filename = f"{name}.json"
        if not (coeff_root / filename).is_file():
            continue
        variant = "auto_" + re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        display = "HMM_AUTO_" + name + "__qe20260506"
        candidates.append(
            {
                "display_name": display[:180],
                "variant_name": variant,
                "virtual_coeff_filename": filename,
                "hypothesis": f"Auto next tighten candidate from script screen: {name}; score={row.get('robust_screen_score')}; 10d={row.get('net_mean_db_ret_10d')}",
            }
        )
        used.add(name)
        if len(candidates) >= 3:
            break
    if not candidates:
        log("No follow-up candidates selected.")
        return None

    candidates_path = STATE_DIR / f"auto_next_candidates_round{round_next}.json"
    write_json(candidates_path, {"candidates": candidates})
    proc = run_subprocess(
        [
            sys.executable,
            "scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py",
            "--candidates-json",
            str(candidates_path),
            "--coeff-root",
            str(coeff_root),
            "--source-qe-task",
            state["active_task_id"],
        ],
        output_path=STATE_DIR / f"auto_next_register_round{round_next}.json",
        timeout=900,
    )
    registry = json.loads(proc.stdout)
    payload = create_followup_payload(
        ROOT / ".codex_tmp/hmm_stage3_sparse_qe_20260505/payload.json",
        candidates,
        registry["registered"],
        state["active_task_id"],
    )
    payload_path = STATE_DIR / f"auto_next_payload_round{round_next}.json"
    write_json(payload_path, payload)
    response = api_post(api_base, "/quantevolver/evolution/custom-tasks", payload, timeout=240)
    write_json(STATE_DIR / f"auto_next_create_response_round{round_next}.json", response)
    task_id = response.get("task_id")
    if not task_id:
        match = re.search(r"qe_\d{8}_\d{6}_[0-9a-f]+", json.dumps(response))
        task_id = match.group(0) if match else None
    if not task_id:
        raise RuntimeError(f"follow-up task id not found: {response}")
    return {
        "active_task_id": task_id,
        "round": round_next,
        "analysis_completed": False,
        "auto_next_launched_from": analysis["task_id"],
        "tested_candidate_names": sorted(used),
    }


def main() -> None:
    if not acquire_lock():
        log("Another monitor invocation is still running; exiting.")
        return
    try:
        state = read_json(STATE_PATH, {})
        api_base = state.get("api_base") or API_BASE_DEFAULT
        task_id = state["active_task_id"]
        round_no = int(state.get("round", 1))
        payload = api_get(api_base, f"/quantevolver/evolution/tasks/{task_id}", timeout=240)
        task = unwrap_task(payload)
        progress_path = STATE_DIR / f"progress_{task_id}_{now_tag()}.json"
        write_json(progress_path, payload)
        status = str(task.get("status") or "").lower()
        log(f"task={task_id} round={round_no} status={status} current={task.get('current_loop')}/{task.get('max_loops')}")
        if status not in {"completed", "failed", "cancelled", "paused"}:
            return
        if state.get("analysis_completed") and state.get("last_analyzed_task_id") == task_id:
            log("Task already analyzed.")
            return
        analysis = analyze_task(api_base, task_id, round_no)
        state["analysis_completed"] = True
        state["last_analyzed_task_id"] = task_id
        state.setdefault("analyses", []).append(analysis)
        write_json(STATE_PATH, state)

        if status == "completed" and not analysis.get("candidate_beats_loop10") and round_no < MAX_QE_ROUNDS:
            update = launch_followup(api_base, state, analysis)
            if update:
                state.update(update)
                write_json(STATE_PATH, state)
                log(f"launched follow-up task {state['active_task_id']} round={state['round']}")
        else:
            log("No follow-up launched.")
    finally:
        release_lock()


if __name__ == "__main__":
    main()
