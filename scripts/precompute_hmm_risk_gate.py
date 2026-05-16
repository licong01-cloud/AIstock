#!/usr/bin/env python3
"""Precompute HMM sector risk gate artifact from existing trained models.

Reads a JSON payload from stdin (same DB/model params as precompute_hmm_coefficients.py),
runs causal forward-filter inference on each sector, and outputs a risk gate artifact
that marks sectors as "blocked" when in fading state with high confidence.

This script does NOT retrain HMM models — it reuses existing old covfix models.

Usage (WSL):
    echo '{"model_path": "...", "test_start": "2024-07-01", ...}' | \
        python scripts/precompute_hmm_risk_gate.py --output-path /path/to/output.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from precompute_hmm_coefficients import (
    build_legacy_observations,
    confidence_from_posterior,
    forward_filter_posteriors,
    parse_stdin,
    restore_hmm,
)


def compute_risk_gates(
    posteriors: np.ndarray,
    dates_out: list[date],
    state_labels: dict[str, str],
    start_d: date,
    end_d: date,
    confidence_threshold: float,
    confidence_scale: float,
    trigger_confidence_threshold: float,
    trigger_duration_days: int,
) -> tuple[dict[str, dict], list[dict]]:
    """Compute daily gate decisions and state-transition triggers for one sector.

    Design insight: 3-state HMM produces ~41% fading days with near-1.0 confidence,
    so static "fading + high confidence" gates block too many sectors. Instead, we use
    a TRANSITION-BASED gate: only block when a sector ENTERS fading state (transition
    from trending/neutral to fading), and the block expires after trigger_duration_days.

    Returns:
        (daily_gate_entries, trigger_entries)
        daily_gate_entries: {date_iso: {state, confidence, blocked, block_reason}}
        trigger_entries: [{date_iso, transition, confidence, expires_date}]
    """
    states = posteriors.argmax(axis=1)
    daily_gates: dict[str, dict] = {}
    triggers: list[dict] = []

    prev_label: str | None = None
    active_block_expires: date | None = None

    for i, td in enumerate(dates_out):
        prob = posteriors[i]
        state_idx = int(states[i])
        label = state_labels.get(str(state_idx), "neutral")
        conf = confidence_from_posterior(prob, confidence_scale)

        if td < start_d:
            prev_label = label
            continue
        if td > end_d:
            break

        blocked = False
        block_reason = None

        # Trigger: state transition INTO fading
        if prev_label is not None and label == "fading" and prev_label != "fading":
            transition = f"{prev_label}_to_{label}"
            expire_idx = min(i + trigger_duration_days, len(dates_out) - 1)
            expires_date = dates_out[expire_idx]
            triggers.append({
                "date": td.isoformat(),
                "transition": transition,
                "confidence": round(conf, 4),
                "trigger_type": "block",
                "expires_date": expires_date.isoformat(),
            })
            active_block_expires = expires_date
            blocked = True
            block_reason = "transition_to_fading"

        # Continue block if within trigger duration
        elif active_block_expires is not None and td <= active_block_expires and label == "fading":
            blocked = True
            block_reason = "active_trigger"

        # Clear expired block
        elif active_block_expires is not None and td > active_block_expires:
            active_block_expires = None

        daily_gates[td.isoformat()] = {
            "state": label,
            "confidence": round(conf, 4),
            "blocked": blocked,
            "block_reason": block_reason,
        }

        prev_label = label

    return daily_gates, triggers


def main() -> None:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    parser = argparse.ArgumentParser(description="Precompute HMM risk gate artifact")
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--confidence-threshold", type=float, default=None)
    parser.add_argument("--trigger-confidence-threshold", type=float, default=None)
    parser.add_argument("--trigger-duration-days", type=int, default=None)
    args, _ = parser.parse_known_args()

    params = parse_stdin()
    model_path = params["model_path"]
    test_start = params["test_start"]
    backtest_end = params["backtest_end"]

    confidence_threshold = args.confidence_threshold or params.get("gate_confidence_threshold", 0.25)
    confidence_scale = params.get("confidence_scale", 0.30)
    trigger_confidence_threshold = (
        args.trigger_confidence_threshold or params.get("trigger_confidence_threshold", 0.50)
    )
    trigger_duration_days = (
        args.trigger_duration_days or params.get("trigger_duration_days", 5)
    )

    db_host = params.get("db_host", "127.0.0.1")
    db_port = params.get("db_port", 5432)
    db_name = params.get("db_name", "aistock")
    db_user = params.get("db_user", "postgres")
    db_password = params.get("db_password") or ""

    preset_key = params.get("preset_key", "risk_gate_v1")

    print(f"HMM risk gate precompute: model={model_path}", file=sys.stderr)
    print(f"  date range: {test_start} ~ {backtest_end}", file=sys.stderr)
    print(f"  confidence_threshold={confidence_threshold}", file=sys.stderr)
    print(f"  trigger_confidence_threshold={trigger_confidence_threshold}", file=sys.stderr)
    print(f"  trigger_duration_days={trigger_duration_days}", file=sys.stderr)

    with open(model_path, "r", encoding="utf-8") as f:
        models = json.load(f)
    if not models:
        print(f"ERROR: empty HMM model file: {model_path}", file=sys.stderr)
        sys.exit(1)
    print(f"  loaded {len(models)} sector models", file=sys.stderr)

    first = next(iter(models.values()))
    n_features = len(first.get("means", [[]])[0]) if first.get("means") else 4
    rolling_window = int(first.get("rolling_window", 5))
    has_zscore = "zscore_mean" in first
    zscore_mean = np.asarray(first["zscore_mean"], dtype=np.float64) if has_zscore else None
    zscore_std = np.asarray(first["zscore_std"], dtype=np.float64) if has_zscore else None

    hmm_objs: dict[str, tuple[Any, dict[str, str], dict[str, Any]]] = {}
    for code, info in models.items():
        try:
            labels = info.get("state_labels")
            if not isinstance(labels, dict) or not labels:
                raise KeyError("state_labels required for risk gate (legacy model only)")
            hmm_objs[code] = (restore_hmm(info), labels, info)
        except Exception as exc:
            print(f"  WARNING: failed to restore HMM {code}: {exc}", file=sys.stderr)
    if not hmm_objs:
        print("ERROR: all HMM models failed to restore", file=sys.stderr)
        sys.exit(1)
    print(f"  restored {len(hmm_objs)}/{len(models)} HMM models", file=sys.stderr)

    start_d = date.fromisoformat(test_start)
    end_d = date.fromisoformat(backtest_end)
    history_start = start_d - timedelta(days=int(3.0 * 365 + 30))

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password,
    )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)
    print("  loading DB data...", file=sys.stderr)

    cur.execute(
        """
        SELECT DISTINCT ON (m.l2_code, sd.trade_date)
               m.l2_code AS sector_code, m.l2_name AS sector_name, sd.trade_date,
               sd.sw2_pct_change, sd.sw2_vol, sd.sw2_amount,
               sd.sw2_mf_net_amt, sd.sw2_mf_buy_elg_amt, sd.sw2_mf_sell_elg_amt,
               sd.ts_code
        FROM market.sector_data sd
        JOIN market.sw_index_member m ON sd.ts_code = m.ts_code
        WHERE sd.trade_date BETWEEN %s AND %s
          AND m.in_date <= sd.trade_date
          AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)
        ORDER BY m.l2_code, sd.trade_date, sd.ts_code
        """,
        (history_start, end_d),
    )
    all_sector_rows = cur.fetchall()

    sector_data: dict[str, dict[date, dict[str, Any]]] = {}
    for row in all_sector_rows:
        code = str(row["sector_code"])
        td = row["trade_date"]
        sector_data.setdefault(code, {})[td] = row

    cur.execute(
        """
        SELECT trade_date, pct_chg FROM market.index_daily
        WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
        """,
        (history_start, end_d),
    )
    csi300 = {r["trade_date"]: float(r["pct_chg"] or 0.0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT trade_date, SUM(vol) AS total_vol FROM market.sw_daily
        WHERE trade_date BETWEEN %s AND %s
        GROUP BY trade_date ORDER BY trade_date
        """,
        (history_start, end_d),
    )
    market_vol = {r["trade_date"]: float(r["total_vol"] or 0.0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT ts_code, l2_code FROM market.sw_index_member
        WHERE in_date <= %s AND (out_date IS NULL OR out_date >= %s)
        """,
        (end_d, start_d),
    )
    stock_sector_map = {
        r["ts_code"]: r["l2_code"]
        for r in cur.fetchall()
        if r["ts_code"] and r["l2_code"]
    }
    cur.close()
    conn.close()

    print(
        f"  loaded sectors={len(sector_data)}, CSI300={len(csi300)}, "
        f"market_vol={len(market_vol)}, stock_map={len(stock_sector_map)}",
        file=sys.stderr,
    )
    if not stock_sector_map:
        print("ERROR: empty stock-sector map", file=sys.stderr)
        sys.exit(1)

    # --- Decode sector states and compute gates ---
    print("  computing risk gates...", file=sys.stderr)
    all_daily_gates: dict[str, dict[str, dict]] = {}  # {date: {sector: gate_info}}
    all_daily_triggers: dict[str, list[dict]] = {}  # {date: [trigger_info]}
    sectors_processed = 0

    for idx, (code, (hmm, labels, info)) in enumerate(hmm_objs.items()):
        if code not in sector_data:
            continue

        sorted_dates = sorted(sector_data[code].keys())
        obs, dates_out = build_legacy_observations(
            sector_data[code], sorted_dates, csi300, market_vol, rolling_window, n_features
        )
        if len(obs) and zscore_mean is not None:
            obs = (obs - zscore_mean) / zscore_std

        if len(obs) < 20:
            continue
        expected_features = int(hmm.means_.shape[1])
        if obs.shape[1] != expected_features:
            print(
                f"  WARNING: feature mismatch {code}: obs={obs.shape[1]}, model={expected_features}",
                file=sys.stderr,
            )
            continue

        try:
            posteriors = forward_filter_posteriors(hmm, obs)
        except Exception as exc:
            print(f"  WARNING: forward filter failed {code}: {exc}", file=sys.stderr)
            continue

        daily_gates, triggers = compute_risk_gates(
            posteriors=posteriors,
            dates_out=dates_out,
            state_labels=labels,
            start_d=start_d,
            end_d=end_d,
            confidence_threshold=confidence_threshold,
            confidence_scale=confidence_scale,
            trigger_confidence_threshold=trigger_confidence_threshold,
            trigger_duration_days=trigger_duration_days,
        )

        for d_iso, gate_info in daily_gates.items():
            all_daily_gates.setdefault(d_iso, {})[code] = gate_info

        for trigger in triggers:
            all_daily_triggers.setdefault(trigger["date"], []).append(
                {**trigger, "sector_code": code}
            )

        sectors_processed += 1
        if (idx + 1) % 20 == 0:
            print(f"  processed {idx + 1}/{len(hmm_objs)} sectors", file=sys.stderr)

    if not all_daily_gates:
        print("ERROR: no risk gate data produced", file=sys.stderr)
        sys.exit(1)

    # --- Compute summary statistics ---
    total_days = len(all_daily_gates)
    blocked_counts = []
    for d_iso, sectors in all_daily_gates.items():
        blocked_counts.append(sum(1 for g in sectors.values() if g["blocked"]))
    avg_blocked = sum(blocked_counts) / len(blocked_counts) if blocked_counts else 0
    days_with_blocks = sum(1 for c in blocked_counts if c > 0)

    print(f"  sectors_processed={sectors_processed}", file=sys.stderr)
    print(f"  total_days={total_days}", file=sys.stderr)
    print(f"  avg_blocked_sectors_per_day={avg_blocked:.2f}", file=sys.stderr)
    print(f"  days_with_any_block={days_with_blocks}/{total_days}", file=sys.stderr)
    print(f"  total_triggers={sum(len(v) for v in all_daily_triggers.values())}", file=sys.stderr)

    # --- Build output artifact ---
    artifact = {
        "artifact_type": "hmm_risk_gate_v1",
        "model_path": model_path,
        "preset_key": preset_key,
        "test_start": test_start,
        "backtest_end": backtest_end,
        "sector_count": sectors_processed,
        "gate_config": {
            "confidence_threshold": confidence_threshold,
            "confidence_scale": confidence_scale,
            "fading_state_label": "fading",
            "block_new_buys_only": True,
            "soft_gate_multiplier": None,
            "trigger_confidence_threshold": trigger_confidence_threshold,
            "trigger_duration_days": trigger_duration_days,
        },
        "summary": {
            "total_days": total_days,
            "avg_blocked_sectors_per_day": round(avg_blocked, 2),
            "days_with_any_block": days_with_blocks,
            "total_triggers": sum(len(v) for v in all_daily_triggers.values()),
        },
        "daily_gates": all_daily_gates,
        "daily_triggers": all_daily_triggers,
        "stock_sector_map": stock_sector_map,
    }

    output_json = json.dumps(artifact, ensure_ascii=False, indent=None, separators=(",", ":"))

    if args.output_path:
        out_path = Path(args.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_json, encoding="utf-8")
        print(f"  written to {out_path} ({len(output_json)} bytes)", file=sys.stderr)
    else:
        sys.stdout.write(output_json)

    print("DONE", file=sys.stderr)


if __name__ == "__main__":
    main()
