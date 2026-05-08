"""Persistence helpers for event-signal validation results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def load_validation_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _profile_config_hash(conn: Any, profile_id: Optional[str], fallback: str) -> str:
    if not profile_id:
        return fallback
    with conn.cursor() as cur:
        cur.execute(
            "SELECT config_hash FROM market.event_signal_policy_profile WHERE profile_id = %s",
            (profile_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row and row[0] else fallback


def save_validation_result(
    conn: Any,
    payload: dict[str, Any],
    *,
    report_path: Optional[str] = None,
    json_path: Optional[str] = None,
) -> None:
    """Upsert one event_signal_validation_result row from a validation payload."""

    profile_id = payload.get("profile_id")
    policy_config_hash = _profile_config_hash(conn, profile_id, str(payload.get("policy_config_hash") or "unknown"))
    artifact_paths = dict(payload.get("artifact_paths") or {})
    if json_path or payload.get("json_path"):
        artifact_paths["json"] = json_path or payload.get("json_path")
    if report_path or payload.get("report_path"):
        artifact_paths["markdown"] = report_path or payload.get("report_path")
    candidate_scope = payload.get("candidate_signal_scope") or {
        "profile_id": profile_id,
        "validation_mode": payload.get("validation_mode"),
        "simulator_version": payload.get("simulator_version"),
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.event_signal_validation_result
                (
                    validation_key, profile_id, effect_rule_id, candidate_signal_scope,
                    experiment_id, loop_id, loop_path, validation_mode, simulator_version,
                    time_mode, date_from, date_to, policy_config_hash, input_snapshot,
                    baseline_metrics, overlay_metrics, delta_metrics, hit_stats,
                    acceptance_gates, decision, decision_reason, report_path,
                    artifact_paths, validated_at, updated_at
                )
            VALUES
                (
                    %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s::jsonb, %s, %s, %s, %s::jsonb, NOW(), NOW()
                )
            ON CONFLICT (validation_key) DO UPDATE SET
                profile_id = EXCLUDED.profile_id,
                effect_rule_id = EXCLUDED.effect_rule_id,
                candidate_signal_scope = EXCLUDED.candidate_signal_scope,
                experiment_id = EXCLUDED.experiment_id,
                loop_id = EXCLUDED.loop_id,
                loop_path = EXCLUDED.loop_path,
                validation_mode = EXCLUDED.validation_mode,
                simulator_version = EXCLUDED.simulator_version,
                time_mode = EXCLUDED.time_mode,
                date_from = EXCLUDED.date_from,
                date_to = EXCLUDED.date_to,
                policy_config_hash = EXCLUDED.policy_config_hash,
                input_snapshot = EXCLUDED.input_snapshot,
                baseline_metrics = EXCLUDED.baseline_metrics,
                overlay_metrics = EXCLUDED.overlay_metrics,
                delta_metrics = EXCLUDED.delta_metrics,
                hit_stats = EXCLUDED.hit_stats,
                acceptance_gates = EXCLUDED.acceptance_gates,
                decision = EXCLUDED.decision,
                decision_reason = EXCLUDED.decision_reason,
                report_path = EXCLUDED.report_path,
                artifact_paths = EXCLUDED.artifact_paths,
                validated_at = NOW(),
                updated_at = NOW()
            """,
            (
                payload["validation_key"],
                profile_id,
                payload.get("effect_rule_id"),
                _json_dumps(candidate_scope),
                payload["experiment_id"],
                payload["loop_id"],
                str(payload.get("loop_path") or ""),
                payload.get("validation_mode") or "stacked_profile",
                payload.get("simulator_version") or "unknown",
                payload.get("time_mode") or "backtest",
                payload["date_from"],
                payload["date_to"],
                policy_config_hash,
                _json_dumps(payload.get("input_snapshot") or {}),
                _json_dumps(payload.get("baseline_metrics") or {}),
                _json_dumps(payload.get("overlay_metrics") or {}),
                _json_dumps(payload.get("delta_metrics") or {}),
                _json_dumps(payload.get("hit_stats") or {}),
                _json_dumps(payload.get("acceptance_gates") or {}),
                payload.get("decision") or "REVIEW",
                payload.get("decision_reason") or "",
                report_path or payload.get("report_path"),
                _json_dumps(artifact_paths),
            ),
        )
