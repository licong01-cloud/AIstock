"""Upgrade exact legacy QE cutoff defaults on unstarted runnable records.

Dry-run is the default.  Completed, failed, interrupted, and running records
are never selected.  Intentional custom windows are preserved because only
known system-default date pairs with the canonical base split are upgraded.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.quantevolver.config_composer import (  # noqa: E402
    QE_DEFAULT_BACKTEST_END,
    QE_DEFAULT_SIGNAL_END,
)


CANONICAL_BASE_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
}
LEGACY_DEFAULT_MARKERS = {
    ("2026-04-28", "2026-04-27"),
    ("2026-03-10", "2026-03-09"),
    ("2026-03-10", None),
    ("2025-12-01", None),
}
UNSTARTED_EXPERIMENT_STATUSES = (
    "created",
    "draft",
    "pending",
    "queued",
    "ready",
    "submitted",
)
RUNNABLE_TEMPLATE_STATUSES = ("draft", "approved", "run_requested")


@dataclass(frozen=True)
class PlannedUpdate:
    record_id: str
    status: str
    old_value: dict[str, Any]
    new_value: dict[str, Any]


def upgrade_data_split(value: Any) -> tuple[Any, int]:
    """Return a copy with one exact legacy system split upgraded."""

    if not isinstance(value, dict):
        return value, 0
    if any(value.get(key) != expected for key, expected in CANONICAL_BASE_SPLIT.items()):
        return value, 0
    marker = (value.get("test_end"), value.get("backtest_end"))
    if marker not in LEGACY_DEFAULT_MARKERS:
        return value, 0
    upgraded = dict(value)
    upgraded["test_end"] = QE_DEFAULT_SIGNAL_END
    upgraded["backtest_end"] = QE_DEFAULT_BACKTEST_END
    return upgraded, 1


def upgrade_nested_config(value: Any) -> tuple[Any, int]:
    """Recursively upgrade explicit data_split objects in template config."""

    if isinstance(value, list):
        changed = 0
        result = []
        for item in value:
            next_item, item_changed = upgrade_nested_config(item)
            result.append(next_item)
            changed += item_changed
        return result, changed
    if not isinstance(value, dict):
        return value, 0

    direct, direct_changed = upgrade_data_split(value)
    if direct_changed:
        return direct, direct_changed

    result = copy.deepcopy(value)
    changed = 0
    for key, item in value.items():
        next_item, item_changed = upgrade_nested_config(item)
        result[key] = next_item
        changed += item_changed
    return result, changed


def plan_updates(
    experiments: Iterable[tuple[str, str, dict[str, Any]]],
    templates: Iterable[tuple[str, str, dict[str, Any]]],
) -> tuple[list[PlannedUpdate], list[PlannedUpdate]]:
    experiment_updates: list[PlannedUpdate] = []
    template_updates: list[PlannedUpdate] = []
    for record_id, status, data_split in experiments:
        upgraded, changed = upgrade_data_split(data_split)
        if changed:
            experiment_updates.append(
                PlannedUpdate(record_id, status, data_split, upgraded)
            )
    for record_id, status, config_json in templates:
        upgraded, changed = upgrade_nested_config(config_json)
        if changed:
            template_updates.append(
                PlannedUpdate(record_id, status, config_json, upgraded)
            )
    return experiment_updates, template_updates


def _load_candidates(cur: Any) -> tuple[list[tuple], list[tuple]]:
    cur.execute(
        """
        SELECT e.experiment_id, e.status, e.data_split
        FROM qe_experiments e
        WHERE e.status = ANY(%s)
          AND e.data_split IS NOT NULL
          AND e.started_at IS NULL
          AND NOT COALESCE(e.is_evolution_loop, FALSE)
          AND e.parent_experiment_id IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM qe_evolution_tasks t
              WHERE t.base_experiment_id = e.experiment_id
          )
        ORDER BY e.experiment_id
        """,
        (list(UNSTARTED_EXPERIMENT_STATUSES),),
    )
    experiments = list(cur.fetchall())
    cur.execute(
        """
        SELECT template_id, status, config_json
        FROM qe_execution_templates
        WHERE status = ANY(%s)
          AND config_json IS NOT NULL
          AND submitted_experiment_id IS NULL
          AND submitted_task_id IS NULL
        ORDER BY template_id
        """,
        (list(RUNNABLE_TEMPLATE_STATUSES),),
    )
    return experiments, list(cur.fetchall())


def run(*, apply: bool) -> dict[str, Any]:
    with get_conn() as conn:
        conn.set_session(readonly=not apply, autocommit=False)
        with conn.cursor() as cur:
            experiments, templates = _load_candidates(cur)
            experiment_updates, template_updates = plan_updates(experiments, templates)
            if apply:
                for update in experiment_updates:
                    cur.execute(
                        """
                        UPDATE qe_experiments
                        SET data_split = %s::jsonb, updated_at = NOW()
                        WHERE experiment_id = %s
                          AND status = %s
                          AND started_at IS NULL
                          AND NOT COALESCE(is_evolution_loop, FALSE)
                          AND parent_experiment_id IS NULL
                          AND NOT EXISTS (
                              SELECT 1
                              FROM qe_evolution_tasks t
                              WHERE t.base_experiment_id = qe_experiments.experiment_id
                          )
                          AND data_split = %s::jsonb
                        """,
                        (
                            json.dumps(update.new_value),
                            update.record_id,
                            update.status,
                            json.dumps(update.old_value),
                        ),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            f"experiment changed concurrently: {update.record_id}"
                        )
                for update in template_updates:
                    cur.execute(
                        """
                        UPDATE qe_execution_templates
                        SET config_json = %s::jsonb, updated_at = NOW()
                        WHERE template_id = %s
                          AND status = %s
                          AND submitted_experiment_id IS NULL
                          AND submitted_task_id IS NULL
                          AND config_json = %s::jsonb
                        """,
                        (
                            json.dumps(update.new_value),
                            update.record_id,
                            update.status,
                            json.dumps(update.old_value),
                        ),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            f"template changed concurrently: {update.record_id}"
                        )
                conn.commit()
            else:
                conn.rollback()

    return {
        "mode": "apply" if apply else "dry_run",
        "new_test_end": QE_DEFAULT_SIGNAL_END,
        "new_backtest_end": QE_DEFAULT_BACKTEST_END,
        "experiment_updates": [item.record_id for item in experiment_updates],
        "template_updates": [item.record_id for item in template_updates],
        "completed_or_running_records_touched": 0,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upgrade exact legacy QE cutoff defaults on unstarted records."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist planned updates. Omit for read-only dry-run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    print(json.dumps(run(apply=args.apply), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
