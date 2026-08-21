from __future__ import annotations

from datetime import date
from typing import Any, Mapping

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_forward.models import (
    AdvisoryForwardModelObservationV1,
    AdvisoryForwardRunV1,
)
from backend.services.advisory_forward.errors import AdvisoryForwardActiveEpisodeStateConflictError
from backend.services.advisory_program import (
    ACTION_EXIT,
    PRICE_BASIS_NEXT_OPEN,
    AdvisoryProgram,
    AdvisoryRecommendationListItem,
    AdvisoryRecommendationListVersion,
    AdvisoryReviewDecision,
    AdvisoryReviewRun,
    _episode_sql_params,
    _list_item_sql_params,
    _list_version_sql_params,
    _review_run_sql_params,
    decision_to_dict,
    program_to_dict,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.services.trading_core.errors import InvalidStateTransitionError


RETRYABLE_MODEL_OBSERVATION_REASON_CODES = frozenset(
    {
        "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
        "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
    }
)


def is_retryable_model_observation(observation: Mapping[str, Any]) -> bool:
    status = str(observation.get("status") or "")
    if status == "FAILED":
        return True
    return status == "UNAVAILABLE" and str(observation.get("reason_code") or "") in (
        RETRYABLE_MODEL_OBSERVATION_REASON_CODES
    )


class AdvisoryForwardPGRepository:
    def __init__(self, *, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def begin_attempt(self, run: AdvisoryForwardRunV1) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_forward_run (
                        forward_run_id, program_id, program_version, binding_version_id,
                        decision_as_of_trade_date, target_trade_date, publication_status,
                        settlement_status, last_stage, attempt_count, model_resolution_json,
                        run_payload_json, created_at, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (program_id, target_trade_date) DO UPDATE SET
                        attempt_count = CASE
                            WHEN app.advisory_forward_run.publication_status = 'PUBLISHED'
                            THEN app.advisory_forward_run.attempt_count
                            ELSE app.advisory_forward_run.attempt_count + 1
                        END,
                        updated_at = EXCLUDED.updated_at,
                        last_stage = CASE
                            WHEN app.advisory_forward_run.publication_status = 'PUBLISHED'
                            THEN app.advisory_forward_run.last_stage
                            ELSE EXCLUDED.last_stage
                        END,
                        program_version = CASE
                            WHEN app.advisory_forward_run.publication_status = 'PUBLISHED'
                            THEN app.advisory_forward_run.program_version
                            ELSE EXCLUDED.program_version
                        END,
                        binding_version_id = CASE
                            WHEN app.advisory_forward_run.publication_status = 'PUBLISHED'
                            THEN app.advisory_forward_run.binding_version_id
                            ELSE EXCLUDED.binding_version_id
                        END,
                        decision_as_of_trade_date = CASE
                            WHEN app.advisory_forward_run.publication_status = 'PUBLISHED'
                            THEN app.advisory_forward_run.decision_as_of_trade_date
                            ELSE EXCLUDED.decision_as_of_trade_date
                        END
                    RETURNING *
                    """,
                    (
                        run.forward_run_id,
                        run.program_id,
                        run.program_version,
                        run.binding_version_id,
                        run.decision_as_of_trade_date,
                        run.target_trade_date,
                        run.publication_status,
                        run.settlement_status,
                        run.last_stage,
                        run.attempt_count,
                        psycopg2.extras.Json(run.model_resolution_json),
                        psycopg2.extras.Json(run.run_payload_json),
                        run.created_at,
                        run.updated_at,
                    ),
                )
                return dict(cur.fetchone())

    def mark_failure(
        self,
        *,
        forward_run_id: str,
        stage: str,
        reason_code: str,
        error: dict[str, Any],
        waiting_data: bool,
    ) -> dict[str, Any]:
        status = "WAITING_DATA" if waiting_data else "FAILED"
        settlement_stage = stage == "TARGET_OPEN_SETTLE"
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE app.advisory_forward_run
                    SET publication_status = CASE
                            WHEN %s OR publication_status = 'PUBLISHED' THEN publication_status
                            ELSE %s
                        END,
                        settlement_status = CASE
                            WHEN settlement_status IN ('SETTLED','NOT_ENTERED') THEN settlement_status
                            WHEN %s AND publication_status = 'PUBLISHED' THEN %s
                            ELSE settlement_status
                        END,
                        last_stage = CASE
                            WHEN settlement_status IN ('SETTLED','NOT_ENTERED') THEN last_stage
                            WHEN NOT %s AND publication_status = 'PUBLISHED' THEN last_stage
                            ELSE %s
                        END,
                        last_reason_code = CASE
                            WHEN settlement_status IN ('SETTLED','NOT_ENTERED') THEN last_reason_code
                            WHEN NOT %s AND publication_status = 'PUBLISHED' THEN last_reason_code
                            ELSE %s
                        END,
                        last_error_json = CASE
                            WHEN settlement_status IN ('SETTLED','NOT_ENTERED') THEN last_error_json
                            WHEN NOT %s AND publication_status = 'PUBLISHED' THEN last_error_json
                            ELSE %s
                        END,
                        updated_at = NOW()
                    WHERE forward_run_id = %s
                    RETURNING *
                    """,
                    (
                        settlement_stage,
                        status,
                        settlement_stage,
                        status,
                        settlement_stage,
                        stage,
                        settlement_stage,
                        reason_code,
                        settlement_stage,
                        psycopg2.extras.Json(error),
                        forward_run_id,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "advisory forward run does not exist",
                context={"forward_run_id": forward_run_id},
            )
        return dict(row)

    def commit_publication(
        self,
        *,
        forward_run_id: str,
        expected_program_version: int,
        expected_binding_version_id: str,
        review_run: AdvisoryReviewRun,
        list_version: AdvisoryRecommendationListVersion,
        items: list[AdvisoryRecommendationListItem],
        model_resolution: dict[str, Any],
        publication_payload: dict[str, Any],
    ) -> dict[str, Any]:
        payload_hash = canonical_json_sha256(publication_payload)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM app.advisory_forward_run WHERE forward_run_id = %s FOR UPDATE",
                    (forward_run_id,),
                )
                forward = cur.fetchone()
                if forward is None:
                    raise InvalidStateTransitionError("advisory forward run does not exist")
                if forward["publication_status"] == "PUBLISHED":
                    if forward["publication_payload_sha256"] != payload_hash:
                        raise InvalidStateTransitionError(
                            "advisory forward publication payload conflicts with the persisted fact",
                            context={"forward_run_id": forward_run_id},
                        )
                    return dict(forward)
                cur.execute(
                    "SELECT version, status FROM app.advisory_program WHERE program_id = %s FOR UPDATE",
                    (review_run.program_id,),
                )
                program_row = cur.fetchone()
                if (
                    program_row is None
                    or int(program_row["version"]) != expected_program_version
                    or program_row["status"] != "ENABLED"
                ):
                    raise InvalidStateTransitionError("advisory Program changed during forward publication")
                cur.execute(
                    """SELECT binding_version_id FROM app.advisory_strategy_binding_version
                       WHERE binding_version_id = %s AND program_id = %s
                         AND activation_status = 'ACTIVE' FOR UPDATE""",
                    (expected_binding_version_id, review_run.program_id),
                )
                if cur.fetchone() is None:
                    raise InvalidStateTransitionError("advisory binding changed during forward publication")
                cur.execute(
                    """
                    INSERT INTO app.advisory_review_run (
                        review_run_id, program_id, binding_version_id, trade_date, run_type,
                        status, data_source, selection_run_id, selection_run_ids,
                        runtime_config_json, started_at, finished_at, error_json, created_by,
                        run_payload_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    _review_run_sql_params(review_run),
                )
                cur.execute(
                    """
                    INSERT INTO app.advisory_recommendation_list_version (
                        list_version_id, program_id, binding_version_id, review_run_id, trade_date,
                        previous_list_version_id, version_status, target_count, active_count,
                        entered_count, held_count, exited_count, waiting_count, changed_count,
                        turnover_rate, overlap_rate, summary_json, created_at, list_payload_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    _list_version_sql_params(list_version),
                )
                for item in items:
                    cur.execute(
                        """
                        INSERT INTO app.advisory_recommendation_list_item (
                            list_item_id, list_version_id, program_id, binding_version_id, episode_id,
                            symbol, item_state, action, previous_action, rank, score, previous_rank,
                            previous_score, entry_price, exit_price, price_basis, effective_trade_date,
                            reason_code, operation_advice_json, component_scores_json, evidence_json,
                            created_at, item_payload_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        _list_item_sql_params(item),
                    )
                cur.execute(
                    """
                    UPDATE app.advisory_forward_run
                    SET publication_status='PUBLISHED', settlement_status='NOT_DUE',
                        selection_run_id=%s, review_run_id=%s, list_version_id=%s,
                        active_episode_state_hash=%s, publication_payload_sha256=%s, model_resolution_json=%s,
                        run_payload_json=%s, last_stage='AFTER_CLOSE_PUBLISH',
                        last_reason_code=NULL, last_error_json=NULL,
                        published_at=NOW(), updated_at=NOW()
                    WHERE forward_run_id=%s
                    RETURNING *
                    """,
                    (
                        review_run.selection_run_id,
                        review_run.review_run_id,
                        list_version.list_version_id,
                        publication_payload["active_episode_state_hash"],
                        payload_hash,
                        psycopg2.extras.Json(model_resolution),
                        psycopg2.extras.Json(publication_payload),
                        forward_run_id,
                    ),
                )
                return dict(cur.fetchone())

    def save_observation(self, observation: AdvisoryForwardModelObservationV1) -> dict[str, Any]:
        payload = observation.payload()
        payload_hash = observation.payload_sha256()
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT program_id, binding_version_id, decision_as_of_trade_date,
                              target_trade_date, publication_status, model_resolution_json
                       FROM app.advisory_forward_run WHERE forward_run_id=%s FOR UPDATE""",
                    (observation.forward_run_id,),
                )
                forward = cur.fetchone()
                _validate_observation_identity(observation, forward)
                cur.execute(
                    "SELECT * FROM app.advisory_forward_model_observation WHERE forward_run_id=%s FOR UPDATE",
                    (observation.forward_run_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    if existing["payload_sha256"] == payload_hash:
                        if is_retryable_model_observation(existing):
                            cur.execute(
                                """
                                UPDATE app.advisory_forward_model_observation
                                SET updated_at=NOW()
                                WHERE forward_run_id=%s
                                RETURNING *
                                """,
                                (observation.forward_run_id,),
                            )
                            existing = cur.fetchone()
                        _clear_observation_failure(cur, observation.forward_run_id)
                        return dict(existing)
                    if not is_retryable_model_observation(existing):
                        raise InvalidStateTransitionError(
                            "terminal forward model observation payload cannot change",
                            context={"forward_run_id": observation.forward_run_id},
                        )
                    if existing["model_descriptor_sha256"] != observation.model_descriptor_sha256:
                        raise InvalidStateTransitionError(
                            "forward model observation descriptor identity cannot change",
                            context={"forward_run_id": observation.forward_run_id},
                        )
                    cur.execute(
                        """
                        UPDATE app.advisory_forward_model_observation SET
                            status=%s, reason_code=%s, message=%s, package_id=%s,
                            manifest_sha256=%s, style_profile_id=%s, style_profile_hash=%s,
                            bundle_id=%s, outcome_bundle_id=%s, price_range_bundle_id=%s,
                            feature_schema_version=%s, candidate_count=%s, shortlist_count=%s,
                            maturity_trade_date=%s, prediction_payload_json=%s,
                            observation_payload_json=%s, payload_sha256=%s, updated_at=NOW()
                        WHERE forward_run_id=%s RETURNING *
                        """,
                        _observation_update_params(observation, payload, payload_hash),
                    )
                    saved = dict(cur.fetchone())
                    _clear_observation_failure(cur, observation.forward_run_id)
                    return saved
                cur.execute(
                    """
                    INSERT INTO app.advisory_forward_model_observation (
                        observation_id, forward_run_id, program_id, binding_version_id,
                        decision_as_of_trade_date, target_trade_date, status, reason_code, message,
                        package_id, manifest_sha256, style_profile_id, style_profile_hash,
                        model_descriptor_sha256, bundle_id, outcome_bundle_id, price_range_bundle_id,
                        feature_schema_version, candidate_count, shortlist_count, maturity_trade_date,
                        prediction_payload_json, observation_payload_json, payload_sha256,
                        created_at, updated_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING *
                    """,
                    _observation_insert_params(observation, payload, payload_hash),
                )
                saved = dict(cur.fetchone())
                _clear_observation_failure(cur, observation.forward_run_id)
                return saved

    def mark_observation_failure(
        self,
        *,
        forward_run_id: str,
        reason_code: str,
        error: dict[str, Any],
    ) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE app.advisory_forward_run
                    SET last_stage='MODEL_OBSERVATION', last_reason_code=%s,
                        last_error_json=%s, updated_at=NOW()
                    WHERE forward_run_id=%s AND publication_status='PUBLISHED'
                    RETURNING *
                    """,
                    (reason_code, psycopg2.extras.Json(error), forward_run_id),
                )
                row = cur.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "published advisory forward run does not exist",
                context={"forward_run_id": forward_run_id},
            )
        return dict(row)

    def get(self, forward_run_id: str) -> dict[str, Any]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_forward_run WHERE forward_run_id=%s", (forward_run_id,))
                run = cur.fetchone()
                if run is None:
                    raise InvalidStateTransitionError("advisory forward run does not exist")
                cur.execute(
                    "SELECT * FROM app.advisory_forward_model_observation WHERE forward_run_id=%s",
                    (forward_run_id,),
                )
                observation = cur.fetchone()
        return {"forward_run": dict(run), "model_observation": dict(observation) if observation else None}

    def list_runs(self, *, program_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT * FROM app.advisory_forward_run"
        params: list[Any] = []
        if program_id:
            sql += " WHERE program_id=%s"
            params.append(program_id)
        sql += " ORDER BY target_trade_date DESC, program_id LIMIT %s"
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def retryable_model_observations(self, *, limit: int = 1) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("retryable model observation limit must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT forward_run.*
                    FROM app.advisory_forward_run AS forward_run
                    JOIN app.advisory_forward_model_observation AS observation
                      ON observation.forward_run_id = forward_run.forward_run_id
                    WHERE forward_run.publication_status = 'PUBLISHED'
                      AND observation.model_descriptor_sha256 IS NOT NULL
                      AND observation.updated_at <= NOW() - INTERVAL '5 minutes'
                      AND (
                        observation.status = 'FAILED'
                        OR (
                          observation.status = 'UNAVAILABLE'
                          AND observation.reason_code = ANY(%s)
                        )
                      )
                    ORDER BY observation.updated_at ASC, forward_run.target_trade_date ASC,
                             forward_run.program_id ASC
                    LIMIT %s
                    """,
                    (sorted(RETRYABLE_MODEL_OBSERVATION_REASON_CODES), int(limit)),
                )
                return [dict(row) for row in cur.fetchall()]

    def pending_settlements(self, *, on_or_before: date) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT * FROM app.advisory_forward_run
                    WHERE publication_status='PUBLISHED'
                      AND settlement_status IN ('NOT_DUE','WAITING_DATA','FAILED')
                      AND target_trade_date <= %s
                    ORDER BY target_trade_date, program_id
                    """,
                    (on_or_before,),
                )
                return [dict(row) for row in cur.fetchall()]

    def commit_settlement(
        self,
        *,
        forward_run_id: str,
        expected_active_episode_state_hash: str,
        expected_program_version: int,
        expected_program_status: str,
        result: Any,
        decisions: list[AdvisoryReviewDecision],
        program: AdvisoryProgram,
        settlement_payload: dict[str, Any],
    ) -> dict[str, Any]:
        payload_hash = canonical_json_sha256(settlement_payload)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_forward_run WHERE forward_run_id=%s FOR UPDATE", (forward_run_id,))
                forward = cur.fetchone()
                if forward is None or forward["publication_status"] != "PUBLISHED":
                    raise InvalidStateTransitionError("advisory forward run is not published")
                if forward["settlement_status"] in {"SETTLED", "NOT_ENTERED"}:
                    if forward["settlement_payload_sha256"] != payload_hash:
                        raise InvalidStateTransitionError("advisory forward settlement payload conflicts")
                    return dict(forward)
                cur.execute(
                    "SELECT version, status FROM app.advisory_program WHERE program_id=%s FOR UPDATE",
                    (program.program_id,),
                )
                program_row = cur.fetchone()
                if program_row is None:
                    raise InvalidStateTransitionError("advisory Program does not exist")
                if int(program_row["version"]) != expected_program_version:
                    raise InvalidStateTransitionError(
                        "advisory Program changed during forward settlement",
                        context={"program_id": program.program_id},
                    )
                if str(program_row["status"]) != expected_program_status:
                    raise InvalidStateTransitionError(
                        "advisory Program status changed during forward settlement",
                        context={"program_id": program.program_id},
                    )
                cur.execute(
                    """SELECT DISTINCT ON (episode_id) episode_id, episode_payload_json
                       FROM app.advisory_episode_return WHERE program_id=%s
                       ORDER BY episode_id, updated_at DESC, created_at DESC""",
                    (program.program_id,),
                )
                active_payloads = [row["episode_payload_json"] for row in cur.fetchall() if row["episode_payload_json"].get("status") == "ACTIVE"]
                if canonical_json_sha256(sorted(active_payloads, key=lambda row: row["episode_id"])) != expected_active_episode_state_hash:
                    raise AdvisoryForwardActiveEpisodeStateConflictError(
                        "active episode state changed during forward settlement",
                        context={"forward_run_id": forward_run_id},
                    )
                for episode in result.active_pool:
                    cur.execute(
                        """INSERT INTO app.advisory_episode_return (
                            episode_id, program_id, program_version, symbol, episode_status,
                            signal_date, effective_entry_date, entry_price, entry_price_basis,
                            entry_rank, entry_score, current_rank, current_score, exit_signal_date,
                            effective_exit_date, exit_price, exit_price_basis, exit_reason,
                            holding_trading_days, return_bps, is_win, win_rate_inclusion_status,
                            max_runup_bps, max_drawdown_bps, still_active_mark_price,
                            price_quality_status, weak_rank_confirm_days, source_run_id,
                            evidence_json, episode_payload_json, created_at, updated_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        _episode_sql_params(episode),
                    )
                for decision in decisions:
                    cur.execute(
                        """INSERT INTO app.advisory_daily_review (
                            program_id, program_version, episode_id, binding_version_id, review_run_id,
                            list_version_id, watchlist_item_id, code, trade_date, evidence_id, score,
                            rank, current_price, entry_band_json, stop_price, take_price, action,
                            reason_code, policy_sha256, guidance_status, price_basis,
                            feature_availability_ts, t1_note, layer, review_status,
                            fusion_evidence_json, decision_input_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,NULL,%s,%s,NULL,%s,%s,%s,NULL,NULL,NULL,%s,%s,%s,%s,%s,%s,NULL,'advisory_program',%s,%s,%s)""",
                        _decision_sql_params(decision),
                    )
                metrics = dict(result.metrics)
                cur.execute(
                    """INSERT INTO app.advisory_program_metric_snapshot (
                        program_id, snapshot_date, enabled_since, entered_episode_count,
                        active_count, take_profit_count, stop_loss_count, win_rate,
                        avg_return_bps, median_return_bps, max_drawdown_bps,
                        avg_holding_days, last_review_status, metrics_json
                    ) VALUES (%s,CURRENT_DATE,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    _metric_sql_params(program.program_id, metrics),
                )
                cur.execute(
                    """UPDATE app.advisory_program SET status=%s, last_review_status=%s,
                       latest_review_trade_date=%s, program_payload_json=%s, updated_at=NOW()
                       WHERE program_id=%s""",
                    (
                        program.status,
                        program.last_review_status,
                        program.latest_review_trade_date,
                        psycopg2.extras.Json(program_to_dict(program)),
                        program.program_id,
                    ),
                )
                cur.execute(
                    "UPDATE app.advisory_review_run SET status=%s, finished_at=NOW() WHERE review_run_id=%s",
                    ("SUCCEEDED" if result.review_status == "SUCCEEDED" else "WAITING_DATA", forward["review_run_id"]),
                )
                settlement_status = (
                    "WAITING_DATA"
                    if result.review_status == "WAITING_DATA"
                    else "SETTLED"
                    if decisions
                    else "NOT_ENTERED"
                )
                cur.execute(
                    """UPDATE app.advisory_forward_run SET settlement_status=%s,
                       active_episode_state_hash=%s, settlement_payload_sha256=%s,
                       run_payload_json=run_payload_json || %s, last_stage='TARGET_OPEN_SETTLE',
                       last_reason_code=%s, last_error_json=NULL,
                       settled_at=CASE WHEN %s IN ('SETTLED','NOT_ENTERED') THEN NOW() ELSE NULL END,
                       updated_at=NOW()
                       WHERE forward_run_id=%s RETURNING *""",
                    (
                        settlement_status,
                        expected_active_episode_state_hash,
                        payload_hash,
                        psycopg2.extras.Json({"settlement": settlement_payload}),
                        "ADVISORY_FORWARD_TARGET_OPEN_WAITING_DATA" if settlement_status == "WAITING_DATA" else None,
                        settlement_status,
                        forward_run_id,
                    ),
                )
                return dict(cur.fetchone())


def _observation_insert_params(observation: AdvisoryForwardModelObservationV1, payload: dict[str, Any], payload_hash: str) -> tuple[Any, ...]:
    return (
        observation.observation_id, observation.forward_run_id, observation.program_id,
        observation.binding_version_id, observation.decision_as_of_trade_date,
        observation.target_trade_date, observation.status, observation.reason_code,
        observation.message, observation.package_id, observation.manifest_sha256,
        observation.style_profile_id, observation.style_profile_hash,
        observation.model_descriptor_sha256, observation.bundle_id,
        observation.outcome_bundle_id, observation.price_range_bundle_id,
        observation.feature_schema_version, observation.candidate_count,
        observation.shortlist_count, observation.maturity_trade_date,
        psycopg2.extras.Json(observation.prediction_payload_json),
        psycopg2.extras.Json(payload), payload_hash, observation.created_at, observation.updated_at,
    )


def _clear_observation_failure(cur: Any, forward_run_id: str) -> None:
    cur.execute(
        """
        UPDATE app.advisory_forward_run
        SET last_stage='MODEL_OBSERVATION', last_reason_code=NULL,
            last_error_json=NULL, updated_at=NOW()
        WHERE forward_run_id=%s AND publication_status='PUBLISHED'
        """,
        (forward_run_id,),
    )


def _validate_observation_identity(
    observation: AdvisoryForwardModelObservationV1,
    forward: Mapping[str, Any] | None,
) -> None:
    if forward is None or forward["publication_status"] != "PUBLISHED":
        raise InvalidStateTransitionError(
            "model observation requires a published advisory forward run",
            context={"forward_run_id": observation.forward_run_id},
        )
    expected = {
        "program_id": forward["program_id"],
        "binding_version_id": forward["binding_version_id"],
        "decision_as_of_trade_date": forward["decision_as_of_trade_date"],
        "target_trade_date": forward["target_trade_date"],
    }
    actual = {
        "program_id": observation.program_id,
        "binding_version_id": observation.binding_version_id,
        "decision_as_of_trade_date": observation.decision_as_of_trade_date,
        "target_trade_date": observation.target_trade_date,
    }
    if actual != expected:
        raise InvalidStateTransitionError(
            "model observation identity differs from its advisory forward run",
            context={"forward_run_id": observation.forward_run_id},
        )
    resolution = dict(forward.get("model_resolution_json") or {})
    if resolution.get("status") == "CONFIGURED" and (
        observation.model_descriptor_sha256 != resolution.get("descriptor_sha256")
        or observation.bundle_id != resolution.get("bundle_id")
    ):
        raise InvalidStateTransitionError(
            "model observation differs from the publication-frozen descriptor",
            context={"forward_run_id": observation.forward_run_id},
        )


def _observation_update_params(observation: AdvisoryForwardModelObservationV1, payload: dict[str, Any], payload_hash: str) -> tuple[Any, ...]:
    return (
        observation.status, observation.reason_code, observation.message, observation.package_id,
        observation.manifest_sha256, observation.style_profile_id, observation.style_profile_hash,
        observation.bundle_id, observation.outcome_bundle_id, observation.price_range_bundle_id,
        observation.feature_schema_version, observation.candidate_count, observation.shortlist_count,
        observation.maturity_trade_date, psycopg2.extras.Json(observation.prediction_payload_json),
        psycopg2.extras.Json(payload), payload_hash, observation.forward_run_id,
    )


def _decision_sql_params(decision: AdvisoryReviewDecision) -> tuple[Any, ...]:
    return (
        decision.program_id, decision.program_version, decision.episode_id,
        decision.binding_version_id, decision.review_run_id, decision.list_version_id,
        decision.symbol, decision.trade_date, decision.score, decision.rank,
        decision.exit_price if decision.action == ACTION_EXIT else decision.entry_price,
        decision.action, decision.reason_code,
        decision.evidence_json.get("review_policy_sha256") or "unknown",
        decision.evidence_json.get("guidance_status") or "rule_default",
        decision.evidence_json.get("price_basis") or PRICE_BASIS_NEXT_OPEN,
        decision.created_at, decision.review_status,
        psycopg2.extras.Json(decision.evidence_json), psycopg2.extras.Json(decision_to_dict(decision)),
    )


def _metric_sql_params(program_id: str, metrics: dict[str, Any]) -> tuple[Any, ...]:
    return (
        program_id, metrics.get("enabled_since"), metrics.get("entered_episode_count"),
        metrics.get("active_count"), metrics.get("take_profit_count"), metrics.get("stop_loss_count"),
        metrics.get("win_rate"), metrics.get("avg_return_bps"), metrics.get("median_return_bps"),
        metrics.get("max_drawdown_bps"), metrics.get("avg_holding_days"),
        metrics.get("last_review_status"), psycopg2.extras.Json(metrics),
    )
