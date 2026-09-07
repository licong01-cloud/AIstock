"""Transactional bootstrap and exact verifier for ``hmm_risk_schema_v1``."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from .pg_pool import get_conn


SCHEMA_NAME = "hmm_risk"
SCHEMA_VERSION = "hmm_risk_schema_v1"
SCHEMA_COMMENT = "hmm_risk_schema_v1 advisory-only HMM L1/L2 state, alert, event, job and retrospective evidence"


EXPECTED_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "daily_generation_run": (
        "run_id",
        "idempotency_key",
        "request_hash",
        "request_payload",
        "status",
        "candidate_id",
        "candidate_manifest_hash",
        "state_model_set_id",
        "state_model_set_hash",
        "l1_model_sha256",
        "l2_model_sha256",
        "trade_date_policy",
        "requested_trade_date",
        "resolved_trade_date",
        "as_of_date",
        "generator_version",
        "rule_version",
        "input_manifest",
        "input_hash",
        "owner_id",
        "fencing_token",
        "row_version",
        "lease_expires_at",
        "heartbeat_at",
        "max_runtime_seconds",
        "expected_count",
        "succeeded_count",
        "failed_count",
        "l1_expected_count",
        "l1_succeeded_count",
        "l2_expected_count",
        "l2_succeeded_count",
        "missing_evidence",
        "error_code",
        "error_message",
        "error_context",
        "cancel_requested_at",
        "cancel_requested_by",
        "queued_at",
        "started_at",
        "completed_at",
        "created_at",
        "updated_at",
    ),
    "sector_state_timeline": (
        "state_id",
        "run_id",
        "candidate_id",
        "candidate_manifest_hash",
        "snapshot_id",
        "config_id",
        "trade_date",
        "as_of_date",
        "sector_level",
        "sector_code",
        "sector_name",
        "hmm_state",
        "state_probabilities",
        "state_confidence",
        "state_origin",
        "confidence_definition_version",
        "parser_contract",
        "adapter_version",
        "observation_version",
        "state_model_set_id",
        "model_artifact_sha256",
        "input_hash",
        "result_hash",
        "mapping_snapshot_hash",
        "generator_version",
        "rule_version",
        "transition_from",
        "transition_kind",
        "severity",
        "dedupe_key",
        "revision",
        "supersedes_state_id",
        "evidence",
        "created_at",
    ),
    "daily_alert": (
        "alert_id",
        "run_id",
        "state_id",
        "candidate_id",
        "trade_date",
        "sector_level",
        "sector_code",
        "severity",
        "transition_from",
        "transition_to",
        "rule_version",
        "generator_version",
        "explanation_version",
        "explanation",
        "input_hash",
        "result_hash",
        "dedupe_key",
        "revision",
        "supersedes_alert_id",
        "created_at",
    ),
    "risk_event": (
        "event_revision_id",
        "event_id",
        "dedupe_key",
        "candidate_id",
        "sector_level",
        "sector_code",
        "event_type",
        "rule_version",
        "status",
        "revision",
        "first_alert_id",
        "latest_alert_id",
        "opened_trade_date",
        "last_trade_date",
        "resolved_trade_date",
        "resolution_reason",
        "supersedes_event_revision_id",
        "result_hash",
        "evidence",
        "created_at",
    ),
    "retrospective_report": (
        "report_id",
        "candidate_id",
        "candidate_manifest_hash",
        "model_artifact_sha256",
        "start_trade_date",
        "end_trade_date",
        "sector_level",
        "report_spec",
        "report_spec_hash",
        "source_manifest",
        "source_hash",
        "status",
        "metrics",
        "evidence",
        "result_hash",
        "sample_count",
        "error_code",
        "error_message",
        "error_context",
        "created_at",
        "completed_at",
    ),
}

EXPECTED_CONSTRAINTS: Mapping[str, frozenset[str]] = {
    "daily_generation_run": frozenset(
        {
            "pk_hmm_risk_daily_generation_run",
            "uq_hmm_risk_run_idempotency",
            "fk_hmm_risk_run_candidate",
            "ck_hmm_risk_run_status",
            "ck_hmm_risk_run_trade_date_policy",
            "ck_hmm_risk_run_requested_date",
            "ck_hmm_risk_run_resolved_dates",
            "ck_hmm_risk_run_hash_pair",
            "ck_hmm_risk_run_fencing",
            "ck_hmm_risk_run_row_version",
            "ck_hmm_risk_run_runtime",
            "ck_hmm_risk_run_counters",
            "ck_hmm_risk_run_level_counters",
            "ck_hmm_risk_run_missing_evidence",
            "ck_hmm_risk_run_owner_lease",
            "ck_hmm_risk_run_cancel_pair",
            "ck_hmm_risk_run_terminal",
        }
    ),
    "sector_state_timeline": frozenset(
        {
            "pk_hmm_risk_sector_state",
            "fk_hmm_risk_state_run",
            "fk_hmm_risk_state_supersedes",
            "uq_hmm_risk_state_revision",
            "uq_hmm_risk_state_input",
            "ck_hmm_risk_state_dates",
            "ck_hmm_risk_state_level",
            "ck_hmm_risk_state_code",
            "ck_hmm_risk_state_name",
            "ck_hmm_risk_state_value",
            "ck_hmm_risk_state_probability_shape",
            "ck_hmm_risk_state_confidence",
            "ck_hmm_risk_state_origin",
            "ck_hmm_risk_state_transition_from",
            "ck_hmm_risk_state_severity",
            "ck_hmm_risk_state_revision",
        }
    ),
    "daily_alert": frozenset(
        {
            "pk_hmm_risk_daily_alert",
            "fk_hmm_risk_alert_run",
            "fk_hmm_risk_alert_state",
            "fk_hmm_risk_alert_supersedes",
            "uq_hmm_risk_alert_state",
            "uq_hmm_risk_alert_revision",
            "uq_hmm_risk_alert_input",
            "ck_hmm_risk_alert_level",
            "ck_hmm_risk_alert_severity",
            "ck_hmm_risk_alert_transition_from",
            "ck_hmm_risk_alert_transition_to",
            "ck_hmm_risk_alert_revision",
        }
    ),
    "risk_event": frozenset(
        {
            "pk_hmm_risk_event_revision",
            "fk_hmm_risk_event_first_alert",
            "fk_hmm_risk_event_latest_alert",
            "fk_hmm_risk_event_supersedes",
            "uq_hmm_risk_event_revision",
            "uq_hmm_risk_event_result",
            "ck_hmm_risk_event_level",
            "ck_hmm_risk_event_type",
            "ck_hmm_risk_event_status",
            "ck_hmm_risk_event_revision",
            "ck_hmm_risk_event_dates",
            "ck_hmm_risk_event_resolution",
        }
    ),
    "retrospective_report": frozenset(
        {
            "pk_hmm_risk_report",
            "uq_hmm_risk_report_identity",
            "ck_hmm_risk_report_dates",
            "ck_hmm_risk_report_level",
            "ck_hmm_risk_report_status",
            "ck_hmm_risk_report_sample_count",
            "ck_hmm_risk_report_terminal",
        }
    ),
}

EXPECTED_INDEXES = frozenset(
    {
        "idx_hmm_risk_run_claim",
        "idx_hmm_risk_run_lease",
        "idx_hmm_risk_state_lookup",
        "idx_hmm_risk_state_run",
        "idx_hmm_risk_alert_lookup",
        "idx_hmm_risk_event_lookup",
        "idx_hmm_risk_report_lookup",
    }
)
EXPECTED_VIEWS = frozenset({"sector_state_current", "daily_alert_current", "risk_event_current"})

# Filled from the canonical PostgreSQL catalog snapshot produced by this file.
# The digest covers column order/type/nullability/default, constraint definitions,
# non-constraint indexes and view definitions. Comments are checked separately so
# that a drift error identifies the human-readable contract that failed.
EXPECTED_STRUCTURE_SHA256 = "5e7dec052b1db18f1320a25c090fc09b5d50a2edb3a7c05320d61e046cb213e0"

ROTATION_L1_PREDICTION_COLUMNS = (
    "prediction_id",
    "product_bundle_id",
    "trade_date",
    "as_of_date",
    "sector_level",
    "sector_code",
    "sector_name",
    "rotation_score",
    "forecast_state",
    "feature_contributions",
    "availability",
    "reason_code",
    "research_surface_status",
    "rotation_l1_capability_status",
    "forward_power_status",
    "forward_confirmation",
    "advisory_status",
    "validation_basis",
    "development_oof_rank_ic",
    "development_oof_rank_ic_hac_lower",
    "development_oof_rank_ic_hac_upper",
    "model_hash",
    "input_hash",
    "mapping_snapshot_hash",
    "tail_accessed",
    "revision",
    "supersedes_prediction_id",
    "created_at",
)
ROTATION_L1_PREDICTION_CONSTRAINTS = frozenset(
    {
        "pk_hmm_risk_rotation_l1_prediction",
        "uq_hmm_risk_rotation_l1_prediction_revision",
        "fk_hmm_risk_rotation_l1_prediction_supersedes",
        "ck_hmm_risk_rotation_l1_prediction_dates",
        "ck_hmm_risk_rotation_l1_prediction_level",
        "ck_hmm_risk_rotation_l1_prediction_state",
        "ck_hmm_risk_rotation_l1_prediction_availability",
        "ck_hmm_risk_rotation_l1_prediction_surface",
        "ck_hmm_risk_rotation_l1_prediction_capability",
        "ck_hmm_risk_rotation_l1_prediction_power",
        "ck_hmm_risk_rotation_l1_prediction_confirmation",
        "ck_hmm_risk_rotation_l1_prediction_advisory",
        "ck_hmm_risk_rotation_l1_prediction_validation_basis",
        "ck_hmm_risk_rotation_l1_prediction_hashes",
        "ck_hmm_risk_rotation_l1_prediction_revision",
        "ck_hmm_risk_rotation_l1_prediction_supersedes_chain",
        "ck_hmm_risk_rotation_l1_prediction_advisory_coupling",
        "ck_hmm_risk_rotation_l1_prediction_metrics",
    }
)
ROTATION_L1_PREDICTION_COLUMN_CONTRACT = {
    "prediction_id": ("uuid", True, None),
    "product_bundle_id": ("text", False, None),
    "trade_date": ("date", True, None),
    "as_of_date": ("date", True, None),
    "sector_level": ("text", True, "'L1'::text"),
    "sector_code": ("text", True, None),
    "sector_name": ("text", True, None),
    "rotation_score": ("double precision", False, None),
    "forecast_state": ("text", False, None),
    "feature_contributions": ("jsonb", False, None),
    "availability": ("text", True, None),
    "reason_code": ("text", False, None),
    "research_surface_status": ("text", True, None),
    "rotation_l1_capability_status": ("text", True, None),
    "forward_power_status": ("text", True, None),
    "forward_confirmation": ("text", True, None),
    "advisory_status": ("text", True, None),
    "validation_basis": ("text", True, None),
    "development_oof_rank_ic": ("double precision", False, None),
    "development_oof_rank_ic_hac_lower": ("double precision", False, None),
    "development_oof_rank_ic_hac_upper": ("double precision", False, None),
    "model_hash": ("character(64)", True, None),
    "input_hash": ("character(64)", True, None),
    "mapping_snapshot_hash": ("character(64)", True, None),
    "tail_accessed": ("boolean", True, "false"),
    "revision": ("integer", True, None),
    "supersedes_prediction_id": ("uuid", False, None),
    "created_at": ("timestamp with time zone", True, "now()"),
}
ROTATION_L1_PREDICTION_CONSTRAINT_TOKENS = {
    "pk_hmm_risk_rotation_l1_prediction": ("PRIMARY KEY", "prediction_id"),
    "uq_hmm_risk_rotation_l1_prediction_revision": (
        "UNIQUE",
        "model_hash",
        "trade_date",
        "sector_code",
        "revision",
    ),
    "fk_hmm_risk_rotation_l1_prediction_supersedes": ("FOREIGN KEY", "supersedes_prediction_id"),
    "ck_hmm_risk_rotation_l1_prediction_dates": ("CHECK", "as_of_date", "trade_date"),
    "ck_hmm_risk_rotation_l1_prediction_level": ("CHECK", "sector_level", "L1"),
    "ck_hmm_risk_rotation_l1_prediction_state": ("CHECK", "forecast_state", "trending", "neutral", "fading"),
    "ck_hmm_risk_rotation_l1_prediction_availability": (
        "CHECK",
        "availability",
        "rotation_score",
        "feature_contributions",
        "reason_code",
    ),
    "ck_hmm_risk_rotation_l1_prediction_surface": ("CHECK", "research_surface_status"),
    "ck_hmm_risk_rotation_l1_prediction_capability": ("CHECK", "rotation_l1_capability_status"),
    "ck_hmm_risk_rotation_l1_prediction_power": ("CHECK", "forward_power_status"),
    "ck_hmm_risk_rotation_l1_prediction_confirmation": ("CHECK", "forward_confirmation"),
    "ck_hmm_risk_rotation_l1_prediction_advisory": ("CHECK", "advisory_status"),
    "ck_hmm_risk_rotation_l1_prediction_validation_basis": ("CHECK", "validation_basis"),
    "ck_hmm_risk_rotation_l1_prediction_hashes": (
        "CHECK",
        "model_hash",
        "input_hash",
        "mapping_snapshot_hash",
    ),
    "ck_hmm_risk_rotation_l1_prediction_revision": ("CHECK", "revision"),
    "ck_hmm_risk_rotation_l1_prediction_supersedes_chain": (
        "CHECK",
        "revision",
        "supersedes_prediction_id",
    ),
    "ck_hmm_risk_rotation_l1_prediction_advisory_coupling": (
        "CHECK",
        "advisory_status",
        "product_bundle_id",
        "tail_accessed",
    ),
    "ck_hmm_risk_rotation_l1_prediction_metrics": (
        "CHECK",
        "development_oof_rank_ic",
        "development_oof_rank_ic_hac_lower",
        "development_oof_rank_ic_hac_upper",
    ),
}


TABLE_DDL = (
    "CREATE SCHEMA IF NOT EXISTS hmm_risk",
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.daily_generation_run (
        run_id TEXT CONSTRAINT pk_hmm_risk_daily_generation_run PRIMARY KEY,
        idempotency_key TEXT NOT NULL CONSTRAINT uq_hmm_risk_run_idempotency UNIQUE,
        request_hash CHAR(64) NOT NULL,
        request_payload JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        candidate_id TEXT NOT NULL,
        candidate_manifest_hash CHAR(64) NOT NULL,
        state_model_set_id TEXT NOT NULL,
        state_model_set_hash CHAR(64) NOT NULL,
        l1_model_sha256 CHAR(64) NOT NULL,
        l2_model_sha256 CHAR(64) NOT NULL,
        trade_date_policy TEXT NOT NULL,
        requested_trade_date DATE,
        resolved_trade_date DATE,
        as_of_date DATE,
        generator_version TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        input_manifest JSONB,
        input_hash CHAR(64),
        owner_id TEXT,
        fencing_token BIGINT NOT NULL DEFAULT 0,
        row_version BIGINT NOT NULL DEFAULT 1,
        lease_expires_at TIMESTAMPTZ,
        heartbeat_at TIMESTAMPTZ,
        max_runtime_seconds INTEGER NOT NULL,
        expected_count INTEGER NOT NULL DEFAULT 0,
        succeeded_count INTEGER NOT NULL DEFAULT 0,
        failed_count INTEGER NOT NULL DEFAULT 0,
        l1_expected_count INTEGER NOT NULL DEFAULT 0,
        l1_succeeded_count INTEGER NOT NULL DEFAULT 0,
        l2_expected_count INTEGER NOT NULL DEFAULT 0,
        l2_succeeded_count INTEGER NOT NULL DEFAULT 0,
        missing_evidence JSONB NOT NULL,
        error_code TEXT,
        error_message TEXT,
        error_context JSONB,
        cancel_requested_at TIMESTAMPTZ,
        cancel_requested_by TEXT,
        queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT fk_hmm_risk_run_candidate FOREIGN KEY (candidate_id)
          REFERENCES hmm_evolution.candidate(candidate_id) ON DELETE RESTRICT,
        CONSTRAINT ck_hmm_risk_run_status CHECK (status IN ('queued','running','succeeded','partial_failed','failed','cancel_requested','cancelled')),
        CONSTRAINT ck_hmm_risk_run_trade_date_policy CHECK (trade_date_policy IN ('explicit','latest_common_completed')),
        CONSTRAINT ck_hmm_risk_run_requested_date CHECK ((trade_date_policy='explicit' AND requested_trade_date IS NOT NULL) OR (trade_date_policy='latest_common_completed' AND requested_trade_date IS NULL)),
        CONSTRAINT ck_hmm_risk_run_resolved_dates CHECK ((resolved_trade_date IS NULL AND as_of_date IS NULL) OR resolved_trade_date=as_of_date),
        CONSTRAINT ck_hmm_risk_run_hash_pair CHECK ((input_manifest IS NULL) = (input_hash IS NULL)),
        CONSTRAINT ck_hmm_risk_run_fencing CHECK (fencing_token >= 0),
        CONSTRAINT ck_hmm_risk_run_row_version CHECK (row_version >= 1),
        CONSTRAINT ck_hmm_risk_run_runtime CHECK (max_runtime_seconds BETWEEN 60 AND 7200),
        CONSTRAINT ck_hmm_risk_run_counters CHECK (expected_count >= 0 AND succeeded_count >= 0 AND failed_count >= 0 AND succeeded_count + failed_count <= expected_count),
        CONSTRAINT ck_hmm_risk_run_level_counters CHECK (l1_expected_count >= 0 AND l1_succeeded_count BETWEEN 0 AND l1_expected_count AND l2_expected_count >= 0 AND l2_succeeded_count BETWEEN 0 AND l2_expected_count),
        CONSTRAINT ck_hmm_risk_run_missing_evidence CHECK (jsonb_typeof(missing_evidence)='array'),
        CONSTRAINT ck_hmm_risk_run_owner_lease CHECK ((status IN ('running','cancel_requested') AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL) OR (status NOT IN ('running','cancel_requested') AND owner_id IS NULL AND lease_expires_at IS NULL AND heartbeat_at IS NULL)),
        CONSTRAINT ck_hmm_risk_run_cancel_pair CHECK ((cancel_requested_at IS NULL) = (cancel_requested_by IS NULL)),
        CONSTRAINT ck_hmm_risk_run_terminal CHECK (
          (status NOT IN ('succeeded','partial_failed','failed','cancelled') AND completed_at IS NULL)
          OR (status='succeeded' AND completed_at IS NOT NULL AND resolved_trade_date IS NOT NULL AND input_hash IS NOT NULL AND expected_count=succeeded_count AND failed_count=0 AND jsonb_array_length(missing_evidence)=0 AND error_code IS NULL)
          OR (status='partial_failed' AND completed_at IS NOT NULL AND resolved_trade_date IS NOT NULL AND input_hash IS NOT NULL AND succeeded_count>0 AND failed_count>0 AND jsonb_array_length(missing_evidence)>0)
          OR (status IN ('failed','cancelled') AND completed_at IS NOT NULL AND error_code IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.sector_state_timeline (
        state_id TEXT CONSTRAINT pk_hmm_risk_sector_state PRIMARY KEY,
        run_id TEXT NOT NULL CONSTRAINT fk_hmm_risk_state_run REFERENCES hmm_risk.daily_generation_run(run_id) ON DELETE RESTRICT,
        candidate_id TEXT NOT NULL,
        candidate_manifest_hash CHAR(64) NOT NULL,
        snapshot_id TEXT NOT NULL,
        config_id TEXT NOT NULL,
        trade_date DATE NOT NULL,
        as_of_date DATE NOT NULL,
        sector_level TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        hmm_state TEXT NOT NULL,
        state_probabilities JSONB NOT NULL,
        state_confidence DOUBLE PRECISION,
        state_origin TEXT NOT NULL,
        confidence_definition_version TEXT NOT NULL,
        parser_contract TEXT NOT NULL,
        adapter_version TEXT NOT NULL,
        observation_version TEXT NOT NULL,
        state_model_set_id TEXT NOT NULL,
        model_artifact_sha256 CHAR(64) NOT NULL,
        input_hash CHAR(64) NOT NULL,
        result_hash CHAR(64) NOT NULL,
        mapping_snapshot_hash CHAR(64) NOT NULL,
        generator_version TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        transition_from TEXT,
        transition_kind TEXT NOT NULL,
        severity TEXT NOT NULL,
        dedupe_key CHAR(64) NOT NULL,
        revision INTEGER NOT NULL,
        supersedes_state_id TEXT CONSTRAINT fk_hmm_risk_state_supersedes REFERENCES hmm_risk.sector_state_timeline(state_id) ON DELETE RESTRICT,
        evidence JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_hmm_risk_state_revision UNIQUE (dedupe_key,revision),
        CONSTRAINT uq_hmm_risk_state_input UNIQUE (dedupe_key,input_hash),
        CONSTRAINT ck_hmm_risk_state_dates CHECK (trade_date=as_of_date),
        CONSTRAINT ck_hmm_risk_state_level CHECK (sector_level IN ('L1','L2')),
        CONSTRAINT ck_hmm_risk_state_code CHECK (btrim(sector_code)<>''),
        CONSTRAINT ck_hmm_risk_state_name CHECK (btrim(sector_name)<>''),
        CONSTRAINT ck_hmm_risk_state_value CHECK (hmm_state IN ('trending','neutral','fading')),
        CONSTRAINT ck_hmm_risk_state_probability_shape CHECK (
          jsonb_typeof(state_probabilities)='object'
          AND state_probabilities ?& ARRAY['trending','neutral','fading']
          AND state_probabilities - 'trending' - 'neutral' - 'fading' = '{}'::jsonb
          AND jsonb_typeof(state_probabilities->'trending')='number'
          AND jsonb_typeof(state_probabilities->'neutral')='number'
          AND jsonb_typeof(state_probabilities->'fading')='number'
          AND (state_probabilities->>'trending')::double precision BETWEEN 0 AND 1
          AND (state_probabilities->>'neutral')::double precision BETWEEN 0 AND 1
          AND (state_probabilities->>'fading')::double precision BETWEEN 0 AND 1
          AND abs(
            (state_probabilities->>'trending')::double precision
            + (state_probabilities->>'neutral')::double precision
            + (state_probabilities->>'fading')::double precision - 1.0
          ) <= 1e-9
        ),
        CONSTRAINT ck_hmm_risk_state_confidence CHECK (state_confidence IS NULL OR state_confidence BETWEEN 0 AND 1),
        CONSTRAINT ck_hmm_risk_state_origin CHECK (state_origin='direct_hmm'),
        CONSTRAINT ck_hmm_risk_state_transition_from CHECK (transition_from IS NULL OR transition_from IN ('trending','neutral','fading')),
        CONSTRAINT ck_hmm_risk_state_severity CHECK (severity IN ('NONE','HIGH','MEDIUM','OPPORTUNITY')),
        CONSTRAINT ck_hmm_risk_state_revision CHECK (revision>0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.daily_alert (
        alert_id TEXT CONSTRAINT pk_hmm_risk_daily_alert PRIMARY KEY,
        run_id TEXT NOT NULL CONSTRAINT fk_hmm_risk_alert_run REFERENCES hmm_risk.daily_generation_run(run_id) ON DELETE RESTRICT,
        state_id TEXT NOT NULL CONSTRAINT fk_hmm_risk_alert_state REFERENCES hmm_risk.sector_state_timeline(state_id) ON DELETE RESTRICT,
        candidate_id TEXT NOT NULL,
        trade_date DATE NOT NULL,
        sector_level TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        severity TEXT NOT NULL,
        transition_from TEXT NOT NULL,
        transition_to TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        generator_version TEXT NOT NULL,
        explanation_version TEXT NOT NULL,
        explanation JSONB NOT NULL,
        input_hash CHAR(64) NOT NULL,
        result_hash CHAR(64) NOT NULL,
        dedupe_key CHAR(64) NOT NULL,
        revision INTEGER NOT NULL,
        supersedes_alert_id TEXT CONSTRAINT fk_hmm_risk_alert_supersedes REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_hmm_risk_alert_state UNIQUE (state_id),
        CONSTRAINT uq_hmm_risk_alert_revision UNIQUE (dedupe_key,revision),
        CONSTRAINT uq_hmm_risk_alert_input UNIQUE (dedupe_key,input_hash),
        CONSTRAINT ck_hmm_risk_alert_level CHECK (sector_level IN ('L1','L2')),
        CONSTRAINT ck_hmm_risk_alert_severity CHECK (severity IN ('HIGH','MEDIUM','OPPORTUNITY')),
        CONSTRAINT ck_hmm_risk_alert_transition_from CHECK (transition_from IN ('trending','neutral','fading')),
        CONSTRAINT ck_hmm_risk_alert_transition_to CHECK (transition_to IN ('trending','neutral','fading')),
        CONSTRAINT ck_hmm_risk_alert_revision CHECK (revision>0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.risk_event (
        event_revision_id TEXT CONSTRAINT pk_hmm_risk_event_revision PRIMARY KEY,
        event_id TEXT NOT NULL,
        dedupe_key CHAR(64) NOT NULL,
        candidate_id TEXT NOT NULL,
        sector_level TEXT NOT NULL,
        sector_code TEXT NOT NULL,
        event_type TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        status TEXT NOT NULL,
        revision INTEGER NOT NULL,
        first_alert_id TEXT NOT NULL CONSTRAINT fk_hmm_risk_event_first_alert REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT,
        latest_alert_id TEXT NOT NULL CONSTRAINT fk_hmm_risk_event_latest_alert REFERENCES hmm_risk.daily_alert(alert_id) ON DELETE RESTRICT,
        opened_trade_date DATE NOT NULL,
        last_trade_date DATE NOT NULL,
        resolved_trade_date DATE,
        resolution_reason TEXT,
        supersedes_event_revision_id TEXT CONSTRAINT fk_hmm_risk_event_supersedes REFERENCES hmm_risk.risk_event(event_revision_id) ON DELETE RESTRICT,
        result_hash CHAR(64) NOT NULL,
        evidence JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_hmm_risk_event_revision UNIQUE (event_id,revision),
        CONSTRAINT uq_hmm_risk_event_result UNIQUE (event_id,result_hash),
        CONSTRAINT ck_hmm_risk_event_level CHECK (sector_level IN ('L1','L2')),
        CONSTRAINT ck_hmm_risk_event_type CHECK (event_type='fading_risk'),
        CONSTRAINT ck_hmm_risk_event_status CHECK (status IN ('open','resolved')),
        CONSTRAINT ck_hmm_risk_event_revision CHECK (revision>0),
        CONSTRAINT ck_hmm_risk_event_dates CHECK (opened_trade_date<=last_trade_date AND (resolved_trade_date IS NULL OR resolved_trade_date>=last_trade_date)),
        CONSTRAINT ck_hmm_risk_event_resolution CHECK ((status='open' AND resolved_trade_date IS NULL AND resolution_reason IS NULL) OR (status='resolved' AND resolved_trade_date IS NOT NULL AND resolution_reason IN ('fading_exit_to_neutral','fading_exit_to_trending')))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.retrospective_report (
        report_id TEXT CONSTRAINT pk_hmm_risk_report PRIMARY KEY,
        candidate_id TEXT NOT NULL,
        candidate_manifest_hash CHAR(64) NOT NULL,
        model_artifact_sha256 CHAR(64) NOT NULL,
        start_trade_date DATE NOT NULL,
        end_trade_date DATE NOT NULL,
        sector_level TEXT NOT NULL,
        report_spec JSONB NOT NULL,
        report_spec_hash CHAR(64) NOT NULL,
        source_manifest JSONB NOT NULL,
        source_hash CHAR(64) NOT NULL,
        status TEXT NOT NULL,
        metrics JSONB,
        evidence JSONB NOT NULL,
        result_hash CHAR(64),
        sample_count INTEGER NOT NULL DEFAULT 0,
        error_code TEXT,
        error_message TEXT,
        error_context JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at TIMESTAMPTZ NOT NULL,
        CONSTRAINT uq_hmm_risk_report_identity UNIQUE (candidate_id,start_trade_date,end_trade_date,sector_level,report_spec_hash,source_hash),
        CONSTRAINT ck_hmm_risk_report_dates CHECK (start_trade_date<=end_trade_date),
        CONSTRAINT ck_hmm_risk_report_level CHECK (sector_level IN ('L1','L2')),
        CONSTRAINT ck_hmm_risk_report_status CHECK (status IN ('succeeded','failed')),
        CONSTRAINT ck_hmm_risk_report_sample_count CHECK (sample_count>=0),
        CONSTRAINT ck_hmm_risk_report_terminal CHECK ((status='succeeded' AND metrics IS NOT NULL AND result_hash IS NOT NULL AND error_code IS NULL) OR (status='failed' AND metrics IS NULL AND result_hash IS NULL AND error_code IS NOT NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hmm_risk.rotation_l1_prediction (
        prediction_id UUID CONSTRAINT pk_hmm_risk_rotation_l1_prediction PRIMARY KEY,
        product_bundle_id TEXT,
        trade_date DATE NOT NULL,
        as_of_date DATE NOT NULL,
        sector_level TEXT NOT NULL DEFAULT 'L1',
        sector_code TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        rotation_score DOUBLE PRECISION,
        forecast_state TEXT,
        feature_contributions JSONB,
        availability TEXT NOT NULL,
        reason_code TEXT,
        research_surface_status TEXT NOT NULL,
        rotation_l1_capability_status TEXT NOT NULL,
        forward_power_status TEXT NOT NULL,
        forward_confirmation TEXT NOT NULL,
        advisory_status TEXT NOT NULL,
        validation_basis TEXT NOT NULL,
        development_oof_rank_ic DOUBLE PRECISION,
        development_oof_rank_ic_hac_lower DOUBLE PRECISION,
        development_oof_rank_ic_hac_upper DOUBLE PRECISION,
        model_hash CHAR(64) NOT NULL,
        input_hash CHAR(64) NOT NULL,
        mapping_snapshot_hash CHAR(64) NOT NULL,
        tail_accessed BOOLEAN NOT NULL DEFAULT FALSE,
        revision INTEGER NOT NULL,
        supersedes_prediction_id UUID CONSTRAINT fk_hmm_risk_rotation_l1_prediction_supersedes
          REFERENCES hmm_risk.rotation_l1_prediction(prediction_id) ON DELETE RESTRICT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_hmm_risk_rotation_l1_prediction_revision UNIQUE (model_hash,trade_date,sector_code,revision),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_dates CHECK (as_of_date<trade_date),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_level CHECK (
          sector_level='L1' AND btrim(sector_code)<>'' AND btrim(sector_name)<>''
          AND (product_bundle_id IS NULL OR btrim(product_bundle_id)<>'')
        ),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_state CHECK (forecast_state IS NULL OR forecast_state IN ('trending','neutral','fading')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_availability CHECK (
          (availability='available' AND rotation_score IS NOT NULL
            AND rotation_score>'-Infinity'::double precision AND rotation_score<'Infinity'::double precision
            AND forecast_state IS NOT NULL AND feature_contributions IS NOT NULL
            AND jsonb_typeof(feature_contributions)='array' AND jsonb_array_length(feature_contributions)=10
            AND NOT jsonb_path_exists(feature_contributions,'$[*] ? (@.type() != "number")')
            AND reason_code IS NULL)
          OR (availability='unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
            AND feature_contributions IS NULL AND reason_code IS NOT NULL AND btrim(reason_code)<>'')
        ),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_surface CHECK (research_surface_status='NOT_AVAILABLE'),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_capability CHECK (rotation_l1_capability_status IN ('NOT_AVAILABLE','RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED','ADVISORY_PREDICTION_AVAILABLE')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_power CHECK (forward_power_status IN ('UNAVAILABLE','INSUFFICIENT','SUFFICIENT')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_confirmation CHECK (forward_confirmation IN ('NOT_STARTED','PENDING_INSUFFICIENT_POWER','PENDING_INCONCLUSIVE','PASSED','FAILED')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_advisory CHECK (advisory_status IN ('NOT_AVAILABLE','AVAILABLE')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_validation_basis CHECK (validation_basis IN ('development_causal_oof','single_date_frozen_model')),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_hashes CHECK (
          model_hash ~ '^[0-9a-f]{64}$' AND input_hash ~ '^[0-9a-f]{64}$' AND mapping_snapshot_hash ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_revision CHECK (revision>0),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_supersedes_chain CHECK (
          (revision=1 AND supersedes_prediction_id IS NULL)
          OR (revision>1 AND supersedes_prediction_id IS NOT NULL)
        ),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_advisory_coupling CHECK (
          advisory_status<>'AVAILABLE' OR (
            rotation_l1_capability_status='ADVISORY_PREDICTION_AVAILABLE'
            AND forward_confirmation='PASSED' AND product_bundle_id IS NOT NULL AND tail_accessed
          )
        ),
        CONSTRAINT ck_hmm_risk_rotation_l1_prediction_metrics CHECK (
          (development_oof_rank_ic IS NULL AND development_oof_rank_ic_hac_lower IS NULL AND development_oof_rank_ic_hac_upper IS NULL)
          OR (
            development_oof_rank_ic IS NOT NULL
            AND development_oof_rank_ic_hac_lower IS NOT NULL
            AND development_oof_rank_ic_hac_upper IS NOT NULL
            AND development_oof_rank_ic>'-Infinity'::double precision AND development_oof_rank_ic<'Infinity'::double precision
            AND development_oof_rank_ic_hac_lower>'-Infinity'::double precision AND development_oof_rank_ic_hac_lower<'Infinity'::double precision
            AND development_oof_rank_ic_hac_upper>'-Infinity'::double precision AND development_oof_rank_ic_hac_upper<'Infinity'::double precision
            AND development_oof_rank_ic_hac_lower<=development_oof_rank_ic
            AND development_oof_rank_ic<=development_oof_rank_ic_hac_upper
          )
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_run_claim ON hmm_risk.daily_generation_run(status,queued_at,run_id) WHERE status='queued'",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_run_lease ON hmm_risk.daily_generation_run(lease_expires_at,run_id) WHERE status IN ('running','cancel_requested')",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_state_lookup ON hmm_risk.sector_state_timeline(candidate_id,sector_level,sector_code,trade_date DESC,revision DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_state_run ON hmm_risk.sector_state_timeline(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_alert_lookup ON hmm_risk.daily_alert(candidate_id,trade_date DESC,sector_level,severity)",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_event_lookup ON hmm_risk.risk_event(candidate_id,status,sector_level,sector_code,last_trade_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_report_lookup ON hmm_risk.retrospective_report(candidate_id,end_trade_date DESC,sector_level)",
    "CREATE INDEX IF NOT EXISTS idx_hmm_risk_rotation_l1_lookup ON hmm_risk.rotation_l1_prediction(trade_date,sector_code,revision DESC)",
)


VIEW_DDL = (
    """
    CREATE OR REPLACE VIEW hmm_risk.sector_state_current AS
    SELECT s.state_id,s.run_id,s.candidate_id,s.candidate_manifest_hash,s.snapshot_id,s.config_id,
           s.trade_date,s.as_of_date,s.sector_level,s.sector_code,s.sector_name,s.hmm_state,
           s.state_probabilities,s.state_confidence,s.state_origin,s.confidence_definition_version,
           s.parser_contract,s.adapter_version,s.observation_version,s.state_model_set_id,
           s.model_artifact_sha256,s.input_hash,s.result_hash,s.mapping_snapshot_hash,
           s.generator_version,s.rule_version,s.transition_from,s.transition_kind,s.severity,
           s.dedupe_key,s.revision,s.supersedes_state_id,s.evidence,s.created_at
    FROM hmm_risk.sector_state_timeline s
    WHERE NOT EXISTS (
      SELECT 1 FROM hmm_risk.sector_state_timeline newer
      WHERE newer.dedupe_key=s.dedupe_key AND newer.revision>s.revision
    )
    """,
    """
    CREATE OR REPLACE VIEW hmm_risk.daily_alert_current AS
    SELECT a.alert_id,a.run_id,a.state_id,a.candidate_id,a.trade_date,a.sector_level,a.sector_code,
           a.severity,a.transition_from,a.transition_to,a.rule_version,a.generator_version,
           a.explanation_version,a.explanation,a.input_hash,a.result_hash,a.dedupe_key,a.revision,
           a.supersedes_alert_id,a.created_at
    FROM hmm_risk.daily_alert a
    WHERE NOT EXISTS (
      SELECT 1 FROM hmm_risk.daily_alert newer
      WHERE newer.dedupe_key=a.dedupe_key AND newer.revision>a.revision
    )
    """,
    """
    CREATE OR REPLACE VIEW hmm_risk.risk_event_current AS
    SELECT e.event_revision_id,e.event_id,e.dedupe_key,e.candidate_id,e.sector_level,e.sector_code,
           e.event_type,e.rule_version,e.status,e.revision,e.first_alert_id,e.latest_alert_id,
           e.opened_trade_date,e.last_trade_date,e.resolved_trade_date,e.resolution_reason,
           e.supersedes_event_revision_id,e.result_hash,e.evidence,e.created_at
    FROM hmm_risk.risk_event e
    WHERE NOT EXISTS (
      SELECT 1 FROM hmm_risk.risk_event newer
      WHERE newer.event_id=e.event_id AND newer.revision>e.revision
    )
    """,
)


TABLE_COMMENTS = {
    "daily_generation_run": "Durable HMM Risk daily generation request, lease, counters and terminal evidence.",
    "sector_state_timeline": "Append-only direct HMM L1/L2 state revisions with frozen input evidence.",
    "daily_alert": "Append-only non-NONE HMM Risk alert revisions derived from state transitions.",
    "risk_event": "Append-only fading-risk event lifecycle revisions.",
    "retrospective_report": "Advisory-only retrospective report receipts and exact source evidence.",
}
VIEW_COMMENTS = {
    "sector_state_current": "Latest revision per HMM Risk state dedupe identity.",
    "daily_alert_current": "Latest revision per HMM Risk alert dedupe identity.",
    "risk_event_current": "Latest revision per stable HMM Risk event identity.",
}
INDEX_COMMENTS = {name: name.replace("idx_hmm_risk_", "HMM Risk ").replace("_", " ") for name in EXPECTED_INDEXES}


def _quote(value: str) -> str:
    return value.replace("'", "''")


def _comment_ddl() -> Iterable[str]:
    yield f"COMMENT ON SCHEMA {SCHEMA_NAME} IS '{_quote(SCHEMA_COMMENT)}'"
    for table, columns in EXPECTED_COLUMNS.items():
        yield f"COMMENT ON TABLE {SCHEMA_NAME}.{table} IS '{_quote(TABLE_COMMENTS[table])}'"
        for column in columns:
            text = f"{table}.{column} exact {SCHEMA_VERSION} contract"
            yield f"COMMENT ON COLUMN {SCHEMA_NAME}.{table}.{column} IS '{_quote(text)}'"
        for constraint in EXPECTED_CONSTRAINTS[table]:
            text = f"{constraint} enforces {SCHEMA_VERSION}"
            yield f"COMMENT ON CONSTRAINT {constraint} ON {SCHEMA_NAME}.{table} IS '{_quote(text)}'"
    for name, text in INDEX_COMMENTS.items():
        yield f"COMMENT ON INDEX {SCHEMA_NAME}.{name} IS '{_quote(text)}'"
    for name, text in VIEW_COMMENTS.items():
        yield f"COMMENT ON VIEW {SCHEMA_NAME}.{name} IS '{_quote(text)}'"
    yield (
        "COMMENT ON TABLE hmm_risk.rotation_l1_prediction IS "
        "'Append-only G2-A L1 rotation prediction revisions; scores are not probabilities.'"
    )
    for column in ROTATION_L1_PREDICTION_COLUMNS:
        yield (
            f"COMMENT ON COLUMN hmm_risk.rotation_l1_prediction.{column} IS "
            f"'rotation_l1_prediction.{column} exact hmm_risk_rotation_l1_prediction_v1 contract'"
        )
    for constraint in sorted(ROTATION_L1_PREDICTION_CONSTRAINTS):
        yield (
            f"COMMENT ON CONSTRAINT {constraint} ON hmm_risk.rotation_l1_prediction IS "
            f"'{constraint} enforces hmm_risk_rotation_l1_prediction_v1'"
        )
    yield (
        "COMMENT ON INDEX hmm_risk.idx_hmm_risk_rotation_l1_lookup IS "
        "'Date and sector L1 rotation revision lookup; model identity remains explicit.'"
    )


def iter_ddl() -> Iterable[str]:
    yield from TABLE_DDL
    yield from VIEW_DDL
    yield from _comment_ddl()


def bootstrap_schema(conn_factory: Callable[[], Any] | None = None) -> None:
    """Apply and verify the schema in one transaction; any drift rolls back."""

    factory = conn_factory or (lambda: get_conn(autocommit=False, manage_transaction=True))
    with factory() as conn:
        with conn.cursor() as cursor:
            for statement in iter_ddl():
                cursor.execute(statement)
        verify_schema(conn)


def _normalize_definition(value: Any) -> str | None:
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def collect_schema_contract(conn: Any) -> dict[str, Any]:
    """Read the exact structural/comment contract from PostgreSQL catalogs."""

    contract: dict[str, Any] = {"schema_comment": None, "tables": {}, "indexes": [], "views": []}

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=%s",
            (SCHEMA_NAME,),
        )
        row = cursor.fetchone()
        contract["schema_comment"] = row[0] if row else None

        for table in EXPECTED_COLUMNS:
            cursor.execute(
                """
                SELECT a.attnum, a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod),
                       a.attnotnull, pg_get_expr(ad.adbin,ad.adrelid),
                       col_description(c.oid,a.attnum)
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                JOIN pg_attribute a ON a.attrelid=c.oid
                LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
                WHERE n.nspname=%s AND c.relname=%s AND c.relkind='r'
                  AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum
                """,
                (SCHEMA_NAME, table),
            )
            column_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT con.conname, con.contype, pg_get_constraintdef(con.oid,true),
                       obj_description(con.oid,'pg_constraint')
                FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=%s AND c.relname=%s ORDER BY con.conname
                """,
                (SCHEMA_NAME, table),
            )
            constraint_rows = cursor.fetchall()

            cursor.execute(
                "SELECT obj_description(c.oid,'pg_class') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s",
                (SCHEMA_NAME, table),
            )
            comment = cursor.fetchone()
            contract["tables"][table] = {
                "comment": comment[0] if comment else None,
                "columns": [
                    {
                        "ordinal": int(item[0]),
                        "name": str(item[1]),
                        "type": _normalize_definition(item[2]),
                        "not_null": bool(item[3]),
                        "default": _normalize_definition(item[4]),
                        "comment": item[5],
                    }
                    for item in column_rows
                ],
                "constraints": [
                    {
                        "name": str(item[0]),
                        "type": str(item[1]),
                        "definition": _normalize_definition(item[2]),
                        "comment": item[3],
                    }
                    for item in constraint_rows
                ],
            }

        cursor.execute(
            """
            SELECT idx.relname, pg_get_indexdef(idx.oid), obj_description(idx.oid,'pg_class')
            FROM pg_index i
            JOIN pg_class idx ON idx.oid=i.indexrelid
            JOIN pg_class tbl ON tbl.oid=i.indrelid
            JOIN pg_namespace n ON n.oid=tbl.relnamespace
            LEFT JOIN pg_constraint con ON con.conindid=idx.oid
            WHERE n.nspname=%s AND tbl.relname=ANY(%s) AND con.oid IS NULL
            ORDER BY idx.relname
            """,
            (SCHEMA_NAME, list(EXPECTED_COLUMNS)),
        )
        contract["indexes"] = [
            {
                "name": str(item[0]),
                "definition": _normalize_definition(item[1]),
                "comment": item[2],
            }
            for item in cursor.fetchall()
        ]

        cursor.execute(
            """
            SELECT c.relname, pg_get_viewdef(c.oid,true), obj_description(c.oid,'pg_class')
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relkind='v' ORDER BY c.relname
            """,
            (SCHEMA_NAME,),
        )
        contract["views"] = [
            {
                "name": str(item[0]),
                "definition": _normalize_definition(item[1]),
                "comment": item[2],
            }
            for item in cursor.fetchall()
        ]
    return contract


def _structure_payload(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "tables": {
            table: {
                "columns": [
                    {key: column[key] for key in ("ordinal", "name", "type", "not_null", "default")}
                    for column in body["columns"]
                ],
                "constraints": [
                    {key: constraint[key] for key in ("name", "type", "definition")}
                    for constraint in body["constraints"]
                ],
            }
            for table, body in contract["tables"].items()
        },
        "indexes": [{key: index[key] for key in ("name", "definition")} for index in contract["indexes"]],
        "views": [{key: view[key] for key in ("name", "definition")} for view in contract["views"]],
    }


def verify_contract_snapshot(contract: Mapping[str, Any]) -> None:
    """Fail closed on any structural or human-readable contract drift."""

    if contract.get("schema_comment") != SCHEMA_COMMENT:
        raise RuntimeError("hmm_risk_schema_drift: schema version/comment mismatch")
    tables = contract.get("tables")
    if not isinstance(tables, Mapping) or set(tables) != set(EXPECTED_COLUMNS):
        raise RuntimeError("hmm_risk_schema_drift: table set")
    for table, expected_columns in EXPECTED_COLUMNS.items():
        body = tables[table]
        if body.get("comment") != TABLE_COMMENTS[table]:
            raise RuntimeError(f"hmm_risk_schema_drift: table comment {table}")
        columns = body.get("columns", [])
        if tuple(column.get("name") for column in columns) != expected_columns:
            raise RuntimeError(f"hmm_risk_schema_drift: columns {table}")
        for column in columns:
            expected_comment = f"{table}.{column['name']} exact {SCHEMA_VERSION} contract"
            if column.get("comment") != expected_comment:
                raise RuntimeError(f"hmm_risk_schema_drift: column comment {table}.{column['name']}")
        constraints = body.get("constraints", [])
        if frozenset(item.get("name") for item in constraints) != EXPECTED_CONSTRAINTS[table]:
            raise RuntimeError(f"hmm_risk_schema_drift: constraints {table}")
        for constraint in constraints:
            expected_comment = f"{constraint['name']} enforces {SCHEMA_VERSION}"
            if constraint.get("comment") != expected_comment:
                raise RuntimeError(f"hmm_risk_schema_drift: constraint comment {constraint['name']}")

    indexes = contract.get("indexes", [])
    if frozenset(item.get("name") for item in indexes) != EXPECTED_INDEXES:
        raise RuntimeError("hmm_risk_schema_drift: indexes")
    for index in indexes:
        if index.get("comment") != INDEX_COMMENTS[index["name"]]:
            raise RuntimeError(f"hmm_risk_schema_drift: index comment {index['name']}")

    views = contract.get("views", [])
    if frozenset(item.get("name") for item in views) != EXPECTED_VIEWS:
        raise RuntimeError("hmm_risk_schema_drift: views")
    for view in views:
        if view.get("comment") != VIEW_COMMENTS[view["name"]]:
            raise RuntimeError(f"hmm_risk_schema_drift: view comment {view['name']}")
        if re.search(r"select\s+\*", str(view.get("definition") or ""), re.IGNORECASE):
            raise RuntimeError(f"hmm_risk_schema_drift: view wildcard {view['name']}")

    actual_hash = _canonical_sha256(_structure_payload(contract))
    if actual_hash != EXPECTED_STRUCTURE_SHA256:
        raise RuntimeError(
            f"hmm_risk_schema_drift: structure hash expected={EXPECTED_STRUCTURE_SHA256} actual={actual_hash}"
        )


def verify_schema(conn: Any) -> None:
    """Read and verify the exact schema contract."""

    verify_contract_snapshot(collect_schema_contract(conn))
    verify_rotation_l1_prediction_schema(conn)


def verify_rotation_l1_prediction_schema(conn: Any) -> None:
    """Verify the separately versioned G2-A prediction table contract."""

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,
                   pg_get_expr(ad.adbin,ad.adrelid),col_description(c.oid,a.attnum)
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_attribute a ON a.attrelid=c.oid
            LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
            WHERE n.nspname=%s AND c.relname='rotation_l1_prediction'
              AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (SCHEMA_NAME,),
        )
        columns = cursor.fetchall()
        cursor.execute(
            """
            SELECT con.conname,pg_get_constraintdef(con.oid,true),obj_description(con.oid,'pg_constraint')
            FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relname='rotation_l1_prediction'
            ORDER BY con.conname
            """,
            (SCHEMA_NAME,),
        )
        constraints = cursor.fetchall()
        cursor.execute(
            """
            SELECT obj_description(c.oid,'pg_class')
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relname='rotation_l1_prediction' AND c.relkind='r'
            """,
            (SCHEMA_NAME,),
        )
        table_row = cursor.fetchone()
        cursor.execute(
            """
            SELECT pg_get_indexdef(idx.oid),obj_description(idx.oid,'pg_class')
            FROM pg_class idx JOIN pg_namespace n ON n.oid=idx.relnamespace
            WHERE n.nspname=%s AND idx.relname='idx_hmm_risk_rotation_l1_lookup'
            """,
            (SCHEMA_NAME,),
        )
        index_row = cursor.fetchone()
    if tuple(row[0] for row in columns) != ROTATION_L1_PREDICTION_COLUMNS:
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: columns")
    if any(
        (_normalize_definition(row[1]), bool(row[2]), _normalize_definition(row[3]))
        != ROTATION_L1_PREDICTION_COLUMN_CONTRACT[row[0]]
        for row in columns
    ):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: column structure")
    if any(
        row[4] != f"rotation_l1_prediction.{row[0]} exact hmm_risk_rotation_l1_prediction_v1 contract"
        for row in columns
    ):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: column comments")
    if frozenset(row[0] for row in constraints) != ROTATION_L1_PREDICTION_CONSTRAINTS:
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: constraints")
    if any(row[2] != f"{row[0]} enforces hmm_risk_rotation_l1_prediction_v1" for row in constraints):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: constraint comments")
    if any(
        any(token.lower() not in str(row[1]).lower() for token in ROTATION_L1_PREDICTION_CONSTRAINT_TOKENS[row[0]])
        for row in constraints
    ):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: constraint definitions")
    if not table_row or table_row[0] != (
        "Append-only G2-A L1 rotation prediction revisions; scores are not probabilities."
    ):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: table comment")
    if (
        not index_row
        or "(trade_date, sector_code, revision DESC)" not in str(index_row[0])
        or index_row[1] != ("Date and sector L1 rotation revision lookup; model identity remains explicit.")
    ):
        raise RuntimeError("hmm_risk_rotation_l1_schema_drift: index comment")


if __name__ == "__main__":
    bootstrap_schema()
