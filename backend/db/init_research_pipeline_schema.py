"""Explicit schema bootstrap for Research Pipeline metadata.

Business services must not create these tables implicitly. Run this module only
as an operator-controlled bootstrap or reviewed migration before enabling the
Research Pipeline API in a new environment.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    from .pg_pool import get_conn
except ImportError:  # pragma: no cover - direct script execution convenience.
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from backend.db.pg_pool import get_conn


RESEARCH_PIPELINE_SCHEMA_VERSION = "research_pipeline_v2_20260519_hmm_backtest"

RESEARCH_PIPELINE_TABLES: tuple[str, ...] = (
    "research_pipeline.schema_version",
    "research_pipeline.experiment",
    "research_pipeline.stage_plan",
    "research_pipeline.stage_attempt",
    "research_pipeline.external_run_link",
    "research_pipeline.artifact_ref",
    "research_pipeline.comparison",
    "research_pipeline.pipeline_event",
    "research_pipeline.backtest_record",
    "research_pipeline.backfill_run",
)

BASE_DDL: list[str] = [
    "CREATE SCHEMA IF NOT EXISTS research_pipeline",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.schema_version (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        description TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.experiment (
        experiment_id TEXT PRIMARY KEY,
        pipeline_type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        status TEXT NOT NULL DEFAULT 'draft',
        criteria_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        baseline_ref_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        issue_url TEXT,
        blocked_reason TEXT,
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_by TEXT NOT NULL DEFAULT 'codex',
        validated_at TIMESTAMPTZ,
        promotion_requested_at TIMESTAMPTZ,
        promoted_at TIMESTAMPTZ,
        rejected_at TIMESTAMPTZ,
        blocked_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_experiment_pipeline_type CHECK (pipeline_type IN ('hmm_research','event_signal_research')),
        CONSTRAINT ck_rp_experiment_status CHECK (
            status IN ('draft','running','stage_failed','validated','rejected','blocked','promotion_requested','promoted')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_experiment_type_status ON research_pipeline.experiment(pipeline_type, status)",
    "CREATE INDEX IF NOT EXISTS idx_rp_experiment_status_updated ON research_pipeline.experiment(status, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.stage_plan (
        stage_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_name TEXT NOT NULL,
        stage_order INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        planned_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        latest_attempt_no INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_rp_stage_plan_experiment_stage UNIQUE (experiment_id, stage_name),
        CONSTRAINT ck_rp_stage_plan_status CHECK (status IN ('queued','running','passed','failed','cancelled','timeout'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_stage_plan_experiment_order ON research_pipeline.stage_plan(experiment_id, stage_order)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.stage_attempt (
        stage_attempt_id TEXT PRIMARY KEY,
        stage_id TEXT NOT NULL REFERENCES research_pipeline.stage_plan(stage_id) ON DELETE CASCADE,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_name TEXT NOT NULL,
        attempt_no INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_message TEXT,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_rp_stage_attempt_experiment_stage_attempt UNIQUE (experiment_id, stage_name, attempt_no),
        CONSTRAINT ck_rp_stage_attempt_status CHECK (status IN ('queued','running','passed','failed','cancelled','timeout'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_stage_attempt_stage_attempt ON research_pipeline.stage_attempt(stage_id, attempt_no DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rp_stage_attempt_experiment_stage_attempt ON research_pipeline.stage_attempt(experiment_id, stage_name, attempt_no DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rp_stage_attempt_status_started ON research_pipeline.stage_attempt(status, started_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.external_run_link (
        link_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        run_type TEXT NOT NULL,
        external_id TEXT NOT NULL,
        external_url TEXT,
        status TEXT,
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_external_run_type CHECK (
            run_type IN ('qe_template','qe_task','qe_loop','qe_archive_run','validation_run','event_signal_validation','hmm_job')
        )
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_external_run_experiment_type ON research_pipeline.external_run_link(experiment_id, run_type)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_rp_external_run_type_id ON research_pipeline.external_run_link(run_type, external_id)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.artifact_ref (
        artifact_ref_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        domain_type TEXT NOT NULL,
        domain_id TEXT,
        artifact_uri TEXT,
        artifact_sha256 TEXT,
        status TEXT NOT NULL DEFAULT 'candidate',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_artifact_domain_type CHECK (
            domain_type IN ('factor','model','strategy_pkg','qe_archive','event_signal','hmm_artifact','file')
        ),
        CONSTRAINT ck_rp_artifact_status CHECK (status IN ('candidate','validated','superseded','deleted'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_artifact_ref_experiment_domain_status ON research_pipeline.artifact_ref(experiment_id, domain_type, status)",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_rp_artifact_ref_identity
        ON research_pipeline.artifact_ref(
            experiment_id,
            domain_type,
            COALESCE(domain_id, ''),
            COALESCE(artifact_uri, ''),
            COALESCE(artifact_sha256, '')
        )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.comparison (
        comparison_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        baseline_ref_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        candidate_ref_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        criteria_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        verdict TEXT NOT NULL,
        reason_md TEXT,
        created_by TEXT NOT NULL DEFAULT 'codex',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_comparison_verdict CHECK (verdict IN ('pass','fail','inconclusive','blocked'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_comparison_experiment_created ON research_pipeline.comparison(experiment_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rp_comparison_experiment_verdict_created ON research_pipeline.comparison(experiment_id, verdict, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.pipeline_event (
        event_id TEXT PRIMARY KEY,
        experiment_id TEXT REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        event_type TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'info',
        message TEXT NOT NULL,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_by TEXT NOT NULL DEFAULT 'codex',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_pipeline_event_severity CHECK (severity IN ('debug','info','warning','error'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_pipeline_event_experiment_created ON research_pipeline.pipeline_event(experiment_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rp_pipeline_event_attempt_created ON research_pipeline.pipeline_event(stage_attempt_id, created_at DESC)",

    """
    CREATE TABLE IF NOT EXISTS research_pipeline.backtest_record (
        record_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        pipeline_type TEXT NOT NULL DEFAULT 'hmm_research',
        research_domain TEXT NOT NULL DEFAULT 'hmm',
        source_type TEXT NOT NULL,
        source_task_id TEXT NOT NULL,
        source_loop_id TEXT NOT NULL,
        source_loop_index INTEGER,
        source_experiment_id TEXT,
        source_created_at TIMESTAMPTZ,
        record_version TEXT NOT NULL DEFAULT 'hmm_backtest_record_v1',
        record_key_sha256 TEXT NOT NULL,
        non_hmm_config_sig TEXT,
        hmm_config_sig TEXT,
        strict_family_sig TEXT,
        archive_family_sig TEXT,
        dedup_status TEXT NOT NULL DEFAULT 'primary',
        qe_archive_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        qe_archive_representative BOOLEAN NOT NULL DEFAULT FALSE,
        rejection_reason TEXT,
        ann DOUBLE PRECISION,
        mdd DOUBLE PRECISION,
        ir DOUBLE PRECISION,
        ic DOUBLE PRECISION,
        rank_ic DOUBLE PRECISION,
        sharpe DOUBLE PRECISION,
        turnover DOUBLE PRECISION,
        metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        hmm_config_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        config_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        recorded_by TEXT NOT NULL DEFAULT 'auto_hook',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_rp_backtest_record_source UNIQUE (source_type, source_task_id, source_loop_id, source_loop_index, record_version),
        CONSTRAINT uq_rp_backtest_record_key UNIQUE (record_key_sha256),
        CONSTRAINT ck_rp_backtest_source_type CHECK (source_type IN ('qe_loop','historical_file','manual_repair')),
        CONSTRAINT ck_rp_backtest_dedup_status CHECK (dedup_status IN ('primary','duplicate_same_config','hmm_variant','excluded'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_experiment_created ON research_pipeline.backtest_record(experiment_id, source_created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_non_hmm_family ON research_pipeline.backtest_record(experiment_id, non_hmm_config_sig)",
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_hmm_family ON research_pipeline.backtest_record(experiment_id, hmm_config_sig)",
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_archive_family ON research_pipeline.backtest_record(experiment_id, archive_family_sig)",
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_representative ON research_pipeline.backtest_record(experiment_id, qe_archive_representative)",
    "CREATE INDEX IF NOT EXISTS idx_rp_backtest_source_task_loop ON research_pipeline.backtest_record(source_task_id, source_loop_index)",
    """
    CREATE TABLE IF NOT EXISTS research_pipeline.backfill_run (
        backfill_run_id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
        backfill_type TEXT NOT NULL DEFAULT 'hmm_backtest_timeline',
        status TEXT NOT NULL DEFAULT 'previewed',
        dry_run BOOLEAN NOT NULL DEFAULT TRUE,
        source_scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_fingerprint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        counts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        stage_attempt_id TEXT REFERENCES research_pipeline.stage_attempt(stage_attempt_id) ON DELETE SET NULL,
        error_message TEXT,
        created_by TEXT NOT NULL DEFAULT 'codex',
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rp_backfill_status CHECK (status IN ('previewed','running','completed','failed','cancelled'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rp_backfill_experiment_created ON research_pipeline.backfill_run(experiment_id, created_at DESC)",
    """
    INSERT INTO research_pipeline.schema_version(version, description)
    VALUES (%s, 'Research Pipeline metadata schema bootstrap')
    ON CONFLICT (version) DO NOTHING
    """,
]

TABLE_COMMENTS: dict[str, str] = {
    "research_pipeline.schema_version": "Applied Research Pipeline schema versions and bootstrap metadata.",
    "research_pipeline.experiment": "Research experiment metadata and lifecycle status without owning produced assets.",
    "research_pipeline.stage_plan": "Planned stages and current stage status for a Research Pipeline experiment.",
    "research_pipeline.stage_attempt": "Immutable stage execution attempts; retries append rows instead of overwriting history.",
    "research_pipeline.external_run_link": "References to external validation, QE, event-signal, or HMM runs owned by other systems.",
    "research_pipeline.artifact_ref": "References to candidate or validated artifacts without taking ownership of asset registries.",
    "research_pipeline.comparison": "Baseline versus candidate metrics, criteria, verdict, and explanation.",
    "research_pipeline.pipeline_event": "Audit event stream for experiment, stage, comparison, issue, and promotion decisions.",
    "research_pipeline.backtest_record": "Queryable HMM backtest timeline records captured by Research Pipeline.",
    "research_pipeline.backfill_run": "Historical backfill preview and execution audit records for Research Pipeline.",
}

COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "research_pipeline.schema_version": {
        "version": "Applied schema version identifier.",
        "applied_at": "Timestamp when the schema version row was recorded.",
        "description": "Human readable schema version description.",
    },
    "research_pipeline.experiment": {
        "experiment_id": "Stable Research Pipeline experiment identifier.",
        "pipeline_type": "Supported research pipeline type such as hmm_research or event_signal_research.",
        "title": "Human readable experiment title.",
        "description": "Optional experiment description.",
        "status": "Experiment lifecycle status from draft through validation, rejection, or promotion request.",
        "criteria_json": "Decision criteria used to evaluate baseline and candidate metrics.",
        "baseline_ref_json": "Reference to baseline artifacts or run context; not an owned asset record.",
        "issue_url": "Issue or review URL associated with a promotion request or research finding.",
        "blocked_reason": "Reason the experiment is blocked when status is blocked.",
        "metadata_json": "Additional metadata for UI and audit use.",
        "created_by": "Actor that created the experiment metadata.",
        "validated_at": "Timestamp when the experiment was marked validated.",
        "promotion_requested_at": "Timestamp when promotion was requested without writing production assets.",
        "promoted_at": "Timestamp for future promotion completion bookkeeping.",
        "rejected_at": "Timestamp when the experiment was rejected.",
        "blocked_at": "Timestamp when the experiment became blocked.",
        "created_at": "Timestamp when the experiment row was created.",
        "updated_at": "Timestamp when the experiment row was last updated.",
    },
    "research_pipeline.stage_plan": {
        "stage_id": "Stable stage plan identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_name": "Stable stage name inside the experiment pipeline.",
        "stage_order": "Stage display and execution ordering.",
        "status": "Current stage status summarized from stage attempts.",
        "planned_config_json": "Stage configuration or expected inputs.",
        "latest_attempt_no": "Latest attempt number recorded for this stage.",
        "created_at": "Timestamp when the stage plan row was created.",
        "updated_at": "Timestamp when the stage plan row was last updated.",
    },
    "research_pipeline.stage_attempt": {
        "stage_attempt_id": "Stable stage attempt identifier.",
        "stage_id": "Owning stage plan identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_name": "Stage name copied for audit and uniqueness.",
        "attempt_no": "Monotonic attempt number for one experiment and stage.",
        "status": "Attempt status including queued, running, passed, failed, cancelled, and timeout.",
        "input_json": "Input payload used when the attempt was requested.",
        "result_json": "Result or metrics payload recorded by later stages.",
        "error_message": "Failure, cancellation, timeout, or blocked reason.",
        "started_at": "Timestamp when the attempt started or was requested.",
        "completed_at": "Timestamp when the attempt reached a terminal status.",
        "created_at": "Timestamp when the attempt row was created.",
        "updated_at": "Timestamp when the attempt row was last updated.",
    },
    "research_pipeline.external_run_link": {
        "link_id": "Stable external run link identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_attempt_id": "Optional stage attempt associated with the external run.",
        "run_type": "External run type such as qe_task, qe_archive_run, validation_run, event_signal_validation, or hmm_job.",
        "external_id": "Identifier in the owning external system.",
        "external_url": "Optional URL for human navigation to the owning system.",
        "status": "Last observed status of the external run reference.",
        "metadata_json": "Additional external run metadata.",
        "created_at": "Timestamp when the external run reference was created.",
    },
    "research_pipeline.artifact_ref": {
        "artifact_ref_id": "Stable artifact reference identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_attempt_id": "Optional stage attempt that produced or validated this reference.",
        "domain_type": "Owning asset domain such as model, strategy_pkg, qe_archive, event_signal, hmm_artifact, or file.",
        "domain_id": "Identifier in the owning asset domain.",
        "artifact_uri": "URI or path reference; ownership remains with the source domain.",
        "artifact_sha256": "Optional content hash for deduplication and audit.",
        "status": "Reference status: candidate, validated, superseded, or deleted.",
        "metadata_json": "Additional artifact reference metadata.",
        "created_at": "Timestamp when the artifact reference row was created.",
        "updated_at": "Timestamp when the artifact reference row was last updated.",
    },
    "research_pipeline.comparison": {
        "comparison_id": "Stable comparison identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_attempt_id": "Optional stage attempt associated with the comparison.",
        "baseline_ref_json": "Reference to baseline data or artifact without taking ownership.",
        "candidate_ref_json": "Reference to candidate data or artifact without taking ownership.",
        "metrics_json": "Comparison metrics used to derive or justify the verdict.",
        "criteria_json": "Criteria applied to the metrics.",
        "verdict": "Comparison verdict: pass, fail, inconclusive, or blocked.",
        "reason_md": "Human readable explanation for the verdict.",
        "created_by": "Actor that recorded the comparison.",
        "created_at": "Timestamp when the comparison row was created.",
    },
    "research_pipeline.pipeline_event": {
        "event_id": "Stable pipeline event identifier.",
        "experiment_id": "Optional related experiment identifier.",
        "stage_attempt_id": "Optional related stage attempt identifier.",
        "event_type": "Machine-readable event type for audit and UI filtering.",
        "severity": "Event severity: debug, info, warning, or error.",
        "message": "Human readable event message.",
        "payload_json": "Structured event payload for audit and UI display.",
        "created_by": "Actor that created the event.",
        "created_at": "Timestamp when the event row was created.",
    },

    "research_pipeline.backtest_record": {
        "record_id": "Stable Research Pipeline backtest record identifier.",
        "experiment_id": "Owning Research Pipeline experiment identifier.",
        "stage_attempt_id": "Optional backtest_recording stage attempt that wrote this record.",
        "pipeline_type": "Research pipeline type, currently hmm_research.",
        "research_domain": "Research domain, currently hmm.",
        "source_type": "Source kind such as qe_loop, historical_file, or manual_repair.",
        "source_task_id": "Source QE task identifier.",
        "source_loop_id": "Source QE loop identifier.",
        "source_loop_index": "Source QE loop index.",
        "source_experiment_id": "Source QE experiment identifier when available.",
        "source_created_at": "Timestamp from the source loop or task when available.",
        "record_version": "Version of the normalized HMM backtest record payload.",
        "record_key_sha256": "Idempotency key hash for safe automatic and backfill writes.",
        "non_hmm_config_sig": "Signature of factor, model, strategy, execution, stock pool, and label horizon configuration.",
        "hmm_config_sig": "Signature of HMM-specific configuration.",
        "strict_family_sig": "Strict full-family signature for duplicate detection.",
        "archive_family_sig": "QE Archive representative-family signature excluding HMM-only sweep fields.",
        "dedup_status": "Timeline dedup classification.",
        "qe_archive_eligible": "Whether this record is eligible for QE Archive consideration.",
        "qe_archive_representative": "Whether this record was selected as the QE Archive representative for its family.",
        "rejection_reason": "Reason this record was excluded or not selected for QE Archive.",
        "ann": "Annualized return metric.",
        "mdd": "Maximum drawdown metric.",
        "ir": "Information ratio or sharpe-like metric.",
        "ic": "IC metric.",
        "rank_ic": "RankIC metric.",
        "sharpe": "Sharpe metric when separately available.",
        "turnover": "Turnover metric when available.",
        "metrics_json": "Additional normalized metrics.",
        "hmm_config_summary_json": "HMM configuration summary for timeline display.",
        "config_summary_json": "Non-HMM investment configuration summary.",
        "source_payload_json": "Small source payload fragment used for audit and repair.",
        "recorded_by": "Recorder actor such as auto_hook or backfill.",
        "created_at": "Timestamp when the backtest record was created.",
        "updated_at": "Timestamp when the backtest record was last updated.",
    },
    "research_pipeline.backfill_run": {
        "backfill_run_id": "Stable Research Pipeline backfill run identifier.",
        "experiment_id": "Target Research Pipeline experiment identifier.",
        "backfill_type": "Backfill type such as hmm_backtest_timeline.",
        "status": "Backfill lifecycle status.",
        "dry_run": "Whether the run only previewed changes.",
        "source_scope_json": "Source selection scope for the backfill.",
        "source_fingerprint_json": "Source file or query fingerprint for reproducibility.",
        "counts_json": "Inserted, updated, skipped, duplicate, and excluded counters.",
        "stage_attempt_id": "Backtest recording stage attempt created by execute mode.",
        "error_message": "Failure message if the backfill failed.",
        "created_by": "Actor that requested the backfill.",
        "started_at": "Timestamp when execution started.",
        "completed_at": "Timestamp when execution completed.",
        "created_at": "Timestamp when the backfill row was created.",
        "updated_at": "Timestamp when the backfill row was last updated.",
    },
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_comment_ddl() -> list[str]:
    ddl = ["COMMENT ON SCHEMA research_pipeline IS 'Research Pipeline metadata and audit schema'"]
    for table, comment in TABLE_COMMENTS.items():
        ddl.append(f"COMMENT ON TABLE {table} IS '{_sql_literal(comment)}'")
    for table, columns in COLUMN_COMMENTS.items():
        for column, comment in columns.items():
            ddl.append(f"COMMENT ON COLUMN {table}.{column} IS '{_sql_literal(comment)}'")
    return ddl


COMMENT_DDL = _build_comment_ddl()
DDL: list[str] = BASE_DDL + COMMENT_DDL


def iter_ddl() -> Iterable[str]:
    """Return immutable DDL statements for tests and explicit bootstrap."""

    return tuple(DDL)


def iter_research_pipeline_tables() -> Iterable[str]:
    return tuple(RESEARCH_PIPELINE_TABLES)


def iter_research_pipeline_columns() -> Iterable[tuple[str, str]]:
    return tuple((table, column) for table, columns in COLUMN_COMMENTS.items() for column in columns)


def init_research_pipeline_schema() -> None:
    """Create Research Pipeline metadata tables explicitly."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                if "%s" in sql:
                    cur.execute(sql, (RESEARCH_PIPELINE_SCHEMA_VERSION,))
                else:
                    cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    init_research_pipeline_schema()
    print(f"Research Pipeline schema initialized: {RESEARCH_PIPELINE_SCHEMA_VERSION}")
