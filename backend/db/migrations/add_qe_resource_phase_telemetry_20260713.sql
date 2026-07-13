BEGIN;

CREATE SCHEMA IF NOT EXISTS qe_archive;

CREATE TABLE IF NOT EXISTS qe_archive.run_resource_session (
    session_id TEXT PRIMARY KEY,
    source_run_key TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    task_id TEXT NOT NULL,
    loop_id TEXT NOT NULL,
    loop_index INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    archive_run_id TEXT REFERENCES qe_archive.run(run_id) ON DELETE SET NULL,
    token_sha256 TEXT NOT NULL,
    phase_pipeline_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    gpu_training_policy TEXT NOT NULL DEFAULT 'exclusive',
    current_phase TEXT NOT NULL DEFAULT 'created',
    last_sequence_no INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'reserved',
    gpu_phase_released_at TIMESTAMPTZ,
    terminal_reason_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_qear_resource_session_attempt UNIQUE (source_run_key, attempt_no),
    CONSTRAINT ck_qear_resource_session_status CHECK (
        status IN ('reserved', 'running', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT ck_qear_resource_session_gpu_policy CHECK (
        gpu_training_policy IN ('exclusive', 'parallel')
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_resource_session_task_loop
    ON qe_archive.run_resource_session(task_id, loop_index, attempt_no DESC);
CREATE INDEX IF NOT EXISTS idx_qear_resource_session_node_phase
    ON qe_archive.run_resource_session(node_id, gpu_training_policy, status, current_phase)
    WHERE phase_pipeline_enabled = TRUE;
CREATE INDEX IF NOT EXISTS idx_qear_resource_session_archive_run
    ON qe_archive.run_resource_session(archive_run_id)
    WHERE archive_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS qe_archive.run_resource_phase (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES qe_archive.run_resource_session(session_id) ON DELETE CASCADE,
    source_run_key TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    phase TEXT NOT NULL,
    phase_status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    sample_count INTEGER,
    process_rss_peak_bytes BIGINT,
    process_vm_hwm_peak_bytes BIGINT,
    gpu_device_index INTEGER,
    gpu_name TEXT,
    gpu_memory_used_peak_bytes BIGINT,
    gpu_process_memory_peak_bytes BIGINT,
    gpu_utilization_avg_pct DOUBLE PRECISION,
    gpu_utilization_peak_pct DOUBLE PRECISION,
    cuda_allocated_peak_bytes BIGINT,
    cuda_reserved_peak_bytes BIGINT,
    cuda_allocated_end_bytes BIGINT,
    cuda_reserved_end_bytes BIGINT,
    resident_requested BOOLEAN,
    resident_active BOOLEAN,
    resident_fallback BOOLEAN,
    fallback_reason_code TEXT,
    release_check_passed BOOLEAN,
    reason_code TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_qear_resource_phase_sequence UNIQUE (session_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_qear_resource_phase_source
    ON qe_archive.run_resource_phase(source_run_key, sequence_no);
CREATE INDEX IF NOT EXISTS idx_qear_resource_phase_reason
    ON qe_archive.run_resource_phase(reason_code, created_at DESC);

COMMENT ON TABLE qe_archive.run_resource_session IS
    'Authenticated per-submission QE resource telemetry session and current phase lease state.';
COMMENT ON TABLE qe_archive.run_resource_phase IS
    'Immutable ordered phase-level GPU, CUDA and process-memory aggregates for QE runs.';
COMMENT ON COLUMN qe_archive.run_resource_session.session_id IS 'Unique resource session id for one real QE submission.';
COMMENT ON COLUMN qe_archive.run_resource_session.source_run_key IS 'Stable QE source key in task_Ln form.';
COMMENT ON COLUMN qe_archive.run_resource_session.attempt_no IS 'Monotonic submission attempt for the source run key.';
COMMENT ON COLUMN qe_archive.run_resource_session.task_id IS 'QE evolution task id.';
COMMENT ON COLUMN qe_archive.run_resource_session.loop_id IS 'Runner loop id in LoopN form.';
COMMENT ON COLUMN qe_archive.run_resource_session.loop_index IS 'One-based loop index.';
COMMENT ON COLUMN qe_archive.run_resource_session.node_id IS 'Compute node owning the submission.';
COMMENT ON COLUMN qe_archive.run_resource_session.archive_run_id IS 'Bound QE Archive run id after archive ingestion.';
COMMENT ON COLUMN qe_archive.run_resource_session.token_sha256 IS 'SHA-256 of the scoped runner upload token; raw token is never stored.';
COMMENT ON COLUMN qe_archive.run_resource_session.phase_pipeline_enabled IS 'Whether an authenticated GPU release may unlock the next training phase.';
COMMENT ON COLUMN qe_archive.run_resource_session.gpu_training_policy IS 'Model-aware GPU lease policy: exclusive for GAT-family training or parallel for shared training.';
COMMENT ON COLUMN qe_archive.run_resource_session.current_phase IS 'Latest accepted monotonic runtime phase.';
COMMENT ON COLUMN qe_archive.run_resource_session.last_sequence_no IS 'Latest accepted event sequence number.';
COMMENT ON COLUMN qe_archive.run_resource_session.status IS 'Resource session lifecycle status.';
COMMENT ON COLUMN qe_archive.run_resource_session.gpu_phase_released_at IS 'Timestamp of a verified GPU release event.';
COMMENT ON COLUMN qe_archive.run_resource_session.terminal_reason_code IS 'Structured terminal or fail-closed reason code.';
COMMENT ON COLUMN qe_archive.run_resource_session.created_at IS 'Session creation timestamp.';
COMMENT ON COLUMN qe_archive.run_resource_session.updated_at IS 'Latest session update timestamp.';
COMMENT ON COLUMN qe_archive.run_resource_session.completed_at IS 'Terminal session timestamp.';
COMMENT ON COLUMN qe_archive.run_resource_phase.id IS 'Surrogate phase aggregate id.';
COMMENT ON COLUMN qe_archive.run_resource_phase.session_id IS 'Parent resource session id.';
COMMENT ON COLUMN qe_archive.run_resource_phase.source_run_key IS 'Stable QE source key copied for bounded lookup.';
COMMENT ON COLUMN qe_archive.run_resource_phase.sequence_no IS 'Strictly monotonic event sequence within the session.';
COMMENT ON COLUMN qe_archive.run_resource_phase.phase IS 'Structured runtime phase name.';
COMMENT ON COLUMN qe_archive.run_resource_phase.phase_status IS 'Phase completion, release, rejection or terminal status.';
COMMENT ON COLUMN qe_archive.run_resource_phase.started_at IS 'Phase start timestamp from the runner.';
COMMENT ON COLUMN qe_archive.run_resource_phase.ended_at IS 'Phase end timestamp from the runner.';
COMMENT ON COLUMN qe_archive.run_resource_phase.duration_seconds IS 'Measured phase wall-clock duration.';
COMMENT ON COLUMN qe_archive.run_resource_phase.sample_count IS 'Number of resource samples aggregated for the phase.';
COMMENT ON COLUMN qe_archive.run_resource_phase.process_rss_peak_bytes IS 'Peak RSS of the runner process tree.';
COMMENT ON COLUMN qe_archive.run_resource_phase.process_vm_hwm_peak_bytes IS 'Peak Linux VmHWM observed across the runner process tree.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_device_index IS 'CUDA/NVIDIA device index.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_name IS 'NVIDIA device model reported by nvidia-smi.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_memory_used_peak_bytes IS 'Peak whole-device GPU memory used during the phase.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_process_memory_peak_bytes IS 'Peak GPU memory attributed to the runner process tree.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_utilization_avg_pct IS 'Average sampled whole-device GPU utilization percentage.';
COMMENT ON COLUMN qe_archive.run_resource_phase.gpu_utilization_peak_pct IS 'Peak sampled whole-device GPU utilization percentage.';
COMMENT ON COLUMN qe_archive.run_resource_phase.cuda_allocated_peak_bytes IS 'Peak PyTorch CUDA allocated bytes sampled in the phase.';
COMMENT ON COLUMN qe_archive.run_resource_phase.cuda_reserved_peak_bytes IS 'Peak PyTorch CUDA reserved bytes sampled in the phase.';
COMMENT ON COLUMN qe_archive.run_resource_phase.cuda_allocated_end_bytes IS 'PyTorch CUDA allocated bytes at phase end.';
COMMENT ON COLUMN qe_archive.run_resource_phase.cuda_reserved_end_bytes IS 'PyTorch CUDA reserved bytes at phase end.';
COMMENT ON COLUMN qe_archive.run_resource_phase.resident_requested IS 'Whether GPU resident mode was requested.';
COMMENT ON COLUMN qe_archive.run_resource_phase.resident_active IS 'Whether GPU resident mode became active.';
COMMENT ON COLUMN qe_archive.run_resource_phase.resident_fallback IS 'Whether resident mode fell back to streaming.';
COMMENT ON COLUMN qe_archive.run_resource_phase.fallback_reason_code IS 'Structured resident fallback reason code.';
COMMENT ON COLUMN qe_archive.run_resource_phase.release_check_passed IS 'Whether deterministic GPU release verification passed.';
COMMENT ON COLUMN qe_archive.run_resource_phase.reason_code IS 'Structured phase event reason code.';
COMMENT ON COLUMN qe_archive.run_resource_phase.metadata IS 'Additional bounded structured phase evidence.';
COMMENT ON COLUMN qe_archive.run_resource_phase.event_sha256 IS 'Canonical event hash used for idempotent replay validation.';
COMMENT ON COLUMN qe_archive.run_resource_phase.created_at IS 'Warehouse persistence timestamp.';

INSERT INTO qe_archive.schema_version(version, description)
VALUES ('qe_archive_v4_20260713', 'QE phase-level GPU/RAM resource telemetry and phase pipeline lease sessions')
ON CONFLICT (version) DO NOTHING;

COMMIT;
