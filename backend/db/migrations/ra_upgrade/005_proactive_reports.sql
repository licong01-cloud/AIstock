CREATE TABLE IF NOT EXISTS assistant_proactive_reports (
    report_id TEXT PRIMARY KEY,
    report_type TEXT NOT NULL,
    report_date DATE NOT NULL,
    summary_md TEXT NOT NULL,
    sections_json JSONB NOT NULL,
    source_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL DEFAULT 'generated',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_apr UNIQUE (report_type, report_date)
);

COMMENT ON TABLE assistant_proactive_reports IS '主动晨报/实验日报，只读聚合 + 证据优先，不触发写入';
COMMENT ON COLUMN assistant_proactive_reports.report_id IS 'Stable proactive report identifier.';
COMMENT ON COLUMN assistant_proactive_reports.report_type IS 'Report type such as morning_brief or experiment_daily.';
COMMENT ON COLUMN assistant_proactive_reports.report_date IS 'Business date the proactive report covers.';
COMMENT ON COLUMN assistant_proactive_reports.summary_md IS 'Evidence-first natural-language report body.';
COMMENT ON COLUMN assistant_proactive_reports.sections_json IS 'Structured sections with facts, source_refs, reason_codes, and warnings.';
COMMENT ON COLUMN assistant_proactive_reports.source_refs_json IS 'Flattened evidence references used by the report.';
COMMENT ON COLUMN assistant_proactive_reports.status IS 'Report generation status.';
COMMENT ON COLUMN assistant_proactive_reports.created_at IS 'Row creation timestamp.';
