-- Apply only to an explicitly selected existing database after preflight.
-- No IF NOT EXISTS: a conflicting prior schema must not masquerade as installed.
BEGIN;
CREATE TABLE public.factor_research_tasks (
    task_id UUID PRIMARY KEY,
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    objective TEXT NOT NULL CHECK (btrim(objective) <> ''),
    task_type TEXT NOT NULL CHECK (task_type IN ('new_factor','diagnosis','improvement','redundancy')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','paused','completed')),
    phase TEXT NOT NULL DEFAULT 'research',
    completed_summary TEXT NOT NULL DEFAULT '',
    next_action TEXT NOT NULL DEFAULT '',
    blocking_reason TEXT NOT NULL DEFAULT '',
    factor_names TEXT[] NOT NULL DEFAULT '{}',
    context_json JSONB NOT NULL CHECK (jsonb_typeof(context_json)='object'),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE public.factor_research_records (
    record_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES public.factor_research_tasks(task_id) ON DELETE RESTRICT,
    task_revision BIGINT NOT NULL CHECK (task_revision > 0),
    record_type TEXT NOT NULL CHECK (record_type IN ('created','progress','attempt','result','decision','request','correction')),
    attempt_id UUID,
    related_record_id UUID REFERENCES public.factor_research_records(record_id) ON DELETE RESTRICT,
    summary TEXT NOT NULL CHECK (btrim(summary) <> ''),
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (task_id,task_revision),
    CHECK (record_type <> 'attempt' OR attempt_id IS NOT NULL),
    CHECK (record_type <> 'correction' OR related_record_id IS NOT NULL)
);
CREATE INDEX factor_research_tasks_status_idx ON public.factor_research_tasks(status,updated_at,task_id);
CREATE INDEX factor_research_tasks_names_idx ON public.factor_research_tasks USING GIN(factor_names);
CREATE INDEX factor_research_records_history_idx ON public.factor_research_records(task_id,task_revision DESC);
CREATE UNIQUE INDEX factor_research_attempt_start_idx ON public.factor_research_records(task_id,attempt_id)
    WHERE record_type='attempt';

COMMENT ON TABLE public.factor_research_tasks IS 'P1 v1：因子研究当前进度，非正式因子 catalog；来源为显式研究请求，质量结论不改变生产因子状态。';
COMMENT ON COLUMN public.factor_research_tasks.task_id IS '客户端预生成 UUID，非空；跨会话研究标识。';
COMMENT ON COLUMN public.factor_research_tasks.title IS '非空研究标题，用于发现相关任务。';
COMMENT ON COLUMN public.factor_research_tasks.objective IS '非空研究问题；不以盈利或因子数量定义完成。';
COMMENT ON COLUMN public.factor_research_tasks.task_type IS '非空：新因子、诊断、改进或冗余研究，不是审批类别。';
COMMENT ON COLUMN public.factor_research_tasks.status IS 'active/paused/completed，结束不表示因子有效。';
COMMENT ON COLUMN public.factor_research_tasks.phase IS '研究者记录的当前具体步骤，非空文本。';
COMMENT ON COLUMN public.factor_research_tasks.completed_summary IS '已完成工作摘要；允许空文本，不存聊天全文。';
COMMENT ON COLUMN public.factor_research_tasks.next_action IS '下一可执行动作，允许空文本；新窗口恢复依据。';
COMMENT ON COLUMN public.factor_research_tasks.blocking_reason IS '实际阻碍；空文本表示未声明阻碍。';
COMMENT ON COLUMN public.factor_research_tasks.factor_names IS '因子逻辑引用数组，可为空；不级联关联业务 catalog。';
COMMENT ON COLUMN public.factor_research_tasks.context_json IS 'schema=P1 v1 object；研究卡、方法版本、用途/窗口/数据引用，source=研究者请求；缺失明确，不存凭据或因子面板。';
COMMENT ON COLUMN public.factor_research_tasks.revision IS '从1递增的乐观并发版本；与记录同事务更新，非空。';
COMMENT ON COLUMN public.factor_research_tasks.created_at IS '数据库生成带时区创建时刻，非空；不代表市场知识时间。';
COMMENT ON COLUMN public.factor_research_tasks.updated_at IS '数据库生成带时区进度更新时间，非空。';
COMMENT ON TABLE public.factor_research_records IS 'P1 v1：实质研究更新/尝试/结论，普通接口只新增；不保存全量日志，不作为正式指标 writer。';
COMMENT ON COLUMN public.factor_research_records.record_id IS '客户端预生成 UUID，非空；兼作幂等请求标识。';
COMMENT ON COLUMN public.factor_research_records.task_id IS '所属研究任务，非空；删除受限以保护历史。';
COMMENT ON COLUMN public.factor_research_records.task_revision IS '提交后任务版本，非空；同任务唯一，权威记录顺序。';
COMMENT ON COLUMN public.factor_research_records.record_type IS 'created/progress/attempt/result/decision/request/correction，非空。';
COMMENT ON COLUMN public.factor_research_records.attempt_id IS '计算尝试 UUID，可空；开始记录必须有值且每任务唯一；不代表新调度队列。';
COMMENT ON COLUMN public.factor_research_records.related_record_id IS '同任务前序或被更正记录，可空；correction 必须有值，服务验证任务一致。';
COMMENT ON COLUMN public.factor_research_records.summary IS '非空里程碑摘要，不存完整会话或成功日志。';
COMMENT ON COLUMN public.factor_research_records.payload_json IS 'schema=P1 v1 object；request用于值级幂等；research/execution/artifacts/decision/external_request保存来源与实际状态；未知保留null及报告，不存凭据、NaN或完整面板。';
COMMENT ON COLUMN public.factor_research_records.created_at IS '数据库生成带时区记录时刻，非空；顺序使用task_revision。';
COMMIT;
