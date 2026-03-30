-- ============================================================
-- QE 实验模块整改 - 数据库迁移脚本
-- 版本: 1.0
-- 日期: 2026-03-02
-- 描述: 完成 qe_experiments 表的列名重命名、新增字段和历史数据回填
--       支持幂等执行（可安全重复运行）
-- 需求: 9.1, 9.2, 9.4
-- ============================================================

-- ============================================================
-- 第一步：列名重命名
-- 将 rdagent_task_id → qe_task_id, rdagent_loop_id → qe_loop_id
-- 使用 IF EXISTS 检查确保幂等性：仅当旧列名存在时才执行重命名
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'qe_experiments' AND column_name = 'rdagent_task_id'
    ) THEN
        ALTER TABLE qe_experiments RENAME COLUMN rdagent_task_id TO qe_task_id;
        RAISE NOTICE '已将 rdagent_task_id 重命名为 qe_task_id';
    ELSE
        RAISE NOTICE 'rdagent_task_id 列不存在或已重命名，跳过';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'qe_experiments' AND column_name = 'rdagent_loop_id'
    ) THEN
        ALTER TABLE qe_experiments RENAME COLUMN rdagent_loop_id TO qe_loop_id;
        RAISE NOTICE '已将 rdagent_loop_id 重命名为 qe_loop_id';
    ELSE
        RAISE NOTICE 'rdagent_loop_id 列不存在或已重命名，跳过';
    END IF;
END $$;

-- ============================================================
-- 第二步：新增字段
-- 使用 IF NOT EXISTS 确保幂等性：列已存在时不会报错
-- ============================================================

-- loop_index: Loop 序号，单次实验默认为 1，演进 Loop 从 2 开始递增
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS loop_index INTEGER DEFAULT 1;

-- parent_experiment_id: 父实验ID，NULL 表示主实验，非 NULL 表示演进子 Loop
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS parent_experiment_id TEXT;

-- is_evolution_loop: 是否为演进 Loop 记录
ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS is_evolution_loop BOOLEAN DEFAULT FALSE;

-- ============================================================
-- 第三步：历史数据回填
-- 将已有记录的 qe_task_id 设置为 experiment_name，
-- qe_loop_id 设置为 'Loop1'，loop_index 设置为 1
-- 仅回填 qe_task_id 为空或仍为旧格式拼接值的记录
-- ============================================================
UPDATE qe_experiments
SET qe_task_id = experiment_name,
    qe_loop_id = 'Loop1',
    loop_index = 1
WHERE qe_task_id IS NULL OR qe_task_id LIKE '%\_%\_%';
