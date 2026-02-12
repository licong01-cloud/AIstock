-- =====================================================
-- 自选股票池 TASK 支持功能 - 数据库迁移脚本
-- 目标：为 watchlist_items 添加 task_id 字段，创建 tasks 表
-- =====================================================

-- 1. 为 app.watchlist_items 添加 task_id 字段
ALTER TABLE app.watchlist_items
ADD COLUMN IF NOT EXISTS task_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS task_name VARCHAR(200);

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_watchlist_items_task_id ON app.watchlist_items (task_id);

-- 2. 创建 tasks 表
CREATE TABLE IF NOT EXISTS app.tasks (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON app.tasks (created_at DESC);

-- 3. 添加注释
COMMENT ON COLUMN app.watchlist_items.task_id IS '来源TASK ID，标识股票加入自选池的来源任务';
COMMENT ON COLUMN app.watchlist_items.task_name IS '来源TASK名称，用于显示';
COMMENT ON TABLE app.tasks IS 'RD-Agent 任务表，记录因子生成等任务信息';

-- 4. 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_tasks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_tasks_updated_at
BEFORE UPDATE ON app.tasks
FOR EACH ROW
EXECUTE FUNCTION update_tasks_updated_at();

-- =====================================================
-- 验证脚本
-- =====================================================

-- 验证 watchlist_items 表结构
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'app' 
  AND table_name = 'watchlist_items'
  AND column_name IN ('task_id', 'task_name')
ORDER BY ordinal_position;

-- 验证 tasks 表是否创建成功
SELECT 
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'app' 
  AND table_name = 'tasks'
ORDER BY ordinal_position;

-- 验证索引是否创建成功
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'app'
  AND (
    indexname = 'idx_watchlist_items_task_id' OR
    indexname = 'idx_tasks_created_at'
  );
