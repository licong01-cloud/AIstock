-- 迁移: 为 aistock_factor_metrics 增加 snapshot_date 字段
-- 用途: 追踪因子 IC 衰变趋势，每次计算记录使用的数据快照时间点
-- 执行: psql -h localhost -U aistock -d aistock -f migrate_add_snapshot_date.sql

BEGIN;

-- 1. 增加 snapshot_date 列
ALTER TABLE aistock_factor_metrics
ADD COLUMN IF NOT EXISTS snapshot_date DATE;

-- 2. 增加 calc_batch_id 列（如果不存在）
ALTER TABLE aistock_factor_metrics
ADD COLUMN IF NOT EXISTS calc_batch_id TEXT;

-- 3. 回填历史数据: 用 data_end 作为 snapshot_date
UPDATE aistock_factor_metrics
SET snapshot_date = data_end
WHERE snapshot_date IS NULL;

-- 4. 删除旧的 UNIQUE 约束
ALTER TABLE aistock_factor_metrics
DROP CONSTRAINT IF EXISTS aistock_factor_metrics_factor_name_eval_window_data_start_d_key;

-- 5. 创建新的 UNIQUE 约束 (用 snapshot_date 替代 calculated_at)
ALTER TABLE aistock_factor_metrics
ADD CONSTRAINT aistock_factor_metrics_name_window_dates_snapshot_key
UNIQUE (factor_name, eval_window, data_start, data_end, snapshot_date);

-- 6. 创建索引: 按因子+快照日期查询衰变趋势
CREATE INDEX IF NOT EXISTS idx_factor_metrics_snapshot
ON aistock_factor_metrics (factor_name, snapshot_date);

-- 7. 增加注释
COMMENT ON COLUMN aistock_factor_metrics.snapshot_date IS '数据快照日期，标识本次计算使用的数据截止时间点，用于追踪因子IC衰变趋势';
COMMENT ON COLUMN aistock_factor_metrics.calc_batch_id IS '计算批次ID(UUID v4)，同一次计算请求中所有因子×窗口记录共享同一个ID';

COMMIT;
