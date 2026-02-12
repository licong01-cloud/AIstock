-- 扩展aistock_factor_catalog表，支持SOTA因子LOOP指标和资产化
-- 执行时间: 2026-02-06

-- 添加因子表达式字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS factor_formulation TEXT;

-- 添加LOOP来源字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS source_loop_id INTEGER;

ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS source_loop_tag TEXT;

-- 添加回测指标字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS ic DOUBLE PRECISION;

ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS annualized_return DOUBLE PRECISION;

ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION;

ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS information_ratio DOUBLE PRECISION;

-- 添加资产状态字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS asset_status TEXT DEFAULT 'pending';

COMMENT ON COLUMN aistock_factor_catalog.asset_status IS '资产状态: pending/synced/failed';

-- 添加资产路径字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS asset_path TEXT;

-- 添加同步时间戳
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMPTZ;

-- 创建索引支持按指标查询
CREATE INDEX IF NOT EXISTS idx_factor_catalog_ic 
ON aistock_factor_catalog(ic) WHERE ic IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_factor_catalog_annualized_return 
ON aistock_factor_catalog(annualized_return) WHERE annualized_return IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_factor_catalog_source_loop 
ON aistock_factor_catalog(source, source_loop_id) WHERE source_loop_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_factor_catalog_asset_status 
ON aistock_factor_catalog(asset_status);

-- 添加注释
COMMENT ON COLUMN aistock_factor_catalog.factor_formulation IS '因子表达式/公式';
COMMENT ON COLUMN aistock_factor_catalog.source_loop_id IS '因子首次成为SOTA的LOOP ID';
COMMENT ON COLUMN aistock_factor_catalog.source_loop_tag IS '因子首次成为SOTA的LOOP标签';
COMMENT ON COLUMN aistock_factor_catalog.ic IS '因子所属LOOP的IC(信息系数)';
COMMENT ON COLUMN aistock_factor_catalog.annualized_return IS '因子所属LOOP的年化收益率';
COMMENT ON COLUMN aistock_factor_catalog.max_drawdown IS '因子所属LOOP的最大回撤';
COMMENT ON COLUMN aistock_factor_catalog.information_ratio IS '因子所属LOOP的信息比率';
COMMENT ON COLUMN aistock_factor_catalog.asset_path IS '因子源代码文件相对路径';
COMMENT ON COLUMN aistock_factor_catalog.last_sync_at IS '最后同步时间';
