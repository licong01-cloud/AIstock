-- 给 aistock_factor_catalog 增加 QE 改造后源代码文件路径字段
-- 执行时间: 2026-02

ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS qe_code_path TEXT;

COMMENT ON COLUMN aistock_factor_catalog.qe_code_path IS 'QE改造后因子源代码文件路径（相对于项目根目录），如 rdagent_assets/qe_factors/Momentum_5D.py';
