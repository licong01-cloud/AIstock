-- 申万二级行业黑白名单配置表
-- 每个申万二级行业只保留一条记录（sw2_code 为主键）
-- 回测和实盘共用同一张表

CREATE TABLE IF NOT EXISTS sw2_pool_config (
    sw2_code        VARCHAR(10) PRIMARY KEY,
    -- 申万二级行业代码，如 '801012'
    sw2_name        VARCHAR(50),
    -- 申万二级行业名称，如 '白酒'
    sw1_code        VARCHAR(10),
    -- 申万一级行业代码，用于前端分组展示
    sw1_name        VARCHAR(50),
    -- 申万一级行业名称
    status          VARCHAR(10) NOT NULL DEFAULT 'blocked'
                        CHECK (status IN ('blocked', 'preferred', 'neutral')),
    -- 行业状态：blocked=硬排除；preferred=软加权（预留，暂未实施）；neutral=不干预
    effective_from  DATE,
    -- 规则生效开始日，NULL=从最早起生效
    effective_to    DATE,
    -- 规则生效结束日，NULL=持续至今
    -- effective_from 和 effective_to 均为 NULL 时表示永久生效
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    -- 是否启用：FALSE=暂时禁用，不参与计算，但保留记录
    reason          TEXT,
    -- 人工填写的判断依据，如"基本面持续恶化，无政策支撑"
    updated_at      TIMESTAMP DEFAULT NOW(),
    -- 最后更新时间
    updated_by      VARCHAR(50)
    -- 更新人
);

COMMENT ON TABLE sw2_pool_config IS '申万二级行业黑白名单配置，用于QE实验股票池预筛选';
COMMENT ON COLUMN sw2_pool_config.sw2_code IS '申万二级行业代码，主键，每个行业只有一条记录';
COMMENT ON COLUMN sw2_pool_config.sw2_name IS '申万二级行业名称';
COMMENT ON COLUMN sw2_pool_config.sw1_code IS '申万一级行业代码，用于前端树形展示分组';
COMMENT ON COLUMN sw2_pool_config.sw1_name IS '申万一级行业名称';
COMMENT ON COLUMN sw2_pool_config.status IS '行业状态：blocked=硬排除；preferred=软加权（预留）；neutral=不干预';
COMMENT ON COLUMN sw2_pool_config.effective_from IS '规则生效开始日，NULL表示从最早起生效';
COMMENT ON COLUMN sw2_pool_config.effective_to IS '规则生效结束日，NULL表示持续至今；两端均为NULL=永久生效';
COMMENT ON COLUMN sw2_pool_config.is_active IS '是否启用：FALSE=暂时禁用，不参与任何计算，但保留记录供审计';
COMMENT ON COLUMN sw2_pool_config.reason IS '人工填写的判断依据';
COMMENT ON COLUMN sw2_pool_config.updated_at IS '最后更新时间';
COMMENT ON COLUMN sw2_pool_config.updated_by IS '最后更新人';

CREATE INDEX IF NOT EXISTS idx_sw2_pool_active_status
    ON sw2_pool_config(is_active, status);
-- 加速查询：按激活状态+行业状态过滤
