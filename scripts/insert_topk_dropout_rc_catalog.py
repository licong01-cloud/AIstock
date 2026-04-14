"""将 TOPK_DROPOUT_RC 策略插入 aistock_strategy_catalog 表.

运行方式: python scripts/insert_topk_dropout_rc_catalog.py

插入后，可在 UI 的策略选择中看到「TopK Dropout + 风控（止损/换手率上限）」策略，
在组合配置中将 rebalance_strategy 设为 TOPK_DROPOUT_RC 即可启用。
"""
import json
import sys
import os

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.db.pg_pool import get_conn


STRATEGY_ID = "topk_dropout_rc"
DISPLAY_NAME = "TopK Dropout + 风控（止损/换手率上限）"
DESCRIPTION = (
    "基于 TopkDropoutStrategy 的风控增强版本。\n"
    "新增功能：\n"
    "1. 个股止损：亏损达到 stop_loss_pct（默认 10%）时强制卖出\n"
    "2. 换手率上限：每日卖出市值不超过 max_daily_turnover_pct（默认 30%）\n"
    "3. 行业 HMM 热度跟踪（可选）：enable_sector_hmm=True 时启用\n\n"
    "策略代码: TOPK_DROPOUT_RC\n"
    "原有 TOPK_DROPOUT 策略不受影响，可随时切换回去。"
)
STRATEGY_TYPE = "rebalance"

PARAM_SCHEMA = {
    "stop_loss_pct": {
        "type": "float",
        "default": 0.10,
        "min": 0.01,
        "max": 0.99,
        "description": "止损阈值（亏损百分比），默认 0.10 即 10%",
    },
    "max_daily_turnover_pct": {
        "type": "float",
        "default": 0.30,
        "min": 0.01,
        "max": 1.0,
        "description": "每日最大换手率上限，默认 0.30 即 30%",
    },
    "enable_sector_hmm": {
        "type": "bool",
        "default": False,
        "description": "是否启用行业 HMM 热度跟踪调整评分",
    },
    "sector_hmm_model_path": {
        "type": "string",
        "default": None,
        "description": "行业 HMM 模型文件路径（WSL路径，enable_sector_hmm=True 时必填）",
    },
    "hmm_coefficients_file": {
        "type": "string",
        "default": None,
        "description": "HMM 预计算系数文件名（由 config_composer 自动生成，QE回测专用）",
    },
    "hmm_signal_preset": {
        "type": "string",
        "default": None,
        "description": "HMM 信号系数预设名称（如 preset_A）",
    },
}

DEFAULT_KWARGS = {
    "stop_loss_pct": 0.10,
    "max_daily_turnover_pct": 0.30,
    "enable_sector_hmm": False,
}

# 读取源代码
SOURCE_CODE_RELPATH = "backend/rebalance_strategies/topk_dropout_rc.py"
source_code_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    SOURCE_CODE_RELPATH,
)
with open(source_code_path, "r", encoding="utf-8") as f:
    source_code = f.read()


def main():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查是否已存在
            cur.execute(
                "SELECT strategy_id FROM aistock_strategy_catalog WHERE strategy_id = %s",
                (STRATEGY_ID,),
            )
            if cur.fetchone():
                print(f"策略 {STRATEGY_ID} 已存在，执行 UPDATE...")
                cur.execute("""
                    UPDATE aistock_strategy_catalog
                    SET display_name = %s,
                        description = %s,
                        strategy_type = %s,
                        source_code = %s,
                        source_code_relpath = %s,
                        default_kwargs = %s,
                        param_schema = %s,
                        catalog_source = 'custom',
                        market = 'cn',
                        freq = 'day',
                        in_selection_center = TRUE,
                        updated_at = NOW()
                    WHERE strategy_id = %s
                """, (
                    DISPLAY_NAME, DESCRIPTION, STRATEGY_TYPE,
                    source_code, SOURCE_CODE_RELPATH,
                    json.dumps(DEFAULT_KWARGS, ensure_ascii=False),
                    json.dumps(PARAM_SCHEMA, ensure_ascii=False),
                    STRATEGY_ID,
                ))
                print(f"UPDATE 完成，影响行数: {cur.rowcount}")
            else:
                print(f"插入新策略 {STRATEGY_ID}...")
                cur.execute("""
                    INSERT INTO aistock_strategy_catalog (
                        strategy_id, display_name, description, strategy_type,
                        catalog_source, market, freq,
                        source_code, source_code_relpath,
                        default_kwargs, param_schema,
                        in_selection_center,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        'custom', 'cn', 'day',
                        %s, %s,
                        %s, %s,
                        TRUE,
                        NOW(), NOW()
                    )
                """, (
                    STRATEGY_ID, DISPLAY_NAME, DESCRIPTION, STRATEGY_TYPE,
                    source_code, SOURCE_CODE_RELPATH,
                    json.dumps(DEFAULT_KWARGS, ensure_ascii=False),
                    json.dumps(PARAM_SCHEMA, ensure_ascii=False),
                ))
                print(f"INSERT 完成")

        conn.commit()

    print(f"\n策略 '{DISPLAY_NAME}' (ID: {STRATEGY_ID}) 已成功写入 aistock_strategy_catalog")
    print(f"策略代码: TOPK_DROPOUT_RC")
    print(f"可在 UI 策略选择中看到，或在组合配置中设置 rebalance_strategy = 'TOPK_DROPOUT_RC'")


if __name__ == "__main__":
    main()
