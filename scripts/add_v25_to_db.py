#!/usr/bin/env python3
"""添加v25到execution_algorithm_catalog数据库"""
import sys
import os
import json

sys.path.insert(0, '/mnt/f/Dev/AIstock')

print('=== 添加v25到execution_algorithm_catalog ===\n')

from backend.db.pg_pool import get_conn

# 准备JSON数据
default_config = {
    'early_model_path': '/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt',
    'late_model_path': '/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt',
    'device': 'cuda'
}

param_schema = {
    'type': 'object',
    'properties': {
        'early_model_path': {'type': 'string', 'description': 'v25 early模型文件路径'},
        'late_model_path': {'type': 'string', 'description': 'v25 late模型文件路径'},
        'device': {'type': 'string', 'enum': ['cpu', 'cuda'], 'default': 'cuda'}
    }
}

sql = """
INSERT INTO public.execution_algorithm_catalog
    (algo_code, algo_name, source, description, source_code, default_config, param_schema, supported_freqs, min_bars, sort_order, is_enabled)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (algo_code) DO UPDATE SET
    algo_name = EXCLUDED.algo_name,
    description = EXCLUDED.description,
    source_code = EXCLUDED.source_code,
    default_config = EXCLUDED.default_config,
    param_schema = EXCLUDED.param_schema,
    supported_freqs = EXCLUDED.supported_freqs,
    min_bars = EXCLUDED.min_bars,
    sort_order = EXCLUDED.sort_order,
    is_enabled = EXCLUDED.is_enabled,
    updated_at = NOW()
"""

params = (
    'V25_TWO_STAGE',
    'v25 Two-Stage执行计划 (Oracle权重)',
    'custom',
    'v25方案B: 基于Oracle权重的two-stage执行。Early模型预测前30分钟分布(权重约88.79%), Late模型基于前30统计预测后210分钟(权重约11.21%)。KL散度0.2773，比v24改进47.6%，预期PA +7.3~+8.2 bps。',
    '''# v25 Two-Stage
pred_early = early_model(gap_bucket, gap_ratio, day_features)
early_weight = pred_early.sum()
early_peak = pred_early.argmax() / 29
early_conc = pred_early.max() / pred_early.mean()
pred_late = late_model(gap_bucket, gap_ratio, is_buy, early_weight, early_peak, early_conc)
plan = concat([pred_early, pred_late])
plan = plan / plan.sum()''',
    json.dumps(default_config),
    json.dumps(param_schema),
    ['1m'],
    30,
    4,
    True
)

print('执行SQL插入...')

try:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

    print('✅ v25已成功添加到数据库！\n')

    # 验证
    print('验证插入结果...')
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT algo_code, algo_name, sort_order, is_enabled
                FROM public.execution_algorithm_catalog
                ORDER BY sort_order
            """)
            rows = cur.fetchall()

            print('\n当前执行算法列表:')
            print(f"{'算法代码':<20} {'算法名称':<50} {'排序':<6} {'启用'}")
            print('-' * 85)

            for row in rows:
                enabled = '✓' if row[3] else '✗'
                highlight = '⭐' if row[0] == 'V25_TWO_STAGE' else '  '
                print(f"{highlight} {row[0]:<18} {row[1]:<48} {row[2]:<6} {enabled}")

    print('\n✅ 完成！v25现在可以在所有QE实验场景的下拉框中选择！')
    print('\n适用场景:')
    print('  - 组合配置')
    print('  - QE单次实验')
    print('  - 自动演进')
    print('  - 自定义演进')
    print('  - 基于Loop的演进')

except Exception as e:
    print(f'\n❌ 执行失败: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
