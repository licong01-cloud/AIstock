import sys
sys.path.insert(0, '/mnt/f/Dev/AIstock')
from backend.db.pg_pool import get_conn

sql = '''INSERT INTO public.execution_algorithm_catalog
    (algo_code, algo_name, source, description, source_code, default_config, param_schema, supported_freqs, min_bars, sort_order, is_enabled)
VALUES
(\'V25_TWO_STAGE\', \'v25 Two-Stage执行计划\', \'custom\',
 \'v25方案B: Oracle权重two-stage执行。KL=0.2773，比v24改进47.6%，预期PA +7.3~+8.2 bps\',
 \'pred_early = early_model(...); pred_late = late_model(...); plan = concat([pred_early, pred_late])\',
 \'{"early_model_path": "/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt", "late_model_path": "/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt", "device": "cuda"}\',
 \'{"type": "object"}\',
 \'{1m}\', 30, 4, true)
ON CONFLICT (algo_code) DO UPDATE SET algo_name = EXCLUDED.algo_name, description = EXCLUDED.description, updated_at = NOW()'''

print('添加v25...')
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
print('✅ 完成')

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT algo_code, algo_name FROM execution_algorithm_catalog ORDER BY sort_order')
        for row in cur.fetchall():
            print(f'  {row[0]}: {row[1]}')
