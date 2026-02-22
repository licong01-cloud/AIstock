import os
import re
import psycopg2

file_path = r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

init_pool_func = '''    def _init_static_stock_pool(self, current_date):
        if self.max_market_cap is None or self.max_market_cap <= 0:
            return

        import os
        import pandas as pd
        h5_path = "daily_basic.h5"
        if not os.path.exists(h5_path):
            logger.warning(f"Static pool init failed: {h5_path} not found. Skipping market cap filter.")
            self._static_stock_pool = None
            return
            
        try:
            df = pd.read_hdf(h5_path)
            dates = df.index.get_level_values('datetime').unique()
            valid_dates = dates[dates <= pd.Timestamp(current_date)]
            if len(valid_dates) == 0:
                logger.warning(f"No data available in {h5_path} before {current_date}.")
                self._static_stock_pool = None
                return
            
            target_date = valid_dates.max()
            df_slice = df.xs(target_date, level='datetime')
            
            valid_stocks = df_slice[df_slice['db_total_mv'] <= self.max_market_cap].index.tolist()
            self._static_stock_pool = set(valid_stocks)
            logger.info(f"Initialized static stock pool from {h5_path} at {target_date}, {len(valid_stocks)} stocks pass the market cap <= {self.max_market_cap} filter.")
        except Exception as e:
            logger.error(f"Error initializing static stock pool: {e}")
            self._static_stock_pool = None

'''

pattern = r'    def _get_market_cap\(self, stock_id, trade_step\):.*?(?:return float\(mv\))?\n'
if 'def _init_static_stock_pool' not in content:
    content = re.sub(pattern, init_pool_func, content, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

workspace_path = r'F:\Dev\RD-Agent-main\qe_workspace\qe_exp_b2a5ff59\custom_strategy.py'
if os.path.exists(workspace_path):
    with open(workspace_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print('Updated workspace custom_strategy.py')

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %s WHERE source_code_relpath LIKE '%enhanced_topk_dropout_v4_copy%'", (content,))
    conn.commit()
    print('DB updated:', cur.rowcount)
except Exception as e:
    print('DB Error:', e)
