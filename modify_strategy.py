import os
import re
import psycopg2

file_path = r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update __init__
if 'self._is_static_pool_initialized' not in content:
    content = content.replace(
        'self.max_market_cap = max_market_cap',
        'self.max_market_cap = max_market_cap\n        self._is_static_pool_initialized = False\n        self._static_stock_pool = None'
    )

# 2. Add _init_static_stock_pool and remove _get_market_cap
init_pool_func = '''    def _init_static_stock_pool(self, current_date):
        if self.max_market_cap is None or self.max_market_cap <= 0:
            return

        import os
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
            
            # db_total_mv unit matches max_market_cap
            valid_stocks = df_slice[df_slice['db_total_mv'] <= self.max_market_cap].index.tolist()
            self._static_stock_pool = set(valid_stocks)
            logger.info(f"Initialized static stock pool from {h5_path} at {target_date}, {len(valid_stocks)} stocks pass the market cap <= {self.max_market_cap} filter.")
        except Exception as e:
            logger.error(f"Error initializing static stock pool: {e}")
            self._static_stock_pool = None
'''

if 'def _init_static_stock_pool' not in content:
    content = re.sub(r'    def _get_market_cap\(self, stock_id, trade_step\):.*?raise ValueError\(f"\[\{stock_id\}\] 缺少必要的总市值数据 \(\$db_total_mv\)，拒绝执行兜底，请检查数据完整性！"\)\n\n', init_pool_func, content, flags=re.DOTALL)

# 3. Inject init call in generate_trade_decision
trigger_code = '''        cur_dt = pd.Timestamp(trade_start_time).date() if trade_start_time is not None else None
        
        if self.max_market_cap is not None and self.max_market_cap > 0 and not self._is_static_pool_initialized:
            self._init_static_stock_pool(trade_start_time)
            self._is_static_pool_initialized = True
'''
if 'self._init_static_stock_pool' not in content:
    content = content.replace('        cur_dt = pd.Timestamp(trade_start_time).date() if trade_start_time is not None else None', trigger_code)

# 4. Modify the filter logic
old_filter = '''            # 过滤总市值超过阈值的股票 (如果配置了 max_market_cap)
            if self.max_market_cap is not None and self.max_market_cap > 0 and len(qualified_stocks) > 0:
                passed_stocks = []
                for stock_id in qualified_stocks:
                    mcap = self._get_market_cap(stock_id, trade_step)
                    if mcap > self.max_market_cap:
                        self._buy_skip_stats.setdefault("skipped_max_market_cap", 0)
                        self._buy_skip_stats["skipped_max_market_cap"] += 1
                        continue
                    passed_stocks.append(stock_id)
                qualified_stocks = passed_stocks'''

new_filter = '''            # 过滤总市值超过阈值的股票 (基于初始化时的静态股票池)
            if self.max_market_cap is not None and self.max_market_cap > 0 and len(qualified_stocks) > 0:
                if self._static_stock_pool is not None:
                    passed_stocks = []
                    for stock_id in qualified_stocks:
                        if stock_id in self._static_stock_pool:
                            passed_stocks.append(stock_id)
                        else:
                            self._buy_skip_stats.setdefault("skipped_max_market_cap", 0)
                            self._buy_skip_stats["skipped_max_market_cap"] += 1
                    qualified_stocks = passed_stocks'''

if '基于初始化时的静态股票池' not in content:
    content = content.replace(old_filter, new_filter)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print('File modification applied locally.')

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %s WHERE file_path LIKE '%enhanced_topk_dropout_v4_copy%'", (content,))
    conn.commit()
    print('Database updated successfully.')
except Exception as e:
    print('DB Error:', e)
