import os
import psycopg2

file_path = r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_block = '''        # 过滤总市值超过阈值的股票 (如果配置了 max_market_cap)
        if self.max_market_cap is not None and self.max_market_cap > 0 and len(qualified_stocks) > 0:
            filtered_stocks = []
            for stock_id in qualified_stocks.index:
                mcap = self._get_market_cap(stock_id, trade_step)
                if mcap > self.max_market_cap:
                    self._buy_skip_stats.setdefault("skipped_max_market_cap", 0)
                    self._buy_skip_stats["skipped_max_market_cap"] += 1
                else:
                    filtered_stocks.append(stock_id)
            qualified_stocks = qualified_stocks.loc[filtered_stocks]'''

new_block = '''        # 过滤总市值超过阈值的股票 (如果配置了 max_market_cap)
        if self.max_market_cap is not None and self.max_market_cap > 0 and len(qualified_stocks) > 0:
            filtered_stocks = []
            if getattr(self, "_static_stock_pool", None) is not None:
                for stock_id in qualified_stocks.index:
                    if stock_id not in self._static_stock_pool:
                        self._buy_skip_stats.setdefault("skipped_max_market_cap", 0)
                        self._buy_skip_stats["skipped_max_market_cap"] += 1
                    else:
                        filtered_stocks.append(stock_id)
                qualified_stocks = qualified_stocks.loc[filtered_stocks]'''

content = content.replace(old_block, new_block)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

workspace_path = r'F:\Dev\RD-Agent-main\qe_workspace\qe_exp_c73611a2\custom_strategy.py'
if os.path.exists(workspace_path):
    with open(workspace_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print('Updated workspace custom_strategy.py')

try:
    conn = psycopg2.connect(host='127.0.0.1', port=5432, user='postgres', password='lc78080808', dbname='aistock')
    cur = conn.cursor()
    cur.execute("UPDATE aistock_strategy_catalog SET source_code = %(code)s WHERE source_code_relpath LIKE %(path)s", 
                {'code': content, 'path': '%enhanced_topk_dropout_v4_copy%'})
    conn.commit()
    print('DB updated rows:', cur.rowcount)
except Exception as e:
    print('DB Error:', e)
