import re

path = r'F:\Dev\AIstock\rdagent_assets\qe_strategies\enhanced_topk_dropout_v4_copy.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. _get_yesterday_close
content = re.sub(
    r'    def _get_yesterday_close\(self, stock_id, trade_step\):\n\s+"""获取昨收价，避免未来函数"""\n\s+try:\n\s+last_step = trade_step - 1\n\s+if last_step < 0:\n\s+return None\n\s+last_start_time, last_end_time = self\.trade_calendar\.get_step_time\(last_step\)\n\s+# 取上一交易日的成交价（由于配置中 deal_price: close，这就是昨收价）\n\s+return self\.trade_exchange\.get_deal_price\(\n\s+stock_id=stock_id,\n\s+start_time=last_start_time,\n\s+end_time=last_end_time,\n\s+direction=OrderDir\.SELL\n\s+\)\n\s+except Exception:\n\s+return None',
    r'''    def _get_yesterday_close(self, stock_id, trade_step):
        """获取昨收价，拒绝执行兜底，获取不到直接抛出异常"""
        last_step = trade_step - 1
        if last_step < 0:
            return None
        last_start_time, last_end_time = self.trade_calendar.get_step_time(last_step)
        val = self.trade_exchange.get_deal_price(
            stock_id=stock_id,
            start_time=last_start_time,
            end_time=last_end_time,
            direction=OrderDir.SELL
        )
        if val is None or np.isnan(val):
            raise ValueError(f"[{stock_id}] 获取昨收价失败，拒绝兜底！")
        return float(val)''',
    content
)

# 2. _get_daily_change
content = re.sub(
    r'    def _get_daily_change\(self, stock_id, trade_step\):\n\s+start_time, end_time = self\.trade_calendar\.get_step_time\(trade_step\)\n\s+try:\n\s+return self\.trade_exchange\.get_quote_info\(\n\s+stock_id=stock_id,\n\s+start_time=start_time,\n\s+end_time=end_time,\n\s+field="\$change",\n\s+method="ts_data_last",\n\s+\)\n\s+except Exception:\n\s+return None',
    r'''    def _get_daily_change(self, stock_id, trade_step):
        start_time, end_time = self.trade_calendar.get_step_time(trade_step)
        val = self.trade_exchange.get_quote_info(
            stock_id=stock_id,
            start_time=start_time,
            end_time=end_time,
            field="$change",
            method="ts_data_last",
        )
        if val is None or np.isnan(val):
            raise ValueError(f"[{stock_id}] 获取 $change 失败，拒绝兜底！")
        return float(val)''',
    content
)

# 3. _get_market_cap
content = re.sub(
    r'    def _get_market_cap\(self, stock_id, trade_step\):\n\s+"""尝试获取总市值用于选股前过滤（单位：通常为万元或元，取决于导出逻辑）"""\n\s+start_time, end_time = self\.trade_calendar\.get_step_time\(trade_step\)\n\s+try:\n\s+# 在 AIstock 的 qlib 导出中，总市值通常映射为 db_total_mv\n\s+# 我们优先尝试获取 db_total_mv\n\s+mv = self\.trade_exchange\.get_quote_info\(\n\s+stock_id=stock_id,\n\s+start_time=start_time,\n\s+end_time=end_time,\n\s+field="\$db_total_mv",\n\s+method="ts_data_last",\n\s+\)\n\s+if mv is not None and not np\.isnan\(mv\):\n\s+return float\(mv\)\n\s+# 如果没有，可能映射为了 total_mv\n\s+mv2 = self\.trade_exchange\.get_quote_info\(\n\s+stock_id=stock_id,\n\s+start_time=start_time,\n\s+end_time=end_time,\n\s+field="\$total_mv",\n\s+method="ts_data_last",\n\s+\)\n\s+if mv2 is not None and not np\.isnan\(mv2\):\n\s+return float\(mv2\)\n\s+except Exception:\n\s+pass\n\s+return None',
    r'''    def _get_market_cap(self, stock_id, trade_step):
        """获取总市值用于选股前过滤，拒绝兜底"""
        start_time, end_time = self.trade_calendar.get_step_time(trade_step)
        mv = self.trade_exchange.get_quote_info(
            stock_id=stock_id,
            start_time=start_time,
            end_time=end_time,
            field="$db_total_mv",
            method="ts_data_last",
        )
        if mv is None or np.isnan(mv):
            raise ValueError(f"[{stock_id}] 缺少必要的总市值数据 ($db_total_mv)，拒绝执行兜底，请检查数据完整性！")
        return float(mv)''',
    content
)

# 4. entry_prices and entry_amounts checking loops
content = re.sub(
    r'        for stock_id in list\(self\.entry_prices\.keys\(\)\):\n\s+try:\n\s+amt = float\(self\.trade_position\.get_stock_amount\(stock_id\)\)\n\s+except Exception:\n\s+amt = None',
    r'''        for stock_id in list(self.entry_prices.keys()):
            amt = float(self.trade_position.get_stock_amount(stock_id))''',
    content
)

content = re.sub(
    r'        for stock_id in current_holdings:\n\s+if stock_id not in self\.entry_prices and stock_id not in self\._warn_missing_entry_prices:\n\s+try:\n\s+amt = float\(self\.trade_position\.get_stock_amount\(stock_id\)\)\n\s+except Exception:\n\s+amt = None',
    r'''        for stock_id in current_holdings:
            if stock_id not in self.entry_prices and stock_id not in self._warn_missing_entry_prices:
                amt = float(self.trade_position.get_stock_amount(stock_id))''',
    content
)

# 5. Yesterday close in 2. and 3. and 5.
content = re.sub(
    r'            # 如果没拿到昨收，退回使用成本价或者当天空开价（仅作兜底，这里尽量避免触发）\n\s+if eval_price is None or eval_price <= 0:\n\s+continue',
    r'''            if eval_price is None or eval_price <= 0:
                raise ValueError(f"[{stock_id}] 获取昨收价失败，拒绝执行兜底！")''',
    content
)

content = re.sub(
    r'            if eval_price is None or eval_price <= 0:\n\s+continue',
    r'''            if eval_price is None or eval_price <= 0:
                raise ValueError(f"[{stock_id}] 获取昨收价失败，拒绝执行兜底！")''',
    content
)

content = re.sub(
    r'            eval_p = self\._get_yesterday_close\(stock_id, trade_step\)\n\s+if eval_p is None:\n\s+eval_p = self\.entry_prices\.get\(stock_id, 0\.0\)',
    r'''            eval_p = self._get_yesterday_close(stock_id, trade_step)
            if eval_p is None or eval_p <= 0:
                raise ValueError(f"[{stock_id}] 获取估算资产的昨收价失败，拒绝兜底！")''',
    content
)

# 6. Market cap check filtering
content = re.sub(
    r'                # mcap 单位为万元，如果获取不到则默认保留（防崩溃），仅在明确超出时过滤\n\s+if mcap is not None and mcap > self\.max_market_cap:',
    r'''                if mcap > self.max_market_cap:''',
    content
)

# 7. daily_chg
content = re.sub(
    r'            daily_chg = self\._get_daily_change\(stock_id, trade_step\)\n\s+if daily_chg is not None:\n\s+try:\n\s+daily_chg_f = float\(daily_chg\)\n\s+except Exception:\n\s+daily_chg_f = None\n\s+if daily_chg_f is not None and np\.isfinite\(daily_chg_f\) and abs\(daily_chg_f\) > 0\.2:',
    r'''            daily_chg = self._get_daily_change(stock_id, trade_step)
            daily_chg_f = float(daily_chg)
            if np.isfinite(daily_chg_f) and abs(daily_chg_f) > 0.2:''',
    content
)

# 8. amount_unit
content = re.sub(
    r'            try:\n\s+amount_unit = self\.trade_exchange\.get_amount_of_trade_unit\(\n\s+factor=factor,\n\s+stock_id=stock_id,\n\s+start_time=trade_start_time,\n\s+end_time=trade_end_time,\n\s+\)\n\s+except Exception:\n\s+amount_unit = None\n\n\s+if amount_unit is None:\n\s+amount_unit = self\._shares_to_adjusted_amount\(float\(self\.lot_size\), factor\)',
    r'''            amount_unit = self.trade_exchange.get_amount_of_trade_unit(
                factor=factor,
                stock_id=stock_id,
                start_time=trade_start_time,
                end_time=trade_end_time,
            )
            if amount_unit is None or float(amount_unit) <= 0:
                raise ValueError(f"[{stock_id}] 获取 amount_unit 失败，拒绝执行兜底！")''',
    content
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Replacement complete.")
