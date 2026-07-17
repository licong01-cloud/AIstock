"""
市场情绪数据获取和计算模块（next_app 内部实现）
优先使用Tushare获取市场情绪相关指标，失败则使用Akshare作为备用
包括ARBR、恐慌指数、市场资金情绪等
从根目录 market_sentiment_data.py 迁移而来，行为保持一致，只是改为依赖
next_app 内部的 data_source_manager_impl 和 infra.network_optimizer。
"""

import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
import warnings
import sys
import io
from .data_source_manager_impl import data_source_manager
from ..infra.network_optimizer import network_optimizer

warnings.filterwarnings('ignore')

# 设置标准输出编码为UTF-8（仅在命令行环境，避免streamlit冲突）
def _setup_stdout_encoding():
    """仅在命令行环境设置标准输出编码"""
    if sys.platform == 'win32' and not hasattr(sys.stdout, '_original_stream'):
        try:
            # 检测是否在streamlit环境中
            import streamlit
            # 在streamlit中不修改stdout
            return
        except ImportError:
            # 不在streamlit环境，可以安全修改
            try:
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
            except Exception:
                pass

_setup_stdout_encoding()


class MarketSentimentDataFetcher:
    """市场情绪数据获取和计算类"""
    
    def __init__(self):
        self.arbr_period = 26  # ARBR计算周期
    
    def get_market_sentiment_data(self, symbol, stock_data=None, analysis_date=None):
        """
        获取完整的市场情绪分析数据
        
        Args:
            symbol: 股票代码
            stock_data: 股票历史数据（如果已有）
            analysis_date: 分析时间点（可选），格式：'YYYYMMDD'
            
        Returns:
            dict: 包含各类市场情绪指标的字典
        """
        sentiment_data = {
            "symbol": symbol,
            "arbr_data": None,          # ARBR指标数据
            "market_index": None,       # 大盘指数数据
            "sector_index": None,       # 板块指数数据
            "turnover_rate": None,      # 换手率数据
            "limit_up_down": None,      # 涨跌停数据
            "margin_trading": None,     # 融资融券数据
            "fear_greed_index": None,   # 市场恐慌贪婪指数
            "market_volume": None,      # 大盘成交量分析
            "index_daily_metrics": None,# 大盘指数每日指标
            "data_success": False
        }
        
        try:
            # 判断是否为中国股票
            is_chinese = self._is_chinese_stock(symbol)
            
            if is_chinese:
                # 1. 计算ARBR指标
                print("📊 正在计算ARBR情绪指标...")
                arbr_data = self._calculate_arbr(symbol, stock_data)
                if arbr_data:
                    sentiment_data["arbr_data"] = arbr_data
                
                # 2. 获取换手率数据
                print("📊 正在获取换手率数据...")
                turnover_data = self._get_turnover_rate(symbol)
                if turnover_data:
                    sentiment_data["turnover_rate"] = turnover_data
                
                # 3. 获取大盘情绪
                print("📊 正在获取大盘情绪数据...")
                market_data = self._get_market_index_sentiment()
                if market_data:
                    sentiment_data["market_index"] = market_data
                
                # 3.1 获取大盘成交量分析
                print("📊 正在分析大盘成交量...")
                market_volume = self._get_market_volume_analysis(analysis_date=analysis_date)
                if market_volume:
                    sentiment_data["market_volume"] = market_volume
                
                # 3.2 获取大盘指数每日指标
                print("📊 正在获取大盘指数每日指标...")
                index_metrics = self._get_index_daily_metrics(analysis_date=analysis_date)
                if index_metrics:
                    sentiment_data["index_daily_metrics"] = index_metrics
                
                # 4. 获取涨跌停数据
                print("📊 正在获取涨跌停数据...")
                limit_data = self._get_limit_up_down_stats()
                if limit_data:
                    sentiment_data["limit_up_down"] = limit_data
                
                # 5. 获取融资融券数据
                print("📊 正在获取融资融券数据...")
                margin_data = self._get_margin_trading_data(symbol, analysis_date=analysis_date)
                if margin_data:
                    sentiment_data["margin_trading"] = margin_data
                margin_history = self._get_margin_trading_history(symbol, days=5, analysis_date=analysis_date)
                if margin_history:
                    sentiment_data["margin_trading_history"] = margin_history
                
                # 6. 获取市场恐慌指数
                print("📊 正在计算市场恐慌指数...")
                fear_greed = self._get_fear_greed_index()
                if fear_greed:
                    sentiment_data["fear_greed_index"] = fear_greed
                
                sentiment_data["data_success"] = True
                print("✅ 市场情绪数据获取完成")
            else:
                # 美股的情绪指标（简化版）
                print("ℹ️ 美股暂不支持完整的市场情绪数据")
                sentiment_data["error"] = "美股暂不支持完整的市场情绪数据"
            
        except Exception as e:
            print(f"❌ 获取市场情绪数据失败: {e}")
            sentiment_data["error"] = str(e)
        
        return sentiment_data

    def _get_market_volume_analysis(self, analysis_date=None):
        """使用Tushare daily_info获取近10个交易日大盘成交量情况"""
        if not data_source_manager.tushare_available:
            print("   ⚠️ Tushare不可用，无法获取大盘成交量数据")
            return None
        
        try:
            print("   [Tushare] 获取daily_info数据...")
            base_date = datetime.strptime(analysis_date, '%Y%m%d') if analysis_date else datetime.now()
            end_date = base_date.strftime('%Y%m%d')
            start_date = (base_date - timedelta(days=40)).strftime('%Y%m%d')
            
            with network_optimizer.apply():
                df = data_source_manager.tushare_api.query(
                    'daily_info',
                    start_date=start_date,
                    end_date=end_date,
                )
            
            if df is None or df.empty:
                print("   [Tushare] 未获取到daily_info数据")
                return None
            
            df = df[df['ts_code'].isin(['SZ_MARKET', 'SH_MARKET'])].copy()
            if df.empty:
                print("   [Tushare] 未发现沪深两市合计数据")
                return None
            
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['vol'] = pd.to_numeric(df['vol'], errors='coerce')
            grouped = df.groupby('trade_date').agg({'amount': 'sum', 'vol': 'sum'}).reset_index()
            grouped = grouped.sort_values('trade_date')
            last_days = grouped.tail(10)
            if last_days.empty:
                print("   [Tushare] 近10个交易日数据为空")
                return None
            
            latest = last_days.iloc[-1]
            previous = last_days.iloc[:-1]
            avg_amount = previous['amount'].mean() if not previous.empty else None
            avg_vol = previous['vol'].mean() if not previous.empty else None
            amount_ratio = (latest['amount'] / avg_amount) if (avg_amount is not None and avg_amount != 0) else None
            vol_ratio = (latest['vol'] / avg_vol) if (avg_vol is not None and avg_vol != 0) else None

            def valid(value):
                return value is not None and not pd.isna(value)

            def classify_ratio(value):
                if not valid(value):
                    return "数据不足"
                if value >= 1.05:
                    return "放量"
                if value <= 0.95:
                    return "缩量"
                return "持平"
            
            trend = classify_ratio(amount_ratio if valid(amount_ratio) else vol_ratio)
            
            daily_records = [
                {
                    "trade_date": row['trade_date'].strftime('%Y-%m-%d'),
                    "total_amount": float(row['amount']) if pd.notna(row['amount']) else None,
                    "total_volume": float(row['vol']) if pd.notna(row['vol']) else None,
                }
                for _, row in last_days.iterrows()
            ]
            
            return {
                "source": "tushare",
                "unit": {
                    "total_amount": "亿元",
                    "total_volume": "亿股"
                },
                "records": daily_records,
                "latest": daily_records[-1] if daily_records else None,
                "average_amount": float(avg_amount) if valid(avg_amount) else None,
                "average_volume": float(avg_vol) if valid(avg_vol) else None,
                "amount_ratio": float(amount_ratio) if valid(amount_ratio) else None,
                "volume_ratio": float(vol_ratio) if valid(vol_ratio) else None,
                "trend": trend
            }
        except Exception as e:
            print(f"   [Tushare] 获取大盘成交量数据失败: {e}")
            return None

    def _get_index_daily_metrics(self, analysis_date=None):
        """获取重点指数每日指标"""
        if not data_source_manager.tushare_available:
            print("   ⚠️ Tushare不可用，无法获取指数每日指标")
            return None
        
        index_map = {
            '000001.SH': '上证综指',
            '399001.SZ': '深证成指',
            '000016.SH': '上证50',
            '000905.SH': '中证500',
            '399005.SZ': '中小板指',
            '399006.SZ': '创业板指',
        }
        base_date = datetime.strptime(analysis_date, '%Y%m%d') if analysis_date else datetime.now()
        end_date = base_date.strftime('%Y%m%d')
        start_date = (base_date - timedelta(days=30)).strftime('%Y%m%d')
        
        results = {}
        try:
            for ts_code, name in index_map.items():
                try:
                    with network_optimizer.apply():
                        df = data_source_manager.tushare_api.index_dailybasic(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date,
                        )
                    if df is None or df.empty:
                        continue
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    df = df.sort_values('trade_date')
                    latest = df[df['trade_date'] <= pd.to_datetime(end_date)]
                    if latest.empty:
                        latest = df
                    latest_row = latest.iloc[-1]
                    prev_row = latest.iloc[-2] if len(latest) > 1 else None

                    def to_float(row, key):
                        if row is None:
                            return None
                        val = row.get(key)
                        return float(val) if pd.notna(val) else None

                    def diff(cur_row, prev_row, key):
                        cur = to_float(cur_row, key)
                        prev = to_float(prev_row, key)
                        if cur is None or prev is None:
                            return None
                        return cur - prev

                    recent_records = []
                    for _, row in df.tail(6).iterrows():
                        recent_records.append({
                            "trade_date": row['trade_date'].strftime('%Y-%m-%d'),
                            "turnover_rate": to_float(row, 'turnover_rate'),
                            "pe": to_float(row, 'pe'),
                            "pb": to_float(row, 'pb')
                        })
                    results[ts_code] = {
                        "index_name": name,
                        "trade_date": latest_row['trade_date'].strftime('%Y-%m-%d'),
                        "turnover_rate": to_float(latest_row, 'turnover_rate'),
                        "turnover_rate_f": to_float(latest_row, 'turnover_rate_f'),
                        "pe": to_float(latest_row, 'pe'),
                        "pe_ttm": to_float(latest_row, 'pe_ttm'),
                        "pb": to_float(latest_row, 'pb'),
                        "total_mv": to_float(latest_row, 'total_mv'),
                        "float_mv": to_float(latest_row, 'float_mv'),
                        "total_share": to_float(latest_row, 'total_share'),
                        "float_share": to_float(latest_row, 'float_share'),
                        "free_share": to_float(latest_row, 'free_share'),
                        "turnover_rate_change": diff(latest_row, prev_row, 'turnover_rate'),
                        "pe_change": diff(latest_row, prev_row, 'pe'),
                        "pb_change": diff(latest_row, prev_row, 'pb'),
                        "turnover_rate_5d_avg": float(df['turnover_rate'].tail(5).mean()) if not df['turnover_rate'].tail(5).isna().all() else None,
                        "pe_5d_avg": float(df['pe'].tail(5).mean()) if not df['pe'].tail(5).isna().all() else None,
                        "pb_5d_avg": float(df['pb'].tail(5).mean()) if not df['pb'].tail(5).isna().all() else None,
                        "recent_records": recent_records,
                    }
                except Exception as inner_e:
                    print(f"   [Tushare] 获取指数 {ts_code} 数据失败: {inner_e}")
                    continue
            if not results:
                return None
            return {
                "source": "tushare",
                "indices": results
            }
        except Exception as e:
            print(f"   [Tushare] 获取指数每日指标失败: {e}")
            return None
    
    def _is_chinese_stock(self, symbol):
        """判断是否为中国股票"""
        return symbol.isdigit() and len(symbol) == 6
    
    def _calculate_arbr(self, symbol, stock_data=None):
        """
        计算ARBR指标
        AR = (N日内(H-O)之和 / N日内(O-L)之和) × 100
        BR = (N日内(H-CY)之和 / N日内(CY-L)之和) × 100
        """
        try:
            # 如果没有提供stock_data，则重新获取（支持akshare和tushare自动切换）
            if stock_data is None or stock_data.empty:
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=150)).strftime('%Y%m%d')
                
                # 使用数据源管理器获取数据
                df = data_source_manager.get_stock_hist_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust='qfq'
                )
                
                if df is None or df.empty:
                    return None
                
                # 数据源管理器返回的数据列名已经是小写，无需重命名
            else:
                # 使用已有数据
                df = stock_data.copy()
                # 确保列名正确
                if 'Open' in df.columns:
                    df = df.rename(columns={
                        'Open': 'open',
                        'Close': 'close',
                        'High': 'high',
                        'Low': 'low',
                        'Volume': 'volume'
                    })
                df = df.reset_index()
                if 'Date' in df.columns:
                    df = df.rename(columns={'Date': 'date'})
            
            # 确保日期列为datetime类型
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # 计算各项差值
            df['HO'] = df['high'] - df['open']    # 最高价-开盘价
            df['OL'] = df['open'] - df['low']     # 开盘价-最低价
            df['HCY'] = df['high'] - df['close'].shift(1)  # 最高价-前收
            df['CYL'] = df['close'].shift(1) - df['low']   # 前收-最低价
            
            # 计算AR指标
            df['AR'] = (df['HO'].rolling(window=self.arbr_period).sum() / 
                       df['OL'].rolling(window=self.arbr_period).sum()) * 100
            
            # 计算BR指标
            df['BR'] = (df['HCY'].rolling(window=self.arbr_period).sum() / 
                       df['CYL'].rolling(window=self.arbr_period).sum()) * 100
            
            # 处理无穷大和空值
            df['AR'] = df['AR'].replace([np.inf, -np.inf], np.nan)
            df['BR'] = df['BR'].replace([np.inf, -np.inf], np.nan)
            
            # 移除空值
            df = df.dropna(subset=['AR', 'BR'])
            
            if df.empty:
                return None
            
            # 获取最新值和统计信息
            latest = df.iloc[-1]
            ar_value = latest['AR']
            br_value = latest['BR']
            
            # 解读ARBR
            interpretation = self._interpret_arbr(ar_value, br_value)
            
            # 生成交易信号
            signals = self._generate_arbr_signals(ar_value, br_value)
            
            # 计算历史统计
            stats = {
                "ar_mean": df['AR'].mean(),
                "ar_std": df['AR'].std(),
                "ar_min": df['AR'].min(),
                "ar_max": df['AR'].max(),
                "br_mean": df['BR'].mean(),
                "br_std": df['BR'].std(),
                "br_min": df['BR'].min(),
                "br_max": df['BR'].max(),
            }
            
            # 计算信号统计
            df['ar_signal'] = 0
            df['br_signal'] = 0
            df.loc[df['AR'] > 150, 'ar_signal'] = -1
            df.loc[df['AR'] < 70, 'ar_signal'] = 1
            df.loc[df['BR'] > 300, 'br_signal'] = -1
            df.loc[df['BR'] < 50, 'br_signal'] = 1
            df['combined_signal'] = df['ar_signal'] + df['br_signal']
            
            buy_signals = len(df[df['combined_signal'] > 0])
            sell_signals = len(df[df['combined_signal'] < 0])
            neutral_signals = len(df) - buy_signals - sell_signals
            
            signal_stats = {
                "buy_signals": buy_signals,
                "sell_signals": sell_signals,
                "neutral_signals": neutral_signals,
                "total_signals": len(df),
                "buy_ratio": f"{buy_signals/len(df)*100:.1f}%" if len(df) > 0 else "0%",
                "sell_ratio": f"{sell_signals/len(df)*100:.1f}%" if len(df) > 0 else "0%"
            }
            
            return {
                "latest_ar": float(ar_value),
                "latest_br": float(br_value),
                "interpretation": interpretation,
                "signals": signals,
                "statistics": stats,
                "signal_statistics": signal_stats,
                "calculation_date": latest.get('date', datetime.now()).strftime('%Y-%m-%d') if pd.notna(latest.get('date')) else datetime.now().strftime('%Y-%m-%d'),
                "period": self.arbr_period
            }
            
        except Exception as e:
            print(f"计算ARBR指标失败: {e}")
            return None
    
    def _interpret_arbr(self, ar_value, br_value):
        """解读ARBR数值的含义"""
        interpretation = []
        
        # AR指标解读
        if ar_value > 180:
            interpretation.append("AR极度超买（>180），市场过热，风险极高，建议谨慎")
        elif ar_value > 150:
            interpretation.append("AR超买（>150），市场情绪过热，注意回调风险")
        elif ar_value < 40:
            interpretation.append("AR极度超卖（<40），市场过冷，可能存在机会")
        elif ar_value < 70:
            interpretation.append("AR超卖（<70），市场情绪低迷，可关注反弹机会")
        else:
            interpretation.append(f"AR处于正常区间（{ar_value:.2f}），市场情绪相对平稳")
        
        # BR指标解读
        if br_value > 400:
            interpretation.append("BR极度超买（>400），投机情绪过热，警惕泡沫")
        elif br_value > 300:
            interpretation.append("BR超买（>300），投机情绪旺盛，注意风险")
        elif br_value < 30:
            interpretation.append("BR极度超卖（<30），投机情绪冰点，可能触底")
        elif br_value < 50:
            interpretation.append("BR超卖（<50），投机情绪低迷，关注企稳信号")
        else:
            interpretation.append(f"BR处于正常区间（{br_value:.2f}），投机情绪适中")
        
        # ARBR关系解读
        if ar_value > 100 and br_value > 100:
            interpretation.append("多头力量强劲（AR>100且BR>100），但需警惕过热风险")
        elif ar_value < 100 and br_value < 100:
            interpretation.append("空头力量占优（AR<100且BR<100），市场情绪偏空")
        
        if ar_value > br_value:
            interpretation.append("人气指标强于意愿指标（AR>BR），市场基础较好，投资者信心相对稳定")
        else:
            interpretation.append("意愿指标强于人气指标（BR>AR），投机性较强，需注意资金稳定性")
        
        return interpretation
    
    def _generate_arbr_signals(self, ar_value, br_value):
        """生成ARBR交易信号"""
        signals = []
        signal_strength = 0
        
        # AR信号
        if ar_value > 150:
            signals.append("AR卖出信号")
            signal_strength -= 1
        elif ar_value < 70:
            signals.append("AR买入信号")
            signal_strength += 1
        
        # BR信号
        if br_value > 300:
            signals.append("BR卖出信号")
            signal_strength -= 1
        elif br_value < 50:
            signals.append("BR买入信号")
            signal_strength += 1
        
        # 综合信号
        if signal_strength >= 2:
            overall = "强烈买入信号"
        elif signal_strength == 1:
            overall = "买入信号"
        elif signal_strength == -1:
            overall = "卖出信号"
        elif signal_strength <= -2:
            overall = "强烈卖出信号"
        else:
            overall = "中性信号"
        
        return {
            "individual_signals": signals if signals else ["中性"],
            "overall_signal": overall,
            "signal_strength": signal_strength
        }
    
    def _get_turnover_rate(self, symbol):
        """获取换手率数据（优先Tushare，失败则使用Akshare）"""
        # 统一的换手率解读函数
        def interpret_turnover_rate(turnover_rate):
            interpretation = ""
            if turnover_rate != 'N/A':
                try:
                    turnover = float(turnover_rate)
                    if turnover > 20:
                        interpretation = "换手率极高（>20%），资金活跃度极高，可能存在炒作"
                    elif turnover > 10:
                        interpretation = "换手率较高（>10%），交易活跃"
                    elif turnover > 5:
                        interpretation = "换手率正常（5%-10%），交易适中"
                    elif turnover > 2:
                        interpretation = "换手率偏低（2%-5%），交易相对清淡"
                    else:
                        interpretation = "换手率很低（<2%），交易清淡"
                except Exception:
                    pass
            return interpretation
        
        # 优先使用Tushare（通过统一数据访问接口）
        if data_source_manager.tushare_available:
            try:
                print(f"   [Tushare] 正在获取换手率数据...")
                ts_code = data_source_manager._convert_to_ts_code(symbol)
                
                # 尝试获取最近几个交易日的数据（因为可能是非交易日）
                for days_back in range(5):
                    trade_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
                    try:
                        with network_optimizer.apply():
                            df = data_source_manager.tushare_api.daily_basic(
                                ts_code=ts_code,
                                trade_date=trade_date
                            )
                            
                        if df is not None and not df.empty:
                            row = df.iloc[0]
                            turnover_rate = row.get('turnover_rate', 'N/A')
                            
                            interpretation = interpret_turnover_rate(turnover_rate)
                            
                            print(f"   [Tushare] ✅ 成功获取换手率: {turnover_rate}%")
                            return {
                                "current_turnover_rate": turnover_rate,
                                "interpretation": interpretation,
                                "source": "tushare"
                            }
                    except Exception:
                        continue
                        
            except Exception as te:
                print(f"   [Tushare] ❌ 获取失败: {te}")
        
        # Tushare失败，尝试Akshare作为备用
        try:
            print(f"   [Akshare] 正在获取换手率数据（备用数据源）...")
            # 获取A股实时行情数据
            with network_optimizer.apply():
                df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                stock_data = df[df['代码'] == symbol]
                if not stock_data.empty:
                    row = stock_data.iloc[0]
                    turnover_rate = row.get('换手率', 'N/A')
                    
                    interpretation = interpret_turnover_rate(turnover_rate)
                    
                    print(f"   [Akshare] ✅ 成功获取换手率: {turnover_rate}%")
                    return {
                        "current_turnover_rate": turnover_rate,
                        "interpretation": interpretation,
                        "source": "akshare"
                    }
        except Exception as e:
            print(f"   [Akshare] ❌ 获取换手率失败: {e}")
        
        return None
    
    def _get_market_index_sentiment(self):
        """获取大盘指数情绪（优先Tushare，失败则使用Akshare）"""
        # 优先使用Tushare（通过统一数据访问接口）
        if data_source_manager.tushare_available:
            try:
                print(f"   [Tushare] 正在获取大盘指数数据...")
                
                # 尝试获取最近几个交易日的数据（因为可能是非交易日）
                for days_back in range(5):
                    trade_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
                    try:
                        # 获取上证指数数据
                        with network_optimizer.apply():
                            df = data_source_manager.tushare_api.index_daily(
                                ts_code='000001.SH',
                                start_date=trade_date,
                                end_date=trade_date
                            )
                        
                        if df is not None and not df.empty:
                            row = df.iloc[0]
                            change_pct = row.get('pct_chg', 0)
                            
                            print(f"   [Tushare] ✅ 成功获取大盘指数涨跌幅: {change_pct}%")
                            return {
                                "index_name": "上证指数",
                                "change_percent": change_pct,
                                "source": "tushare"
                            }
                    except Exception:
                        continue
                        
            except Exception as te:
                print(f"   [Tushare] ❌ 获取失败: {te}")
        
        # Tushare失败，尝试Akshare作为备用
        try:
            print(f"   [Akshare] 正在获取大盘指数数据（备用数据源）...")
            # 使用正确的symbol参数
            with network_optimizer.apply():
                df = ak.stock_zh_index_spot_em(symbol="上证系列指数")
            if df is not None and not df.empty:
                # 查找上证指数（代码为000001）
                sh_index = df[df['代码'] == '000001']
                if not sh_index.empty:
                    row = sh_index.iloc[0]
                    change_pct = row.get('涨跌幅', 0)
                    
                    # 获取涨跌家数
                    try:
                        with network_optimizer.apply():
                            market_summary = ak.stock_zh_a_spot_em()
                        if market_summary is not None and not market_summary.empty:
                            up_count = len(market_summary[market_summary['涨跌幅'] > 0])
                            down_count = len(market_summary[market_summary['涨跌幅'] < 0])
                            total_count = len(market_summary)
                            flat_count = total_count - up_count - down_count
                            
                            # 计算市场情绪指数
                            sentiment_score = (up_count - down_count) / total_count * 100
                            
                            # 解读市场情绪
                            if sentiment_score > 30:
                                sentiment = "市场情绪极度乐观"
                            elif sentiment_score > 10:
                                sentiment = "市场情绪偏多"
                            elif sentiment_score > -10:
                                sentiment = "市场情绪中性"
                            elif sentiment_score > -30:
                                sentiment = "市场情绪偏空"
                            else:
                                sentiment = "市场情绪极度悲观"
                            
                            print(f"   [Akshare] ✅ 成功获取大盘数据")
                            return {
                                "index_name": "上证指数",
                                "change_percent": change_pct,
                                "up_count": up_count,
                                "down_count": down_count,
                                "flat_count": flat_count,
                                "total_count": total_count,
                                "sentiment_score": f"{sentiment_score:.2f}",
                                "sentiment_interpretation": sentiment,
                                "source": "akshare"
                            }
                    except Exception as e:
                        print(f"   [Akshare] 获取涨跌家数失败: {e}")
                        
                    print(f"   [Akshare] ✅ 成功获取指数涨跌幅")
                    return {
                        "index_name": "上证指数",
                        "change_percent": change_pct,
                        "source": "akshare"
                    }
        except Exception as e:
            print(f"   [Akshare] ❌ 获取大盘指数失败: {e}")
        
        return None
    
    def _get_limit_up_down_stats(self):
        """获取涨跌停统计数据"""
        try:
            # 获取今日涨停和跌停统计
            today = datetime.now().strftime('%Y%m%d')
            
            # 获取涨停股票
            try:
                with network_optimizer.apply():
                    limit_up_df = ak.stock_zt_pool_em(date=today)
                limit_up_count = len(limit_up_df) if limit_up_df is not None and not limit_up_df.empty else 0
            except Exception:
                limit_up_count = 0
            
            # 获取跌停股票
            try:
                with network_optimizer.apply():
                    limit_down_df = ak.stock_zt_pool_dtgc_em(date=today)
                limit_down_count = len(limit_down_df) if limit_down_df is not None and not limit_down_df.empty else 0
            except Exception:
                limit_down_count = 0
            
            # 计算涨跌停比例
            if limit_up_count + limit_down_count > 0:
                limit_ratio = limit_up_count / (limit_up_count + limit_down_count) * 100
            else:
                limit_ratio = 50
            
            # 解读涨跌停情况
            if limit_ratio > 70:
                interpretation = "涨停股远多于跌停股，市场情绪火热"
            elif limit_ratio > 60:
                interpretation = "涨停股多于跌停股，市场情绪较好"
            elif limit_ratio > 40:
                interpretation = "涨跌停数量相当，市场情绪分化"
            elif limit_ratio > 30:
                interpretation = "跌停股多于涨停股，市场情绪较弱"
            else:
                interpretation = "跌停股远多于涨停股，市场情绪低迷"
            
            return {
                "limit_up_count": limit_up_count,
                "limit_down_count": limit_down_count,
                "limit_ratio": f"{limit_ratio:.1f}%",
                "interpretation": interpretation,
                "date": today
            }
        except Exception as e:
            print(f"获取涨跌停数据失败: {e}")
        return None
    
    def _get_margin_trading_data(self, symbol, analysis_date=None):
        """获取融资融券数据"""
        try:
            # 优先使用Tushare接口
            if data_source_manager.tushare_available:
                ts_code = data_source_manager._convert_to_ts_code(symbol)
                base_date = datetime.strptime(analysis_date, '%Y%m%d') if analysis_date else datetime.now()

                def to_float(value):
                    try:
                        if value is None:
                            return None
                        return float(value)
                    except (TypeError, ValueError):
                        return None

                for days_back in range(7):
                    trade_date = (base_date - timedelta(days=days_back)).strftime('%Y%m%d')
                    try:
                        with network_optimizer.apply():
                            df = data_source_manager.tushare_api.margin(trade_date=trade_date, ts_code=ts_code)
                        if df is not None and not df.empty:
                            row = df.iloc[0]
                            margin_balance = to_float(row.get('rzye'))
                            short_balance = to_float(row.get('rqye'))
                            margin_buy = to_float(row.get('rzmre'))
                            margin_repay = to_float(row.get('rzche'))
                            short_sell = to_float(row.get('rqmcl'))
                            short_repay = to_float(row.get('rqchl'))

                            interpretation = []
                            if margin_balance is not None and short_balance not in (None, 0):
                                ratio = margin_balance / short_balance if short_balance else None
                                if ratio is not None:
                                    if ratio > 10:
                                        interpretation.append("融资余额远大于融券余额，投资者看多情绪强")
                                    elif ratio > 3:
                                        interpretation.append("融资余额明显高于融券余额，偏多情绪")
                                    elif ratio < 1:
                                        interpretation.append("融券余额超过融资余额，空头力量偏强")
                                    else:
                                        interpretation.append("融资融券相对平衡")
                            else:
                                interpretation.append("融资或融券余额缺失，无法判断多空力量")

                            return {
                                "margin_balance": margin_balance,
                                "short_balance": short_balance,
                                "margin_buy": margin_buy,
                                "margin_repay": margin_repay,
                                "short_sell": short_sell,
                                "short_repay": short_repay,
                                "interpretation": interpretation,
                                "date": trade_date,
                                "source": "tushare"
                            }
                    except Exception as te:
                        print(f"   [Tushare] 融资融券接口失败: {te}")
                        continue

            # Tushare失败，尝试AKShare
            try:
                exchange = 'sz'
                if symbol.startswith(('6', '9')):
                    exchange = 'sh'
                fetch_fn = ak.stock_margin_underlying_info_szse if exchange == 'sz' else ak.stock_margin_underlying_info_sse
                with network_optimizer.apply():
                    df = fetch_fn(date=datetime.now().strftime('%Y%m%d'))
                if df is not None and not df.empty:
                    stock_data = df[df['证券代码'] == symbol]
                    if not stock_data.empty:
                        latest = stock_data.iloc[0]
                        margin_balance = latest.get('融资余额', 0)
                        short_balance = latest.get('融券余额', 0)
                        interpretation = []
                        if margin_balance and short_balance:
                            if margin_balance > short_balance * 10:
                                interpretation.append("融资余额远大于融券余额，投资者看多情绪强")
                            elif margin_balance > short_balance * 3:
                                interpretation.append("融资余额大于融券余额，投资者偏看多")
                            elif margin_balance < short_balance:
                                interpretation.append("融券余额高于融资余额，市场偏空")
                            else:
                                interpretation.append("融资融券相对平衡")
                        return {
                            "margin_balance": margin_balance,
                            "short_balance": short_balance,
                            "interpretation": interpretation if interpretation else ["缺少参考值"],
                            "date": datetime.now().strftime('%Y-%m-%d'),
                            "source": f"akshare-{exchange}"
                        }
            except Exception as ak_e:
                print(f"   [Akshare] 获取融资融券数据失败: {ak_e}")

            # 兜底：获取整体汇总
            try:
                with network_optimizer.apply():
                    df = ak.stock_margin_szsh()
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    return {
                        "margin_balance": latest.get('融资余额', 'N/A'),
                        "short_balance": latest.get('融券余额', 'N/A'),
                        "interpretation": ["使用市场整体融资融券数据（个股数据缺失）"],
                        "date": latest.get('交易日期', 'N/A'),
                        "source": "akshare-summary"
                    }
            except Exception as summary_e:
                print(f"   [Akshare] 获取汇总融资融券数据失败: {summary_e}")

        except Exception as e:
            print(f"获取融资融券数据失败: {e}")
        return None

    def _get_margin_trading_history(self, symbol, days=5, analysis_date=None):
        """获取近N个交易日融资融券历史数据"""
        if not data_source_manager.tushare_available:
            print("   ⚠️ Tushare不可用，无法获取融资融券历史数据")
            return None

        try:
            ts_code = data_source_manager._convert_to_ts_code(symbol)
            base_date = datetime.strptime(analysis_date, '%Y%m%d') if analysis_date else datetime.now()
            end_date = base_date.strftime('%Y%m%d')
            start_date = (base_date - timedelta(days=days * 3)).strftime('%Y%m%d')

            with network_optimizer.apply():
                df = data_source_manager.tushare_api.margin_detail(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )

            if df is None or df.empty:
                print("   [Tushare] 未获取到融资融券历史数据")
                return None

            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date').tail(days)

            records = []
            for _, row in df.iterrows():
                records.append({
                    "trade_date": row['trade_date'].strftime('%Y-%m-%d'),
                    "margin_balance": float(row.get('rzye', 0)) if pd.notna(row.get('rzye')) else None,
                    "margin_buy": float(row.get('rzmre', 0)) if pd.notna(row.get('rzmre')) else None,
                    "margin_repay": float(row.get('rzche', 0)) if pd.notna(row.get('rzche')) else None,
                    "short_balance": float(row.get('rqye', 0)) if pd.notna(row.get('rqye')) else None,
                    "short_sell": float(row.get('rqmcl', 0)) if pd.notna(row.get('rqmcl')) else None,
                    "short_repay": float(row.get('rqchl', 0)) if pd.notna(row.get('rqchl')) else None,
                    "net_margin_buy": float(row.get('rzmre', 0) - row.get('rzche', 0)) if pd.notna(row.get('rzmre')) and pd.notna(row.get('rzche')) else None,
                    "net_short_sell": float(row.get('rqmcl', 0) - row.get('rqchl', 0)) if pd.notna(row.get('rqmcl')) and pd.notna(row.get('rqchl')) else None,
                })

            def calc_change(field):
                values = [rec[field] for rec in records if rec[field] is not None]
                if len(values) >= 2:
                    return values[-1] - values[0]
                return None

            summary = {
                "source": "tushare",
                "records": records,
                "first_date": records[0]['trade_date'] if records else None,
                "last_date": records[-1]['trade_date'] if records else None,
                "margin_balance_change": calc_change('margin_balance'),
                "short_balance_change": calc_change('short_balance'),
                "net_margin_buy_total": sum(rec['net_margin_buy'] for rec in records if rec['net_margin_buy'] is not None),
                "net_short_sell_total": sum(rec['net_short_sell'] for rec in records if rec['net_short_sell'] is not None),
            }

            return summary
        except Exception as e:
            print(f"获取融资融券历史数据失败: {e}")
            return None
    
    def _get_fear_greed_index(self):
        """计算市场恐慌贪婪指数（基于多个指标综合计算）"""
        try:
            # 基于多个市场指标计算恐慌贪婪指数
            # 1. 涨跌家数比例
            # 2. 涨跌停比例
            # 3. 成交量变化
            
            score = 50  # 基准分数
            factors = []
            
            # 获取涨跌家数
            try:
                with network_optimizer.apply():
                    market_summary = ak.stock_zh_a_spot_em()
                if market_summary is not None and not market_summary.empty:
                    up_count = len(market_summary[market_summary['涨跌幅'] > 0])
                    down_count = len(market_summary[market_summary['涨跌幅'] < 0])
                    total = len(market_summary)
                    
                    up_ratio = up_count / total
                    # 根据涨跌家数比例调整分数（权重30%）
                    score += (up_ratio - 0.5) * 60
                    factors.append(f"涨跌家数比例: {up_ratio:.1%}")
            except Exception:
                pass
            
            # 确保分数在0-100之间
            score = max(0, min(100, score))
            
            # 解读恐慌贪婪指数
            if score >= 75:
                level = "极度贪婪"
                interpretation = "市场情绪极度乐观，投资者贪婪，需警惕回调风险"
            elif score >= 60:
                level = "贪婪"
                interpretation = "市场情绪乐观，投资者偏向贪婪"
            elif score >= 40:
                level = "中性"
                interpretation = "市场情绪中性，投资者相对理性"
            elif score >= 25:
                level = "恐慌"
                interpretation = "市场情绪悲观，投资者偏向恐慌"
            else:
                level = "极度恐慌"
                interpretation = "市场情绪极度悲观，投资者恐慌，可能存在超卖机会"
            
            return {
                "score": f"{score:.1f}",
                "level": level,
                "interpretation": interpretation,
                "factors": factors
            }
        except Exception as e:
            print(f"计算恐慌贪婪指数失败: {e}")
        return None
    
    def format_sentiment_data_for_ai(self, sentiment_data):
        """
        将市场情绪数据格式化为适合AI阅读的文本
        """
        if not sentiment_data or not sentiment_data.get("data_success"):
            return "未能获取市场情绪数据"
        
        text_parts = []
        
        # ARBR指标
        if sentiment_data.get("arbr_data"):
            arbr = sentiment_data["arbr_data"]
            text_parts.append(f"""
【ARBR市场情绪指标】
- 计算周期：{arbr.get('period', 26)}日
- AR值：{arbr.get('latest_ar', 'N/A'):.2f}（人气指标）
- BR值：{arbr.get('latest_br', 'N/A'):.2f}（意愿指标）
- 信号：{arbr.get('signals', {}).get('overall_signal', 'N/A')}
- 解读：
{chr(10).join(['  * ' + item for item in arbr.get('interpretation', [])])}

ARBR统计数据：
- AR历史均值：{arbr.get('statistics', {}).get('ar_mean', 0):.2f}
- BR历史均值：{arbr.get('statistics', {}).get('br_mean', 0):.2f}
- 历史买入信号比例：{arbr.get('signal_statistics', {}).get('buy_ratio', 'N/A')}
- 历史卖出信号比例：{arbr.get('signal_statistics', {}).get('sell_ratio', 'N/A')}
""")
        
        def format_number(value, unit=None):
            if value is None or pd.isna(value):
                return "N/A"
            if isinstance(value, (int, float)):
                if abs(value) >= 1e12:
                    text = f"{value / 1e12:.2f}万亿"
                elif abs(value) >= 1e8:
                    text = f"{value / 1e8:.2f}亿"
                else:
                    text = f"{value:,.2f}"
                if unit and not text.endswith(unit):
                    text += unit
                return text
            return str(value)

        # 换手率
        if sentiment_data.get("turnover_rate"):
            turnover = sentiment_data["turnover_rate"]
            text_parts.append(f"""
【换手率数据】
- 当前换手率：{turnover.get('current_turnover_rate', 'N/A')}%
- 解读：{turnover.get('interpretation', 'N/A')}
""")

        def format_margin_record(record):
            return (
                f"  * {record.get('trade_date', 'N/A')}: "
                f"融资余额 {format_number(record.get('margin_balance'))}元，"
                f"净融资买入 {format_number(record.get('net_margin_buy'))}元，"
                f"融券余额 {format_number(record.get('short_balance'))}元，"
                f"净融券卖出 {format_number(record.get('net_short_sell'))}元"
            )
        
        # 大盘情绪
        if sentiment_data.get("market_index"):
            market = sentiment_data["market_index"]
            text_parts.append(f"""
【大盘市场情绪】
- 指数：{market.get('index_name', 'N/A')}
- 涨跌幅：{market.get('change_percent', 'N/A')}%
""")
            if market.get('sentiment_score'):
                text_parts.append(f"""- 市场情绪得分：{market.get('sentiment_score', 'N/A')}
- 涨家数：{market.get('up_count', 'N/A')}只
- 跌家数：{market.get('down_count', 'N/A')}只
- 平家数：{market.get('flat_count', 'N/A')}只
- 市场情绪：{market.get('sentiment_interpretation', 'N/A')}
""")
        
        # 大盘成交量分析
        if sentiment_data.get("market_volume"):
            volume = sentiment_data["market_volume"]
            latest = volume.get("latest", {})
            text_parts.append(f"""
【大盘成交量分析】
- 数据来源：{volume.get('source', 'tushare')}
- 最近交易日：{latest.get('trade_date', 'N/A')}
- 总成交额：{format_number(latest.get('total_amount'), '亿元')}
- 总成交量：{format_number(latest.get('total_volume'), '亿股')}
- 近10日平均成交额：{format_number(volume.get('average_amount'), '亿元')}
- 当前成交额/均值：{volume.get('amount_ratio', 'N/A') if volume.get('amount_ratio') is not None else 'N/A'}
- 趋势判断：{volume.get('trend', 'N/A')}（>1.05视为放量，<0.95视为缩量）
""")

            records = volume.get("records", [])
            if records:
                text_parts.append("近10个交易日成交额/量概览：")
                for rec in records[-10:]:
                    text_parts.append(
                        f"  * {rec['trade_date']}：成交额 {format_number(rec.get('total_amount'), '亿元')}，成交量 {format_number(rec.get('total_volume'), '亿股')}"
                    )

        # 涨跌停统计
        if sentiment_data.get("limit_up_down"):
            limit = sentiment_data["limit_up_down"]
            text_parts.append(f"""
【涨跌停统计】
- 涨停股数量：{limit.get('limit_up_count', 0)}只
- 跌停股数量：{limit.get('limit_down_count', 0)}只
- 涨停占比：{limit.get('limit_ratio', 'N/A')}
- 解读：{limit.get('interpretation', 'N/A')}
""")
        
        # 融资融券
        if sentiment_data.get("margin_trading"):
            margin = sentiment_data["margin_trading"]
            interpretation_text = '; '.join(margin.get('interpretation', [])) if margin.get('interpretation') else 'N/A'
            text_parts.append(f"""
【融资融券数据】（来源：{margin.get('source', 'unknown')}）
- 数据日期：{margin.get('date', 'N/A')}
- 融资余额：{format_number(margin.get('margin_balance'))}元
- 融券余额：{format_number(margin.get('short_balance'))}元
- 当日融资买入/偿还：{format_number(margin.get('margin_buy'))} / {format_number(margin.get('margin_repay'))}
- 当日融券卖出/偿还：{format_number(margin.get('short_sell'))} / {format_number(margin.get('short_repay'))}
- 解读：{interpretation_text}
""")

        # 融资融券历史
        if sentiment_data.get("margin_trading_history"):
            history = sentiment_data["margin_trading_history"]
            records = history.get('records', [])
            text_parts.append(f"""
【融资融券历史（近5个交易日）】（来源：{history.get('source', 'tushare')}）
- 观察区间：{history.get('first_date', 'N/A')} ~ {history.get('last_date', 'N/A')}
- 融资余额变化：{format_number(history.get('margin_balance_change'))}元
- 融券余额变化：{format_number(history.get('short_balance_change'))}元
- 净融资买入合计：{format_number(history.get('net_margin_buy_total'))}元
- 净融券卖出合计：{format_number(history.get('net_short_sell_total'))}元
""")

            for rec in records:
                text_parts.append(format_margin_record(rec))
        
        # 恐慌贪婪指数
        if sentiment_data.get("fear_greed_index"):
            fear_greed = sentiment_data["fear_greed_index"]
            text_parts.append(f"""
【市场恐慌贪婪指数】
- 指数得分：{fear_greed.get('score', 'N/A')}/100
- 情绪等级：{fear_greed.get('level', 'N/A')}
- 解读：{fear_greed.get('interpretation', 'N/A')}
""")

        # 指数每日指标
        if sentiment_data.get("index_daily_metrics"):
            metrics = sentiment_data["index_daily_metrics"]
            indices = metrics.get('indices', {})
            if indices:
                text_parts.append("""
【重点指数每日指标】
- 指数涵盖：上证综指、深证成指、上证50、中证500、中小板指、创业板指
- 指标说明：turnover_rate(换手率)、pe/pb(估值)、total_mv(总市值)、float_mv(流通市值)
""")

                def change_text(value):
                    if value is None or pd.isna(value):
                        return "持平"
                    if abs(value) < 1e-4:
                        return "持平"
                    arrow = "↑" if value > 0 else "↓"
                    return f"{arrow}{abs(value):.2f}"

                for code, info in indices.items():
                    text_parts.append(
                        f"  * {info.get('index_name', code)}（{info.get('trade_date', 'N/A')}）\n"
                        f"    - 换手率：{format_number(info.get('turnover_rate'))}%（较前日{change_text(info.get('turnover_rate_change'))}） / 自由换手率：{format_number(info.get('turnover_rate_f'))}%\n"
                        f"      近5日均值：{format_number(info.get('turnover_rate_5d_avg'))}%\n"
                        f"    - 估值：PE {format_number(info.get('pe'))}（较前日{change_text(info.get('pe_change'))}）/ PE(TTM) {format_number(info.get('pe_ttm'))} / PB {format_number(info.get('pb'))}（较前日{change_text(info.get('pb_change'))}）\n"
                        f"      近5日均值：PE {format_number(info.get('pe_5d_avg'))} / PB {format_number(info.get('pb_5d_avg'))}\n"
                        f"    - 市值：总市值 {format_number(info.get('total_mv'))} / 流通市值 {format_number(info.get('float_mv'))}"
                    )

                    recent = info.get('recent_records', [])
                    if recent:
                        text_parts.append("    - 最近走势：")
                        for rec in recent[-5:]:
                            text_parts.append(
                                f"       · {rec.get('trade_date', 'N/A')} | 换手率 {format_number(rec.get('turnover_rate'))}% | PE {format_number(rec.get('pe'))} | PB {format_number(rec.get('pb'))}"
                            )
        
        return "\n".join(text_parts)


# 测试函数
if __name__ == "__main__":
    print("测试市场情绪数据获取...")
    fetcher = MarketSentimentDataFetcher()
    
    # 测试平安银行
    symbol = "000001"
    print(f"\n正在获取 {symbol} 的市场情绪数据...")
    
    sentiment_data = fetcher.get_market_sentiment_data(symbol)
    
    if sentiment_data.get("data_success"):
        print("\n" + "="*60)
        print("市场情绪数据获取成功！")
        print("="*60)
        
        formatted_text = fetcher.format_sentiment_data_for_ai(sentiment_data)
        print(formatted_text)
    else:
        print(f"\n获取失败: {sentiment_data.get('error', '未知错误')}")
