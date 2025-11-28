#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力选股模块
使用pywencai获取主力资金净流入前100名股票，并进行智能筛选
"""

from numpy.ma import minimum_fill_value
import pandas as pd
import pywencai
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import time
import traceback
from debug_logger import debug_logger
from network_optimizer import network_optimizer

class MainForceStockSelector:
    """主力选股类"""
    
    def __init__(self):
        self.raw_data = None
        self.filtered_stocks = None
    
    def get_main_force_stocks(self, start_date: str = None, days_ago: int = None,
                             min_market_cap: float = None, max_market_cap: float = None,
                             market: str = 'all') -> Tuple[bool, pd.DataFrame, str]:
        """
        获取主力资金净流入前100名股票
        
        Args:
            start_date: 开始日期，格式如"2025年10月1日"，如果不提供则使用days_ago
            days_ago: 距今多少天
            min_market_cap: 最小市值限制
            max_market_cap: 最大市值限制
            market: 市场选择，'all'=全部, 'asr'=A股+科创板, 'bse'=北交所
            
        Returns:
            (success, dataframe, message)
        """
        try:
            # 如果没有提供开始日期，根据days_ago计算
            if not start_date:
                date_obj = datetime.now() - timedelta(days=days_ago)
                start_date = f"{date_obj.year}年{date_obj.month}月{date_obj.day}日"
            
            debug_logger.info("主力选股数据获取开始", 
                            start_date=start_date,
                            days_ago=days_ago,
                            market=market,
                            min_market_cap=min_market_cap,
                            max_market_cap=max_market_cap)
            
            print(f"\n{'='*60}")
            print(f"🔍 主力选股 - 数据获取中")
            print(f"{'='*60}")
            print(f"开始日期: {start_date}")
            
            # 根据市场选择确定查询条件
            market_filter = ""
            market_desc = ""
            if market == 'bse':
                market_filter = "北交所，"
                market_desc = "北交所股票"
            elif market == 'asr':
                market_filter = ""
                market_desc = "A股+科创板股票"
            else:
                market_filter = ""
                market_desc = "全部股票（A股+科创板+北交所）"
            
            print(f"目标: 获取{market_desc}主力资金净流入排名前100名股票")
            debug_logger.info("市场选择", market=market, market_desc=market_desc, market_filter=market_filter)
            
            # 构建查询语句 - 使用多个备选方案，所有方案都要求计算区间涨跌幅
            queries = [
                # 方案1: 完整查询（最优）
                f"{start_date}以来{market_filter}主力资金净流入排名，并计算区间涨跌幅，市值{min_market_cap}-{max_market_cap}亿之间，非st，"
                f"所属同花顺行业，总市值，净利润，营收，市盈率，市净率，"
                f"盈利能力评分，成长能力评分，营运能力评分，偿债能力评分，"
                f"现金流评分，资产质量评分，流动性评分，资本充足性评分",
                
                # 方案2: 简化查询
                f"{start_date}以来{market_filter}主力资金净流入，并计算区间涨跌幅，市值{min_market_cap}-{max_market_cap}亿，非st，"
                f"所属同花顺行业，总市值，净利润，营收，市盈率，市净率",
                
                # 方案3: 基础查询
                f"{start_date}以来{market_filter}主力资金净流入排名，并计算区间涨跌幅，市值{min_market_cap}-{max_market_cap}亿，非st，"
                f"所属行业，总市值",
                
                # 方案4: 最简查询
                f"{start_date}以来{market_filter}主力资金净流入前100名，并计算区间涨跌幅，市值{min_market_cap}-{max_market_cap}亿，非st，所属行业，总市值",
            ]
            
            # 尝试不同的查询方案
            query_start_time = time.time()
            all_errors = []  # 记录所有错误信息
            
            for i, query in enumerate(queries, 1):
                query_attempt_start = time.time()
                debug_logger.info(f"尝试查询方案 {i}/{len(queries)}", 
                                query_preview=query[:100] + "..." if len(query) > 100 else query,
                                query_length=len(query))
                print(f"\n尝试方案 {i}/{len(queries)}...")
                print(f"查询语句: {query[:100]}...")
                
                try:
                    debug_logger.debug("调用pywencai.get", query_index=i, query_length=len(query))
                    api_start_time = time.time()
                    result = pywencai.get(query=query, loop=True)
                    api_elapsed = time.time() - api_start_time
                    
                    debug_logger.debug("pywencai返回结果", 
                                      query_index=i,
                                      result_type=type(result).__name__,
                                      result_is_none=(result is None),
                                      elapsed=f"{api_elapsed:.2f}s")
                    
                    if result is None:
                        error_info = f"方案{i}: pywencai返回None"
                        debug_logger.warning(error_info, query_index=i, elapsed=f"{api_elapsed:.2f}s")
                        all_errors.append(error_info)
                        print(f"  ⚠️ 方案{i}返回None，尝试下一个方案")
                        continue
                    
                    # 转换为DataFrame
                    debug_logger.debug("开始转换DataFrame", query_index=i, result_type=type(result).__name__)
                    df_result = self._convert_to_dataframe(result)
                    
                    if df_result is None:
                        error_info = f"方案{i}: DataFrame转换返回None"
                        debug_logger.warning(error_info, query_index=i, result_type=type(result).__name__)
                        all_errors.append(error_info)
                        print(f"  ⚠️ 方案{i}DataFrame转换失败，尝试下一个方案")
                        continue
                    
                    if df_result.empty:
                        error_info = f"方案{i}: DataFrame为空"
                        debug_logger.warning(error_info, query_index=i, df_shape="0x0")
                        all_errors.append(error_info)
                        print(f"  ⚠️ 方案{i}数据为空，尝试下一个方案")
                        continue
                    
                    # 成功获取数据
                    query_elapsed = time.time() - query_attempt_start
                    debug_logger.info(f"查询方案{i}成功", 
                                    query_index=i,
                                    stock_count=len(df_result),
                                    columns_count=len(df_result.columns),
                                    elapsed=f"{query_elapsed:.2f}s")
                    print(f"  ✅ 方案{i}成功！获取到 {len(df_result)} 只股票")
                    self.raw_data = df_result
                    
                    # 显示获取到的列名
                    print(f"\n获取到的数据字段:")
                    for col in df_result.columns[:15]:  # 只显示前15个字段
                        print(f"  - {col}")
                    if len(df_result.columns) > 15:
                        print(f"  ... 还有 {len(df_result.columns) - 15} 个字段")
                    
                    debug_logger.info("主力选股数据获取成功",
                                    market=market,
                                    stock_count=len(df_result),
                                    total_elapsed=f"{time.time() - query_start_time:.2f}s")
                    return True, df_result, f"成功获取{len(df_result)}只股票数据"
                
                except Exception as e:
                    query_elapsed = time.time() - query_attempt_start
                    error_type = type(e).__name__
                    error_msg = str(e)
                    error_traceback = traceback.format_exc()
                    
                    error_info = f"方案{i}: {error_type} - {error_msg}"
                    all_errors.append(error_info)
                    
                    debug_logger.error(f"查询方案{i}失败", 
                                     query_index=i,
                                     error_type=error_type,
                                     error_message=error_msg,
                                     elapsed=f"{query_elapsed:.2f}s",
                                     query_preview=query[:100] + "..." if len(query) > 100 else query)
                    
                    debug_logger.debug("错误堆栈跟踪", 
                                      query_index=i,
                                      traceback=error_traceback)
                    
                    print(f"  ❌ 方案{i}失败: {error_type} - {error_msg}")
                    print(f"     错误详情: {error_msg[:200]}")
                    time.sleep(2)  # 失败后等待2秒再试
                    continue
            
            # 所有方案都失败
            # 如果是北交所，尝试使用AKShare备用方案
            if market == 'bse':
                debug_logger.info("pywencai所有方案失败，尝试AKShare备用方案", market=market)
                print(f"\n⚠️ pywencai所有方案失败，尝试使用AKShare备用方案获取北交所股票...")
                
                akshare_result = self._get_bse_stocks_from_akshare(
                    start_date, min_market_cap, max_market_cap
                )
                
                if akshare_result[0]:  # success
                    return akshare_result
            
            total_elapsed = time.time() - query_start_time
            error_msg = "所有查询方案都失败了，请检查网络或稍后重试"
            
            debug_logger.error("主力选股数据获取失败",
                             market=market,
                             total_queries=len(queries),
                             all_errors=all_errors,
                             total_elapsed=f"{total_elapsed:.2f}s",
                             error_summary="所有查询方案均失败")
            
            print(f"\n❌ {error_msg}")
            print(f"\n详细错误信息:")
            print(f"  总尝试次数: {len(queries)}")
            print(f"  总耗时: {total_elapsed:.2f}秒")
            print(f"  市场选择: {market_desc}")
            print(f"\n各方案错误详情:")
            for idx, err in enumerate(all_errors, 1):
                print(f"  {idx}. {err}")
            
            return False, None, error_msg
        
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"获取主力选股数据失败: {str(e)}"
            error_traceback = traceback.format_exc()
            
            debug_logger.error("主力选股数据获取异常",
                             error_type=error_type,
                             error_message=str(e),
                             market=market,
                             start_date=start_date,
                             days_ago=days_ago,
                             traceback=error_traceback)
            
            print(f"\n❌ {error_msg}")
            print(f"  异常类型: {error_type}")
            print(f"  异常详情: {str(e)}")
            return False, None, error_msg
    
    def _convert_to_dataframe(self, result) -> pd.DataFrame:
        """转换问财返回结果为DataFrame"""
        try:
            result_type = type(result).__name__
            debug_logger.debug("开始转换DataFrame", result_type=result_type)
            
            if isinstance(result, pd.DataFrame):
                debug_logger.debug("结果已经是DataFrame", 
                                  shape=f"{result.shape[0]}x{result.shape[1]}",
                                  columns_count=len(result.columns))
                return result
            elif isinstance(result, dict):
                debug_logger.debug("结果是字典类型", 
                                  dict_keys=list(result.keys())[:10],
                                  dict_size=len(result))
                
                # 检查是否有嵌套的tableV1结构
                if 'tableV1' in result:
                    debug_logger.debug("找到tableV1嵌套结构")
                    table_data = result['tableV1']
                    table_data_type = type(table_data).__name__
                    
                    if isinstance(table_data, pd.DataFrame):
                        debug_logger.debug("tableV1是DataFrame", 
                                          shape=f"{table_data.shape[0]}x{table_data.shape[1]}")
                        return table_data
                    elif isinstance(table_data, list):
                        debug_logger.debug("tableV1是列表", list_length=len(table_data))
                        return pd.DataFrame(table_data)
                    else:
                        debug_logger.warning("tableV1类型不支持", table_data_type=table_data_type)
                
                # 直接转换字典
                debug_logger.debug("直接转换字典为DataFrame")
                return pd.DataFrame([result])
            elif isinstance(result, list):
                debug_logger.debug("结果是列表类型", list_length=len(result))
                if len(result) > 0:
                    debug_logger.debug("列表第一个元素类型", first_element_type=type(result[0]).__name__)
                return pd.DataFrame(result)
            else:
                debug_logger.warning("结果类型不支持转换", result_type=result_type)
                return None
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            error_traceback = traceback.format_exc()
            
            debug_logger.error("DataFrame转换失败",
                             error_type=error_type,
                             error_message=error_msg,
                             result_type=type(result).__name__ if 'result' in locals() else 'unknown',
                             traceback=error_traceback)
            
            print(f"  转换DataFrame失败: {error_type} - {error_msg}")
            return None
    
    def _get_bse_stocks_from_akshare(self, start_date: str, 
                                      min_market_cap: float = None,
                                      max_market_cap: float = None) -> Tuple[bool, pd.DataFrame, str]:
        """
        使用AKShare获取北交所股票数据（备用方案）
        仅在market='bse'且pywencai失败时调用
        
        Args:
            start_date: 开始日期（格式如"2025年10月1日"）
            min_market_cap: 最小市值限制（亿元）
            max_market_cap: 最大市值限制（亿元）
            
        Returns:
            (success, dataframe, message)
        """
        try:
            debug_logger.info("开始使用AKShare获取北交所股票数据", 
                            start_date=start_date,
                            min_market_cap=min_market_cap,
                            max_market_cap=max_market_cap)
            
            import akshare as ak
            
            # 方法1: 尝试使用stock_bj_a_spot_em（北交所专用实时行情接口）
            try:
                print(f"  [AKShare] 尝试方法1: stock_bj_a_spot_em（北交所实时行情）...")
                with network_optimizer.apply():
                    df = ak.stock_bj_a_spot_em()
                
                if df is not None and not df.empty:
                    print(f"  ✅ 方法1成功: 获取到 {len(df)} 只北交所股票")
                    debug_logger.info("AKShare方法1成功", stock_count=len(df))
                    
                    # 处理数据格式，使其与pywencai返回的格式兼容
                    df_processed = self._process_akshare_bse_data(df, min_market_cap, max_market_cap)
                    
                    if df_processed is not None and not df_processed.empty:
                        self.raw_data = df_processed
                        return True, df_processed, f"成功获取{len(df_processed)}只北交所股票数据（AKShare）"
                    
            except Exception as e:
                error_msg = str(e)
                debug_logger.warning("AKShare方法1失败", error=error_msg)
                print(f"  ⚠️ 方法1失败: {error_msg[:100]}")
            
            # 方法2: 尝试使用stock_zh_a_spot_em并筛选北交所股票
            try:
                print(f"  [AKShare] 尝试方法2: stock_zh_a_spot_em（包含京A股）...")
                with network_optimizer.apply():
                    df_all = ak.stock_zh_a_spot_em()
                
                if df_all is not None and not df_all.empty:
                    # 筛选北交所股票（代码以8或4开头）
                    df_bse = df_all[
                        (df_all['代码'].astype(str).str.startswith('8')) | 
                        (df_all['代码'].astype(str).str.startswith('4'))
                    ]
                    
                    if not df_bse.empty:
                        print(f"  ✅ 方法2成功: 筛选出 {len(df_bse)} 只北交所股票")
                        debug_logger.info("AKShare方法2成功", stock_count=len(df_bse))
                        
                        # 处理数据格式
                        df_processed = self._process_akshare_bse_data(df_bse, min_market_cap, max_market_cap)
                        
                        if df_processed is not None and not df_processed.empty:
                            self.raw_data = df_processed
                            return True, df_processed, f"成功获取{len(df_processed)}只北交所股票数据（AKShare）"
                    
            except Exception as e:
                error_msg = str(e)
                debug_logger.warning("AKShare方法2失败", error=error_msg)
                print(f"  ⚠️ 方法2失败: {error_msg[:100]}")
            
            # 所有方法都失败
            debug_logger.error("AKShare所有方法均失败")
            return False, None, "AKShare备用方案也失败"
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            debug_logger.error("AKShare备用方案异常", error_type=error_type, error_message=error_msg)
            return False, None, f"AKShare备用方案异常: {error_msg}"
    
    def _process_akshare_bse_data(self, df: pd.DataFrame,
                                   min_market_cap: float = None,
                                   max_market_cap: float = None) -> pd.DataFrame:
        """
        处理AKShare返回的北交所股票数据，使其格式与pywencai兼容
        
        Args:
            df: AKShare返回的DataFrame
            min_market_cap: 最小市值限制（亿元）
            max_market_cap: 最大市值限制（亿元）
            
        Returns:
            处理后的DataFrame
        """
        try:
            if df is None or df.empty:
                return None
            
            # 创建处理后的DataFrame
            processed_df = df.copy()
            
            # 确保关键列存在
            required_cols = ['代码', '名称']
            missing_cols = [col for col in required_cols if col not in processed_df.columns]
            if missing_cols:
                debug_logger.warning("AKShare数据缺少必需列", missing_cols=missing_cols)
                return None
            
            # 处理市值列
            if '总市值' not in processed_df.columns:
                if '市值' in processed_df.columns:
                    processed_df['总市值'] = processed_df['市值']
                elif '总市值(元)' in processed_df.columns:
                    processed_df['总市值'] = processed_df['总市值(元)']
                else:
                    # 尝试从其他列计算或设置默认值
                    processed_df['总市值'] = 0
            
            # 市值筛选（转换为亿元）
            if '总市值' in processed_df.columns:
                # 假设AKShare返回的市值单位是元，转换为亿元
                if processed_df['总市值'].max() > 10000:  # 如果大于10000，可能是以万元为单位
                    processed_df['总市值_亿元'] = processed_df['总市值'] / 10000
                else:
                    processed_df['总市值_亿元'] = processed_df['总市值'] / 100000000
                
                # 应用市值筛选
                if min_market_cap is not None:
                    processed_df = processed_df[processed_df['总市值_亿元'] >= min_market_cap]
                if max_market_cap is not None:
                    processed_df = processed_df[processed_df['总市值_亿元'] <= max_market_cap]
            
            # 添加必要的空列（如果pywencai返回数据中有这些列，而AKShare没有）
            # 这样后续处理不会出错
            default_cols = {
                '区间涨跌幅': 0,
                '主力资金净流入': 0,
                '所属行业': '北交所',
                '所属同花顺行业': '北交所',
                '净利润': None,
                '营收': None,
            }
            
            for col, default_val in default_cols.items():
                if col not in processed_df.columns:
                    processed_df[col] = default_val
            
            debug_logger.info("AKShare数据处理完成", 
                            original_count=len(df),
                            processed_count=len(processed_df),
                            after_market_cap_filter=len(processed_df))
            
            return processed_df
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            debug_logger.error("处理AKShare数据失败", error_type=error_type, error_message=error_msg)
            return None
    
    def filter_stocks(self, df: pd.DataFrame, 
                     max_range_change: float = None,
                     min_market_cap: float = None,
                     max_market_cap: float = None) -> pd.DataFrame:
        """
        智能筛选股票 - 基于涨跌幅和市值
        
        Args:
            df: 原始股票数据DataFrame
            max_range_change: 最大涨跌幅限制
            min_market_cap: 最小市值限制
            max_market_cap: 最大市值限制
            
        Returns:
            筛选后的DataFrame
        """
        if df is None or df.empty:
            return df
        
        print(f"\n{'='*60}")
        print(f"🔍 智能筛选中...")
        print(f"{'='*60}")
        print(f"筛选条件:")
        print(f"  - 区间涨跌幅 < {max_range_change}%")
        print(f"  - 市值范围: {min_market_cap}-{max_market_cap}亿")
        
        original_count = len(df)
        filtered_df = df.copy()
        
        # 1. 筛选区间涨跌幅（智能匹配列名）
        # 优先精确匹配，按优先级查找
        interval_pct_col = None
        possible_interval_pct_names = [
            '区间涨跌幅:前复权', 
            '区间涨跌幅:前复权(%)', 
            '区间涨跌幅(%)', 
            '区间涨跌幅', 
            '涨跌幅:前复权', 
            '涨跌幅:前复权(%)',
            '涨跌幅(%)',
            '涨跌幅'
        ]
        
        # 优先精确匹配
        for name in possible_interval_pct_names:
            for col in df.columns:
                if name in col:
                    interval_pct_col = col
                    break
            if interval_pct_col:
                break
        
        if interval_pct_col:
            print(f"\n使用字段: {interval_pct_col}")
            
            # 转换为数值并筛选
            filtered_df[interval_pct_col] = pd.to_numeric(filtered_df[interval_pct_col], errors='coerce')
            before = len(filtered_df)
            filtered_df = filtered_df[
                (filtered_df[interval_pct_col].notna()) & 
                (filtered_df[interval_pct_col] < max_range_change)
            ]
            print(f"  区间涨跌幅筛选: {before} -> {len(filtered_df)} 只")
        else:
            print(f"  ⚠️ 未找到区间涨跌幅字段，跳过涨跌幅筛选")
            print(f"  可用字段: {list(df.columns[:10])}")
        
        # 2. 筛选市值
        market_cap_cols = [col for col in df.columns if '总市值' in col or '市值' in col]
        if market_cap_cols:
            col_name = market_cap_cols[0]
            print(f"\n使用字段: {col_name}")
            
            # 转换为数值（单位可能是亿或元）
            filtered_df[col_name] = pd.to_numeric(filtered_df[col_name], errors='coerce')
            
            # 判断单位（如果值很大，可能是元）
            max_val = filtered_df[col_name].max()
            if max_val > 100000:  # 大于10万，认为是元
                print(f"  检测到单位为元，转换为亿")
                filtered_df[col_name] = filtered_df[col_name] / 100000000
            
            before = len(filtered_df)
            filtered_df = filtered_df[
                (filtered_df[col_name].notna()) & 
                (filtered_df[col_name] >= min_market_cap) &
                (filtered_df[col_name] <= max_market_cap)
            ]
            print(f"  市值筛选: {before} -> {len(filtered_df)} 只")
        
        # 3. 去除ST股票（额外保险）
        if '股票简称' in filtered_df.columns:
            before = len(filtered_df)
            filtered_df = filtered_df[~filtered_df['股票简称'].str.contains('ST', na=False)]
            if before != len(filtered_df):
                print(f"  ST股票过滤: {before} -> {len(filtered_df)} 只")
        
        print(f"\n筛选完成: {original_count} -> {len(filtered_df)} 只股票")
        
        self.filtered_stocks = filtered_df
        return filtered_df
    
    def get_top_stocks(self, df: pd.DataFrame, top_n: int = None) -> pd.DataFrame:
        """
        获取主力资金净流入前N名股票
        
        Args:
            df: 筛选后的股票数据
            top_n: 返回前N名
            
        Returns:
            前N名股票DataFrame
        """
        if df is None or df.empty:
            return df
        
        # 查找主力资金相关列（智能匹配）
        main_fund_col = None
        main_fund_patterns = [
            '区间主力资金流向',      # 实际列名
            '区间主力资金净流入',
            '主力资金流向',
            '主力资金净流入',
            '主力净流入'
        ]
        for pattern in main_fund_patterns:
            matching = [col for col in df.columns if pattern in col]
            if matching:
                main_fund_col = matching[0]
                break
        
        if main_fund_col:
            print(f"\n使用字段排序: {main_fund_col}")
            
            # 转换为数值并排序
            df[main_fund_col] = pd.to_numeric(df[main_fund_col], errors='coerce')
            top_df = df.nlargest(top_n, main_fund_col)
            
            print(f"获取主力资金净流入前 {len(top_df)} 名")
            return top_df
        else:
            # 如果没有主力资金列，直接返回前N条
            print(f"未找到主力资金列，返回前{top_n}条数据")
            return df.head(top_n)
    
    def format_stock_list_for_analysis(self, df: pd.DataFrame) -> List[Dict]:
        """
        格式化股票列表，准备提交给AI分析师
        
        Args:
            df: 股票数据DataFrame
            
        Returns:
            格式化后的股票列表
        """
        if df is None or df.empty:
            return []
        
        stock_list = []
        
        for idx, row in df.iterrows():
            stock_data = {
                'symbol': row.get('股票代码', 'N/A'),
                'name': row.get('股票简称', 'N/A'),
                'industry': row.get('所属同花顺行业', row.get('所属行业', 'N/A')),
                'market_cap': row.get('总市值[20241209]', row.get('总市值', 'N/A')),
                'range_change': None,
                'main_fund_inflow': None,
                'pe_ratio': row.get('市盈率', 'N/A'),
                'pb_ratio': row.get('市净率', 'N/A'),
                'revenue': row.get('营业收入', row.get('营收', 'N/A')),
                'net_profit': row.get('净利润', 'N/A'),
                'scores': {},
                'raw_data': row.to_dict()
            }
            
            # 提取区间涨跌幅（使用智能匹配）
            interval_pct_col = None
            possible_names = [
                '区间涨跌幅:前复权', '区间涨跌幅:前复权(%)', '区间涨跌幅(%)', 
                '区间涨跌幅', '涨跌幅:前复权', '涨跌幅:前复权(%)', '涨跌幅(%)', '涨跌幅'
            ]
            for name in possible_names:
                for col in df.columns:
                    if name in col:
                        interval_pct_col = col
                        break
                if interval_pct_col:
                    break
            if interval_pct_col:
                stock_data['range_change'] = row.get(interval_pct_col, 'N/A')
            
            # 提取主力资金（智能匹配）
            main_fund_col = None
            main_fund_patterns = [
                '区间主力资金流向', '区间主力资金净流入', 
                '主力资金流向', '主力资金净流入', '主力净流入'
            ]
            for pattern in main_fund_patterns:
                matching = [col for col in df.columns if pattern in col]
                if matching:
                    main_fund_col = matching[0]
                    break
            if main_fund_col:
                stock_data['main_fund_inflow'] = row.get(main_fund_col, 'N/A')
            
            # 提取评分
            score_keywords = ['评分', '能力']
            for col in df.columns:
                if any(keyword in col for keyword in score_keywords):
                    stock_data['scores'][col] = row.get(col, 'N/A')
            
            stock_list.append(stock_data)
        
        return stock_list
    
    def print_stock_summary(self, stock_list: List[Dict]):
        """打印股票摘要信息"""
        print(f"\n{'='*80}")
        print(f"📊 候选股票列表 ({len(stock_list)}只)")
        print(f"{'='*80}")
        print(f"{'序号':<4} {'代码':<8} {'名称':<12} {'行业':<15} {'主力资金':<12} {'涨跌幅':<8}")
        print(f"{'-'*80}")
        
        for i, stock in enumerate(stock_list, 1):
            symbol = stock['symbol']
            name = stock['name'][:10] if isinstance(stock['name'], str) else 'N/A'
            industry = stock['industry'][:13] if isinstance(stock['industry'], str) else 'N/A'
            
            # 格式化主力资金
            main_fund = stock['main_fund_inflow']
            if isinstance(main_fund, (int, float)):
                if abs(main_fund) >= 100000000:  # 大于1亿
                    main_fund_str = f"{main_fund/100000000:.2f}亿"
                else:
                    main_fund_str = f"{main_fund/10000:.2f}万"
            else:
                main_fund_str = 'N/A'
            
            # 格式化涨跌幅
            change = stock['range_change']
            if isinstance(change, (int, float)):
                change_str = f"{change:.2f}%"
            else:
                change_str = 'N/A'
            
            print(f"{i:<4} {symbol:<8} {name:<12} {industry:<15} {main_fund_str:<12} {change_str:<8}")
        
        print(f"{'='*80}\n")

# 全局实例
main_force_selector = MainForceStockSelector()

