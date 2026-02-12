import os
import logging
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from ..db.pg_pool import get_conn

logger = logging.getLogger("aistock.tushare_sync")

class TushareSyncService:
    """Tushare 数据同步服务
    
    功能：
    1. 同步每日基本面信息 (Daily Basic)
    2. 同步资金流向数据 (Money Flow)
    3. 同步复权因子 (Adj Factor)
    
    设计：
    - 使用增量同步模式，减少 API 调用消耗。
    - 适配 PG 数据库存储，供 DSL 因子计算消费。
    """
    
    def __init__(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            logger.warning("未配置 TUSHARE_TOKEN，数据同步功能受限。")
        self.pro = ts.pro_api(token)

    def sync_daily_basic(self, trade_date: str = None):
        """同步每日指标：PE, PB, 总市值等"""
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")
            
        try:
            df = self.pro.daily_basic(trade_date=trade_date)
            if df.empty:
                logger.info(f"日期 {trade_date} 无基本面数据。")
                return
            
            self._upsert_to_pg(df, "market.daily_basic", ["ts_code", "trade_date"])
            logger.info(f"成功同步 {trade_date} 基本面数据 {len(df)} 条。")
        except Exception as e:
            logger.error(f"同步基本面数据失败: {e}")

    def sync_money_flow(self, trade_date: str = None):
        """同步每日资金流向：主力流入、大单买入等"""
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")
            
        try:
            df = self.pro.moneyflow(trade_date=trade_date)
            if df.empty:
                logger.info(f"日期 {trade_date} 无资金流数据。")
                return
            
            self._upsert_to_pg(df, "market.moneyflow_ts", ["ts_code", "trade_date"])
            logger.info(f"成功同步 {trade_date} 资金流数据 {len(df)} 条。")
        except Exception as e:
            logger.error(f"同步资金流数据失败: {e}")

    def _upsert_to_pg(self, df: pd.DataFrame, table_name: str, keys: list):
        """高效批量 Upsert 逻辑"""
        if df.empty:
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 创建临时表
                temp_table = f"temp_{table_name.replace('.', '_')}_{int(datetime.now().timestamp())}"
                columns = df.columns.tolist()
                col_defs = ", ".join([f"{col} TEXT" for col in columns]) # 临时表统一用 TEXT，依靠插入时转换
                cur.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs}) ON COMMIT DROP")

                # 2. 批量插入临时表
                from io import StringIO
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, temp_table, sep='\t', null='')

                # 3. 执行 Upsert
                # 构建更新子句
                update_cols = [col for col in columns if col not in keys]
                set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                
                # 构建插入子句
                col_names = ", ".join(columns)
                target_table = table_name
                
                upsert_sql = f"""
                    INSERT INTO {target_table} ({col_names})
                    SELECT {col_names}::"any" FROM {temp_table}
                    ON CONFLICT ({", ".join(keys)})
                    DO UPDATE SET {set_clause}
                """
                # 注意：由于临时表全是 TEXT，直接 SELECT 可能报错，这里使用更加通用的方式
                # 重新构建 SQL，为每个字段增加显式转换（如果有必要）或者依靠 PG 自动转换
                
                insert_cols = ", ".join(columns)
                select_cols = ", ".join([f"{c}::{self._get_col_type(target_table, c)}" for c in columns])
                
                final_upsert_sql = f"""
                    INSERT INTO {target_table} ({insert_cols})
                    SELECT {insert_cols} FROM (
                        SELECT {select_cols} FROM {temp_table}
                    ) AS sub
                    ON CONFLICT ({", ".join(keys)})
                    DO UPDATE SET {set_clause}
                """
                
                try:
                    cur.execute(final_upsert_sql)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Upsert 失败: {e}")
                    raise

    def _get_col_type(self, table: str, col: str) -> str:
        """简单的类型映射，确保从 TEXT 临时表转换正确"""
        # 这里可以根据数据库元数据获取，目前先硬编码常用字段
        float_cols = {"close", "open", "high", "low", "pe", "pe_ttm", "pb", "ps", "ps_ttm", 
                      "total_mv", "circ_mv", "total_share", "float_share", "free_share",
                      "turnover_rate", "turnover_rate_f", "volume_ratio", "dv_ratio", "dv_ttm",
                      "buy_sm_amount", "sell_sm_amount", "buy_md_amount", "sell_md_amount",
                      "buy_lg_amount", "sell_lg_amount", "buy_elg_amount", "sell_elg_amount",
                      "net_mf_amount", "adj_factor"}
        int_cols = {"buy_sm_vol", "sell_sm_vol", "buy_md_vol", "sell_md_vol", 
                    "buy_lg_vol", "sell_lg_vol", "buy_elg_vol", "sell_elg_vol", 
                    "net_mf_vol", "trade_count"}
        
        if col in float_cols:
            return "DOUBLE PRECISION"
        if col in int_cols:
            return "INTEGER"
        if col == "trade_date":
            return "DATE"
        return "TEXT"
                
    def get_latest_trade_date(self) -> str:
        """获取最近一个交易日"""
        df = self.pro.trade_cal(exchange='', is_open='1', end_date=datetime.now().strftime("%Y%m%d"))
        if not df.empty:
            return df.iloc[-1]['cal_date']
        return datetime.now().strftime("%Y%m%d")
