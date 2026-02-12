"""Archiving Service for RD-Agent & AIstock execution results.

Implements the archiving pipeline described in Section 6 of Phase3_Detail_Design_RD-Agent_AIstock_Final.md.
Supports Level 0 (Core), Level 1 (Enhanced), and Level 2 (Deep) archiving.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

from ..db.pg_pool import get_conn

logger = logging.getLogger("aistock.archive")

class ArchivingService:
    def archive_strategy_run(
        self,
        strategy_id: str,
        source: str,
        version_tag: Optional[str] = None,
        task_run_id: Optional[str] = None,
        loop_id: Optional[int] = None,
        metrics: Optional[Dict[str, Any]] = None,
        equity_curve: Optional[List[Dict[str, Any]]] = None,
        benchmark_curve: Optional[List[Dict[str, Any]]] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None
    ) -> str:
        """归档一次策略运行的核心结果 (Level 0)"""
        
        # 生成唯一且确定的 run_id，防止重复归档
        # 格式: {strategy_id}_{source}_{task_run_id}_{loop_id} 或时间戳
        if task_run_id and loop_id is not None:
            run_id = f"{strategy_id}_{source}_{task_run_id}_{loop_id}"
        else:
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            run_id = f"{strategy_id}_{source}_{timestamp}"
        
        sql = """
            INSERT INTO archive.strategy_run_record (
                run_id, strategy_id, version_tag, task_run_id, loop_id, 
                source, start_date, end_date, metrics, equity_curve, benchmark_curve
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                metrics = EXCLUDED.metrics,
                equity_curve = EXCLUDED.equity_curve,
                benchmark_curve = EXCLUDED.benchmark_curve,
                updated_at = NOW()
        """
        
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        run_id, strategy_id, version_tag, task_run_id, loop_id,
                        source, start_date, end_date, 
                        json.dumps(metrics) if metrics else None,
                        json.dumps(equity_curve) if equity_curve else None,
                        json.dumps(benchmark_curve) if benchmark_curve else None
                    ))
            logger.info(f"成功归档策略运行记录: {run_id}")
            return run_id
        except Exception as e:
            logger.error(f"归档策略运行记录失败: {e}")
            raise

    def archive_positions(self, run_id: str, positions: List[Dict[str, Any]]):
        """归档持仓记录 (Level 0)"""
        if not positions:
            return
            
        sql = """
            INSERT INTO archive.position_record (
                run_id, trade_date, symbol, weight, quantity, price, meta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for p in positions:
            rows.append((
                run_id, p['trade_date'], p['symbol'], 
                p.get('weight'), p.get('quantity'), p.get('price'),
                json.dumps(p.get('meta')) if p.get('meta') else None
            ))
            
        try:
            from psycopg2.extras import execute_values
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, rows)
            logger.info(f"成功归档 {len(rows)} 条持仓记录 (run_id: {run_id})")
        except Exception as e:
            logger.error(f"归档持仓记录失败: {e}")
            raise

    def archive_trades(self, run_id: str, trades: List[Dict[str, Any]]):
        """归档成交记录 (Level 0)"""
        if not trades:
            return
            
        sql = """
            INSERT INTO archive.trade_record (
                run_id, trade_date, symbol, side, price, quantity, amount, cost, meta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for t in trades:
            rows.append((
                run_id, t['trade_date'], t['symbol'], t['side'],
                t.get('price'), t.get('quantity'), t.get('amount'), t.get('cost'),
                json.dumps(t.get('meta')) if t.get('meta') else None
            ))
            
        try:
            from psycopg2.extras import execute_values
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, rows)
            logger.info(f"成功归档 {len(rows)} 条成交记录 (run_id: {run_id})")
        except Exception as e:
            logger.error(f"归档成交记录失败: {e}")
            raise

    def archive_factor_exposures(self, run_id: str, exposures: List[Dict[str, Any]]):
        """归档因子暴露记录 (Level 1)"""
        if not exposures:
            return
            
        sql = """
            INSERT INTO archive.factor_exposure_record (
                run_id, trade_date, symbol, factor_name, exposure_value, contribution
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for e in exposures:
            rows.append((
                run_id, e['trade_date'], e['symbol'], e['factor_name'],
                e.get('exposure_value'), e.get('contribution')
            ))
            
        try:
            from psycopg2.extras import execute_values
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, rows)
            logger.info(f"成功归档 {len(rows)} 条因子暴露记录 (run_id: {run_id})")
        except Exception as e:
            logger.error(f"归档因子暴露失败: {e}")
            raise

    def archive_risk_events(self, run_id: str, events: List[Dict[str, Any]]):
        """归档风险事件记录 (Level 2)"""
        if not events:
            return
            
        sql = """
            INSERT INTO archive.risk_event_record (
                run_id, trade_date, event_type, symbol, description, meta
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for ev in events:
            rows.append((
                run_id, ev['trade_date'], ev['event_type'], ev.get('symbol'),
                ev.get('description'), json.dumps(ev.get('meta')) if ev.get('meta') else None
            ))
            
        try:
            from psycopg2.extras import execute_values
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, rows)
            logger.info(f"成功归档 {len(rows)} 条风险事件记录 (run_id: {run_id})")
        except Exception as e:
            logger.error(f"归档风险事件失败: {e}")
            raise

# 单例
archive_service = ArchivingService()
