"""
Tushare cyq_perf 和 cyq_chips 数据入库脚本
支持全量同步和增量同步
"""
import os
import sys
import json
import uuid
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

# 加载环境变量
from dotenv import load_dotenv
dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path)

import pandas as pd
import tushare as ts
import psycopg2
from psycopg2.extras import execute_values


# 配置
BATCH_SLEEP = 0.2  # 300次/分钟
CYQ_PERF_LIMIT = 4900  # 单次限制5000，预留100条余量
CYQ_CHIPS_LIMIT = 1900  # 单次限制2000，预留100条余量


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=os.getenv("TDX_DB_PORT", "5432"),
        user=os.getenv("TDX_DB_USER"),
        password=os.getenv("TDX_DB_PASSWORD"),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        options="-c client_encoding=utf8",
    )


def get_tushare_pro():
    """获取Tushare Pro API实例"""
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise ValueError("TUSHARE_TOKEN环境变量未设置")
    return ts.pro_api(token)


def _create_job(conn, job_type: str, summary: Dict[str, Any]) -> uuid.UUID:
    """创建任务记录"""
    job_id = uuid.uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_jobs (job_id, job_type, status, created_at, started_at, summary)
            VALUES (%s, %s, 'running', NOW(), NOW(), %s)
            """,
            (str(job_id), job_type, json.dumps(summary, ensure_ascii=False)),
        )
    conn.commit()
    return job_id


def _update_job_progress(conn, job_id: uuid.UUID, stats: Dict[str, Any]) -> None:
    """更新任务进度"""
    total = int(stats.get("total_items") or 0)
    done = int(stats.get("completed_items") or 0)
    progress = 0.0 if total <= 0 else max(0.0, min(100.0, 100.0 * float(done) / float(total)))
    
    payload = {
        "counters": {
            "total": total,
            "done": done,
            "success": int(stats.get("success_count") or 0),
            "failed": int(stats.get("failed_count") or 0),
            "inserted_rows": int(stats.get("inserted_rows") or 0),
        },
        "progress": progress,
        "last_update": datetime.now().isoformat(),
    }
    
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET summary = COALESCE(summary::jsonb, '{}'::jsonb) || %s::jsonb
             WHERE job_id = %s
            """,
            (json.dumps(payload, ensure_ascii=False), str(job_id)),
        )
    conn.commit()


def _complete_job(conn, job_id: uuid.UUID, status: str, message: str = "") -> None:
    """完成任务"""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market.ingestion_jobs
               SET status = %s,
                   finished_at = NOW()
             WHERE job_id = %s
            """,
            (status, str(job_id)),
        )
    conn.commit()
    # 记录完成日志 - 数据库使用'completed'作为状态值
    log_level = "INFO" if status == "success" else "ERROR"
    _log_job(conn, job_id, log_level, message)


def _log_job(conn, job_id: uuid.UUID, level: str, message: str) -> None:
    """记录任务日志"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market.ingestion_logs (job_id, level, message, ts)
            VALUES (%s, %s, %s, NOW())
            """,
            (str(job_id), level, message),
        )
    conn.commit()


def fetch_cyq_perf_for_date(pro, trade_date: str, job_id: Optional[uuid.UUID] = None, conn = None) -> List[Dict]:
    """
    按日期获取cyq_perf数据
    单日全市场约6000条 > 单次限制5000条，必须使用分页
    """
    offset = 0
    limit = CYQ_PERF_LIMIT
    all_data = []
    max_pages = 2
    
    for page in range(max_pages):
        try:
            df = pro.cyq_perf(
                trade_date=trade_date,
                limit=limit,
                offset=offset
            )
            
            if df is None or df.empty:
                break
            
            records = df.to_dict('records')
            all_data.extend(records)
            
            if job_id and conn:
                _log_job(conn, job_id, "INFO", f"cyq_perf {trade_date} 第{page+1}页获取 {len(records)} 条")
            
            # 如果返回数据量小于limit，说明已取完
            if len(df) < limit:
                break
            
            offset += limit
            time.sleep(BATCH_SLEEP)
            
        except Exception as e:
            if job_id and conn:
                _log_job(conn, job_id, "ERROR", f"cyq_perf {trade_date} 第{page+1}页获取失败: {e}")
            raise
    
    return all_data


def upsert_cyq_perf(conn, records: List[Dict], job_id: Optional[uuid.UUID] = None) -> int:
    """批量插入或更新cyq_perf数据"""
    if not records:
        return 0
    
    # 准备数据
    values = []
    for r in records:
        values.append((
            r.get('trade_date'),
            r.get('ts_code'),
            r.get('his_low'),
            r.get('his_high'),
            r.get('cost_5pct'),
            r.get('cost_15pct'),
            r.get('cost_50pct'),
            r.get('cost_85pct'),
            r.get('cost_95pct'),
            r.get('weight_avg'),
            r.get('winner_rate'),
        ))
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO market.cyq_perf (
                trade_date, ts_code, his_low, his_high, cost_5pct, cost_15pct,
                cost_50pct, cost_85pct, cost_95pct, weight_avg, winner_rate
            ) VALUES %s
            ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                his_low = EXCLUDED.his_low,
                his_high = EXCLUDED.his_high,
                cost_5pct = EXCLUDED.cost_5pct,
                cost_15pct = EXCLUDED.cost_15pct,
                cost_50pct = EXCLUDED.cost_50pct,
                cost_85pct = EXCLUDED.cost_85pct,
                cost_95pct = EXCLUDED.cost_95pct,
                weight_avg = EXCLUDED.weight_avg,
                winner_rate = EXCLUDED.winner_rate,
                updated_at = NOW();
            """,
            values,
            page_size=1000,
        )
    conn.commit()
    
    return len(records)


def run_cyq_perf_ingestion(
    start_date: str,
    end_date: str,
    mode: str = "incremental",
    job_id: Optional[uuid.UUID] = None
) -> Dict[str, Any]:
    """
    执行cyq_perf数据入库
    
    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        mode: 模式 init/incremental
        job_id: 任务ID
    """
    conn = get_db_connection()
    pro = get_tushare_pro()
    
    try:
        # 如果没有job_id，创建新任务
        if not job_id:
            job_id = _create_job(conn, "init" if mode == "init" else "incremental", {
                "dataset": "cyq_perf",
                "start_date": start_date,
                "end_date": end_date,
                "mode": mode,
            })
        
        _log_job(conn, job_id, "INFO", f"开始 cyq_perf {mode} 同步: {start_date} ~ {end_date}")
        
        # 生成交易日列表
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 工作日
        trading_dates = [d.strftime('%Y%m%d') for d in date_range]
        
        total_dates = len(trading_dates)
        processed_dates = 0
        total_inserted = 0
        failed_dates = []
        
        # 更新进度
        _update_job_progress(conn, job_id, {
            "total_items": total_dates,
            "completed_items": 0,
            "success_count": 0,
            "failed_count": 0,
            "inserted_rows": 0,
        })
        
        for trade_date in trading_dates:
            try:
                # 获取数据
                records = fetch_cyq_perf_for_date(pro, trade_date, job_id, conn)
                
                if records:
                    # 入库
                    inserted = upsert_cyq_perf(conn, records, job_id)
                    total_inserted += inserted
                    _log_job(conn, job_id, "INFO", f"{trade_date} 入库 {inserted} 条")
                else:
                    _log_job(conn, job_id, "WARN", f"{trade_date} 无数据")
                
                processed_dates += 1
                
                # 更新进度
                _update_job_progress(conn, job_id, {
                    "total_items": total_dates,
                    "completed_items": processed_dates,
                    "success_count": processed_dates - len(failed_dates),
                    "failed_count": len(failed_dates),
                    "inserted_rows": total_inserted,
                })
                
            except Exception as e:
                failed_dates.append(trade_date)
                _log_job(conn, job_id, "ERROR", f"{trade_date} 处理失败: {e}")
                processed_dates += 1
        
        # 完成任务
        status = "success" if not failed_dates else "failed"
        message = f"完成: {processed_dates}/{total_dates} 天, 插入 {total_inserted} 条"
        if failed_dates:
            message += f", 失败 {len(failed_dates)} 天: {failed_dates[:5]}..."
        
        _complete_job(conn, job_id, status, message)
        _log_job(conn, job_id, "INFO", message)
        
        return {
            "success": True,
            "job_id": str(job_id),
            "total_dates": total_dates,
            "processed_dates": processed_dates,
            "failed_dates": failed_dates,
            "total_inserted": total_inserted,
        }
        
    except Exception as e:
        _complete_job(conn, job_id, "failed", str(e))
        _log_job(conn, job_id, "ERROR", f"任务失败: {e}")
        raise
    finally:
        conn.close()


def fetch_cyq_chips_for_stock(
    pro,
    ts_code: str,
    start_date: str,
    end_date: str,
    job_id: Optional[uuid.UUID] = None,
    conn = None
) -> List[Dict]:
    """
    按股票获取cyq_chips数据
    单只股票一段时间的数据可能超过2000条，需要分页
    """
    all_data = []
    offset = 0
    limit = CYQ_CHIPS_LIMIT
    
    while True:
        try:
            df = pro.cyq_chips(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset
            )
            
            if df is None or df.empty:
                break
            
            records = df.to_dict('records')
            all_data.extend(records)
            
            if len(df) < limit:
                break
            
            offset += limit
            time.sleep(BATCH_SLEEP)
            
        except Exception as e:
            if job_id and conn:
                _log_job(conn, job_id, "ERROR", f"cyq_chips {ts_code} 获取失败: {e}")
            raise
    
    return all_data


def upsert_cyq_chips(conn, records: List[Dict], job_id: Optional[uuid.UUID] = None) -> int:
    """批量插入或更新cyq_chips数据"""
    if not records:
        return 0
    
    values = []
    for r in records:
        values.append((
            r.get('trade_date'),
            r.get('ts_code'),
            r.get('price'),
            r.get('percent'),
        ))
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO market.cyq_chips (trade_date, ts_code, price, percent)
            VALUES %s
            ON CONFLICT (trade_date, ts_code, price) DO UPDATE SET
                percent = EXCLUDED.percent,
                updated_at = NOW();
            """,
            values,
            page_size=1000,
        )
    conn.commit()
    
    return len(records)


def run_cyq_chips_ingestion(
    start_date: str,
    end_date: str,
    ts_codes: Optional[List[str]] = None,
    mode: str = "incremental",
    job_id: Optional[uuid.UUID] = None
) -> Dict[str, Any]:
    """
    执行cyq_chips数据入库
    
    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
        ts_codes: 股票代码列表，None则获取全部
        mode: 模式 init/incremental
        job_id: 任务ID
    """
    conn = get_db_connection()
    pro = get_tushare_pro()
    
    try:
        if not job_id:
            job_id = _create_job(conn, "init" if mode == "init" else "incremental", {
                "dataset": "cyq_chips",
                "start_date": start_date,
                "end_date": end_date,
                "mode": mode,
            })
        
        _log_job(conn, job_id, "INFO", f"开始 cyq_chips {mode} 同步: {start_date} ~ {end_date}")
        
        # 获取股票列表
        if not ts_codes:
            # 从stock_basic获取全部A股
            df_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
            ts_codes = df_stocks['ts_code'].tolist()
        
        total_stocks = len(ts_codes)
        processed_stocks = 0
        total_inserted = 0
        failed_stocks = []
        
        _update_job_progress(conn, job_id, {
            "total_items": total_stocks,
            "completed_items": 0,
            "success_count": 0,
            "failed_count": 0,
            "inserted_rows": 0,
        })
        
        for ts_code in ts_codes:
            try:
                records = fetch_cyq_chips_for_stock(pro, ts_code, start_date, end_date, job_id, conn)
                
                if records:
                    inserted = upsert_cyq_chips(conn, records, job_id)
                    total_inserted += inserted
                
                processed_stocks += 1
                
                # 限流保护：每只股票的每个交易日都有一次API调用
                # 500次/分钟限制，留100次余量，休眠120毫秒
                time.sleep(0.12)
                
                # 每10只股票更新一次进度
                if processed_stocks % 10 == 0:
                    _update_job_progress(conn, job_id, {
                        "total_items": total_stocks,
                        "completed_items": processed_stocks,
                        "success_count": processed_stocks - len(failed_stocks),
                        "failed_count": len(failed_stocks),
                        "inserted_rows": total_inserted,
                    })
                    _log_job(conn, job_id, "INFO", f"进度: {processed_stocks}/{total_stocks} ({processed_stocks/total_stocks*100:.1f}%)")
                
            except Exception as e:
                failed_stocks.append(ts_code)
                _log_job(conn, job_id, "ERROR", f"{ts_code} 处理失败: {e}")
                processed_stocks += 1
        
        # 最终更新
        _update_job_progress(conn, job_id, {
            "total_items": total_stocks,
            "completed_items": processed_stocks,
            "success_count": processed_stocks - len(failed_stocks),
            "failed_count": len(failed_stocks),
            "inserted_rows": total_inserted,
        })
        
        status = "success" if not failed_stocks else "failed"
        message = f"完成: {processed_stocks}/{total_stocks} 只股票, 插入 {total_inserted} 条"
        if failed_stocks:
            message += f", 失败 {len(failed_stocks)} 只"
        
        _complete_job(conn, job_id, status, message)
        _log_job(conn, job_id, "INFO", message)
        
        return {
            "success": True,
            "job_id": str(job_id),
            "total_stocks": total_stocks,
            "processed_stocks": processed_stocks,
            "failed_stocks": failed_stocks,
            "total_inserted": total_inserted,
        }
        
    except Exception as e:
        _complete_job(conn, job_id, "failed", str(e))
        _log_job(conn, job_id, "ERROR", f"任务失败: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tushare cyq数据入库')
    parser.add_argument('--dataset', type=str, required=True, choices=['cyq_perf', 'cyq_chips'],
                        help='数据集名称')
    parser.add_argument('--mode', type=str, required=True, choices=['init', 'incremental'],
                        help='同步模式: init(全量) / incremental(增量)')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD')
    parser.add_argument('--job-id', type=str, help='任务ID（可选）')
    parser.add_argument('--batch-sleep', type=float, default=0.2,
                        help='批次间休眠时间（秒），默认0.2秒')
    parser.add_argument('--truncate', action='store_true',
                        help='init模式时先清空表（仅cyq_perf支持）')
    
    args = parser.parse_args()
    
    # 更新全局配置（在模块级别代码中直接赋值即可）
    BATCH_SLEEP = args.batch_sleep
    
    # 设置默认日期
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y%m%d')
    
    if not args.start_date:
        if args.mode == 'incremental':
            # 增量模式默认同步最近7天
            args.start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        else:
            # 全量模式默认从2018年开始
            args.start_date = '20180101'
    
    job_id = uuid.UUID(args.job_id) if args.job_id else None
    
    print("=" * 60)
    print(f"Tushare {args.dataset} {args.mode} 同步")
    print(f"日期范围: {args.start_date} ~ {args.end_date}")
    print("=" * 60)
    
    try:
        # 处理truncate（仅cyq_perf支持）
        if args.truncate and args.dataset == 'cyq_perf' and args.mode == 'init':
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE market.cyq_perf")
                    conn.commit()
                print("[WARN] TRUNCATE market.cyq_perf executed before ingestion")
            finally:
                conn.close()
        
        if args.dataset == 'cyq_perf':
            result = run_cyq_perf_ingestion(args.start_date, args.end_date, args.mode, job_id)
        else:
            result = run_cyq_chips_ingestion(args.start_date, args.end_date, None, args.mode, job_id)
        
        print("\n" + "=" * 60)
        print("[OK] 同步完成")
        print("=" * 60)
        print("任务ID: " + str(result['job_id']))
        if 'total_dates' in result:
            print(f"  处理日期: {result['processed_dates']}/{result['total_dates']}")
        if 'total_stocks' in result:
            print(f"  处理股票: {result['processed_stocks']}/{result['total_stocks']}")
        print(f"  插入记录: {result['total_inserted']}")
        
    except Exception as e:
        print(f"\n[ERROR] 同步失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
