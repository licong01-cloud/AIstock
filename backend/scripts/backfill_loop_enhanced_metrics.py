"""A2: 已入库 Loop 增强诊断回填脚本

遍历所有 completed 且 enhanced_metrics 为空的 Loop，
调用修复后的 enhanced-metrics API 触发数据缓存到 DB。

使用方式:
    python backfill_loop_enhanced_metrics.py [--dry-run] [--concurrency 2]
"""
import argparse
import asyncio
import logging
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# 加载 .env
from pathlib import Path
from dotenv import load_dotenv
_env = Path(__file__).resolve().parents[2] / ".env"
if _env.exists():
    load_dotenv(_env, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_loops_needing_backfill():
    """查询所有 completed 但 enhanced_metrics 为空的 Loop。"""
    from db.pg_pool import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT loop_id, task_id, loop_index, metrics_json
                FROM qe_evolution_loops
                WHERE status = 'completed'
                  AND (
                    metrics_json IS NULL
                    OR NOT metrics_json ? 'enhanced_metrics'
                    OR metrics_json->'enhanced_metrics' = 'null'::jsonb
                    OR metrics_json->'enhanced_metrics' = '{}'::jsonb
                  )
                ORDER BY created_at
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in rows]


async def backfill_one_loop(session, base_url: str, task_id: str, loop_id: str, loop_index: int) -> dict:
    """调用 AIStock enhanced-metrics 代理端点触发回填。"""
    import httpx

    # 从 loop_id 提取 task_id 和 LoopN 格式
    # loop_id 格式通常是 "{task_id}_Loop{N}"
    rdagent_loop_id = f"Loop{loop_index}"

    url = f"{base_url}/api/v1/quantevolver/evolution/tasks/{task_id}/loops/{rdagent_loop_id}/enhanced-metrics"
    try:
        resp = await session.get(url, timeout=60.0)
        if resp.status_code == 200:
            data = resp.json()
            sections = list(data.get("data", data).keys()) if isinstance(data.get("data", data), dict) else []
            return {"ok": True, "loop_id": loop_id, "sections": sections}
        else:
            return {"ok": False, "loop_id": loop_id, "error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "loop_id": loop_id, "error": str(e)}


async def run_backfill(base_url: str, dry_run: bool, concurrency: int):
    """执行回填主流程。"""
    import httpx

    loops = get_loops_needing_backfill()
    logger.info(f"发现 {len(loops)} 个 Loop 需要增强诊断回填")

    if not loops:
        logger.info("无需回填，退出")
        return

    if dry_run:
        for loop in loops:
            logger.info(f"  [DRY-RUN] {loop['loop_id']} (task={loop['task_id']}, index={loop['loop_index']})")
        return

    semaphore = asyncio.Semaphore(concurrency)
    success = 0
    failed = 0

    async with httpx.AsyncClient() as session:
        async def bounded_backfill(loop):
            async with semaphore:
                return await backfill_one_loop(
                    session, base_url,
                    loop["task_id"], loop["loop_id"], loop["loop_index"]
                )

        tasks = [bounded_backfill(loop) for loop in loops]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result["ok"]:
                success += 1
                logger.info(f"  OK: {result['loop_id']} → sections: {result.get('sections', [])}")
            else:
                failed += 1
                logger.warning(f"  FAIL: {result['loop_id']} → {result['error']}")

    logger.info(f"回填完成: {success} 成功, {failed} 失败, 共 {len(loops)} 个 Loop")


def main():
    parser = argparse.ArgumentParser(description="Loop 增强诊断数据回填")
    parser.add_argument("--base-url", default="http://localhost:8001",
                        help="AIStock 后端 URL (默认 http://localhost:8001)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅列出需要回填的 Loop，不执行")
    parser.add_argument("--concurrency", type=int, default=2,
                        help="并发数 (默认 2)")
    args = parser.parse_args()

    asyncio.run(run_backfill(args.base_url, args.dry_run, args.concurrency))


if __name__ == "__main__":
    main()
