"""C3: 已入库 SOTA 模型训练数据回填脚本

对 aistock_model_catalog 中训练诊断字段为空的 SOTA 模型，
从 RDAgent 侧 qlib_results_enhanced.json 提取训练诊断数据回填。

使用方式:
    python backfill_model_training_data.py [--dry-run] [--concurrency 2]
"""
import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# 加载 .env
from pathlib import Path
from dotenv import load_dotenv
_env = Path(__file__).resolve().parents[2] / ".env"
if _env.exists():
    load_dotenv(_env, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_models_needing_backfill():
    """查询训练诊断字段为空的 SOTA 模型。"""
    from db.pg_pool import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT model_id, task_run_id, loop_id
                FROM aistock_model_catalog
                WHERE is_sota = TRUE
                  AND best_epoch IS NULL
                  AND training_failed IS NOT TRUE
                ORDER BY generated_at_utc
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in rows]


def update_model_training_data(model_id: str, training_diag: dict):
    """更新单个模型的训练诊断数据。"""
    from db.pg_pool import get_conn

    train_curves = None
    if training_diag.get("train_loss_curve") or training_diag.get("val_loss_curve"):
        train_curves = json.dumps({
            "train_loss": training_diag.get("train_loss_curve", []),
            "val_loss": training_diag.get("val_loss_curve", []),
        })

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE aistock_model_catalog
                SET best_epoch = %s,
                    total_epochs = %s,
                    convergence_ratio = %s,
                    overfit_ratio = %s,
                    training_failed = %s,
                    train_loss_final = %s,
                    val_loss_final = %s,
                    training_curves = %s
                WHERE model_id = %s
            """, (
                training_diag.get("best_epoch"),
                training_diag.get("total_epochs"),
                training_diag.get("convergence_ratio"),
                training_diag.get("overfit_ratio"),
                training_diag.get("training_failed", False),
                training_diag.get("final_train_loss"),
                training_diag.get("final_val_loss"),
                train_curves,
                model_id,
            ))
            return cur.rowcount > 0


async def fetch_training_data(session, base_url: str, task_id: str, loop_id: int) -> dict:
    """从 RDAgent enhanced-metrics API 获取训练诊断数据。"""
    url = f"{base_url}/api/v1/qe_workspace/tasks/{task_id}/loops/Loop{loop_id}/enhanced-metrics"
    try:
        resp = await session.get(url, timeout=60.0)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("training_diagnostics", {})
    except Exception as e:
        logger.debug(f"API 获取失败 [{task_id}/Loop{loop_id}]: {e}")
    return {}


async def run_backfill(rdagent_url: str, dry_run: bool, concurrency: int):
    """执行回填主流程。"""
    import httpx

    models = get_models_needing_backfill()
    logger.info(f"发现 {len(models)} 个 SOTA 模型需要训练数据回填")

    if not models:
        logger.info("无需回填，退出")
        return

    if dry_run:
        for m in models:
            logger.info(f"  [DRY-RUN] {m['model_id']} (task={m['task_run_id']}, loop={m['loop_id']})")
        return

    semaphore = asyncio.Semaphore(concurrency)
    success = 0
    failed = 0
    no_data = 0

    async with httpx.AsyncClient() as session:
        async def process_one(model):
            async with semaphore:
                training_diag = await fetch_training_data(
                    session, rdagent_url,
                    model["task_run_id"], model["loop_id"]
                )
                return model, training_diag

        tasks = [process_one(m) for m in models]
        for coro in asyncio.as_completed(tasks):
            model, training_diag = await coro
            if not training_diag or not training_diag.get("best_epoch"):
                no_data += 1
                logger.info(f"  NO-DATA: {model['model_id']} (无训练诊断数据)")
                continue

            try:
                if update_model_training_data(model["model_id"], training_diag):
                    success += 1
                    logger.info(
                        f"  OK: {model['model_id']} → "
                        f"best_epoch={training_diag.get('best_epoch')}, "
                        f"convergence={training_diag.get('convergence_ratio')}"
                    )
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"  FAIL: {model['model_id']} → {e}")

    logger.info(
        f"回填完成: {success} 成功, {no_data} 无数据, {failed} 失败, 共 {len(models)} 个模型"
    )


def main():
    parser = argparse.ArgumentParser(description="SOTA 模型训练数据回填")
    parser.add_argument("--rdagent-url", default="http://localhost:9000",
                        help="RDAgent API URL (默认 http://localhost:9000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅列出需要回填的模型，不执行")
    parser.add_argument("--concurrency", type=int, default=2,
                        help="并发数 (默认 2)")
    args = parser.parse_args()

    asyncio.run(run_backfill(args.rdagent_url, args.dry_run, args.concurrency))


if __name__ == "__main__":
    main()
