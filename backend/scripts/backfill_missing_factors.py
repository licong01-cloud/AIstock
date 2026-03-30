"""批量修复因子遗漏：重新同步所有因子数量不一致的 task。

用法:
    python scripts/backfill_missing_factors.py --dry-run    # 只扫描，不执行
    python scripts/backfill_missing_factors.py              # 扫描 + 执行重同步

原因: v2_alignment_preview 之前使用 sub_tasks_fallback，只取最后 SOTA loop 的因子，
      漏掉跨 loop 累积的因子。修复后改为从 parquet_schema 获取完整因子列表。

2026-03-15 创建
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests

# ─── 配置 ───────────────────────────────────────────────────────────────────
DB_DSN = "dbname=aistock user=postgres password=lc78080808 host=127.0.0.1 port=5432"
AISTOCK_API = "http://localhost:8001"
RDAGENT_API = "http://localhost:9000"
SYNC_TIMEOUT = 120  # 每个 task 同步超时(秒)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def get_db_factor_counts() -> dict:
    """从 DB 获取每个 task 的因子数和因子名列表。"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        SELECT source_task_id,
               array_agg(factor_name ORDER BY factor_name) as factor_names,
               COUNT(*) as factor_count
        FROM aistock_factor_catalog
        WHERE source = 'rdagent_task_sync'
        GROUP BY source_task_id
        ORDER BY source_task_id
    """)
    result = {}
    for row in cur.fetchall():
        result[row[0]] = {"names": row[1], "count": row[2]}
    conn.close()
    return result


def get_api_factor_count(task_id: str) -> dict:
    """调用 v2_alignment_preview 获取实际 SOTA 因子数。"""
    resp = requests.get(f"{RDAGENT_API}/tasks/{task_id}/v2_alignment_preview", timeout=60)
    data = resp.json()
    if data.get("ok"):
        return {
            "ok": True,
            "count": data.get("sota_factors_count", 0),
            "names": data.get("sota_factors", []),
            "source": data.get("sota_source", "?"),
        }
    return {"ok": False, "error": data.get("error", "unknown")}


def resync_task(task_id: str) -> dict:
    """通过 AIstock API 重新同步一个 task。"""
    resp = requests.post(
        f"{AISTOCK_API}/api/v1/rdagent/tasks/sync",
        json={"task_ids": [task_id], "operator": "backfill_script", "mode": "log_only"},
        timeout=SYNC_TIMEOUT,
    )
    data = resp.json()
    if data.get("ok") and data.get("results"):
        r = data["results"][0]
        return {
            "ok": r.get("ok", False),
            "status": r.get("sync_status"),
            "error": r.get("error"),
            "factor_count": r.get("diagnostics", {}).get("factor_catalog_sync", {}).get("total", 0),
        }
    return {"ok": False, "error": str(data)}


def main():
    parser = argparse.ArgumentParser(description="批量修复因子遗漏")
    parser.add_argument("--dry-run", action="store_true", help="只扫描，不执行同步")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("因子遗漏批量修复脚本")
    log.info(f"模式: {'DRY-RUN (只扫描)' if args.dry_run else 'EXECUTE (扫描+同步)'}")
    log.info("=" * 60)

    # 1. 获取 DB 数据
    log.info("步骤 1: 扫描 DB 中已同步的 task...")
    db_data = get_db_factor_counts()
    log.info(f"  DB 中共 {len(db_data)} 个已同步 task")

    # 2. 对比 API
    log.info("步骤 2: 对比 v2_alignment_preview API...")
    needs_resync = []   # (task_id, db_count, api_count, api_names, db_names)
    skipped_ok = []     # (task_id, db_count, api_count) — DB > API 但 API 因子全在 DB 中
    api_errors = []     # (task_id, db_count, error)

    for i, (task_id, info) in enumerate(db_data.items()):
        db_count = info["count"]
        db_names = info["names"]
        api_info = get_api_factor_count(task_id)

        if not api_info["ok"]:
            api_errors.append((task_id, db_count, api_info["error"]))
            continue

        api_count = api_info["count"]
        api_names = api_info["names"]

        if api_count > db_count:
            needs_resync.append((task_id, db_count, api_count, api_names, db_names))
        elif api_count < db_count:
            # DB > API: 用户可能手动同步了非 SOTA loop 的因子，不清理，只重同步补全
            db_has_all_api = set(api_names).issubset(set(db_names))
            if not db_has_all_api:
                needs_resync.append((task_id, db_count, api_count, api_names, db_names))
            else:
                skipped_ok.append((task_id, db_count, api_count))
        # else: OK, skip

        if (i + 1) % 10 == 0:
            log.info(f"  已扫描 {i + 1}/{len(db_data)}...")

    # 3. 汇总
    log.info("")
    log.info("=" * 60)
    log.info("扫描结果汇总")
    log.info(f"  因子不足 (需重同步):  {len(needs_resync)} 个 task")
    log.info(f"  DB>API 但已包含全部 (跳过): {len(skipped_ok)} 个 task")
    log.info(f"  API 错误 (跳过):      {len(api_errors)} 个 task")
    log.info("=" * 60)

    if skipped_ok:
        log.info("")
        log.info("DB>API 但 API 因子已全在 DB 中 (用户手动同步了额外 loop 因子，跳过):")
        for tid, db_c, api_c in skipped_ok:
            log.info(f"  {tid}  DB={db_c}  API={api_c}  (多出 {db_c - api_c} 个非SOTA因子)")

    if api_errors:
        log.info("")
        log.info("API 错误的 task (需手动处理):")
        for tid, db_c, err in api_errors:
            log.info(f"  {tid}  DB={db_c}  error={err}")

    if args.dry_run:
        log.info("")
        log.info("[DRY-RUN] 以下 task 需要重同步:")
        total_missing = 0
        for tid, db_c, api_c, _, _ in needs_resync:
            diff = api_c - db_c
            total_missing += diff
            log.info(f"  {tid}  DB={db_c} -> API={api_c}  (+{diff})")
        log.info(f"  合计缺少 {total_missing} 个因子")

        return

    # 4. 执行重同步
    log.info("")
    log.info(f"步骤 3: 重同步 {len(needs_resync)} 个 task...")
    all_resync = needs_resync

    success = 0
    failed = 0
    for i, (tid, db_c, api_c, _, _) in enumerate(all_resync):
        log.info(f"  [{i + 1}/{len(all_resync)}] {tid}  DB={db_c} -> API={api_c}")
        try:
            result = resync_task(tid)
            if result["ok"]:
                log.info(f"    OK: synced {result['factor_count']} factors")
                success += 1
            else:
                log.info(f"    FAIL: {result['error']}")
                failed += 1
        except Exception as e:
            log.error(f"    EXCEPTION: {e}")
            failed += 1
        # 避免过快请求
        time.sleep(0.5)

    # 6. 最终报告
    log.info("")
    log.info("=" * 60)
    log.info("执行完成")
    log.info(f"  成功: {success}")
    log.info(f"  失败: {failed}")
    log.info(f"  API错误(跳过): {len(api_errors)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
