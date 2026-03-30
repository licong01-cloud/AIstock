"""批量补录缺失因子 v2 — 通过 aligned_sota_factors API 获取因子代码并直接入库。

与 v1 不同，v2 不再调用 AIstock 重同步 API（那会重新触发完整 sync 流程），
而是直接通过 RDAgent aligned API 获取每个因子的独立源代码 + formulation，
然后 UPSERT 到 aistock_factor_catalog。

用法:
    cd F:/Dev/AIstock/backend
    python scripts/backfill_missing_factors_v2.py --dry-run    # 只扫描，不写入
    python scripts/backfill_missing_factors_v2.py              # 扫描 + 执行补录

数据源:
    - diagnose_missing_factors_result.json: 诊断结果（78 个缺失 task）
    - RDAgent aligned API: /api/extractors/sota_factors/v2/{task_id}/aligned
    - RDAgent loops API: /tasks/{task_id}/loops（获取回测指标）

2026-03-16 创建
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv("F:/Dev/AIstock/.env")

import psycopg2

import requests

# ─── 配置 ───────────────────────────────────────────────────────────────────
DB_DSN = (
    f"dbname={os.getenv('TDX_DB_NAME','aistock')} "
    f"user={os.getenv('TDX_DB_USER','postgres')} "
    f"password={os.getenv('TDX_DB_PASSWORD','lc78080808')} "
    f"host={os.getenv('TDX_DB_HOST','127.0.0.1')} "
    f"port={os.getenv('TDX_DB_PORT','5432')}"
)
RDAGENT_API = os.getenv("RDAGENT_RESULTS_API_BASE_URL", "http://127.0.0.1:9000")
DIAGNOSIS_JSON = os.path.join(os.path.dirname(__file__), "diagnose_missing_factors_result.json")
TASK_ASSETS_ROOT = Path(os.getenv("RDAGENT_TASK_ASSETS_DIR", "rdagent_assets/rdagent_tasks"))
API_TIMEOUT = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ─── 工具函数 ────────────────────────────────────────────────────────────────

def _normalize_code_for_dedup(code: str) -> str:
    """对因子代码进行归一化处理，用于去重哈希计算。"""
    lines = []
    for line in code.split('\n'):
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        comment_idx = stripped.find('#')
        if comment_idx > 0:
            stripped = stripped[:comment_idx].rstrip()
        lines.append(stripped)
    return '\n'.join(lines)


def compute_dedup_hash(code: str, factor_name: str) -> str:
    """计算因子代码去重哈希。"""
    normalized = _normalize_code_for_dedup(code)
    if not normalized.strip():
        return hashlib.sha256(f"name_only:{factor_name}".encode()).hexdigest()
    return hashlib.sha256(normalized.encode()).hexdigest()


def get_db_factors_for_task(conn, task_id: str) -> set:
    """获取 DB 中某 task 已入库的因子名集合。"""
    cur = conn.cursor()
    cur.execute(
        "SELECT factor_name FROM aistock_factor_catalog WHERE source_task_id = %s AND source = 'rdagent_task_sync'",
        (task_id,),
    )
    return {row[0] for row in cur.fetchall()}


def get_aligned_factors(task_id: str) -> dict:
    """调用 RDAgent aligned API 获取因子详情。"""
    url = f"{RDAGENT_API}/api/extractors/sota_factors/v2/{task_id}/aligned"
    resp = requests.get(url, timeout=API_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def get_loops_metrics(task_id: str) -> dict:
    """从 loops API 获取因子→回测指标映射。"""
    url = f"{RDAGENT_API}/tasks/{task_id}/loops"
    resp = requests.get(url, timeout=API_TIMEOUT)
    resp.raise_for_status()
    loops_data = resp.json()

    factor_metrics = {}
    for loop in loops_data.get("loops", []):
        if not loop.get("is_sota", False):
            continue
        metrics = {
            "loop_id": loop.get("loop_id"),
            "annualized_return": loop.get("annualized_return"),
            "max_drawdown": loop.get("max_drawdown"),
            "information_ratio": loop.get("information_ratio"),
            "ic": loop.get("valid_score"),
            "is_sota": True,
        }
        for fname in loop.get("tested_factors", []):
            if isinstance(fname, str) and fname not in factor_metrics:
                factor_metrics[fname] = metrics
    return factor_metrics


def upsert_factor(conn, task_id: str, fname: str, idx: int,
                  source_code: str, expression: str | None,
                  metrics: dict, now_utc: datetime,
                  task_dir: Path | None) -> bool:
    """UPSERT 一个因子到 aistock_factor_catalog。返回是否成功。"""
    dedup_hash = compute_dedup_hash(source_code, fname)

    # 保存因子代码文件
    asset_path_value = None
    if task_dir:
        factors_dir = task_dir / "factors"
        factors_dir.mkdir(parents=True, exist_ok=True)
        factor_file = factors_dir / f"{fname}.py"
        factor_file.write_text(source_code, encoding="utf-8")
        try:
            aistock_root = task_dir.parent.parent.parent
            asset_path_value = factor_file.relative_to(aistock_root).as_posix()
        except ValueError:
            asset_path_value = str(factor_file)

    ann_ret = metrics.get("annualized_return")
    max_dd = metrics.get("max_drawdown")
    info_ratio = metrics.get("information_ratio")
    ic_val = metrics.get("ic")
    loop_id = metrics.get("loop_id")
    perf_json = json.dumps(metrics, ensure_ascii=False) if metrics else None

    dedup_group_id = str(uuid.uuid4())[:12]

    sql = """
        INSERT INTO aistock_factor_catalog (
            factor_name, source, catalog_version, generated_at_utc, catalog_source,
            expression, is_sota_factor, first_sota_task_id,
            source_task_id, source_code_relpath, source_code_origin,
            source_loop_tag, source_index,
            ic, annualized_return, max_drawdown, sharpe,
            performance_metrics, best_performance_ann_ret, best_performance_sharpe,
            dedup_hash, dedup_group_id, is_dedup_primary, code_text, asset_path
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (factor_name, source) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            expression = COALESCE(EXCLUDED.expression, aistock_factor_catalog.expression),
            source_task_id = EXCLUDED.source_task_id,
            source_code_relpath = EXCLUDED.source_code_relpath,
            source_code_origin = EXCLUDED.source_code_origin,
            source_loop_tag = EXCLUDED.source_loop_tag,
            source_index = EXCLUDED.source_index,
            ic = EXCLUDED.ic,
            annualized_return = EXCLUDED.annualized_return,
            max_drawdown = EXCLUDED.max_drawdown,
            sharpe = EXCLUDED.sharpe,
            performance_metrics = EXCLUDED.performance_metrics,
            best_performance_ann_ret = EXCLUDED.best_performance_ann_ret,
            best_performance_sharpe = EXCLUDED.best_performance_sharpe,
            first_sota_task_id = COALESCE(aistock_factor_catalog.first_sota_task_id, EXCLUDED.first_sota_task_id),
            dedup_hash = COALESCE(EXCLUDED.dedup_hash, aistock_factor_catalog.dedup_hash),
            dedup_group_id = COALESCE(EXCLUDED.dedup_group_id, aistock_factor_catalog.dedup_group_id),
            is_dedup_primary = EXCLUDED.is_dedup_primary,
            code_text = COALESCE(EXCLUDED.code_text, aistock_factor_catalog.code_text),
            asset_path = COALESCE(EXCLUDED.asset_path, aistock_factor_catalog.asset_path)
    """

    cur = conn.cursor()
    cur.execute(sql, (
        fname, "rdagent_task_sync", "backfill_v2", now_utc, "backfill_missing_factors_v2",
        expression, True, task_id,
        task_id, f"aligned/{fname}.py", "aligned_api_backfill",
        str(loop_id) if loop_id is not None else None, idx,
        ic_val, ann_ret, max_dd, info_ratio,
        perf_json, ann_ret, info_ratio,
        dedup_hash, dedup_group_id, True,
        source_code,
        asset_path_value,
    ))
    conn.commit()
    return True


def update_sota_factors_count(conn, task_id: str):
    """重新计算并更新 aistock_task_catalog.sota_factors_count。"""
    cur = conn.cursor()
    cur.execute(
        """UPDATE aistock_task_catalog
           SET sota_factors_count = (
               SELECT COUNT(*) FROM aistock_factor_catalog
               WHERE source_task_id = %s AND source = 'rdagent_task_sync'
           )
           WHERE task_id = %s""",
        (task_id, task_id),
    )
    conn.commit()


# ─── 主流程 ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="批量补录缺失因子 v2")
    parser.add_argument("--dry-run", action="store_true", help="只扫描，不执行写入")
    parser.add_argument("--diagnosis-json", default=DIAGNOSIS_JSON, help="诊断结果 JSON 路径")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("缺失因子批量补录 v2")
    log.info(f"模式: {'DRY-RUN (只扫描)' if args.dry_run else 'EXECUTE (扫描+补录)'}")
    log.info("=" * 70)

    # 1. 读取诊断结果
    with open(args.diagnosis_json, "r", encoding="utf-8") as f:
        diagnosis = json.load(f)

    # 筛选有缺失因子的 task
    tasks_with_missing = [
        r for r in diagnosis
        if r.get("api_ok") and len(r.get("missing_in_db", [])) > 0
    ]
    total_missing = sum(len(r["missing_in_db"]) for r in tasks_with_missing)
    log.info(f"诊断结果: {len(tasks_with_missing)} 个 task 共 {total_missing} 个因子缺失")

    if not tasks_with_missing:
        log.info("无缺失因子，退出")
        return

    # 2. 逐 task 补录
    conn = psycopg2.connect(DB_DSN)
    now_utc = datetime.now(timezone.utc)

    total_inserted = 0
    total_errors = 0
    total_skipped = 0
    task_success = 0
    task_failed = 0

    for ti, task_info in enumerate(tasks_with_missing):
        task_id = task_info["task_id"]
        diag_missing = set(task_info["missing_in_db"])
        log.info(f"\n[{ti+1}/{len(tasks_with_missing)}] {task_id} — 诊断缺失 {len(diag_missing)} 个因子")

        try:
            # 获取 DB 当前状态（诊断后可能已有变化）
            db_existing = get_db_factors_for_task(conn, task_id)
            actually_missing = diag_missing - db_existing
            if not actually_missing:
                log.info(f"  已全部入库，跳过")
                total_skipped += len(diag_missing)
                continue

            log.info(f"  DB 已有 {len(db_existing)} 个，实际缺失 {len(actually_missing)} 个")

            if args.dry_run:
                for fname in sorted(actually_missing):
                    log.info(f"  [DRY-RUN] 缺失: {fname}")
                total_inserted += len(actually_missing)
                continue

            # 调用 aligned API
            aligned_resp = get_aligned_factors(task_id)
            if not aligned_resp.get("success"):
                raise RuntimeError(f"aligned API 失败: {aligned_resp.get('error', 'unknown')}")

            aligned_factors = aligned_resp.get("aligned_factors", {})

            # 获取回测指标
            factor_metrics = get_loops_metrics(task_id)

            # task 资产目录
            task_dir = TASK_ASSETS_ROOT / task_id
            if not task_dir.exists():
                task_dir = None
                log.warning(f"  task 资产目录不存在: {TASK_ASSETS_ROOT / task_id}")

            task_inserted = 0
            task_errors = 0

            for idx, fname in enumerate(sorted(actually_missing)):
                finfo = aligned_factors.get(fname)
                if not finfo:
                    log.warning(f"  因子 {fname} 不在 aligned API 返回中，跳过")
                    task_errors += 1
                    continue

                source_code = finfo.get("source_code", "")
                if not source_code:
                    log.warning(f"  因子 {fname} 无 source_code，跳过")
                    task_errors += 1
                    continue

                # 构造 expression
                formulation = finfo.get("factor_formulation", "")
                description = finfo.get("factor_description", "")
                if formulation and description:
                    expression = f"{formulation} | {description}"
                elif formulation:
                    expression = formulation
                elif description:
                    expression = description
                else:
                    expression = None

                metrics = factor_metrics.get(fname, {})

                try:
                    upsert_factor(conn, task_id, fname, idx, source_code, expression,
                                  metrics, now_utc, task_dir)
                    task_inserted += 1
                    log.info(f"  补录成功: {fname} (ic={metrics.get('ic')}, ann_ret={metrics.get('annualized_return')})")
                except Exception as e:
                    log.error(f"  补录失败: {fname} — {e}")
                    conn.rollback()
                    task_errors += 1

            # 更新 sota_factors_count
            if task_inserted > 0:
                update_sota_factors_count(conn, task_id)

            total_inserted += task_inserted
            total_errors += task_errors
            task_success += 1
            log.info(f"  完成: +{task_inserted} 入库, {task_errors} 错误")

        except Exception as e:
            log.error(f"  task 处理失败: {e}")
            task_failed += 1
            total_errors += len(diag_missing)

        time.sleep(0.3)

    conn.close()

    # 3. 汇总
    log.info("\n" + "=" * 70)
    log.info("执行完成")
    log.info(f"  task 成功: {task_success}")
    log.info(f"  task 失败: {task_failed}")
    if args.dry_run:
        log.info(f"  [DRY-RUN] 待补录因子: {total_inserted}")
        log.info(f"  [DRY-RUN] 已跳过（已入库）: {total_skipped}")
    else:
        log.info(f"  因子补录成功: {total_inserted}")
        log.info(f"  因子补录失败: {total_errors}")
        log.info(f"  因子跳过（已入库）: {total_skipped}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
