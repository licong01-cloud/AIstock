"""诊断所有 task 的因子缺失情况。

输出:
1. 每个 task 的 DB 因子数 vs API 因子数
2. 每个 task 缺失的具体因子名
3. 元数据完整性检查 (code_text, asset_path, ic 是否为 NULL)
4. 汇总统计表

用法:
    cd F:\Dev\AIstock\backend
    python scripts/diagnose_missing_factors.py
"""

import json
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv("F:/Dev/AIstock/.env")

import psycopg2
import requests

DB_DSN = f"dbname={os.getenv('TDX_DB_NAME','aistock')} user={os.getenv('TDX_DB_USER','postgres')} password={os.getenv('TDX_DB_PASSWORD','lc78080808')} host={os.getenv('TDX_DB_HOST','127.0.0.1')} port={os.getenv('TDX_DB_PORT','5432')}"
RDAGENT_API = os.getenv("RDAGENT_RESULTS_API_BASE_URL", "http://127.0.0.1:9000")
TIMEOUT = 60


def get_db_tasks_and_factors():
    """获取 DB 中所有已同步 task 的因子详情。"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    # 获取所有已同步的 task
    cur.execute("""
        SELECT task_id, sync_status, sota_factors_count
        FROM aistock_task_catalog
        WHERE sync_status = 'success'
        ORDER BY task_id
    """)
    tasks = {row[0]: {"sync_status": row[1], "db_sota_count_catalog": row[2]} for row in cur.fetchall()}

    # 获取每个 task 的因子详情
    cur.execute("""
        SELECT source_task_id, factor_name,
               (code_text IS NOT NULL AND code_text != '') as has_code,
               (asset_path IS NOT NULL AND asset_path != '') as has_path,
               ic IS NOT NULL as has_ic,
               annualized_return IS NOT NULL as has_ann_ret,
               (expression IS NOT NULL AND expression != '') as has_expr
        FROM aistock_factor_catalog
        WHERE source = 'rdagent_task_sync'
        ORDER BY source_task_id, factor_name
    """)

    factors_by_task = {}
    for row in cur.fetchall():
        tid = row[0]
        if tid not in factors_by_task:
            factors_by_task[tid] = []
        factors_by_task[tid].append({
            "name": row[1],
            "has_code": row[2],
            "has_path": row[3],
            "has_ic": row[4],
            "has_ann_ret": row[5],
            "has_expr": row[6],
        })

    conn.close()
    return tasks, factors_by_task


def get_api_factors(task_id):
    """调用 v2_alignment_preview 获取 API 侧因子列表。"""
    try:
        resp = requests.get(
            f"{RDAGENT_API}/tasks/{task_id}/v2_alignment_preview",
            timeout=TIMEOUT,
        )
        data = resp.json()
        if data.get("ok"):
            return {
                "ok": True,
                "factors": data.get("sota_factors", []),
                "count": data.get("sota_factors_count", 0),
                "source": data.get("sota_source", "?"),
            }
        return {"ok": False, "error": data.get("error", "unknown")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("=" * 80)
    print("因子缺失诊断报告")
    print("=" * 80)

    # 1. 获取 DB 数据
    print("\n[1/3] 查询数据库...")
    tasks, factors_by_task = get_db_tasks_and_factors()
    print(f"  已同步 task 数: {len(tasks)}")
    print(f"  有因子记录的 task 数: {len(factors_by_task)}")

    # 2. 逐个 task 对比 API
    print(f"\n[2/3] 逐个对比 v2_alignment_preview API...")
    results = []

    all_task_ids = sorted(set(list(tasks.keys()) + list(factors_by_task.keys())))

    for i, tid in enumerate(all_task_ids):
        db_factors = factors_by_task.get(tid, [])
        db_names = set(f["name"] for f in db_factors)
        db_count = len(db_factors)

        # 元数据完整性
        no_code = [f["name"] for f in db_factors if not f["has_code"]]
        no_path = [f["name"] for f in db_factors if not f["has_path"]]
        no_ic = [f["name"] for f in db_factors if not f["has_ic"]]
        no_expr = [f["name"] for f in db_factors if not f["has_expr"]]

        api_info = get_api_factors(tid)

        if api_info["ok"]:
            api_names = set(api_info["factors"])
            api_count = api_info["count"]
            missing_in_db = sorted(api_names - db_names)
            extra_in_db = sorted(db_names - api_names)
        else:
            api_names = set()
            api_count = -1
            missing_in_db = []
            extra_in_db = []

        results.append({
            "task_id": tid,
            "db_count": db_count,
            "api_count": api_count,
            "api_ok": api_info["ok"],
            "api_error": api_info.get("error", ""),
            "missing_in_db": missing_in_db,
            "extra_in_db": extra_in_db,
            "no_code": no_code,
            "no_path": no_path,
            "no_ic": no_ic,
            "no_expr": no_expr,
        })

        if (i + 1) % 5 == 0:
            print(f"  已扫描 {i + 1}/{len(all_task_ids)}...")
        time.sleep(0.2)

    # 3. 输出报告
    print(f"\n[3/3] 生成报告...\n")

    # 分类统计
    tasks_missing_factors = [r for r in results if r["api_ok"] and len(r["missing_in_db"]) > 0]
    tasks_no_code = [r for r in results if len(r["no_code"]) > 0]
    tasks_no_ic = [r for r in results if len(r["no_ic"]) > 0]
    tasks_api_error = [r for r in results if not r["api_ok"]]

    print("=" * 80)
    print("汇总统计")
    print("=" * 80)
    print(f"  总 task 数:                    {len(results)}")
    print(f"  API 正常的 task:               {sum(1 for r in results if r['api_ok'])}")
    print(f"  API 错误的 task:               {len(tasks_api_error)}")
    print(f"  因子数量缺失的 task:           {len(tasks_missing_factors)}")
    print(f"  有因子缺 code_text 的 task:    {len(tasks_no_code)}")
    print(f"  有因子缺 IC 指标的 task:       {len(tasks_no_ic)}")
    total_missing = sum(len(r["missing_in_db"]) for r in tasks_missing_factors)
    print(f"  总缺失因子数:                  {total_missing}")

    # 因子数量缺失详情表
    if tasks_missing_factors:
        print(f"\n{'=' * 80}")
        print("因子数量缺失详情")
        print(f"{'=' * 80}")
        print(f"{'Task ID':<40} {'DB':>4} {'API':>4} {'缺失':>4}  缺失因子")
        print("-" * 80)
        for r in sorted(tasks_missing_factors, key=lambda x: -len(x["missing_in_db"])):
            missing_str = ", ".join(r["missing_in_db"][:5])
            if len(r["missing_in_db"]) > 5:
                missing_str += f" ...+{len(r['missing_in_db'])-5}"
            print(f"{r['task_id']:<40} {r['db_count']:>4} {r['api_count']:>4} {len(r['missing_in_db']):>4}  {missing_str}")

    # 元数据缺失详情
    if tasks_no_code:
        print(f"\n{'=' * 80}")
        print("因子缺少 code_text 详情")
        print(f"{'=' * 80}")
        print(f"{'Task ID':<40} {'缺code':>6}  因子名")
        print("-" * 80)
        for r in tasks_no_code:
            names_str = ", ".join(r["no_code"][:5])
            if len(r["no_code"]) > 5:
                names_str += f" ...+{len(r['no_code'])-5}"
            print(f"{r['task_id']:<40} {len(r['no_code']):>6}  {names_str}")

    if tasks_no_ic:
        print(f"\n{'=' * 80}")
        print("因子缺少 IC 指标详情")
        print(f"{'=' * 80}")
        print(f"{'Task ID':<40} {'缺IC':>5}  因子名")
        print("-" * 80)
        for r in tasks_no_ic:
            names_str = ", ".join(r["no_ic"][:5])
            if len(r["no_ic"]) > 5:
                names_str += f" ...+{len(r['no_ic'])-5}"
            print(f"{r['task_id']:<40} {len(r['no_ic']):>5}  {names_str}")

    if tasks_api_error:
        print(f"\n{'=' * 80}")
        print("API 错误的 task")
        print(f"{'=' * 80}")
        for r in tasks_api_error:
            print(f"  {r['task_id']}  DB={r['db_count']}  error={r['api_error']}")

    # 保存 JSON 详情
    report_path = os.path.join(os.path.dirname(__file__), "..", "scripts", "diagnose_missing_factors_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n详细 JSON 已保存: {report_path}")


if __name__ == "__main__":
    main()
