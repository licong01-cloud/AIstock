#!/usr/bin/env python3
"""手工因子入库脚本（Windows 端，Claude 对话直接调用）。

用法:
    # 单因子入库（代码+LLM分类，不计算指标）
    python register_manual_factor.py save --name m_mom_close_5d --code-file factor.py --description "5日动量"

    # 批量计算已入库因子的独立指标
    python register_manual_factor.py compute-metrics m_factor1 m_factor2

    # 计算全部可用因子的独立指标
    python register_manual_factor.py compute-metrics --all

    # 完整流程（入库+指标+LLM分类）
    python register_manual_factor.py full-pipeline --name m_mom_close_5d --code-file factor.py

模式:
    --api       通过 AIstock API 执行（默认，需后端运行）
    --direct-db 直接操作 DB + WSL（不需要 API）
"""
import argparse
import json
import os
import sys
from pathlib import Path

# 默认 API 地址
API_BASE = os.getenv("AISTOCK_API_BASE", "http://127.0.0.1:8001/api/v1/quantevolver")


def _api_request(method: str, path: str, data=None, timeout=600):
    """发送 API 请求。"""
    import requests
    url = f"{API_BASE}{path}"
    if method == "GET":
        resp = requests.get(url, params=data, timeout=timeout)
    else:
        resp = requests.post(url, json=data, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def cmd_save(args):
    """入库因子（代码+LLM分类）。"""
    code_text = Path(args.code_file).read_text(encoding="utf-8")
    print(f"入库因子: {args.name} (代码 {len(code_text)} 字符)")

    if args.direct_db:
        _save_direct_db(args.name, code_text, args.description)
    else:
        result = _api_request("POST", "/factors/manual", {
            "factor_name": args.name,
            "code_text": code_text,
            "description": args.description,
            "expression": args.expression,
        })
        print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_compute_metrics(args):
    """批量计算独立指标。"""
    if args.all:
        print("计算全部可用因子的独立指标...")
        data = {"all_available": True}
    else:
        names = args.factor_names
        if not names:
            print("请指定因子名或 --all")
            sys.exit(1)
        print(f"计算 {len(names)} 个因子的独立指标: {', '.join(names)}")
        data = {"factor_names": names}

    if args.direct_db:
        _compute_metrics_direct_db(data)
    else:
        result = _api_request("POST", "/factors/batch-compute-metrics-unified", data, timeout=1800)
        _print_metrics_result(result)


def cmd_full_pipeline(args):
    """完整流程：入库+指标+LLM分类。"""
    code_text = Path(args.code_file).read_text(encoding="utf-8")
    print(f"完整流水线: {args.name} (代码 {len(code_text)} 字符)")

    result = _api_request("POST", "/factors/manual/full-pipeline", {
        "factor_name": args.name,
        "code_text": code_text,
        "description": args.description,
        "expression": args.expression,
    }, timeout=600)
    _print_pipeline_result(result)


def cmd_validate(args):
    """验证因子代码。"""
    code_text = Path(args.code_file).read_text(encoding="utf-8")
    print(f"验证因子: {args.name}")

    result = _api_request("POST", "/factors/manual/validate", {
        "factor_name": args.name,
        "code_text": code_text,
    })
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_template(args):
    """获取因子代码模板。"""
    result = _api_request("GET", "/factors/manual/template", {"factor_name": args.name or "m_example"})
    print(result.get("template", ""))


# ── Direct DB 模式 ──

def _save_direct_db(name, code_text, description=None):
    """直接操作 DB 入库（不通过 API）。"""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
    os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")

    from backend.db.pg_pool import get_conn
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc).isoformat()
    sql = """
        INSERT INTO aistock_factor_catalog (
            factor_name, source, catalog_version, generated_at_utc, catalog_source,
            code_text, description_cn
        )
        VALUES (%s, 'manual', 'manual_v1', %s, 'manual', %s, %s)
        ON CONFLICT (factor_name, source) DO UPDATE SET
            catalog_version = EXCLUDED.catalog_version,
            generated_at_utc = EXCLUDED.generated_at_utc,
            code_text = EXCLUDED.code_text,
            description_cn = COALESCE(EXCLUDED.description_cn, aistock_factor_catalog.description_cn)
        RETURNING id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (name, now_utc, code_text, description))
            row = cur.fetchone()
            catalog_id = row[0] if row else None
    print(f"入库成功: {name} (catalog_id={catalog_id})")

    # 尝试 LLM 分类
    try:
        from backend.services.quantevolver.factor_analyst import FactorAnalyst
        analyst = FactorAnalyst()
        cls_result = analyst.analyze_single_factor(name, "manual", use_llm=True)
        print(f"分类: {cls_result.get('category')} | 评级: {cls_result.get('grade')}")
    except Exception as e:
        print(f"LLM 分类失败（因子已入库）: {e}")


def _compute_metrics_direct_db(data):
    """直接调用 WSL 计算指标（不通过 API）。"""
    import asyncio
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
    os.environ.setdefault("TDX_DB_PASSWORD", "lc78080808")

    from backend.services.manual_factor_service import ManualFactorService
    svc = ManualFactorService()
    result = asyncio.run(svc.batch_compute_metrics(
        factor_names=data.get("factor_names"),
        all_available=data.get("all_available", False),
    ))
    _print_metrics_result(result)


# ── 输出格式化 ──

def _print_metrics_result(result):
    """格式化输出指标计算结果。"""
    if not result.get("success"):
        print(f"失败: {result.get('error')}")
        return

    factors = result.get("factors", {})
    print(f"\n计算完成，共 {len(factors)} 个因子，耗时 {result.get('total_duration_sec', 0):.1f}s\n")

    db_result = result.get("db_result", {})
    print(f"DB: inserted={db_result.get('inserted', 0)}, skipped={db_result.get('skipped', 0)}")
    if db_result.get("errors"):
        for e in db_result["errors"]:
            print(f"  ERROR: {e}")

    print(f"\n{'因子名':<35} {'IC':>8} {'ICIR':>8} {'RankIC':>8} {'Sharpe':>8} {'AnnRet':>8}")
    print("-" * 85)
    for fname, windows in sorted(factors.items()):
        full = windows.get("full", {})
        ic = full.get("ic_mean")
        icir = full.get("icir")
        ric = full.get("rank_ic_mean")
        sharpe = full.get("top_sharpe")
        ann_ret = full.get("top_annual_return")
        print(f"{fname:<35} {_fmt(ic):>8} {_fmt(icir):>8} {_fmt(ric):>8} {_fmt(sharpe):>8} {_fmt(ann_ret, pct=True):>8}")


def _print_pipeline_result(result):
    """格式化输出完整流水线结果。"""
    if not result.get("success"):
        print(f"失败 [{result.get('stage', '?')}]: {result.get('error')}")
        return

    print(f"\n完整流水线完成: {result.get('factor_name')}")

    val = result.get("validation", {})
    print(f"  验证: {'通过' if val.get('success') else '失败'} | shape={val.get('shape')} | {val.get('duration_sec', 0):.1f}s")

    save = result.get("save", {})
    cls = save.get("classification") or {}
    print(f"  分类: {cls.get('category', '?')} | 评级: {cls.get('grade', '?')}")

    metrics = result.get("metrics", {})
    if metrics.get("success"):
        fm = metrics.get("factors", {}).get(result["factor_name"], {}).get("full", {})
        print(f"  IC={_fmt(fm.get('ic_mean'))} ICIR={_fmt(fm.get('icir'))} "
              f"Sharpe={_fmt(fm.get('top_sharpe'))} AnnRet={_fmt(fm.get('top_annual_return'), pct=True)}")


def _fmt(v, pct=False):
    if v is None:
        return "-"
    if pct:
        return f"{v*100:.1f}%"
    return f"{v:.4f}"


def main():
    parser = argparse.ArgumentParser(description="手工因子入库工具")
    parser.add_argument("--direct-db", action="store_true", help="直接操作 DB（不通过 API）")
    sub = parser.add_subparsers(dest="command", required=True)

    # save
    p_save = sub.add_parser("save", help="入库因子")
    p_save.add_argument("--name", required=True, help="因子名（m_ 开头）")
    p_save.add_argument("--code-file", required=True, help="因子代码文件路径")
    p_save.add_argument("--description", default=None)
    p_save.add_argument("--expression", default=None)

    # compute-metrics
    p_cm = sub.add_parser("compute-metrics", help="批量计算独立指标")
    p_cm.add_argument("factor_names", nargs="*", help="因子名列表")
    p_cm.add_argument("--all", action="store_true", help="全部可用因子")

    # full-pipeline
    p_fp = sub.add_parser("full-pipeline", help="完整流水线")
    p_fp.add_argument("--name", required=True)
    p_fp.add_argument("--code-file", required=True)
    p_fp.add_argument("--description", default=None)
    p_fp.add_argument("--expression", default=None)

    # validate
    p_val = sub.add_parser("validate", help="验证因子代码")
    p_val.add_argument("--name", required=True)
    p_val.add_argument("--code-file", required=True)

    # template
    p_tpl = sub.add_parser("template", help="获取因子代码模板")
    p_tpl.add_argument("--name", default="m_example")

    args = parser.parse_args()

    cmds = {
        "save": cmd_save,
        "compute-metrics": cmd_compute_metrics,
        "full-pipeline": cmd_full_pipeline,
        "validate": cmd_validate,
        "template": cmd_template,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
