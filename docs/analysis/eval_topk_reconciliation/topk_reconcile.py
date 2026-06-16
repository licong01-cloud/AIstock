#!/usr/bin/env python3
"""独立对账脚本 — 校验 eval 口径 refactor 的 Tier-1 Top-K 指标。

不复用后端代码（backend/services/quantevolver/templates/read_exp_res.py），
从一个 run 的 pred.pkl + label.pkl 独立重算 Top-K 指标，与后端写入
qe_archive.run_metric / enhanced_metrics.prediction_diagnostics 的值对比。
若一致 → 后端实现正确；若不一致 → 标出差异、查 bug。

镜像的后端算法（read_exp_res.py:1201-1290，2026-06-16 复核）：
  1. pred: 每 trade_date 按 score 降序，rank = groupby(date).score.rank(ascending=False, method='first')，rank=1 最优
  2. 与 label 按 (trade_date, instrument) 合并，dropna(score, rank, label)
  3. 逐日：top = rank<=k；topk_return@k = mean(top.label)；hit_rate@k = mean(top.label>0)；dispersion@k = std(top.label)
     within_portfolio_rankic(后端) = mean_over_days( spearman(day.rank, day.label) )
  4. 跨日平均；topk_decay = topk_return@20 - topk_return@50
  5. 数据不足 → null（不 0 冒充）

用法:
  python topk_reconcile.py --pred /path/pred.pkl --label /path/label.pkl [--k 20 50]
  # pred/label = qlib SignalRecord 产物（MultiIndex (datetime, instrument)）
"""
from __future__ import annotations
import argparse
import json
import sys

import numpy as np
import pandas as pd


def _to_trade_frame(obj, value_name: str, candidates: tuple[str, ...]) -> pd.DataFrame:
    """把 qlib pkl(Series 或 DataFrame, MultiIndex (datetime, instrument)) 归一为
    [trade_date, instrument, <value_name>] 长表。镜像 _artifact_to_trade_frame。"""
    if isinstance(obj, pd.Series):
        df = obj.to_frame(name=obj.name or value_name)
    else:
        df = obj.copy()
    # 选值列
    if value_name not in df.columns:
        col = next((c for c in candidates if c in df.columns), None)
        if col is None:
            # 取第一列（label.pkl 常为 LABEL0 单列）
            col = df.columns[0]
        df = df.rename(columns={col: value_name})
    df = df.reset_index()
    # 识别 datetime / instrument 两个索引列
    cols = list(df.columns)
    dt_col = next((c for c in cols if str(c).lower() in ("datetime", "date", "trade_date", "level_0")), cols[0])
    inst_col = next((c for c in cols if str(c).lower() in ("instrument", "code", "symbol", "level_1")), cols[1])
    out = df[[dt_col, inst_col, value_name]].rename(columns={dt_col: "trade_date", inst_col: "instrument"})
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.normalize()
    out[value_name] = pd.to_numeric(out[value_name], errors="coerce")
    return out


def reconcile(pred_path: str, label_path: str, ks=(20, 50)) -> dict:
    pred_obj = pd.read_pickle(pred_path)
    label_obj = pd.read_pickle(label_path)
    pred = _to_trade_frame(pred_obj, "score", ("score", "pred", "prediction"))
    label = _to_trade_frame(label_obj, "label", ("label", "LABEL0", "label0"))

    pred["rank"] = (
        pred.sort_values(["trade_date", "score"], ascending=[True, False])
        .groupby("trade_date")["score"].rank(ascending=False, method="first")
    )
    joined = pred.merge(label, on=["trade_date", "instrument"], how="inner")
    joined = joined.replace([np.inf, -np.inf], np.nan).dropna(subset=["score", "rank", "label"])
    if joined.empty:
        return {"status": "insufficient_data", "note": "no non-null joined rows"}

    per_day = {k: {"ret": [], "hit": [], "disp": []} for k in ks}
    rankics: list[float] = []
    for _, day in joined.groupby("trade_date"):
        if day["rank"].notna().sum() >= 2 and day["label"].notna().sum() >= 2:
            c = day["rank"].corr(day["label"], method="spearman")
            if pd.notna(c):
                rankics.append(float(c))
        for k in ks:
            top = day[day["rank"] <= k]
            if len(top):
                per_day[k]["ret"].append(float(top["label"].mean()))
                per_day[k]["hit"].append(float((top["label"] > 0).mean()))
                if len(top) >= 2:
                    per_day[k]["disp"].append(float(top["label"].std(ddof=1)))

    def avg(x):
        return round(float(np.mean(x)), 6) if x else None

    res = {"status": "ok", "trade_days": int(joined["trade_date"].nunique()),
           "joined_rows": int(len(joined)), "rankic_days": len(rankics)}
    for k in ks:
        res[f"topk_return_{k}"] = avg(per_day[k]["ret"])
        res[f"topk_hit_rate_{k}"] = avg(per_day[k]["hit"])
        res[f"topk_dispersion_{k}"] = avg(per_day[k]["disp"])
    if res.get("topk_return_20") is not None and res.get("topk_return_50") is not None:
        res["topk_decay"] = round(res["topk_return_20"] - res["topk_return_50"], 6)
    # 后端口径: corr(rank, label), rank=1最优 → 好模型为负
    res["within_portfolio_rankic_backend_sign"] = avg(rankics)
    # 惯例口径(正=好): 取负, 便于直觉解读 + 与 rank_ic 同号比对
    res["within_portfolio_rankic_conventional"] = round(-avg(rankics), 6) if rankics else None
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="pred.pkl 路径(qlib SignalRecord)")
    ap.add_argument("--label", required=True, help="label.pkl 路径")
    ap.add_argument("--k", nargs="+", type=int, default=[20, 50])
    ap.add_argument("--backend-json", default=None,
                    help="可选: 后端 prediction_diagnostics JSON 路径, 给出则自动 diff")
    args = ap.parse_args()
    res = reconcile(args.pred, args.label, tuple(args.k))
    print("=== INDEPENDENT RECONCILE ===")
    print(json.dumps(res, ensure_ascii=False, indent=2))
    if args.backend_json:
        be = json.load(open(args.backend_json, encoding="utf-8"))
        print("=== DIFF vs backend (|delta|>1e-4 标记 ⚠️) ===")
        for key in ("topk_return_20", "topk_return_50", "topk_hit_rate_20",
                    "topk_decay", "topk_dispersion_20"):
            a, b = res.get(key), be.get(key)
            if a is None or b is None:
                print(f"  {key}: indep={a} backend={b} (含 null, 人工判断)")
            else:
                d = abs(a - b)
                print(f"  {key}: indep={a} backend={b} delta={d:.2e} {'⚠️' if d > 1e-4 else 'OK'}")
        # within_portfolio_rankic: 后端用 corr(rank,label) 反号
        a = res.get("within_portfolio_rankic_backend_sign")
        b = be.get("within_portfolio_rankic")
        print(f"  within_portfolio_rankic(后端反号口径): indep={a} backend={b}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
