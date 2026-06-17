# Eval Top-K 对账操作手册

> 用途：eval 口径 refactor(impl_eval_topk_rubric_20260616.md)合入并跑出第一个带 Top-K 的新 run 后，**独立校验**后端算的 `topk_return@20/50 / hit_rate / decay / dispersion / within_portfolio_rankic` 是否正确。
> 脚本 `topk_reconcile.py` **不复用后端代码**，独立重算后对比 → 真正的交叉验证。
> 这是 strategy session 验收 §8 的"抽样对账"工具，事先备好，合入后即用。

## 前提
eval-rubric PR 合入 + 后端部署(用户重启)+ 至少一个**新 run**(部署后跑的)产出 Top-K（存量 run 无 Top-K，不可对账）。

## 步骤

### 1. 取该 run 的 pred.pkl + label.pkl
- **P2 之前(interim)**：Top-K 在回测 read_exp_res 步算，pred/label 仍在 workspace 的 `mlruns/<exp_id>/<run_id>/artifacts/{pred.pkl,label.pkl}`，**workspace 清理前**取出（或临时跑一个 qrun 保留 artifact）。
- **P2 之后**：用 `prediction_store_pull_pred(run_id)` / model_store 从中心 artifact 拉。

### 2. 取后端已算的 Top-K 值
- `qe_archive_query_topk_quality(run_id|task_id, k)`（新 MCP 工具），或
- `qe_experiment_get_enhanced_metrics(experiment_id)` → 取 `prediction_diagnostics.topk_*` → 存成 JSON（键名 topk_return_20 等），用于 `--backend-json` 自动 diff。

### 3. 跑对账
```
python topk_reconcile.py --pred <pred.pkl> --label <label.pkl> --k 20 50 \
       --backend-json <backend_prediction_diagnostics.json>
```
输出独立重算值 + 与后端的逐项 diff。

## 验收口径
- `topk_return_20/50`、`topk_hit_rate_20`、`topk_decay`、`topk_dispersion_20`：**|delta| < 1e-4 视为一致**（后端 round 到 6 位）。
- 任一项 |delta| 超阈 → 标 ⚠️，查后端实现(read_exp_res 的 rank/merge/dropna/逐日聚合)。
- 含 null 的项人工判断（数据不足时两边都应 null，不应一边 0 一边 null）。

## ⚠️ 注意
1. **within_portfolio_rankic 符号**：PR #1184 后端**已采纳 positive=good**（`read_exp_res.py:1246-1250` 取 `-Spearman(rank,label)`，rank=1 最优）。脚本 `within_portfolio_rankic_conventional` 与后端同号，`--backend-json` 直接 diff（|delta|<1e-4 视为一致）。旧的 `within_portfolio_rankic_backend_sign`（未取负）仅供历史 run 比对。T6 评审 nit 已闭环。
2. **label 列名**：qlib label.pkl 常为 `LABEL0`，脚本自动识别（label/LABEL0/首列）。
3. **日对齐**：脚本按 `trade_date`(normalize 到日)合并;若后端用不同 horizon 对齐的 label,需确认 label.pkl 是同一份(read_exp_res 用的就是 recorder 存的 label.pkl,一致)。
4. **topk_return 定义**：逐日 mean(rank≤k 的 label) → 跨日平均(非池化)；脚本与后端均如此。

## 失败时
若 diff 超阈，输出独立值 + 后端值 + delta 给 strategy session 判定；常见原因：rank method 不一致、merge 丢行、dropna 顺序、逐日 vs 池化聚合差异。
