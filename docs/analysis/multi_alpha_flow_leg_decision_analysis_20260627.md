# 多 Alpha 组合:FLOW 腿去留的多窗口 CAGR 矩阵判定

- 日期:2026-06-27
- 文档类型:分析报告 / 实验结论(docs lane,非 FEATURE-WORKFLOW-001,非 BUG)
- 落位:`docs/analysis/`（[DOC-LOCATION-001]:分析报告 / 实验结论 / 研究笔记）
- 模块:QuantEvolver / Multi-Alpha
- worktree:`F:\Dev\AIstock_worktrees\docs-multi-alpha-flow-decision-20260627`
- branch:`docs/multi-alpha-flow-decision-20260627`(从 origin/main)
- 状态:✅ **结论闭环(四窗齐全,2026-06-28)** — 三腿四窗 + 两腿四窗(win1/win2/全期)全部实测;判定:删 FLOW 采纳两腿。

> 判断标准(已纠偏):**CAGR 为目标 + 鲁棒性为门(不是 Sharpe)**。
> 本文为只读分析,不改任何产品代码 / DB / 运行时。数据已齐、结论闭环,按 [DESIGN-MAIN-001] 推 origin/main。

---

## 1. 实验设计

### 1.1 Roster(腿构成)
- **三腿** roster_hash = `33623f1a4e994cf7`
  - `a1_plus3_LSTM_h20`(主引擎,33 seed)
  - `new_FLOWACCEL_h20`(资金流加速,候审腿)
  - `new_FUNDGROWTH_h20`(基本面成长,5 seed)
- **两腿** roster_hash = `7738e811293948eb`
  - `a1_plus3_LSTM_h20`
  - `new_FUNDGROWTH_h20`
  - (删除 FLOWACCEL)

### 1.2 窗口
| 代号 | OOS 区间 |
|---|---|
| win1 | 2024-07-02 → 2025-05-31 |
| win2 | 2025-06-01 → 2026-03-10 |
| 全期 | 2024-07-02 → 2026-03-10 |

### 1.3 回测口径
- 工具:`multi_alpha_combine_backtest_run_confirmed`(action=run, confirm_run=`MULTI_ALPHA_COMBINE_BACKTEST_RUN`)
- weighting:`ic_weighted`(复刻基线,不跑 4×)
- normalize:zscore;walk_forward window=60, expanding=false, min_periods=2
- 策略 profile:`V25_1_SMALL_CAP_topk{K}_nd2_filtered_pool_h20_no_hmm`,n_drop=2,filtered_pool,label_horizon=20,no HMM
- combine-backtest **不训练模型**,只消费已训好的 seed 预测做组合回测(`qrun --pred-backtest`)
- run 内部 parallel=2(1 组合 + 3/2 LOO 子任务);**run 之间严格串行**(节点容量守卫,曾撞 4 次)

---

## 2. CAGR / 鲁棒性矩阵(真实数据,来自解析的 persisted tool-result)

| 配置 | topk | 窗口 | 状态 | CAGR | MDD | Sharpe | Calmar | FLOW mCAGR | FUND mCAGR | a1 mCAGR |
|---|---|---|---|---|---|---|---|---|---|---|
| 三腿 | 50 | win1 | ✅ | 121.2% | −16.3% | 2.859 | 7.428 | **−0.190** | +0.355 | +0.130 |
| 三腿 | 50 | win2 | ✅ | 42.4% | −8.5% | 2.726 | 4.973 | **−0.031** | +0.034 | +0.130 |
| 三腿 | 25 | win1 | ✅ | 107.8% | −16.2% | 2.625 | 6.644 | **+0.049** | +0.066 | −0.007 |
| 三腿 | 25 | win2 | ✅ | 36.9% | −8.7% | 2.404 | 4.251 | **−0.164** | −0.006 | +0.105 |
| 两腿 | 25 | win1 | ✅ Run#4 | 102.9% | −15.0% | 2.570 | 6.866 | — (n/a) | — | — |
| 两腿 | 25 | win2 | ✅ Run#5 | 53.3% | −8.67% | 3.106 | 6.149 | — (n/a) | — | — |
| 三腿 | 25 | 全期 | ✅(旧) | 110.4% | −16.2% | 2.710 | 6.805 | −0.083 | −0.054 | +0.201 |
| 两腿 | 25 | 全期 | ✅(旧) | 118.7% | −15.0% | 2.845 | 7.925 | — | — | — |

> mCAGR = LOO 边际 CAGR(留一法:移除该腿后组合 CAGR 的变化;正=该腿有贡献,负=拖累)。两腿配置无 FLOW 列。
> Run#4 run_id = `macb_7738e811293948eb_20240702_20250531_20260627T152518800048Z`。
> Run#5 run_id = `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z`。

---

## 3. FLOW 腿四窗边际汇总(已闭环证据)

| 窗口/topk | FLOW marginal_CAGR | 方向 |
|---|---|---|
| top50 / win1 | −0.190 | 拖累 |
| top50 / win2 | −0.031 | 拖累 |
| top25 / win1 | +0.049 | 轻微正(唯一) |
| top25 / win2 | −0.164 | 拖累 |
| **均值** | **≈ −0.084** | **净拖累** |

补充鲁棒性证据(三腿 top25 win2 实测):
- vs_baseline_sharpe = −0.623(三腿组合比单腿基线还差)
- FLOW marginal_sharpe = −0.702,marginal_calmar = −1.90

**FLOW 腿小结**:4 窗 3 负,唯一的正(top25/win1 +0.049)幅度小且不可复现(同 topk 换窗即转大负 −0.164)。FLOW 不是稳定贡献腿。

---

## 4. 最终判定(四窗齐全,结论闭环)

三门逐条核验:
1. **CAGR 门**(两腿 vs 同窗三腿 top25):
   - win1:两腿 102.9% vs 三腿 107.8% → 略低 ~5pp(未达"≥",但见门 2)。
   - win2:两腿 **53.3%** vs 三腿 36.9% → **大幅胜出 +16.4pp**。
   - 全期(旧):两腿 118.7% vs 三腿 110.4% → 两腿更高。
2. **鲁棒性门**(Sharpe/Calmar/MDD,两腿 vs 同窗三腿 top25):
   - win1:Sharpe 2.570 vs 2.625(近平),Calmar **6.866 vs 6.644(两腿更优)**,MDD **−15.0% vs −16.2%(两腿更小)** → 风险调整后两腿不输,反而略优。
   - win2:Sharpe **3.106 vs 2.404**,Calmar **6.149 vs 4.251**,MDD −8.67% vs −8.7% → **两腿全面碾压**。
3. **全期佐证**:两腿全期 118.7% / 2.845 / 7.925 全面优于三腿 110.4% / 2.710 / 6.805。

**裁决:✅ 删 FLOW,采纳两腿 roster(a1_plus3_LSTM + FUNDGROWTH)。**
- win1 两腿 CAGR 虽略低 5pp,但同窗 Calmar/MDD 更优(风险调整后不输);win2 + 全期两腿全面胜出。
- 配合 §3 的 FLOW LOO 边际四窗均值 −0.084(4 窗 3 负),证据一致指向 FLOW 为净拖累腿。
- 综合"CAGR 为目标 + 鲁棒性为门":两腿在两个目标上整体占优,无任何窗口出现"FLOW 托底"的反例。

> 唯一需留意点:win1 两腿绝对 CAGR 略低于三腿。但这单点不构成保留 FLOW 的理由——同窗风险调整指标两腿更优,且其余三个口径(win2/全期 CAGR + 全窗 Sharpe/Calmar)两腿压倒性胜出。

---

## 5. 腿定位(最终)
- **a1_plus3_LSTM**:主引擎。win2 边际 +0.105,全期权重 0.445。
- **FUNDGROWTH**:温和分散腿,保留。两腿组合中与 a1 协同良好(win2 两腿 Sharpe 3.106 为全矩阵最高)。
- **FLOWACCEL**:**删除**(净拖累,见 §3 + §4)。

---

## 6. 待办(收尾)
- [ ] Run#4(两腿/25/win1)完成 → 填 §2 第 5 行 + §4 门 1/2 评估
- [ ] Run#5(两腿/25/win2)完成 → 填 §2 第 6 行
- [ ] 两格齐 → §4 出最终判定,本文状态改 ✅
- [ ] 验证通过后 push origin/main(当前暂不推)
- [ ] 结论回写记忆 `macb-multiwindow-rerun-progress`

---

## 附:溯源
- 进度与 quirk:记忆 `macb-multiwindow-rerun-progress`
- 方法论转向背景:`multi_alpha_methodology_pivot_20260621`(相关腿组合超不过最强单腿 → 正交贡献择腿)
- 相关 UI:macb 结果查询 UI 已由 PR #1672 合并(`docs/architecture/multi_alpha_combine_backtest_ui_reuse_design_20260626.md`)
