# HMM Risk Gate 离线验证报告（2026-05-17）

## 设计演进

### 初始设计（失败）
- 方案：fading 状态 + confidence > threshold → 阻止新买入
- 问题：3-state HMM 的后验概率极度集中（95%+ 的 fading 实例 confidence=1.0），导致 41% 的行业-日被阻止
- 结论：confidence-based 过滤对 3-state 模型无效

### 最终设计（有效）
- 方案：**Transition-based gate** — 只在行业状态从 trending/neutral 转入 fading 时触发阻断，持续 N 个交易日后自动过期
- 优势：每天平均只阻止 ~5 只 Top50 候选股票（vs 之前的 53 只），真正实现"低频高精度"

## 验证结果

### 5 天 trigger duration（推荐）

```
Total days: 442
Days with blocks: 385 (87.1%)
Avg blocked per day: 5.37 (from Top50 candidates)
Total blocked instances: 2373
Blocked sample count: 2372
Allowed sample count: 22092

Forward Return Comparison (blocked vs allowed):
   1D: blocked=0.117%  allowed=0.340%  spread=+0.223%  [PASS]
   3D: blocked=0.662%  allowed=0.999%  spread=+0.337%  [PASS]
   5D: blocked=1.183%  allowed=1.661%  spread=+0.478%  [PASS]
  10D: blocked=3.062%  allowed=3.118%  spread=+0.056%  [PASS]
  20D: blocked=6.529%  allowed=6.326%  spread=-0.203%  [FAIL]

10D win rate: blocked=56.9%  allowed=57.8%
```

### 3 天 trigger duration（对照）

```
   1D: spread=+0.198%
   3D: spread=+0.281%
   5D: spread=+0.419%
  10D: spread=-0.340%
  20D: spread=-1.079%
```

### Duration 对比

| Duration | 1D | 3D | 5D | 10D | 20D |
|----------|-----|-----|-----|------|------|
| 3 天 | +0.198% | +0.281% | +0.419% | -0.340% | -1.079% |
| 5 天 | +0.223% | +0.337% | +0.478% | +0.056% | -0.203% |

## 关键发现

1. **Transition-based gate 在 5D 周期有效**：被阻止股票的 5D 前向收益比允许股票低 0.478%
2. **5 天 duration 与 QE 5D rebalance 完美匹配**：gate 在下一次调仓前有效，调仓后自动解除
3. **20D 反转说明 fading 是短期信号**：不应做长期阻断
4. **稀疏度合理**：每天只阻止 ~5 只候选（Top50 的 10%），不会过度干预

## 模型特征分析

- old covfix 3-state HMM 的状态分布：neutral 54.5%, fading 41.2%, trending 4.2%
- 后验概率极度集中：95%+ 的实例 confidence=1.0（margin > 0.30）
- 每天约 5 次状态转换进入 fading（2236 triggers / 442 days）

## 下一步

1. **Phase 1**: 将 risk gate 集成到 Selection Center（HMMRiskGateDecisionProvider）
2. **Phase 2**: 集成到 QE 策略模板，运行 4-arm shadow loop 对比
3. **考虑**: 是否需要额外条件（如只阻止 Top50 边界 rank 31~60 的股票，保护 Top30 强 alpha）

## 产物

- 预计算脚本: `scripts/precompute_hmm_risk_gate.py`
- 验证脚本: `scripts/validate_hmm_risk_gate.py`
- 5D artifact: `.codex_tmp/hmm_risk_gate_validation/hmm_risk_gate_duration_5d.json`
- 验证结果: `.codex_tmp/hmm_risk_gate_validation/results_5d/validation_summary.json`
