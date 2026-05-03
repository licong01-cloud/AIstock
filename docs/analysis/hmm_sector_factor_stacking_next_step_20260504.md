# HMM + sector-factor 叠加下一步分析（2026-05-04）

## 结论

可以继续做 HMM + sector-factor 叠加，但不建议把 sector-factor 直接替代当前最佳 HMM。基于 `qe_20260502_231229_0565` 的 4 个 loop，当前最强仍是 old covfix 主线；sector-factor-only 与当时的 hybrid 都没有超过 old covfix。因此下一步更合理的路径是：

1. 先用已注册的 5 个 old-covfix remap 候选找出“当前最佳 HMM 系数版本”。
2. 再把 sector-factor 作为二阶段 confirmation/gating 叠加在最佳 HMM 之上。
3. 不把 sector-factor 做成独立替代，不直接扩大全市场换手。

## 回测依据

`qe_20260502_231229_0565` 的 4 个 HMM loop：

```text
Loop  HMM 版本/含义                                           年化收益  最大回撤   Sharpe
----  -------------------------------------------------------  --------  --------  -------
L1    old covfix 当前最佳基线                                  51.60%    -14.99%   2.4568
L2    old_covfix_primary_b020_p005 弱化映射                    49.19%    -15.44%   2.3488
L3    sf_turnover_fast_q20_b010_p005 sector-factor-only        48.51%    -15.49%   2.2912
L4    hyb_old_primary_turnover_flow_core_c70 hybrid            49.00%    -15.37%   2.3319
```

参考 no-HMM 近似强基线：`qe_20260501_011054_c90a` Loop26 年化约 48.88%，Sharpe 约 2.2815。

由此可见：

- old covfix 明显强于 L2/L3/L4，是当前应保留的主线。
- sector-factor-only 有正收益但没有超过 old covfix，也不能证明能替代 HMM。
- hybrid c70 比 sector-factor-only 好，但仍弱于 old covfix；它更像是“有潜力的确认信号”，不是已经验证过的增强版本。

## 为什么仍值得叠加

`qe_20260502_231229_0565` 没有完全否定 sector-factor，原因是当时的 hybrid 并不是“当前最佳 old covfix + sector-factor gate”，而是基于弱化 mapping 的离线 candidate。它主要验证了 sector-factor 是否能形成可用的行业确认信号；没有验证在 old covfix 已经有效时，sector-factor 是否能减少错误 boost/penalty。

可行假设是：

- HMM 负责 regime/state 方向，sector-factor 只做确认。
- sector-factor 不独立制造大范围 boost，只阻止低置信度 boost 或软化不确认的 penalty。
- 如果 sector-factor 的价值存在，更可能体现在减少错误调仓、降低回撤或降低无效换手，而不一定直接提高 IC。

## 候选叠加方向

```text
方向                                      机制                                                        风险
----------------------------------------  ----------------------------------------------------------  --------------------------------------------
boost confirm                             HMM trending 时，仅 sector-factor 同向才保留 boost，否则回到 1.0  可能错过 HMM 单独有效的趋势行业
penalty confirm                           HMM fading 时，仅 sector-factor 同向才保留 penalty，否则软化到 0.98/1.0  可能削弱 old covfix 的风险保护
both-side confirm                         HMM 非中性系数都需要 sector-factor 同向确认                     容易过度中性化，收益可能下降
risk-only sector overlay                  sector-factor 只额外惩罚弱行业，不提供新增 boost                  防守性强，但可能牺牲收益弹性
confidence scaling                        按 sector-factor 置信度把最佳 HMM 系数插值到 1.0                  参数更多，需要严格网格和 holdout
enter-bucket only                         只对 TopK 边缘/新进股票应用 sector-factor gate                    更贴近组合换手，但实现和解释更复杂
```

优先建议从 3 个最小候选开始：

1. `best_hmm_sector_boost_confirm`: 只确认趋势增强，不碰风险惩罚。
2. `best_hmm_sector_penalty_confirm`: 只确认/软化风险惩罚，不新增趋势增强。
3. `best_hmm_sector_both_confirm`: HMM 非中性系数都需要 sector-factor 同向确认。

## 与已注册版本的关系

现在可直接在 QE 中选择验证的版本：

- old covfix 基线 1 个。
- 2026-05-02 的 3 个候选：弱化 old mapping、sector-factor-only、hybrid c70。
- 2026-05-04 的 5 个 old-covfix remap 候选。

还不能直接在 QE 中选择的版本：

- “最佳 remap + sector-factor gate”的新组合版本尚未生成 coefficient artifact，也尚未注册 DB snapshot。
- 这些组合应等 5 个 remap 候选完成 QE 对比后，再基于真实胜出的 HMM 版本生成，避免在未确定最佳 HMM 前扩大候选矩阵。

## 建议实验矩阵

第一阶段，先跑 HMM remap：

```text
no-HMM
old covfix baseline
penalty_only_f096_b000
boost_only_p105
penalty094_boost103
penalty095_boost104
penalty095_boost106
2026-05-02 old_covfix_primary_b020_p005
2026-05-02 sector-factor-only
2026-05-02 hybrid c70
```

第二阶段，只对第一阶段胜出的 HMM 做 sector-factor 叠加：

```text
best_HMM
best_HMM + boost_confirm
best_HMM + penalty_confirm
best_HMM + both_confirm
best_HMM + risk_only_sector_overlay
```

## 验证要求

- 数据窗口保持 `test_start=2024-07-01`、`backtest_end=2026-04-27`、`test_end=2026-04-28`，避免窗口漂移。
- 所有 HMM/sector-factor coefficient 都必须是预计算 artifact，`strict_no_leakage=true`，不允许运行时 fallback。
- 每个候选必须记录 changed days、进入/退出股票数、换手、交易成本、最大回撤、Sharpe、最终 NAV，而不只看年化收益。
- 如果叠加版本年化收益不高于 best_HMM，但显著降低回撤或换手，可以作为防守型候选保留；否则应继续保留 best_HMM，不做 sector-factor 叠加上线。

## 当前判断

- 可以考虑重新做 HMM 优化，且 5 个 old-covfix remap 已经注册为可直接 QE 验证候选。
- 可以考虑 sector-factor 叠加，但应该作为第二阶段、基于最佳 HMM 的 gating 实验。
- 当前不能宣称 sector-factor 叠加已经有效；已有回测只能说明直接替代或弱 hybrid 未超过 old covfix。
