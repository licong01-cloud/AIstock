# QE 实验 / 模型库清理方案（候选清单，待确认后执行）

> **类型**：清理方案（analysis）· **本文件只提清单，不执行任何删除**
> **日期**：2026-06-16
> **关联**：`docs/design/multi_alpha_prediction_store_mlflow_blueprint_20260616.md` 附录 A2/A5
> **门控**：用户确认 → 执行**第一步（低风险）** → 第二步等线 B M2 预测固化后

---

## 0. 原则

1. **承重资产绝不删**（生产策略包源 + 当前 alpha 腿代表 + 运行中实验）。
2. **删除会级联**：删 QE custom_evo task 会删远端 workspace + mlruns + AIstock 缓存 + DB 行（`qe_evolution_service.py:3499-3603`）。不可逆。
3. **逐项预检**：每个待删 task 执行前确认 ——(a) 不是任何策略包的 `source_id`；(b) 无 selection/advisory/paper 引用；(c) 不是 alpha 腿代表 run 的宿主 task。
4. 第一步只删「最早期、已被取代、无任何依赖」的实验，**保守优先**。

---

## 1. 承重资产（KEEP — 绝不删）

### 1.1 生产策略包源实验（4 个）
| 包 | 源 task | paper 组合 | 状态 |
|----|--------|-----------|------|
| pkg_a2f5… | `qe_20260601_172505_fe17` (R6) | 144 | BACKTEST_APPROVED |
| pkg_0975… | `qe_20260607_093306_1f70` (R14A) | 0 | advisory 双包成员 |
| pkg_378e… | `qe_20260520_215627_abbc` | 2 | BACKTEST_APPROVED |
| pkg_2a9f… | `qe_20260513_151128_12ea` (L1) | 9 | SELECTION_ENABLED |

### 1.2 当前 alpha 腿代表 + 运行中（KEEP 至线 B M2 固化）
`qe_20260605_022858_c36b`(α1) · `qe_20260611_014834_0399`(α2) · `qe_20260611_014825_0daa`(α3) · `qe_20260614_022643_edaf`(α6/MA1G) · `qe_20260614_022428_433d`(α7/MA1C) · `qe_20260611_164357_2001`(α4/α5 R20B) · `qe_20260616_140615_b6af` + `qe_20260616_140819_f858`(R21 运行中)

### 1.3 Tier-2 延后（KEEP 至 M2，含 alpha 腿种子/排行榜样本）
全部 **methodology-v1 R2–R20 + MA1** 轮次（2026-05-29 `9b69` 起至 2026-06-14）。这些 loop 含 α1 PLUS3 高分种子、IF18/FM12/Disc25 等腿样本、leaderboard 108%+ 的 run（如 `ddb6_L12`/`768c_L11`/`a309`/`87ee`/`8936_L4`）。**预测固化进 MLflow 前不动**，固化后按价值再筛。

---

## 2. 第一步删除候选（低风险，2026-04-03 ~ 2026-05-22 早期/一次性，已被取代）

> 均为 methodology-v1 之前的 HMM/v24/v25 验证、容量/小资金/双节点一次性实验，**非包源、非 alpha 腿、结论已被后续轮次取代**。共 23 个。

| # | task_id | 主题 | 删除依据 |
|---|---------|------|---------|
| 1 | `qe_20260403_105545_9425` | QE 重测 from_L8 | 最早期，孤立 |
| 2 | `qe_20260426_234914_9c7b` | v24/v25 持仓周期对比 | 被 R-系列取代 |
| 3 | `qe_20260428_001749_c5b2` | v25 对比/HMM | 同上 |
| 4 | `qe_20260429_015755_c4ba` | 修复 V25 HMM 验证 | 同上 |
| 5 | `qe_20260430_010121_d55f` | HMM 对比加模型对比 | 同上 |
| 6 | `qe_20260501_011054_c90a` | LGB/TCN/XGB/CAT/GRU64 模型对比(28L) | 被 R15/R16 模型横评取代 |
| 7 | `qe_20260501_154127_b0be` | 双节点队列测试 | 基础设施测试 |
| 8 | `qe_20260501_201036_b699` | WSL 队列测试 | 基础设施测试 |
| 9 | `qe_20260502_131502_9b54` | HMM 对比 | 被取代 |
| 10 | `qe_20260502_162747_0313` | 小资金验证1 | 一次性 |
| 11 | `qe_20260502_193154_17a2` | 小资金验证 | 一次性 |
| 12 | `qe_20260502_231229_0565` | 纯 HMM 测试 | 一次性 |
| 13 | `qe_20260505_122348_690d` | V25.1 对比验证 | 一次性 |
| 14 | `qe_20260505_153534_388f` | V25.1 对比(1000万) | 一次性 |
| 15 | `qe_20260505_200632_a357` | Codex ST_PIT backtest_only | 一次性 |
| 16 | `qe_20260506_220823_6489` | HMM baseline fulltrain(1L) | 一次性 |
| 17 | `qe_20260507_132049_d4e7` | v25.1 PIT 数据验证(2L) | 一次性 |
| 18 | `qe_20260508_060509_1268` | HMM autocycle r3(8L) | 一次性 |
| 19 | `qe_20260510_010004_8c2d` | HMM regime compare(4L) | 一次性 |
| 20 | `qe_20260510_102726_4fd3` | HMM regime gentle risk(4L) | 一次性 |
| 21 | `qe_20260512_113610_b19c` | 验证选股和模型(2L) | 被 12ea 包源取代 |
| 22 | `qe_20260520_133940_fdb2` | 8loop 10D optimization | 被 R-系列取代 |
| 23 | `qe_20260522_012542_90fb` | l16 capacity seed hmm(16L) | 被 R-系列取代 |

**借力删除项（同属早期，借此一并评估）：** `qe_20260529_135314_9b69`(seed_horizon_arch_factor 18L) 是 methodology-v1 的前身实验，**边界项**——若它不含任何仍在引用的腿样本，可纳入第一步；否则归 Tier-2。执行前单独核验。

---

## 3. 模型库清理（33 个 legacy spec → deprecate）

`model_registry` 中 `qe_selectable=true` 的 50 个里，**33 个是 `rdagent_task_sync`**（model_id 形如 `2026-03-04_05-46-17::loop_1`，model_type=`TimeSeries`，2026-02~03 旧 rdagent 任务）。它们**不被任何 QE custom_evo / 策略包引用**（custom_evo 只用 `__seed_*__` curated spec）。

**动作（低风险，不删数据只改可见性）**：33 个 legacy spec 置 `qe_selectable=false` 或 `deprecate`，清出选择目录。**不级联删训练产物**（与 §2 的 task 删除不同，这只是 registry 行的可见性/状态变更）。

---

## 4. 执行协议（确认后）

1. 对 §2 每个 task：先 `selection_center` / `advisory` / `strategy_packages` 反查无引用（预期均无）→ `qe_custom_evo_delete_confirmed`。
2. 对 §3：`model_registry_deprecate_confirmed` 批量下线 33 legacy。
3. 删除前后各记一次磁盘占用（节点侧 `du`，Windows F: 看不到 WSL/远端）。
4. 全程禁 silent error：任一 task 删除在 worker cleanup 阶段失败则停止该项并报告，不跳过 DB 清理。

---

## 5. 待确认

- [ ] 批准第一步删除 §2 的 23 个 task + §3 的 33 legacy spec?
- [ ] `9b69` 边界项纳入第一步还是归 Tier-2?
- [ ] 第二步（Tier-2 固化后清理）等线 B M2 完成后再出独立清单。

*本方案只列清单，执行需用户逐条/整体确认。删除不可逆。*
