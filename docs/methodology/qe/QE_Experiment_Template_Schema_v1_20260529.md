# QE 实验模板契约 v1（Experiment Template Schema）

- **版本**：v1.0 ｜ **创建**：2026-05-29
- **用途**：把 `QE_Evolution_Methodology_v1` 的"路线 + 动作轴"机械地转换成 QE custom task 的 `loops` 配置。任何工具按本契约生成的配置，可直接喂给 `mcp__aistock-qe-experiment__qe_custom_evo` 创建实验。
- **配套**：`QE_Evolution_Methodology_v1_20260529.md`（方法论本体）。

---

## 1. 单个 Loop 的字段契约

下列字段来自 QE custom task 的真实 loop schema（`qe_custom_evo_get_config` 返回结构），按"动作轴"标注归属：

| 字段 | 类型 | 轴 | 说明 |
|------|------|----|------|
| `label` | str | — | 人类可读标签，**必须编码动作**，如 `"A1 [LSTM] seed=42 h=20"` |
| `loop_index` | int | — | 1-based 序号 |
| `node_id` | str | — | 执行节点，如 `wsl2-5080` / `rdagent-node1` |
| `model_id` | str | A | 模型模板，如 `__seed_LSTM_10D_hs64_d02__` |
| `factor_keys` | str[] | F | 因子组合（来自因子库） |
| `disable_alpha158` | bool | G | 是否关闭 Alpha158 |
| `label_horizon` | int | E | 1/5/10/20 |
| `runtime_flags.random_seed` | int | D | 随机种子 |
| `runtime_flags.seed_policy` | str | D | `fixed` / `ensemble` |
| `runtime_flags.undertrain_mode` | str | C | 如 `epoch_1` / `epoch_3` / `epoch_5`（缺省=正常训练） |
| `runtime_flags.archive_policy` | str | — | `AUTO`（确保入数仓） |
| `strategy_id` | str | H | 组合策略，如 `score_weighted_topk_v2[_capacity_v1]` |
| `strategy_params.topk` | int | H | 选股数 |
| `strategy_params.risk_policy` | obj | K | st_pit 风控等 |
| `execution_algo` | str | J | 如 `V25_1_SMALL_CAP` |
| `enable_sector_hmm` / `hmm_signal_preset` / `hmm_model_version_id` | — | I | HMM 叠加 |
| `stock_pool` / `sector_blacklist` | — | K | 股票池/黑名单 |
| `backtest_only` | bool | — | 复用已训模型只回测 |
| `model_source_task_id` / `model_source_loop_index` | — | — | backtest_only 时的模型来源 |

**纪律（来自方法论 Part 2）**：一个实验里，除"本轮动作轴"外的所有字段必须在所有 loop 间保持一致（= 基线）。

---

## 2. 路线 → loops 生成规则

### Route A（信号/horizon/因子）
```yaml
fix: [model_id, seed_policy=fixed, random_seed=S0, strategy, topk, execution_algo]
vary: label_horizon ∈ [5,10,20]   # 或 factor_keys 子集
loops: 每个取值 1 loop
followup: 胜出取值自动追加 Route C 多 seed
```

### Route B（训练深度/正则）
```yaml
fix: [model_id, factor_keys, label_horizon, strategy, topk]
vary: undertrain_mode ∈ [epoch_1, epoch_3, epoch_5, (normal)]
seed: 每档 ≥2 seed（区分运气 vs 正则化）
loops: |grid(undertrain) × seeds|
```

### Route C（seed 集成）—— 任何挑冠军后强制
```yaml
fix: [全部轴 = 候选冠军配置]
vary: random_seed ∈ [s1..sN]   # N≥5
seed_policy: ensemble
post: 预测层平均 → 报告 mean±std/cv/worst（考核 Part 6.2）
```

### Route D（HMM 减震）
```yaml
base: 已稳定 alpha（backtest_only=true, model_source=胜出loop）
vary: hmm_signal_preset / enable_sector_hmm
constraint: AnnRet 下降 ≤10%，看 MaxDD/Calmar 改善
```

### Route E（模型多样性）
```yaml
fix: [factor_keys, label_horizon, strategy]
vary: model_id ∈ [LSTM, GRU, LGB, ...]   # 先过架构准入体检
post: 算 alpha 间预测相关性，挑 |corr|<0.6 的做融合
```

### Route F（容量/可交易性）
```yaml
fix: [model(backtest_only), factor_keys, label_horizon]
vary: topk ∈ [40,50,80] / strategy=capacity_v1 / execution_algo
metric: 换手、capacity 衰减、冲击成本
```

---

## 3. 任务级配置

```yaml
task:
  task_name: "<route>_<动作>_<YYYYMMDD>"
  target_desc: "<路线目标 + 基线锚点 + 各 Theme 说明>"   # 必含基线指标
  execution_mode: parallel_2        # 或 parallel_4 / serial
  engine_mode: unified
  node_parallelism: { wsl2-5080: 2 }
  auto_start: false                 # 强制：创建后人审，禁止自动跑
  loops: [ ...见第2节... ]
```

---

## 4. 标准实验骨架（多 Theme 对照，推荐）

仿照 `ad82` 的成功结构：把一个实验切成若干 Theme，每个 Theme 锁定一条动作轴：

```yaml
task_name: routeB_seed_x_depth_20260601
target_desc: "训练深度×seed解耦。基线: <id> CAGR=X% (<config>)。
              Theme A(L1-L4): 4 seed 正常训练; Theme B(L5-L8): 4 seed 欠训练 epoch=1/3/5;
              Theme C(L9-L10): topk 调优。"
loops:
  - { label: "A1 seed=42 normal",  loop_index: 1, runtime_flags: {random_seed: 42, seed_policy: fixed} }
  - { label: "A2 seed=2026 normal", loop_index: 2, runtime_flags: {random_seed: 2026, seed_policy: fixed} }
  # ...
  - { label: "B1 seed=42 epoch_1", loop_index: 5, runtime_flags: {random_seed: 42, undertrain_mode: epoch_1} }
  # ...
  - { label: "C1 topk=40", loop_index: 9, strategy_params: {topk: 40} }
```

---

## 5. 生成后自检清单（提交创建前）

- [ ] 除动作轴外所有字段在 loop 间一致？
- [ ] `target_desc` 写明了基线 ID + 基线指标？
- [ ] 涉及挑冠军的轴是否预留了 Route C 多 seed？
- [ ] `auto_start=false`？
- [ ] `runtime_flags.archive_policy=AUTO`（确保结果入数仓供考核取数）？
- [ ] 已按方法论 Part 6.7 预登记本轮考核指标与判据？

---

## 6. 实验完成后取数（对接数仓视图）

| 考核项 | 视图/工具 |
|--------|-----------|
| 双轴指标对照 | `v_run_leaderboard` / `qe_custom_evo_loop_comparison` |
| seed 鲁棒性 | `v_seed_robustness` |
| 因子归因稳定性 | `v_factor_importance_stability` |
| 过拟合红旗 | `v_overfit_flags` |
| 晋升候选 | `v_promotion_candidates` |

视图定义见 `QE_DataWarehouse_Analytics_Design_v1_20260529.md` 与 `backend/db/migrations/qe_archive_analytics_views_20260529.sql`。
