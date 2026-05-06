# TAIL_SUBSTITUTE 备选股选择逻辑修复报告

**修复日期**: 2026-05-01
**修复人员**: Claude Opus 4.7
**问题编号**: QE-V25-TAIL-SUBSTITUTE-001

---

## 问题描述

### 原始问题
当日频策略的买入订单因涨停等原因未成交时，TAIL_SUBSTITUTE 尾盘替补机制选择的股票固定为第 51-65 名，跳过了 Top50 内未被日频策略选中的高排名股票。

### 根本原因
`score_weighted_strategy_v2.py` 中 `_backup_candidates` 列表生成逻辑错误：

```python
# 修复前（错误）
backup_sids = ranked.iloc[self.topk:self.topk + backup_depth].index.tolist()
# 固定选择第 51-65 名，跳过了 Top50 内的剩余股票
```

### 影响范围
- QE 实验：所有使用 ScoreWeightedTopkStrategyV2 的实验
- Paper v2 模拟盘：所有基于该策略的策略包
- 远端机 192.168.50.215：所有节点任务

---

## 修复方案

### 修复逻辑

```python
# 修复后（正确）
backup_depth = 100  # 扩大到前 150 名
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()  # 从第1名开始
already_ordered = set(actual_buys)  # 排除日频已下单
self._backup_candidates = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings and sid not in already_ordered
]
```

### 修复效果

| 场景 | 修复前 | 修复后 |
|------|--------|--------|
| 日频买入排名19/20涨停 | 替补选51-65名 | 替补选21/22/23...（Top50内剩余） |
| Top50全涨停 | 替补选51-65名 | 替补选51/52/53...（继续往后） |
| Top100全涨停 | 无替补（列表只到65） | 替补选101/102...（继续往后） |

---

## 修改文件清单

### 1. 本地 WSL 环境

✅ **主模板**（最重要）
```
F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\score_weighted_strategy_v2.py
```
- 修改位置：Line 268-277
- 修改内容：backup_depth=100, 从第1名开始, 排除已下单

✅ **app_tpl v4**
```
F:\Dev\RD-Agent-main\app_tpl\all\v4\rdagent\scenarios\qlib\experiment\factor_template\score_weighted_strategy_v2.py
```
- 修改位置：Line 270-279
- 修改内容：同上

✅ **app_tpl v5**
```
F:\Dev\RD-Agent-main\app_tpl\all\v5\rdagent\scenarios\qlib\experiment\factor_template\score_weighted_strategy_v2.py
```
- 修改位置：Line 268-277
- 修改内容：同上

✅ **app_tpl v6**
```
F:\Dev\RD-Agent-main\app_tpl\all\v6\rdagent\scenarios\qlib\experiment\factor_template\score_weighted_strategy_v2.py
```
- 修改位置：Line 268-277
- 修改内容：同上

### 2. 远端机 192.168.50.215

✅ **远端主模板**
```
lc999@192.168.50.215:/home/lc999/RD-Agent-main/rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py
```
- 同步方式：rsync
- 同步时间：2026-05-01
- 验证状态：✅ 已验证

---

## 验证结果

### 本地验证

```bash
# 主模板
$ grep -A 2 'backup_depth.*=' score_weighted_strategy_v2.py
backup_depth = 100
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
already_ordered = set(actual_buys)
```

✅ v4: backup_depth = 100, ranked.iloc[:150]
✅ v5: backup_depth = 100, ranked.iloc[:150]
✅ v6: backup_depth = 100, ranked.iloc[:150]

### 远端验证

```bash
$ ssh lc999@192.168.50.215 'grep -A 2 "backup_depth.*=" ...'
backup_depth = 100
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
already_ordered = set(actual_buys)
```

✅ 远端机已同步

---

## 生效范围

### ✅ 立即生效

| 系统 | 生效时机 | 说明 |
|------|---------|------|
| QE 新实验 | 启动新 Loop 时 | 从模板复制代码 |
| Paper v2 新策略包 | 创建新策略包时 | 从模板读取代码 |
| 远端机新任务 | 启动新任务时 | 使用同步后的模板 |

### ❌ 不影响

| 系统 | 原因 |
|------|------|
| QE 已运行实验 | 代码已复制到 workspace，不会自动更新 |
| Paper v2 已部署策略 | 策略包已冻结，不会自动更新 |

---

## 后续建议

### 1. 测试验证

建议启动新的 QE 实验验证修复效果：

```bash
# 检查新实验的 workspace 代码
grep -A 3 "backup_depth" qe_workspace/<NEW_TASK_ID>/Loop1/custom_strategy.py
```

预期输出：
```python
backup_depth = 100
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
already_ordered = set(actual_buys)
```

### 2. 监控指标

修复后，关注以下指标变化：
- FFR (Filled Fulfillment Rate)：预期提升
- PA (Price Advantage)：预期改善
- 持仓膨胀：仍需单独修复（另一个 bug）

### 3. 已运行实验处理

如需对已运行实验应用修复：
1. 手动修改 workspace 中的 `custom_strategy.py`
2. 或重新启动实验

---

## 技术细节

### 权重计算逻辑（未修改）

日频策略仍然使用 softmax 加权：

```python
# score_weighted_strategy.py:359-363
s_norm = s / self.temperature  # temperature=1.0
exp_s = np.exp(s_norm)
weights = exp_s / exp_s.sum()
```

配置参数：
- `weight_method`: softmax
- `temperature`: 1.0
- `max_weight`: 0.05 (单只最多5%)
- `min_weight`: 0.005 (单只至少0.5%)

### 调用链

```
日频策略 (ScoreWeightedTopkStrategyV2)
  ↓ 生成 _backup_candidates (修复点)
  ↓ 传递给
尾盘执行策略 (TailTWAPWithV25TwoStageStrategy)
  ↓ 继承自
TailTWAPWithLimitStrategy
  ↓ _do_realloc_substitute (Line 176)
  ↓ 从 backup_candidates 中按顺序选择可交易股票
```

---

## 修复确认

- [x] 本地主模板修改完成
- [x] app_tpl v4/v5/v6 修改完成
- [x] 远端机同步完成
- [x] 本地验证通过
- [x] 远端验证通过
- [x] 修复报告生成

**修复状态**: ✅ 全部完成

---

## 附录：修改前后对比

### 修改前

```python
# Line 269-274 (主模板)
backup_depth = 15
backup_sids = ranked.iloc[self.topk:self.topk + backup_depth].index.tolist()
self._backup_candidates = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings
]
```

**问题**：
1. 固定选择第 51-65 名
2. 跳过 Top50 内未被日频策略选中的股票
3. 未排除日频已下单的股票

### 修改后

```python
# Line 268-277 (主模板)
# 备选股（供 inner_strategy TAIL_SUBSTITUTE 使用）
# 修复：提供完整的高排名候选列表，不限制在 topk 之外
# TAIL_SUBSTITUTE 会按排名从高到低选择可交易的股票
backup_depth = 100
backup_sids = ranked.iloc[:self.topk + backup_depth].index.tolist()
already_ordered = set(actual_buys)
self._backup_candidates = [
    (sid, float(ranked[sid])) for sid in backup_sids
    if sid not in current_holdings and sid not in already_ordered
]
```

**改进**：
1. 扩大到前 150 名（topk=50 + backup_depth=100）
2. 从第1名开始，包含 Top50 内所有股票
3. 排除已持仓 + 日频已下单的股票
4. TAIL_SUBSTITUTE 能从所有可交易股票中选择排名最高的
