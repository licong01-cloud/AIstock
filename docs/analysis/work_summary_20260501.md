# 2026-05-01 工作总结

## 完成事项

### 1. ✅ TAIL_SUBSTITUTE 备选股选择逻辑修复

**问题**: 固定选择第51-65名，跳过Top50内高排名股票

**修复**:
- 扩大到前150名（topk=50 + backup_depth=100）
- 从第1名开始，包含Top50内所有股票
- 排除已持仓和日频已下单的股票

**修改文件**:
- ✅ `rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`
- ✅ `app_tpl/all/v4/v5/v6` 对应文件
- ✅ 远端机 192.168.50.215 已同步

**Git 提交**: commit `7e092f06`

---

### 2. ✅ 问题深度分析

**方法**: 方案A - 离线复现（pred.pkl + positions 数据）

**关键发现**:
1. **原文档结论错误**:
   - 文档称"替补股票都是50名以外"
   - 实际数据：2024-12-31 买入5只股票全部来自Top50（排名19/20/24/25/30）

2. **发现第二个独立 bug**:
   - 日频策略换仓模式缺少 topk 约束
   - 导致持仓从50膨胀到62只（+24%）
   - 与 TAIL_SUBSTITUTE 无关

3. **数据验证**:
   - 全期441天，99.8%处于换仓模式
   - 85.5%的天数实际买入超过日频预测
   - 平均 extra_buys = 3.62 只/天

---

### 3. ✅ 文档整理

**创建文档**:
1. `qe_v25_strategy_issues_analysis_20260501.md` (15KB)
   - 完整问题分析
   - 数据验证结果
   - 已修复和待修复内容
   - 代码调用链图示

2. `qe_v25_strategy_fix_tasks_20260501.md` (13KB)
   - 6个后续修复任务
   - 优先级排序（高/中/低）
   - 详细实施方案
   - 预计工时：2-3天

3. `tail_substitute_backup_candidates_fix_20260501.md` (7KB)
   - 修复报告
   - 验证结果
   - 生效范围

**文档位置**: `F:\Dev\AIstock\docs\analysis\`

---

### 4. ✅ 记忆系统更新

**新增记忆**:
- `aistock_docs_location.md` - 文档存储规范
- 更新 `MEMORY.md` 索引

**规则**:
- 今后所有分析文档存储到 `F:\Dev\AIstock\docs\analysis\`
- 不再使用 RD-Agent-main 的 docs 目录

---

## 待办事项

### 🔴 高优先级

**TASK-001: 修复换仓模式 topk 约束**
- 预计工时: 4h
- 影响: 所有新实验
- 状态: 待开始

### 🟡 中优先级

**TASK-002: 验证 TAIL_SUBSTITUTE 执行情况**
- 预计工时: 2h
- 目的: 确认是否真的执行了
- 状态: 待开始

**TASK-003: 重跑关键实验验证修复效果**
- 预计工时: 6h
- 对比: 修复前后的回测指标
- 状态: 待开始

### 🟢 低优先级

**TASK-004: 添加持仓数监控指标**
- 预计工时: 1h
- 状态: 待开始

**TASK-005: 优化 backup_candidates 生成效率**
- 预计工时: 2h
- 建议: 暂缓实施
- 状态: 待开始

**TASK-006: 更新相关文档**
- 预计工时: 1h
- 状态: 待开始

---

## 技术要点

### 策略权重计算

**方法**: softmax
```python
s_norm = s / temperature  # temperature=1.0
exp_s = np.exp(s_norm)
weights = exp_s / exp_s.sum()
```

**约束**:
- max_weight: 0.05 (单只最多5%)
- min_weight: 0.005 (单只至少0.5%)
- max_position_ratio: 0.95 (总仓位最多95%)

### 代码调用链

```
日频策略 (ScoreWeightedTopkStrategyV2)
  ↓ 生成 _backup_candidates (✅ 已修复)
  ↓ 传递给
尾盘执行策略 (TailTWAPWithV25TwoStageStrategy)
  ↓ 从 backup_candidates 中按顺序选择可交易股票
```

---

## 文件清单

### 修改的代码文件
- `rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`
- `app_tpl/all/v4/rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`
- `app_tpl/all/v5/rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`
- `app_tpl/all/v6/rdagent/scenarios/qlib/experiment/factor_template/score_weighted_strategy_v2.py`

### 创建的文档
- `F:\Dev\AIstock\docs\analysis\qe_v25_strategy_issues_analysis_20260501.md`
- `F:\Dev\AIstock\docs\analysis\qe_v25_strategy_fix_tasks_20260501.md`
- `F:\Dev\AIstock\docs\analysis\tail_substitute_backup_candidates_fix_20260501.md`

### 分析脚本
- `F:\Dev\RD-Agent-main\scripts\_tmp\reconstruct_loop5.py`
- `F:\Dev\RD-Agent-main\scripts\_tmp\analyze_20241231.py`
- `F:\Dev\RD-Agent-main\scripts\_tmp\verify_backup_candidates.py`

---

## 下一步行动

1. **审核文档**: 检查分析报告和任务清单
2. **启动 TASK-001**: 修复换仓模式 topk 约束
3. **启动 TASK-002**: 验证 TAIL_SUBSTITUTE 执行情况
4. **等待实验结果**: TASK-003 重跑验证

---

**日期**: 2026-05-01
**工作时长**: 约6小时
**状态**: ✅ 第一阶段完成
