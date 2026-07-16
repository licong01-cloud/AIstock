# HMM 演进系统隔离约束强化说明

> **日期**: 2026-07-16  
> **重要性**: 🔴 **Critical - 架构核心约束**  
> **适用范围**: Phase 0-3 全阶段

---

## ⚠️ 核心隔离原则

### Phase 0-3 的定位

**HMM 演进系统（Phase 0-3）是一条完全独立的研究生产线**

```
┌─────────────────────────────────────────────────────────────┐
│                  现有生产系统（禁止修改）                        │
├─────────────────────────────────────────────────────────────┤
│  QE 实验环境                                                  │
│  - QE HMM 配置                                                │
│  - QE 回测流程                                                │
│  - model_train_configs/snapshots                             │
│                                                               │
│  模拟盘环境                                                    │
│  - paper-v2 HMM 配置                                          │
│  - 实盘交易逻辑                                                │
│  - strategy_packages                                          │
└─────────────────────────────────────────────────────────────┘
                          ⬆️
                   🚫 禁止任何修改
                   🚫 禁止读取配置
                   🚫 禁止影响行为

┌─────────────────────────────────────────────────────────────┐
│            HMM 演进系统（独立研究线）                            │
├─────────────────────────────────────────────────────────────┤
│  Phase 0: 数据源抽象层                                         │
│  - 只读取 QE artifact（pred.pkl, label.pkl）                  │
│  - 不读取/修改任何 QE 配置                                      │
│                                                               │
│  Phase 1: 离线评估                                             │
│  - 使用独立的 HMM snapshot                                     │
│  - 不影响 QE 或模拟盘                                          │
│                                                               │
│  Phase 2: 风险监控                                             │
│  - 只读取 DB 数据                                              │
│  - 不修改任何配置或状态                                         │
│                                                               │
│  Phase 3: 滚动训练                                             │
│  - 训练新的 HMM snapshot                                       │
│  - 不替换生产环境的 HMM                                         │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚫 严格禁止的操作

### 1. 禁止读取生产配置

❌ **不得读取**:
```python
# ❌ 禁止
model_train_configs  # QE 使用的 HMM 配置
strategy_packages    # 模拟盘使用的策略包配置
paper_v2.portfolios  # 模拟盘配置
```

✅ **只能使用**:
```python
# ✅ 允许
QE artifact cache (pred.pkl, label.pkl)  # 只读历史数据
market.kline_daily_raw                    # 只读市场数据
market.sw_member                          # 只读板块数据
```

---

### 2. 禁止修改生产状态

❌ **不得修改**:
- QE 实验的 HMM 配置
- 模拟盘的 HMM 配置
- 任何 strategy_package 的状态
- 任何 model_train_snapshot 的状态
- paper-v2 的任何配置

✅ **只能创建新的**:
```python
# ✅ 允许（独立表）
hmm_evolution.offline_evaluation      # 演进系统专用
hmm_evolution.hmm_candidates          # 演进系统专用
hmm_risk.daily_alert                  # 风险监控专用
hmm_risk.sector_heatmap               # 风险监控专用
```

---

### 3. 禁止影响生产行为

❌ **不得影响**:
- QE 回测的执行流程
- 模拟盘的交易决策
- 任何实盘的资金操作
- 现有 HMM 的预测结果

✅ **只能做**:
- 离线评估（使用历史数据）
- 风险预警（只展示，不干预）
- 研发新 HMM（不自动上线）

---

## ✅ 允许的操作范围

### Phase 0: 数据源抽象层

**只读取历史数据**:
```python
class BacktestDataSource:
    async def get_predictions(self, start_date, end_date):
        # ✅ 从 QE workspace 下载 artifact（只读）
        artifact_bytes = await self.qe_client.download_artifact(
            task_id=task_id,
            loop_name=loop_name,
            artifact_name="pred.pkl",  # 只读历史数据
        )
        
        # ✅ 缓存到独立目录
        self.cache_manager.save_artifact(
            "pred.pkl", 
            artifact_bytes,
            cache_dir="tmp/hmm_evolution_cache/"  # 独立缓存
        )
```

**严格隔离**:
```python
# ✅ 允许：读取历史 artifact
/qe_workspace/{task_id}/Loop1/pred.pkl        # 只读
/qe_workspace/{task_id}/Loop1/label.pkl       # 只读

# 🚫 禁止：读取 QE 配置
/qe_workspace/{task_id}/config.json           # 禁止
/qe_workspace/{task_id}/hmm_config.yaml       # 禁止
```

---

### Phase 1: 离线评估

**使用独立的 HMM snapshot**:
```python
class HMMEvolutionService:
    async def evaluate_offline(
        self,
        hmm_snapshot_id: str,  # ✅ 独立的 HMM
        base_loop_ref: str,    # ✅ 历史 QE 数据
    ):
        # ✅ 使用独立的 HMM 进行离线评估
        # 🚫 不读取生产 HMM 的配置
        # 🚫 不修改任何生产状态
        pass
```

**隔离验证**:
```python
# ✅ 允许：创建独立评估记录
INSERT INTO hmm_evolution.offline_evaluation (
    eval_id,
    hmm_snapshot_id,  -- 研发用的 HMM
    base_loop_ref     -- 历史数据引用
);

# 🚫 禁止：修改生产表
UPDATE model_train_snapshots ...   -- 禁止
UPDATE strategy_packages ...       -- 禁止
UPDATE paper_v2.portfolios ...     -- 禁止
```

---

### Phase 2: 风险监控

**只读取数据，不干预决策**:
```python
class HMMRiskMonitorService:
    async def generate_daily_alerts(self, trade_date: date):
        # ✅ 读取市场数据
        sector_map = await self.data_source.get_sector_mapping(trade_date)
        
        # ✅ 生成预警（只展示）
        alerts = self._calculate_risk_alerts(sector_map)
        
        # ✅ 保存到独立表
        await self._save_alerts_to_db(alerts)
        
        # 🚫 不自动触发任何操作
        # 🚫 不修改模拟盘配置
        # 🚫 不发送交易指令
```

---

### Phase 3: 滚动训练

**训练新 HMM，但不自动上线**:
```python
class HMMRollingTrainService:
    async def trigger_retrain(self, config_id: str):
        # ✅ 训练新的 HMM snapshot
        new_snapshot_id = await self._train_hmm(config_id)
        
        # ✅ 注册到独立表
        await self._register_snapshot(new_snapshot_id)
        
        # 🚫 不替换生产 HMM
        # 🚫 不修改 QE 配置
        # 🚫 不修改模拟盘配置
        
        # ✅ 通知人工审批
        await self._notify_for_approval(new_snapshot_id)
```

---

## 🔒 数据库权限控制

### Phase 0-3 的数据库权限

```sql
-- ✅ 允许：只读市场数据
GRANT SELECT ON market.kline_daily_raw TO hmm_evolution_ro;
GRANT SELECT ON market.sw_member TO hmm_evolution_ro;
GRANT SELECT ON market.trade_cal TO hmm_evolution_ro;

-- ✅ 允许：读写演进系统专用表
GRANT SELECT, INSERT, UPDATE ON hmm_evolution.* TO hmm_evolution_rw;
GRANT SELECT, INSERT, UPDATE ON hmm_risk.* TO hmm_evolution_rw;

-- 🚫 禁止：修改生产表
REVOKE INSERT, UPDATE, DELETE ON model_train_configs FROM hmm_evolution_rw;
REVOKE INSERT, UPDATE, DELETE ON model_train_snapshots FROM hmm_evolution_rw;
REVOKE INSERT, UPDATE, DELETE ON strategy_packages FROM hmm_evolution_rw;
REVOKE INSERT, UPDATE, DELETE ON paper_v2.* FROM hmm_evolution_rw;
```

---

## 🛡️ 代码层面的隔离保护

### 1. 配置文件隔离

```python
# backend/services/hmm_evolution/config.py

class HMMEvolutionConfig(BaseSettings):
    """
    HMM 演进系统配置
    
    ⚠️ 隔离约束：
    1. 不得读取 model_train_configs
    2. 不得读取 strategy_packages 配置
    3. 不得读取 paper_v2 配置
    """
    
    # ✅ 独立的数据源配置
    data_source_mode: str = "backtest"
    base_loop_ref: str = "qe_20260502_131502_9b54/Loop1"
    cache_dir: str = "tmp/hmm_evolution_cache/"
    
    # 🚫 不包含生产配置路径
    # qe_config_path: str  -- 禁止
    # paper_config_path: str  -- 禁止
```

---

### 2. 数据源隔离检查

```python
# backend/services/hmm_data_source/backtest_source.py

class BacktestDataSource(HMMDataSourceInterface):
    async def get_predictions(self, start_date, end_date):
        # ✅ 只读取 artifact
        artifact_bytes = await self.qe_client.download_artifact(
            artifact_name="pred.pkl"
        )
        
        # 🚫 显式检查：不读取配置
        if artifact_name.endswith(('.json', '.yaml', '.yml', '.toml')):
            raise PermissionError(
                f"Forbidden: Cannot read config files from QE workspace. "
                f"HMM Evolution must use independent configs."
            )
```

---

### 3. 写操作隔离检查

```python
# backend/services/hmm_evolution/service.py

class HMMEvolutionService:
    # 🚫 禁止的数据库表
    FORBIDDEN_TABLES = [
        'model_train_configs',
        'model_train_snapshots',
        'strategy_packages',
        'paper_v2.portfolios',
        'paper_v2.sessions',
        'paper_v2.orders',
    ]
    
    async def _ensure_isolated_write(self, table_name: str):
        """确保只写入演进系统专用表"""
        if any(forbidden in table_name for forbidden in self.FORBIDDEN_TABLES):
            raise PermissionError(
                f"Forbidden: Cannot write to production table '{table_name}'. "
                f"HMM Evolution must use isolated tables (hmm_evolution.*, hmm_risk.*)."
            )
```

---

## 📋 验收时的隔离检查清单

### Phase 0 验收必须通过

- [ ] ✅ 只读取 QE artifact（pred.pkl, label.pkl）
- [ ] 🚫 不读取任何 QE 配置文件
- [ ] 🚫 不修改任何生产表
- [ ] 🚫 不调用 QE 的 HMM 配置 API
- [ ] 🚫 不调用模拟盘的配置 API

### 代码审查检查点

```bash
# 1. 检查是否读取了生产配置
grep -r "model_train_configs" backend/services/hmm_data_source/
grep -r "strategy_packages" backend/services/hmm_data_source/
# 预期：0 匹配

# 2. 检查是否修改了生产表
grep -r "UPDATE model_train" backend/services/hmm_data_source/
grep -r "DELETE FROM strategy" backend/services/hmm_data_source/
# 预期：0 匹配

# 3. 检查数据库连接权限
psql -U hmm_evolution_rw -d aistock -c "\
  SELECT grantee, privilege_type, table_name \
  FROM information_schema.table_privileges \
  WHERE grantee = 'hmm_evolution_rw' \
    AND (table_name LIKE 'model_train%' OR table_name LIKE 'strategy%')"
# 预期：只有 SELECT 权限，无 INSERT/UPDATE/DELETE
```

---

## 🚀 未来接入流程（Phase 4+）

**如果未来需要接入 QE 或模拟盘，必须：**

1. **独立设计文档**
   - 单独的架构设计审批
   - 明确的接入范围
   - 风险评估和回滚方案

2. **渐进式接入**
   - 先在测试环境验证
   - 充分的 A/B 测试
   - 人工审批每一步

3. **可回滚性**
   - 一键切回旧 HMM
   - 不影响现有流程
   - 保留独立研究线

**Phase 0-3 期间严格禁止任何接入尝试。**

---

## 📄 文档更新

本隔离约束已同步更新到：
- ✅ `hmm_evolution_and_risk_management_system_design_20260716.md` - 第 1.3 节
- ✅ `hmm_evolution_phase0_data_source_detailed_design_20260716.md` - 第 1.2 节
- ✅ `hmm_evolution_phase0_design_review_20260716.md` - 第 3.2 节

---

**核心原则**：HMM 演进系统是**完全独立的研究生产线**，Phase 0-3 期间与 QE/模拟盘**零耦合**。
