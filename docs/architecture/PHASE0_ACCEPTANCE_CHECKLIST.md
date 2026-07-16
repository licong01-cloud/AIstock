# Phase 0 验收清单（强化隔离版）

> **版本**: v1.1 - 强化隔离约束  
> **日期**: 2026-07-16  
> **重要性**: 🔴 Critical  
> **说明**: 本验收清单包含隔离约束的强制检查项

---

## 🔒 隔离约束验收（阻塞项 - 必须全部通过）

### 1. 配置文件隔离

- [ ] ✅ **代码审查**：确认未读取 `model_train_configs` 表
  ```bash
  grep -r "model_train_configs" backend/services/hmm_data_source/
  # 预期输出：0 匹配
  ```

- [ ] ✅ **代码审查**：确认未读取 `strategy_packages` 表
  ```bash
  grep -r "strategy_packages" backend/services/hmm_data_source/
  # 预期输出：0 匹配
  ```

- [ ] ✅ **代码审查**：确认未读取 `paper_v2.*` 表
  ```bash
  grep -r "paper_v2\." backend/services/hmm_data_source/
  # 预期输出：0 匹配
  ```

- [ ] ✅ **代码审查**：确认未读取 QE 配置文件
  ```bash
  grep -rE "(config\.json|hmm_config\.yaml|\.yml|\.toml)" backend/services/hmm_data_source/ | grep -v "^#"
  # 预期输出：只有注释，无实际读取代码
  ```

---

### 2. 数据写入隔离

- [ ] ✅ **代码审查**：确认未修改生产表
  ```bash
  grep -rE "(UPDATE|DELETE|INSERT INTO).*(model_train|strategy_packages|paper_v2)" backend/services/hmm_data_source/
  # 预期输出：0 匹配
  ```

- [ ] ✅ **代码审查**：确认只写入演进系统专用表
  ```bash
  grep -rE "(INSERT INTO|UPDATE|DELETE FROM)" backend/services/hmm_data_source/ | grep -v "hmm_evolution" | grep -v "hmm_risk"
  # 预期输出：0 匹配（除了注释和文档）
  ```

---

### 3. API 调用隔离

- [ ] ✅ **代码审查**：确认未调用 QE HMM 配置 API
  ```python
  # 禁止的 API 调用
  # ❌ /api/qe/experiments/{id}/hmm_config
  # ❌ /api/model_train_configs/{id}
  ```

- [ ] ✅ **代码审查**：确认未调用模拟盘配置 API
  ```python
  # 禁止的 API 调用
  # ❌ /api/paper-v2/portfolios/{id}/config
  # ❌ /api/strategy-packages/{id}
  ```

- [ ] ✅ **单元测试**：Mock QE client 只允许下载 artifact
  ```python
  # tests/backend/services/hmm_data_source/test_backtest_source.py
  def test_qe_client_only_downloads_artifacts(mock_qe_client):
      # 验证只调用 download_artifact()
      # 验证参数只包含 pred.pkl, label.pkl
      # 验证不调用 get_config() 等方法
  ```

---

### 4. 数据库权限隔离（DBA 配合）

- [ ] ✅ **权限检查**：`hmm_evolution_rw` 用户对生产表只有 SELECT 权限
  ```sql
  SELECT 
      grantee, 
      privilege_type, 
      table_schema || '.' || table_name as full_table_name
  FROM information_schema.table_privileges 
  WHERE grantee = 'hmm_evolution_rw'
    AND table_name IN (
        'model_train_configs', 
        'model_train_snapshots', 
        'strategy_packages'
    )
  ORDER BY table_name, privilege_type;
  
  -- 预期输出：只有 SELECT 权限
  -- 不应有 INSERT, UPDATE, DELETE 权限
  ```

- [ ] ✅ **权限检查**：`hmm_evolution_rw` 用户对 paper_v2 schema 只有 SELECT 权限
  ```sql
  SELECT 
      grantee, 
      privilege_type, 
      table_schema || '.' || table_name as full_table_name
  FROM information_schema.table_privileges 
  WHERE grantee = 'hmm_evolution_rw'
    AND table_schema = 'paper_v2'
  ORDER BY table_name, privilege_type;
  
  -- 预期输出：只有 SELECT 权限（如果有）
  -- 或者 0 行（完全不访问）
  ```

---

### 5. 缓存目录隔离

- [ ] ✅ **目录检查**：确认缓存在独立目录
  ```bash
  # 检查缓存路径配置
  grep -r "cache_dir" backend/services/hmm_data_source/
  
  # 预期输出：只使用 tmp/hmm_evolution_cache/
  # 不使用 QE workspace 的缓存目录
  # 不使用模拟盘的缓存目录
  ```

- [ ] ✅ **清理测试**：确认清理缓存不影响生产数据
  ```python
  # tests/backend/services/hmm_data_source/test_cache_manager.py
  def test_clear_cache_only_affects_hmm_evolution_cache():
      cache_manager.clear_cache()
      
      # 验证只删除 tmp/hmm_evolution_cache/ 下的文件
      # 验证不影响 QE workspace
      # 验证不影响模拟盘数据
  ```

---

## ✅ 功能验收（标准检查项）

### 1. 接口定义

- [ ] HMMDataSourceInterface 包含 5 个方法
- [ ] 所有方法有完整的 docstring 和类型注解
- [ ] 异常类型明确定义（5 个异常类）

### 2. 回测数据源

- [ ] 可从 QE workspace 下载 pred.pkl
- [ ] 可从 QE workspace 下载 label.pkl
- [ ] 首次下载保存到缓存（tmp/hmm_evolution_cache/）
- [ ] 后续访问使用缓存（无重复下载）
- [ ] 并发访问正确处理（asyncio.Lock）
- [ ] 日期范围验证生效
- [ ] 返回标准化 DataFrame（列名固定）
- [ ] ⚠️ **交易日历计算准确**（使用 market.trade_cal）

### 3. 实时数据源

- [ ] 可查询 t-1 数据
- [ ] lag_days 参数生效
- [ ] max_query_days 限制生效
- [ ] 查询 DB 返回正确数据
- [ ] 板块映射查询正确（market.sw_member）

### 4. 缓存管理

- [ ] 保存 artifact 到缓存
- [ ] SHA256 校验正确
- [ ] 损坏文件检测生效
- [ ] 清理缓存功能正常
- [ ] 缓存信息查询正确

---

## 🎯 性能验收

- [ ] 回测数据源首次加载 < 30s
- [ ] 回测数据源缓存命中 < 1s
- [ ] 实时数据源查询 < 2s（如果使用）
- [ ] 板块映射查询 < 500ms

**注意**：Phase 0-1 主要使用回测数据源，实时数据源的性能测试可推迟到 Phase 2

---

## 🧪 测试验收

### 单元测试

- [ ] 覆盖率 > 90%
- [ ] 回测数据源：8 个测试用例通过
- [ ] 实时数据源：4 个测试用例通过
- [ ] 缓存管理器：6 个测试用例通过
- [ ] 异常处理：所有异常场景有测试

### 集成测试

- [ ] 数据源切换测试通过
- [ ] 真实 QE artifact 下载测试通过（需 --run-integration）
- [ ] 真实 DB 查询测试通过（可选，Phase 2 前验证）

### 隔离性测试（新增）

- [ ] **Mock 测试**：QE client 只允许下载 artifact
  ```python
  def test_qe_client_rejects_config_download():
      with pytest.raises(PermissionError):
          await source._download_artifact("config.json")
  ```

- [ ] **Mock 测试**：不允许写入生产表
  ```python
  def test_forbid_write_to_production_tables():
      with pytest.raises(PermissionError):
          await service._ensure_isolated_write("model_train_configs")
  ```

---

## 📄 文档验收

- [ ] README 包含使用示例
- [ ] API 文档完整
- [ ] 异常处理说明清晰
- [ ] 性能基准文档化
- [ ] **隔离约束文档完整**（新增）
  - [ ] `HMM_EVOLUTION_ISOLATION_CONSTRAINTS.md` 已创建
  - [ ] 禁止操作清单明确
  - [ ] 代码层面的保护机制已实现

---

## 🔐 安全验收

- [ ] 数据库用户权限最小化（只读生产表）
- [ ] 缓存目录隔离（独立目录）
- [ ] 错误信息不泄露生产配置路径
- [ ] 日志不记录生产敏感信息

---

## 📊 验收流程

### 阶段 1：代码审查（2 小时）

1. **隔离约束检查**（30 分钟）
   - 运行所有 `grep` 命令
   - 验证无违规代码

2. **功能代码审查**（1 小时）
   - 检查类型注解
   - 检查异常处理
   - 检查日志规范

3. **测试代码审查**（30 分钟）
   - 验证测试覆盖率
   - 检查隔离性测试

---

### 阶段 2：测试执行（1 小时）

1. **单元测试**（30 分钟）
   ```bash
   pytest tests/backend/services/hmm_data_source/ -v --cov
   ```

2. **隔离性测试**（15 分钟）
   ```bash
   pytest tests/backend/services/hmm_data_source/ -v -k "isolation or forbid"
   ```

3. **集成测试**（15 分钟）
   ```bash
   pytest tests/backend/services/hmm_data_source/test_integration.py --run-integration
   ```

---

### 阶段 3：权限验证（30 分钟）

1. **数据库权限检查**（15 分钟）
   - 运行所有 SQL 权限查询
   - 验证符合最小权限原则

2. **文件系统权限检查**（15 分钟）
   - 验证缓存目录独立
   - 验证不访问生产目录

---

### 阶段 4：文档审查（30 分钟）

1. **隔离约束文档**（15 分钟）
   - 验证 `HMM_EVOLUTION_ISOLATION_CONSTRAINTS.md` 完整

2. **使用文档**（15 分钟）
   - 验证 README 清晰
   - 验证 API 文档完整

---

## ✅ 验收签字

### 开发负责人
- [ ] 姓名：__________________
- [ ] 日期：__________________
- [ ] 签字：__________________

### 架构审查
- [ ] 姓名：__________________
- [ ] 日期：__________________
- [ ] 签字：__________________
- [ ] 隔离约束验证通过：[ ] 是 [ ] 否

### 安全审查（可选，如果有专门的安全团队）
- [ ] 姓名：__________________
- [ ] 日期：__________________
- [ ] 签字：__________________
- [ ] 权限隔离验证通过：[ ] 是 [ ] 否

---

## 🚫 验收不通过的处理

**如果隔离约束检查未通过**：
- 🔴 **立即停止验收**
- 🔴 **回滚所有代码**
- 🔴 **重新设计**

**如果功能测试未通过**：
- 🟡 修复 bug 后重新验收
- 🟡 不允许带 bug 上线

**如果性能测试未通过**：
- 🟡 分析原因，优化后重新验收
- 🟡 如果无法优化，调整性能目标（需重新评审）

---

## 📋 验收记录模板

```
Phase 0 验收记录
日期：2026-__-__
验收人：__________

隔离约束验收：
  [ ] 配置文件隔离 - 通过/不通过
  [ ] 数据写入隔离 - 通过/不通过
  [ ] API 调用隔离 - 通过/不通过
  [ ] 数据库权限隔离 - 通过/不通过
  [ ] 缓存目录隔离 - 通过/不通过

功能验收：
  [ ] 接口定义 - 通过/不通过
  [ ] 回测数据源 - 通过/不通过
  [ ] 实时数据源 - 通过/不通过
  [ ] 缓存管理 - 通过/不通过

测试验收：
  [ ] 单元测试 - 通过/不通过（覆盖率：___%）
  [ ] 集成测试 - 通过/不通过
  [ ] 隔离性测试 - 通过/不通过

文档验收：
  [ ] README - 完整/不完整
  [ ] API 文档 - 完整/不完整
  [ ] 隔离约束文档 - 完整/不完整

最终结论：
  [ ] ✅ 通过验收，可进入 Phase 1
  [ ] 🟡 有条件通过（列出条件）
  [ ] 🔴 不通过（列出问题）

备注：
________________________________________
________________________________________
```

---

**文档位置**：`docs/architecture/PHASE0_ACCEPTANCE_CHECKLIST.md`
