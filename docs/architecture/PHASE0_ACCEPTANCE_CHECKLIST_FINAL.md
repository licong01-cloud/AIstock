# Phase 0 验收检查清单

> **使用说明**: 按顺序执行每一项检查，所有 ✅ 标记完成后才能合入主分支

---

## 📋 验收检查项

### 1️⃣ 代码质量检查 (开发者自检)

```bash
cd /f/Dev/AIstock
```

#### 1.1 语法检查 ⏳
```bash
python -m py_compile backend/services/hmm_data_source/*.py
```
- [ ] ✅ 所有文件编译通过，无语法错误

#### 1.2 导入检查 ⏳
```bash
python -c "from backend.services.hmm_data_source import BacktestDataSource, RealtimeDataSource, ArtifactCacheManager; print('✅ Import successful')"
```
- [ ] ✅ 所有模块可以正常导入

#### 1.3 代码风格检查 (可选) ⚪
```bash
# 如果项目使用 black/ruff
black --check backend/services/hmm_data_source/
ruff check backend/services/hmm_data_source/
```
- [ ] ⚪ 代码风格符合项目规范（如果适用）

---

### 2️⃣ 隔离约束验证 (阻塞项 🔴)

#### 2.1 运行隔离约束测试 ⏳
```bash
pytest tests/backend/services/hmm_data_source/test_isolation_constraints.py -v
```
- [ ] ✅ `test_no_production_table_imports` - 通过
- [ ] ✅ `test_no_write_operations_to_production_tables` - 通过
- [ ] ✅ `test_only_artifact_files_allowed` - 通过
- [ ] ✅ `test_cache_directory_isolation` - 通过
- [ ] ✅ `test_only_read_operations_on_market_tables` - 通过
- [ ] ✅ `test_no_qe_config_api_calls` - 通过
- [ ] ✅ `test_no_paper_v2_api_calls` - 通过
- [ ] ✅ `test_no_absolute_paths_in_code` - 通过
- [ ] ✅ `test_cache_dir_configurable` - 通过
- [ ] ✅ `test_no_production_credentials_in_code` - 通过

**🔴 如果任何一项失败，立即停止验收并回滚代码**

#### 2.2 手动代码审查 ⏳
```bash
# 检查是否有对生产表的引用
grep -r "model_train_configs" backend/services/hmm_data_source/
grep -r "model_train_snapshots" backend/services/hmm_data_source/
grep -r "strategy_packages" backend/services/hmm_data_source/
grep -r "paper_v2" backend/services/hmm_data_source/
```
- [ ] ✅ 所有 grep 返回空（或只在注释中出现）

---

### 3️⃣ 单元测试验证 (阻塞项 🔴)

#### 3.1 运行所有单元测试 ⏳
```bash
pytest tests/backend/services/hmm_data_source/ -v -m "not integration"
```
- [ ] ✅ 所有测试通过
- [ ] ✅ 无跳过的测试（除 @pytest.mark.integration）
- [ ] ✅ 无测试警告

#### 3.2 测试覆盖率 (可选) ⚪
```bash
pytest tests/backend/services/hmm_data_source/ --cov=backend/services/hmm_data_source --cov-report=term-missing
```
- [ ] ⚪ 覆盖率 > 90%（目标）

---

### 4️⃣ 部署配置验证 (阻塞项 🔴)

#### 4.1 数据库权限配置 ⏳

**需要 DBA 配合执行**:

```sql
-- 1. 创建只读用户
CREATE USER hmm_evolution_ro WITH PASSWORD 'change_me_in_production';

-- 2. 授予 market.* 表 SELECT 权限
GRANT SELECT ON market.kline_daily_raw TO hmm_evolution_ro;
GRANT SELECT ON market.sw_member TO hmm_evolution_ro;
GRANT SELECT ON market.trade_cal TO hmm_evolution_ro;
GRANT SELECT ON market.stock_basic TO hmm_evolution_ro;

-- 3. 创建读写用户
CREATE USER hmm_evolution_rw WITH PASSWORD 'change_me_in_production';
GRANT hmm_evolution_ro TO hmm_evolution_rw;

-- 4. 创建 schema
CREATE SCHEMA IF NOT EXISTS hmm_evolution;
CREATE SCHEMA IF NOT EXISTS hmm_risk;

-- 5. 授予读写权限
GRANT ALL ON SCHEMA hmm_evolution TO hmm_evolution_rw;
GRANT ALL ON SCHEMA hmm_risk TO hmm_evolution_rw;
```

- [ ] ✅ hmm_evolution_ro 用户已创建
- [ ] ✅ hmm_evolution_rw 用户已创建
- [ ] ✅ hmm_evolution schema 已创建
- [ ] ✅ hmm_risk schema 已创建
- [ ] ✅ 权限授予完成

#### 4.2 目录结构创建 ⏳
```bash
mkdir -p tmp/hmm_evolution_cache
mkdir -p logs/hmm_evolution
```
- [ ] ✅ 缓存目录已创建
- [ ] ✅ 日志目录已创建

#### 4.3 .gitignore 更新 ⏳
```bash
# 验证 .gitignore 包含缓存目录
grep "tmp/hmm_evolution_cache/" .gitignore
grep "logs/hmm_evolution/" .gitignore
```
- [ ] ✅ 缓存目录已加入 .gitignore
- [ ] ✅ 日志目录已加入 .gitignore

#### 4.4 部署脚本验证 (可选) ⚪
```bash
python scripts/deploy_hmm_data_source.py
```
- [ ] ⚪ 部署脚本执行成功

---

### 5️⃣ 功能验证 (可选但推荐 ⚪)

#### 5.1 回测数据源基本功能 ⚪
```python
# 在 Python shell 中执行
from backend.services.hmm_data_source import BacktestDataSource
from datetime import date

source = BacktestDataSource(
    base_loop_ref="qe_20260502_131502_9b54/Loop1",
    cache_dir="tmp/hmm_evolution_cache/",
)

# 测试获取可用日期范围
min_date, max_date = await source.get_available_date_range()
print(f"Available: {min_date} to {max_date}")
```
- [ ] ⚪ 可以创建回测数据源实例
- [ ] ⚪ 可以查询可用日期范围

#### 5.2 实时数据源基本功能 ⚪
```python
from backend.services.hmm_data_source import RealtimeDataSource
from datetime import date

source = RealtimeDataSource(lag_days=1)

# 测试获取可用日期范围
min_date, max_date = await source.get_available_date_range()
print(f"Available: {min_date} to {max_date}")

# 测试板块映射
mapping = await source.get_sector_mapping(max_date)
print(f"Sector mapping count: {len(mapping)}")
```
- [ ] ⚪ 可以创建实时数据源实例
- [ ] ⚪ 可以查询可用日期范围
- [ ] ⚪ 可以查询板块映射

#### 5.3 缓存管理功能 ⚪
```python
from backend.services.hmm_data_source import ArtifactCacheManager

cache = ArtifactCacheManager("tmp/test_cache/")

# 测试保存和加载
cache.save_artifact("test/loop", "test.pkl", b"test_data")
data = cache.load_artifact("test/loop", "test.pkl")
assert data == b"test_data"

# 测试缓存信息
info = cache.get_cache_info("test/loop")
print(f"Cache info: {info}")

# 清理
cache.clear_cache("test/loop")
```
- [ ] ⚪ 可以保存 artifact
- [ ] ⚪ 可以加载 artifact
- [ ] ⚪ 可以查询缓存信息
- [ ] ⚪ 可以清理缓存

---

### 6️⃣ 集成测试 (可选 ⚪)

#### 6.1 真实 QE artifact 下载 ⚪
```bash
pytest tests/backend/services/hmm_data_source/test_integration.py::TestDataSourceIntegration::test_real_qe_artifact_download --run-integration -v
```
- [ ] ⚪ 可以从真实 QE workspace 下载 artifact

#### 6.2 真实 DB 查询 ⚪
```bash
pytest tests/backend/services/hmm_data_source/test_integration.py::TestDataSourceIntegration::test_real_db_query --run-integration -v
```
- [ ] ⚪ 可以从真实数据库查询数据

---

### 7️⃣ 文档检查 (非阻塞 ⚪)

#### 7.1 README 完整性 ⚪
```bash
cat backend/services/hmm_data_source/README.md
```
- [ ] ⚪ 包含快速开始示例
- [ ] ⚪ 包含 API 文档链接
- [ ] ⚪ 包含故障排查指南
- [ ] ⚪ 包含配置说明

#### 7.2 设计文档完整性 ⚪
```bash
ls -lh docs/architecture/hmm_evolution*.md docs/architecture/*PHASE*.md
```
- [ ] ⚪ 总体架构设计文档存在
- [ ] ⚪ Phase 0 详细设计文档存在
- [ ] ⚪ 设计审查报告存在
- [ ] ⚪ 实施报告存在

---

### 8️⃣ Git 提交检查 (阻塞项 🔴)

#### 8.1 提交信息检查 ⏳
```bash
git log --oneline -1
```
- [ ] ✅ 提交信息清晰，包含 feat/fix 等前缀
- [ ] ✅ 提交信息包含 Co-Authored-By

#### 8.2 文件变更检查 ⏳
```bash
git diff --stat HEAD~1
```
- [ ] ✅ 只包含 Phase 0 相关文件
- [ ] ✅ 无意外的文件修改
- [ ] ✅ 无敏感信息泄露

#### 8.3 分支状态检查 ⏳
```bash
git status
```
- [ ] ✅ 工作目录干净
- [ ] ✅ 在 feature 分支上（不在 main）
- [ ] ✅ 无未追踪的重要文件

---

## ✅ 最终确认

### 阻塞项汇总 (必须全部通过)

- [ ] 🔴 语法检查通过
- [ ] 🔴 导入检查通过
- [ ] 🔴 隔离约束验证全部通过（10 项）
- [ ] 🔴 单元测试全部通过
- [ ] 🔴 数据库权限配置完成
- [ ] 🔴 目录结构创建完成
- [ ] 🔴 .gitignore 更新完成
- [ ] 🔴 Git 提交检查通过

### 推荐项汇总 (建议完成)

- [ ] ⚪ 测试覆盖率 > 90%
- [ ] ⚪ 回测数据源功能验证
- [ ] ⚪ 实时数据源功能验证
- [ ] ⚪ 缓存管理功能验证
- [ ] ⚪ 部署脚本验证
- [ ] ⚪ 文档完整性检查

---

## 🚦 验收决策

### ✅ 通过标准

**所有阻塞项 (🔴) 必须完成**，推荐项 (⚪) 至少完成 50%。

### ❌ 不通过标准

**任何一个阻塞项失败**，验收不通过，需要修复后重新验收。

### 🔄 下一步

**验收通过后**:

1. 合并到 main 分支
```bash
git checkout main
git merge feature/hmm-evolution-phase0-data-source --no-ff
git push origin main
```

2. 标记版本
```bash
git tag -a phase0-v1.0 -m "Phase 0: HMM 数据源抽象层"
git push origin phase0-v1.0
```

3. 进入 Phase 1 开发
   - 创建 Phase 1 详细设计
   - 开发 HMM 离线评估功能

---

## 📝 验收记录

**验收日期**: ____-__-__  
**验收人**: ________________  
**验收结果**: [ ] 通过 / [ ] 不通过  
**备注**: 

---

**当前状态**: ⏳ 等待验收

**负责人**: 请你完成上述检查项，确认是否可以合入主分支。
