# BUG-2047：候选最小登记与后端运行态验证

## 调用边界

`scripts/register_manual_factor.py` 的 `register-candidate` 子命令在新 Python 进程中先调用 `scripts.factor_research.configure` 显式选定已有数据库，再调用 `ManualFactorService.register_candidate`。它不请求 API，不执行候选代码，不写源码资产，不调用分类、指标或评级。目标选择不构成数据库写入授权。

该新增方法当前只有 CLI 调用者；原有 API 的 `save_factor`、`full_pipeline` 和验证入口未改。但 `backend/routers/quantevolver.py` 的 `/factors/manual/template`、`/factors/manual/validate`、`/factors/manual`、`/factors/manual/full-pipeline` 都导入同一 `backend/services/manual_factor_service.py`。文件级 runtime catalog 因此仍将该文件归属 `backend-main`；不能把整个服务声明为离线工具，也不能把 CLI 的新进程成功当作现有后端已加载新源码。

## 已有证据与写入边界

- HEAD `57913be6a558c2e911e496184639e9cb2a223265` 的 7 项聚焦测试、显式 4 文件 L0，以及精确行业尾部候选的 DEV 插入、幂等重试、源码冲突回滚、Composer/official_offline 消费已通过。服务及 CLI 源码未因本次门禁修复而变化。
- DEV 收据位置：`X:/AIstock_factor_research/september-reassessment-20261023/catalog-delivery/dev-tail-registration.json`。DEV ID32 不是生产目录身份。
- 历史 ROC 候选收据位置：`X:/AIstock_factor_research/qe-catalog-registration-20261021/production-registration.json`。该历史证据不授予本次生产写入权限。
- 本次交付只补 CI 映射与运行态合同；不登记生产候选、不修改 ID206、不启动 QE、不操作用户服务。

## 源码合入后的后端合同

用户重启步骤沿用 [backend-main runbook](backend_main_runtime_restart_runbook.md)，不由当前工作流执行。预期 identity 为源码 PR 的完整 merge SHA，不能使用本次分支 HEAD 或磁盘 checkout 代替运行进程身份。

只读探针固定在 catalog 的 `http://127.0.0.1:8001` origin：

1. health：`GET /api/v1/health`。
2. identity：`GET /api/v1/runtime-identity`，由工作流核对进程加载身份及允许的 main 祖先关系。
3. business smoke：`GET /api/v1/factor-library/factors/price_volume_roc5_divergence_corr20?source=manual&expected_is_available=true`，使用已有 `factor_lifecycle_detail` 语义校验器，要求 `ok=true`、`domain=factor_library`、正整数目录 ID、精确因子名、`source=manual`、布尔可用状态符合声明。404、空对象、错误来源、禁用项和单纯 HTTP 200 均不能通过。

业务探针验证原始 ROC 候选的目录可见性与可用状态，不证明尚未登记的行业尾部候选已在生产，也不替代精确源码哈希、幂等和无额外写入的 DEV 事务证据。当前没有 DDL/migration，database readback 不另行声明为已执行。

用户完成该目标的重启后，才在最新 canonical main 执行：

```powershell
python scripts/aistock_issue_workflow.py post-restart-verify --bug-id BUG-2047 --target backend-main --expected-identity <SOURCE_PR_MERGE_SHA>
```

源码 PR 使用 `Refs #6523`，Issue 在完整 identity/business-smoke 收据通过前保持打开。合入、源码同步、用户重启、后端生效、close-sync、候选生产登记和 QE 启动分别报告，不以任一阶段替代后续阶段。
