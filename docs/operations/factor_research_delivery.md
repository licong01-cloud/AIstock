# 因子研究正式交付：复用现有工具

先读[唯一研究方法](../analysis/factor_research_methodology.md)。本参考不是新的入库实现，也不是生产动作授权。P1 研究 CLI 仅写研究记录，不自动晋升候选。

## 既有能力与准确入口

执行前确认当前目标、源码版本、具体因子、日期和数据组件；不要照搬个人旧 skill 的固定本机路径。以下为当前源码接口，不声称本次已经做过生产调用。

| 操作 | 既有入口 | 副作用与完成证据 |
|---|---|---|
| 手工代码验证 | `/api/v1/quantevolver/factors/manual/validate` | 执行候选代码、产生验证输出；不是只读 GET |
| 正式保存 | `/api/v1/quantevolver/factors/manual`；`ManualFactorService.save_factor` | 写源码文件、catalog、asset_path，并尝试分类；同名会覆盖，必须是明确获准动作 |
| 官方独立指标 | `/api/v1/quantevolver/official-evaluation/compute` | 官方单写源写指标；明确日期/数据源并逐项读回，不能用研究指标替代 |
| 兼容指标入口 | `/api/v1/quantevolver/factors/batch-compute-metrics-unified` | 已转调官方 writer，不是第二个计算标准 |
| 分类/评级 | 现有 FactorAnalyst / FactorRatingService 和正式流程 | 分类失败可能返回空但保存成功；分别报告，不推断全部完成 |
| 相关性 | 现有 correlation owner 工具 | 核对实际 API/有效样本/计算范围；旧说明 compute-incremental 路由不可直接照抄，底层同名方法也不保证只算新旧对 |

`scripts/register_manual_factor.py` 的 API save 方式使用实际 `/factors/manual`；不要使用其直接 DB 方式绕开当前资产关联和正式 writer。无真实接口或合同缺口时给 owner 需求，不编写备用直写 SQL。

## 正式交付原则

1. 明确新因子或版本更新；保存前查同名因子、源码与已知消费者。新研究默认新候选名，不覆盖旧实验引用。未确认引用完整性不声称可以安全淘汰。
2. 按已有流程在既有 DEV 验证对应 DDL/DML，生产仅对获准目标/动作执行；不能从“代码合入”推导入生产授权。
3. 保存、官方指标、分类、评级、相关性分别记录结果和实际日期/批次。不存在必须恰好四窗口的旧断言，以当前官方窗口合同和返回值为准。
4. 相关性数据没有有效配对证据时写未确认，不把数值 0 当独立；不把增量接口名字当作内存/复杂度证明。
5. 在研究记录保存正式交付引用与下一步；失败保留实际已完成部分，不覆盖/回滚其他业务数据，不重新计算已成功结果。

不提供通配因子淘汰 SQL、不自动改 is_available、不安装依赖或重启服务。正式因子库及 QE 需求由相应 owner 实现，本窗口只维护研究方法、记录和候选运行。

## 客户端与数据库状态

Repo 的 factor-research/develop-factor 入口共用方法正文。个人 profile 同步交单一 client-sync owner；先确认同步工具支持业务入口，不由研究模块创建安装器。未同步的旧 profile 不能被报告为已升级。

P1 两表 migration 位于 `backend/migrations/factor_research_p1_20260908.sql`，伴随精确 preflight 和 rollback 文件。仅用明确选中的既有数据库执行，不在 CLI import 时建表。rollback 只适用于空表且无人依赖；有研究记录时采用兼容修复，不删除记录以完成回滚。
