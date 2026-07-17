# HMM 演进与风险管理隔离约束

> 版本：v2.0（2026-07-17）<br>
> 权威上位设计：`hmm_evolution_and_risk_management_system_design_20260716.md` v1.1。

## 1. 核心定义

隔离表示“不干扰现有 QE/Selection/Paper/QMT/实盘状态”，不表示不能读取研究所需数据。

允许：

- 只读 QE task/loop 身份、recorder、可信 artifact manifest 和白名单数据 artifact；
- 只读 canonical market 表；
- 在后续获批阶段写入独立 `hmm_evolution.*` / `hmm_risk.*`；
- 在 repo `tmp/` 下写入受信任 HMM cache 与验证 receipt。

禁止：

- 修改或下载 `model_train_configs`、`model_train_snapshots`、`strategy_packages`；
- 修改 `paper_v2.*`、Selection、QMT、模拟盘、实盘或生产 snapshot 状态；
- 使用不存在的预测表、隐式 `latest` 或不同 candidate 的混合数据；
- 无 manifest pickle、静默 fallback、中性结果伪成功；
- 未审批的 DDL、角色、GRANT、定时调度或自动生产替换。

## 2. Phase 0 只读资源

### QE

- `qe_evolution_tasks.node_id`：任务权威 compute node；
- recorder metadata：只解析 experiment/recorder 身份；
- remote artifact manifest：必须包含 SHA/size/row_count/schema/quality；
- `pred.pkl`、`label.pkl`：仅 manifest 验证后读取。

禁止把配置 JSON/YAML/TOML 加入 artifact 白名单。

### Market

- `market.kline_daily_raw`；
- `market.trading_calendar`；
- `market.sw_index_member`。

查询必须是 SELECT、PIT、显式 as-of；禁止自然日近似和 `CURRENT_DATE` 隐式漂移。

## 3. Cache 信任边界

- cache root 可配置但必须在批准的独立目录；
- loop 使用 digest 目录，artifact 只接受安全 basename；
- QE provenance 为默认必需；测试 provenance 显式 opt-in；
- 原子写、跨进程锁、TTL、容量、淘汰、safe clear 均 fail closed；
- reparse/junction/symlink 不得被递归删除穿透；
- 反序列化只发生在远端 manifest SHA/size 已验证后，row count 随后核对。

## 4. 分阶段写边界

| 阶段 | 允许写 | 禁止写 |
|---|---|---|
| Phase 0 | `tmp/hmm_evolution_cache/`、验证 receipt | 所有 DB 表、配置、交易状态 |
| Phase 1 | 获批 bootstrap 创建的 `hmm_evolution.*` | QE/StrategyPackage/Paper/交易状态 |
| Phase 2 | 获批 bootstrap 创建的 `hmm_risk.*` | `RiskDecision`、can_buy、订单/持仓 |
| Phase 3 | 独立候选 registry/训练任务 | 自动替换生产 HMM；自动 cron 未单独批准时禁止 |

Phase 1+ schema 只能通过独立幂等 Python bootstrap 创建，需 `COMMENT ON`、开发库复跑、
回滚方案和 production DDL gate。Phase 0 helper 不承担任何 DB 部署。

## 5. 测试约束

- 普通 PR：`nox -s hmm_data_source_backend`，无外部依赖、无服务、无 DB 写；
- 外部 smoke：`nox -s hmm_data_source_readonly_integration`，显式环境开关和坐标；
- integration 只允许 SELECT/QE read，不通过试写 production 表验证权限；
- 禁止在测试中 CREATE/DROP 临时 schema/table；
- 未运行受控 integration 时必须报告 pending，不能用 mock/静态 grep 代替。

## 6. 生产门禁

Phase 0 当前三门均为 `noop`：DDL、backend dependency、frontend dependency。代码合入、
生产 DDL、依赖安装、服务运行和 Phase 1 readiness 是不同状态，报告时必须分开。

## 7. 审计问题与修复

- BUG-688：QE client/node 与真实下载 API；
- BUG-689：同步 DB/canonical schema/candidate identity；
- BUG-690：remote manifest/cache 安全；
- BUG-691：专用 CI 和只读 integration gate；
- BUG-692：退役 unsafe deploy helper、收敛文档。

任何后续代码若绕过上述边界，应登记新 BUG，不得以“先跑通”为由加入 fallback。
