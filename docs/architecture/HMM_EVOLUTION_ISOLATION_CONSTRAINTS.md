# HMM 演进与风险管理隔离约束

> 版本：v2.1（2026-07-17）<br>
> 权威上位设计：`hmm_evolution_and_risk_management_system_design_20260716.md` v1.5。

## 1. 核心定义

隔离表示“不干扰现有 QE/Selection/Paper/QMT/实盘状态”，不表示不能读取研究所需数据。

允许：

- 只读 QE task/loop/experiment metadata、recorder 和全部实验资产；
- 只读 canonical market 表的显式 as-of 或最新共同完成水位；
- 在后续获批阶段写入独立 `hmm_evolution.*` / `hmm_risk.*`；
- 在 repo `tmp/` 下写入受信任 HMM cache 与验证 receipt。

禁止：

- 修改、删除或执行 `model_train_configs`、`model_train_snapshots`、QE workspace asset 或
  `strategy_packages`；QE 配置和资产可只读查看，但不得自动应用为生产配置；
- 修改 `paper_v2.*`、Selection、QMT、模拟盘、实盘或生产 snapshot 状态；
- 使用不存在的预测表、未固化 watermark 的动态 `latest` 或不同 candidate 的混合数据；
- 无 manifest pickle、静默 fallback、中性结果伪成功；
- 未审批的 DDL、角色、GRANT、定时调度或自动生产替换。

## 2. Phase 0 与 Phase 1 只读资源

### Phase 0 QE data source

- `qe_evolution_tasks.node_id`：任务权威 compute node；
- recorder metadata：只解析 experiment/recorder 身份；
- remote artifact manifest：必须包含 SHA/size/row_count/schema/quality；
- `pred.pkl`、`label.pkl`：仅 manifest 验证后读取。

历史兼容仅允许一条 fail-closed 路径：当旧 QE workspace 缺少 recorder sidecar 和
remote manifest 时，可 inspection-only 读取有界 `run.log`，但必须同时满足：

- 日志包含唯一 `FINISHED` 的 `Latest recorder`，并由同一 identity 的 Qlib recorder-start
  事件交叉确认；
- complete catalog 中该 identity 同时拥有 `pred.pkl` 和 `label.pkl`；
- repo 内不可变 legacy manifest 对 `run.log`、pred、label 固化 SHA256/size/row_count，运行时逐项复核；
- 不按 mtime、目录顺序、文件大小或近似指标选择 recorder，不写回 QE workspace/Archive/Paper。

禁止把配置 JSON/YAML/TOML 加入 artifact 白名单。

该白名单只约束 Phase 0 自动下载、反序列化和归一化输入，不限制 Phase 1 对 QE 资产的
inspection-only 只读访问。

### Phase 1 QE 全资产 reader

- 可以列举、stat、hash 和读取 task/loop 下的配置、日志、模型参数、报告、pred/label、
  coefficient 和其它实验资产；
- reader 只允许 list/read/stat，不暴露 create/run/kill/cleanup/delete；
- 任意安全资产可以 `inspection_only` 查看；只有具备可信 manifest 和 parser receipt 的资产
  可以成为 `trusted_computational_input`；
- unverified asset 必须显式标记，禁止自动反序列化、执行或参与评分；
- Prediction Store 命中优先零副本；workspace 读取不默认固化永久副本；
- 已存在但损坏的 manifest/blob 不得静默 fallback；读取失败不得返回空内容假成功。

### Market

- `market.kline_daily_raw`；
- `market.trading_calendar`；
- `market.sw_index_member`。

查询必须是 SELECT、PIT。支持显式 as-of 和 `latest_common_completed`；后者必须解析所需数据集
各自 max date，选择共同完成水位并在请求入队时固化。禁止自然日近似、`CURRENT_DATE`、
`date.today()` 或 worker 执行时重新解析 latest。

## 3. Cache 信任边界

- cache root 可配置但必须在部署配置声明的独立目录；
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
- QE asset reader 测试必须覆盖 config/log/model/report 等类型，并证明 mutation client method 不可达；
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

禁止把 schema-only、backend-only、mock-only、静态页面、placeholder scorer 或其它简化版实现
报告为 Phase 1/2/3 完成；只能按 Design Acceptance Matrix 报告已真实交付的设计子集。
