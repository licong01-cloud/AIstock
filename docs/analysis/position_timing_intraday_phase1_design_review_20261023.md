# 分钟择时阶段一详细设计审核记录

日期：2026-10-23。对象：[详细设计](../architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md)、[蓝图v1.1](../architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md)、[延后接入需求](../handoff/position_timing_intraday_localsim_integration_request_20261023.md)。

源码基线：`c8a6ad045fd38c3b57540b1d57fd3bd9e1a48b21`。方式：同一窗口顺序自审、修订后重新阅读；未使用子代理或Claude协调通道，不冒称独立第三方审核。本记录仅证明设计审核，不证明新接口已实现。

## 第一轮：源码复用与最小实施范围

| 检查 | 发现/证据 | 修订 |
|---|---|---|
| 模块所有权 | LocalSIM仍由`backend/services/simulation_execution/localsim/`拥有，现行TWAP-only不因新文档解除 | §1/§7与转交单明确暂不启动LocalSIM；先本页提醒，无第二账本 |
| API状态持有者 | 纯函数仅写next_state但初版示例漏previous_state | 蓝图、详细设计、转交单统一显式前态；无进程全局核心状态 |
| 前端测试发现 | `frontend/package.json`无Jest/Vitest；`playwright.config.ts`收集tests/**/*.spec.ts | 改为`frontend/tests/position-timing-intraday.spec.ts`，不加runner |
| 存储复用 | `PositionTimingArtifactStore._validated_event_payload`只支持旧日频事件 | 新intraday_v1独立schema；只复用canonical/锁/原子写范式，不污染旧事件 |
| 交易日与时段 | 全局TradeCalendarProvider提供交易日，不等于session | 显式session快照、连续竞价产品范围；不按工作日或固定午休缺失量猜数据 |
| 费用与成交 | LocalSIM StepFill/费用是账户结果，提醒不是成交 | 核心回放只输出决策/延迟意图，经济结果延后，native与matched基准分开 |
| 过度工程 | 少量策略需要状态，但不需要常驻调度/新平台 | 仅三规则、现有页面、批轮询、文件存储；两阶段不增加审批/模型收益门禁 |

以上逐项修订后进入第二轮，不用F2结构检查替代语义审核。

## 第二轮：失败路径与因果闭合

| 检查 | 发现/风险 | 修订/结果 |
|---|---|---|
| 初始化/页面恢复 | 日内全量前缀可能被误解释为过去已观察到并补发历史提醒 | §4.1仅最新合格bar产生当前机会，旧前缀只预热；离线逐轮询时钟 |
| 批幂等与崩溃 | 未约定部分计划提交后响应丢失；同key异payload可能先污染状态 | §5.2先冻结批请求，再逐计划幂等，最后批receipt；恢复不回填过去时间 |
| head/claim一致性 | “唯一提交点”不能同时指状态head与独立claim | 明确计划状态head、提醒授权no-replace各自提交；不引入分布式事务 |
| 接纳时钟 | 用请求开始as_of会把随后收到的行情视为已知 | §5.1逐计划以实际输入完成时刻评估，批内时间可不同 |
| 源状态 | 只读GET不能保证外部计划从未被撤销 | 返回最后核验时间，evaluate/claim核resolver，未配置ADVISORY不可伪造 |
| D3/D5有效期 | 初次90秒有效期与长延迟相冲突；重新成立可能无意重置计时 | 连续验证续期、不重置首次提醒；失效则取消，不复活旧意图；计入未完成率 |
| 午休/截止 | 90秒续期可能越过session或价集期限 | 有效期取最小，左闭右开窗口；闭市不claim、不跨午休恢复旧意图 |
| 输入质量 | 有价零量/NaN/OHLC无序不应被当成可执行信号 | 显式数值合同及NO_OBSERVED_TRADE；不补0、不默默换源 |
| UI来源信任 | props source resolver措辞与服务端核验冲突 | 前端仅源刷新回调，后端resolver才是authority；不同责任名称清楚 |

第二轮修订完成后再次检查上位蓝图、详细设计与转交单：入口签名、批路由、LocalSIM延后、四档延迟、核心不成交、无模型前置均一致。设计内无遗留必改项；真实代码、盘中freshness与经济效果仍待后续证据。

## DESIGN-COMPLIANCE-001与F-ID逐项验收

| ID | 本轮设计结论 |
|---|---|
| F-101 | 仅分钟/LocalSIM，先核心提醒，后owner接入；未宣称整个阶段完成 |
| F-102 | 明确未来精确文件清单；无新增服务平台或跨模块实现 |
| F-103 | 计划/来源/人工未知数量/revision合同闭合 |
| F-104 | 时间、单位、raw/limit、bar完成与源失效闭合 |
| F-105 | B0/C1/C2公式、15/5格、累计分批和无价集语义固定 |
| F-106 | 机会与decision身份分离，claim不是送达/成交 |
| F-107 | 有界批API、GET只读、逐项失败显式可见 |
| F-108 | 状态与claim提交点、幂等请求、崩溃恢复有明确合同 |
| F-109 | 本页先挂载、隐藏暂停、过期/旧revision不可重弹 |
| F-110 | 冻结前缀、prepare/run/inspect/retry只作决策回放 |
| F-111 | 延迟含实际轮询/网格等待；意图失效与闭环区别明确 |
| F-112 | LocalSIM后续账户评价，不自建NAV或称提醒即交易 |
| F-113 | 最小业务测试、前缀扰动、定向CI；不堆重复快照 |
| F-114 | 实际接口/样例就绪才转交；非盈利/审批/未来行情门禁 |

禁止简化交付：仅报告设计完成，实现状态NOT_IMPLEMENTED；两个消费者未接入前不称完整产品。禁止静默错误：typed缺失/取消/失败，无行情或策略fallback。禁止改变业务逻辑：旧日频策略、TWAP release、费用和实验结果不动。禁止私增门禁：不设审批、模型正收益、自然日等待或MDE准入；延后跨模块接入是用户明确要求。

## 静态检查与交付边界

第三轮完成结构、链接与变更范围检查。首次F2校验发现合并的Index/Matrix标题未满足矩阵标题组，已拆出正式Design Acceptance Matrix小节；不放宽validator。随后实际执行结果：

```text
python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md --tier F2
PASS: design_items=14 matrix_rows=14 warnings=0
python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_intraday_advice_localsim_blueprint_f2_20261023.md --tier F2
PASS: design_items=12 matrix_rows=12 warnings=0
git diff --check / git diff --cached --check: PASS
本分支新增五文档的相对Markdown链接：14个，broken_links=[]。
```

审核期间origin/main推进到`50ea76e23bd47b1b7425812d4ad4240248fb0b68`；相对源码核查基线只增加两份HMM/LocalSIM BUG元数据，不改变本文依赖的源码合同。文档PR包含此前蓝图与本次详细设计，共六份文档；旧总蓝图仅增加导航与进度说明。

本轮未运行pytest/策略回放/服务，不把未执行项记为PASS。无BUG修复、无源码/依赖/数据库/数据集/进程改动，无需后端重启或BUG close-sync。CI与merge结果以PR中对应head的实际记录为准，不在提交前预填通过。按文档与合入工作流执行，不因文档合入删除未获本次明确清理授权的工作树。
