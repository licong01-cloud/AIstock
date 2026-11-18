# 荐股盘中止损提醒（F1）

## 背景与目标

2026-11-19。依[荐股蓝图 §16.0](advisory_strategy_conditioned_model_blueprint_v1_20260710.md#advisory-intraday-exit-roadmap)实现零 fit 的规则提醒。用户批准规划后直接实施。将现行止损阈值和已确认进入价用于实际已收完的一分钟收盘价，向当日荐股页面给出可解释的风险观察。经济比较属于另一个任务。

## 范围

同一 Program、当前有效绑定、当前交易日已发布名单。用现有名单投影识别荐股范围内和范围外待复评，使用 FORWARD 已确认进入的活跃跟踪过程；排除已有退出信号的过程、模拟回放、无跟踪身份及未进入记录。跨绑定或状态不明不推断持仓。页面在当日推荐增加一个共享面板，两组分开标注；用户开启后，打开且可见的页面每分钟读取一次，也可手动刷新或暂停。历史页不消费此结果。

精确写范围：本文、上位蓝图（仅链接与交付状态）、backend/services/advisory_workbench/stop_alerts.py、backend/routers/advisory_workbench.py、backend/tests/advisory_workbench/test_stop_alerts.py、backend/tests/advisory_workbench/test_api.py（现有 GET 路由数断言）、frontend/src/lib/api/advisoryWorkbench.ts、frontend/src/components/advisory/workbench/MinuteStopAlerts.tsx、frontend/src/components/advisory/workbench/DailyList.tsx、frontend/tests/paper-v2/advisory-workbench.spec.ts。

## 非目标

保持原日频复评、Top20、选股和退出本金意义，沿用已结案结果。提醒不产生SELL指令、订单、自动退出、加仓或买回，也不调整止损阈值、不训练、不跑经济对照。无新增数据库结构、账户数据权限、服务调度或运行启用操作。已有明确SELL计划继续由择时窗口负责。

## 实施方案

1. 通过现有 ReadRepository 的只读快照，读取当前日官方交易日历、启用方案、活动绑定、目标日发布版本、名单及对应有效跟踪状态，使用现有进入价和方案止损配置。名单补列须有可核对的跟踪身份。历史日期、关闭方案、缺名单、未开盘、规则禁用各有明确状态。批量读取完成后释放数据库快照。
2. 复用 TDX 原始元单位读取和 position_timing 的闭合分钟归一化。仅连续竞价时段（不含集合竞价）、已实际收到且收盘后已有5秒的非零成交量分钟可触发。以当前交易日从连续竞价开始的应到分钟检查连续性及新鲜度。当日才确认进入的记录从确认后的首根完整分钟开始检查；午休不期待行情，收盘后只展示该交易日的截至时点结果。只取最早命中分钟，金额/收益沿用日频规则的费前原始基准：收益bps=(分钟收盘价/已确认进入价-1)*10000，等于负阈值也触发。缺失、过时、重复冲突、错日期或不可解析的分钟不冒称未触发。已证明的早期触发可显示，同时注明后续覆盖不全和最后观测时点。相同绑定、跟踪过程、交易日的提醒 ID 稳定，续读不追加记录。
3. 独立 GET /api/v1/advisory/workbench/programs/{program_id}/minute-stop-alerts，绑定请求目标日和发布时间对应的名单ID。响应有观察时间、覆盖条数、首触发时间及参考价、规则阈值、进入价和分开的可卖状态。本次新进入份额标 T+1 限制，其他账户可卖统一待核验，缺账户事实不发卖出指令。有停牌记录时标注其存在，具体时段及账户可卖仍须核对。TDX 读取限制为单进程一个有界并行批次，超时或批次繁忙时未读到的个股明确待核验；没有市场数据回退。行情读取后复核名单、规则和跟踪身份，身份漂移返回来源变化，请求刷新，避免展示已被日频退出替代的旧提示。
4. 面板复用荐股页面组件和读取反馈。只在当日推荐视图且所选名单是上海日期的当日名单时呈现，开启后按上述时钟拉取。返回状态分开显示：未触发、已触发、待核验；可卖至少区分本次进入份额 T+1 受限、已知停牌、无账户证据。展示覆盖分母、行情截至时间，读取失败时撤下旧提醒并显示原因。历史及其它列表保持原合同。

## 风险与失败模式

TDX 源当日行情暂缺，或用户选的是非当日名单时，状态清晰可见。发现触发并不代表有账户可卖数量或已成交；当前展示是荐股策略参考。原始元价格沿用现行止损口径，报价不等于经过复权的账户收益。目标日期、方案、冻结名单和跟踪过程任何漂移，沿用现有失败可见原则。页面未开启或不可见时无持续监测服务。

## 验证方案

先验证边界（分钟收盘、阈值等号、午休、缺行过时、已退出优先、T+1、未知可卖），再验证真实入口、输入字段和失败状态，最后做荐股页面浏览器验证。运行 git diff --check 及 feature workflow validate。运行证据区分本地测试、已存在权威数据的只读加载、用户重启后实际运行和自然行情。

## Design Acceptance Index

| ID | 验收目标 |
|---|---|
| F-701 | 只覆盖当前日两组中已确认的跟踪身份，日频退出优先 |
| F-702 | 不训练，固定原止损口径与闭合分钟，首触发去重并明确覆盖 |
| F-703 | 可卖独立显示，数据错误/过时显式可见 |
| F-704 | 正常 API 与当日页提醒可用，页内刷新与历史隔离 |
| F-705 | 只读集成，保持原日频信号、成交和生产边界 |

## Design Acceptance Matrix

设计 PR #7360 已合入。以下为源码验收。现有 DEV 库已验证只读事务、当日交易日历以及跟踪和停牌表；DEV 当日没有已发布名单，真实盘中提醒和页面运行状态按生产门禁另验。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-701 | stop_alerts.py：名单及跟踪身份 | backend/tests/advisory_workbench/test_stop_alerts.py | PASS_SOURCE | none |
| F-702 | stop_alerts.py：闭合分钟止损 | backend/tests/advisory_workbench/test_stop_alerts.py | PASS_SOURCE | none |
| F-703 | stop_alerts.py：读数和可卖状态 | backend/tests/advisory_workbench/test_stop_alerts.py | PASS_SOURCE | none |
| F-704 | advisory_workbench.py / MinuteStopAlerts.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | PASS_SOURCE | none |
| F-705 | stop_alerts.py：只读快照 | backend/tests/advisory_workbench/test_api.py；backend/tests/advisory_workbench/test_stop_alerts.py | PASS_SOURCE | none |

## 生产门禁

DDL/DML、依赖和模型激活均 noop。源码交付独立于用户负责的后端重启和前端载入。部署后用真实目标日和权威现有分钟回查提醒；当前无符合时点或无触发时只验对应实际状态。
