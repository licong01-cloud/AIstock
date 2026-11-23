# 荐股名单逐列排序 F1 设计

## 背景

用户要求名单默认按排名升序，点击排名、股票、推荐状态、连续推荐、首次生效日、起始基准价、行情快照价、荐股期间累计涨跌、今日涨跌的表头，切换升序和降序。当前 `DailyList.Rows` 没有排序，名单由后端分页，行情来自既有手工刷新快照。

本设计仅增补现有荐股工作台的名单展示能力，沿用工作台设计与行情/推荐期间收益语义。

## 范围

当前名单、移出及其他股票分组、历史版本股票明细共用同一排序组件。各分组独立排序，保留分组、过滤、搜索和股票详情入口。排序覆盖当前分组及过滤条件的完整名单，不能只对当前页排序。

允许写入：

- `docs/architecture/advisory_list_column_sorting_f1_20261123.md`
- `frontend/src/components/advisory/LiveQuotes.tsx`
- `frontend/src/components/advisory/workbench/DailyList.tsx`
- `frontend/src/components/advisory/workbench/DailyListTable.tsx`
- `frontend/src/components/advisory/workbench/dailyListSort.ts`
- `frontend/src/components/advisory/workbench/useDailyItems.ts`
- `frontend/src/components/advisory/workbench/workbench.module.css`
- `frontend/tests/paper-v2/advisory-workbench.spec.ts`

验证和交付临时范围：本任务工作树的 `frontend/node_modules` 仅链接既有依赖，`frontend/.next` 为本地 lint 缓存；隔离 UI 测试和 PR 正文只使用 `X:/AIstock_temp/advisory-list-sorting-20261123` 临时目录及已有 3012 测试前端配置。不得操作用户的 3000/8001 服务，不安装依赖。

## 非目标与边界

不更改荐股顺序的业务权威、股票名单、原始排名、连续推荐/收益计算、费用、PIT、策略包、通知、历史回放或数据库。展示排序不回写后端。历史版本目录、策略对比指标表和跟踪过程表不属于本次股票名单排序范围。

## Design Acceptance Index

- F-001：九个数据列全部可排序；默认排名升序，新列首次点击升序，同列再次点击降序并持续交替。
- F-002：先对完整分组排序再本地分页，切列/切方向回到第一页；20/50/100 条分页保持可用。
- F-003：当前、移出/范围外/观察/待核验分组和历史版本股票明细使用同一实现；股票详情、过滤和搜索保持有效。
- F-004：数值按原值、日期按日期、股票及状态按界面中文文字排序；缺失/非有限数值及未知状态在两种方向均置后；相同值按原排名和原始行次序稳定排序。排名数字不随展示顺序重新编号。
- F-005：首屏先显示已有元数据；按现有上限每次最多 100 条分批读取完整元数据，仅渲染本地当前页。展开的分组首次读取全组行情快照并复用，后续排序/翻页不额外获取行情；手工刷新更新已展开名单快照。关闭分组不预取完整名单或行情。
- F-006：分页读取保持 program/list/binding/snapshot/target 身份、总数与页内容完整；切换目标及时取消旧读取，失败明确提示，禁止把部分数据当完整名单排序。
- F-007：保留现有页面颜色、字体和布局；表头显示排序图标及当前方向，支持键盘、焦点样式和 `aria-sort`。
- F-008：执行当前 Agent 的正确性、完整性与最终版本复核，以及四项 DESIGN-COMPLIANCE-001 检查；源码交付、前端运行验收与既有后台业务任务分别报告。

## 实施方案

1. 将表格抽为共用组件，排序键与取值定义集中管理，使用 `useMemo` 排序、稳定行 key，并在排序后切片分页。
2. 继续使用 `useRead` 读取首批元数据，其余批次使用同一接口并校验身份。初始页可提前展示，完整读取失败则显示错误并禁用完整排序。
3. 从现有 QuoteContext 直接读取数值；复用证券代码规范化函数和现有 register/cache/refresh，不重复实现行情或收益计算。不新增行情定时器、滚动监听或独立请求入口。
4. 表格按完整已读取分组注册行情；注册集合与展示排序、分页解耦。关闭的 details 内分组延后读取，避免首页预取大量隐藏内容。

## 验证方案

最小本地检查为变更文件 lint、TypeScript、直接 Playwright 用例、`git diff --check`、scope 和 Feature Workflow validate。直接用例覆盖全部表头切换、跨服务端页排序、未知值、键盘、历史明细及排序/翻页/滚动不新增行情请求。复用现有快照失败、身份隔离用例，不运行荐股推理或训练。广泛模块回归交由原有 CI/Nightly。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | DailyListTable.tsx; dailyListSort.ts | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-002 | DailyListTable.tsx; useDailyItems.ts | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-003 | DailyList.tsx; DailyListTable.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-004 | dailyListSort.ts; LiveQuotes.tsx | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-005 | DailyListTable.tsx; useDailyItems.ts | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-006 | useDailyItems.ts | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-007 | DailyListTable.tsx; workbench.module.css | frontend/tests/paper-v2/advisory-workbench.spec.ts | passed | 无 |
| F-008 | 本设计与 PR | validation-receipt: 本节最终版本审核和校验摘要 | passed | 无 |

## 风险与失败模式

完整字段排序需要完整分组数据；分批读取、延迟关闭分组和只渲染当前页限制首屏及 DOM 成本。行情逐批到达时未知值保留未知，排序使用同一快照的现有原值，手工刷新后按所选列重排。读取失败保持可见，不把缺失字段补零。

## 生产门禁

仅前端源码，无 DDL/DML、后端修改、依赖安装、训练、服务启停、模型/策略包激活或通知发送。现有源码自动交付授权启用：完成自审、最小验证、当前 HEAD CI 后创建并合入同一 PR、同步必要源码；不以源码合入冒充运行态或业务验收。后台全年回放及六包每日荐股不受本次操作控制。

## 执行与审核记录

分支 `feature/advisory-list-sorting-20261123`，独立工作树 `F:/Dev/AIstock_worktrees/advisory-list-sorting-20261123`，基线 `de9ea352aeac371815196a232b790237b1a122e9`。用户已授权实施，未设置 no_commit/no_pr/no_merge 限制。审核由当前 Agent 执行，不冒充独立审核；后续实测与修订只更新本节和验收矩阵。

预执行审查：PASS。只运行变更文件 lint、TypeScript 和匹配名单排序/快照的直接 UI 用例；复用已安装依赖，不安装软件。3012 须空闲，测试框架只启停其隔离前端，接口由测试路由接管；不操作 3000/8001，也不触发荐股、回放、训练或通知。保留实际结果、失败与修订，不以用例收集代替执行。

第一轮正确性复核：实际项目 TypeScript 和变更文件 lint 通过；12 项 UI 用例执行得到 10 通过、2 失败。失败为测试在表格 section 内寻找位于外层的错误提示，已修正定位，未削弱错误、禁用排序及禁止完整分页的断言。另复用现有有效证券代码校验，防止全组注册绕过行情单元格的合法性检查；为无效列表项增加明确中文错误。修订影响的最终测试须重跑，首轮通过不冒充最终版本结论。

工具例外：预装依赖缺少 Windows `.bin` 命令入口，RTK/npm 入口失败；直接使用已有 `node_modules/typescript/bin/tsc`、`next/dist/bin/next`、`@playwright/test/cli.js`，没有安装依赖。RTK tsc 首次退出码 1 未记为通过。

第二轮完整性复核：两项失败用例已单独复跑，2/2 通过。检查桌面与手机真实组件截图，保留同一页面风格、表内横向滚动与分页；将空名单提示放回分页前，并修正历史测试数据的名单计数。增加无效证券代码保留显示且不发送行情请求的回归。最终小矩阵覆盖排序、快照、名单筛选/身份切换及历史明细。

真实数据只读验证预审：PASS。仅 GET 既有 LSTM 策略的已发布历史版本、工作台简要信息、名单分页、就绪信息与显示行情/推荐收益；捕获结果保存在本任务 X 盘目录，并原样用于隔离前端验证。禁止发布、推理、训练、通知、配置或数据库业务写入；不改变原始字段或用模拟价格替代失败数据。

第三轮回归与兼容性复核：实际项目 TypeScript、lint 通过；22 项相关 UI 用例中 20 通过，包含真实 API 捕获（目标日 2026-11-24、20 只在荐、27 份有效价格，7 次 GET，无业务写入）。两项失败分别为开发模式中已取消首包被计入统计、旧历史视图按固定表头可访问名称查询失败。trace 证实首包状态 -1，随后 0/100 两页均 200；改为精确统计 200 响应，继续要求排序/翻页零新增请求。表头增加固定 aria-label 与 scope，按钮仍描述下一次排序动作；不放松旧历史用例。

最终版本复核：两项兼容问题定点复跑 2/2 通过；稳定版本的最终相关 UI 小矩阵 22/22 通过，0 skipped、0 flaky，耗时 56.8 秒。报告 `X:/AIstock_temp/advisory-list-sorting-20261123/advisory-mocked-TgU1rw/results.json`，SHA-256 `3caf91e6b2e03c32343ee00527839002d8057903ba06c51834576f3cff106d31`。项目 TypeScript 通过，五个变更源码文件 lint 通过，最后修改的表格文件已重新 lint；真实数据捕获测试实际执行且通过。桌面/手机截图已检查。广泛模块回归由原有 PR CI/Nightly 承接，不声称已执行未运行的验证。

DESIGN-COMPLIANCE-001：四项均通过。无简化：九列及所有目标名单明细均实现，完整分组排序后分页；无静默错误：缺失保持未知，分页身份/完整性失败可见且禁止完整排序，原快照失败语义保留；无业务语义改变：只改变展示顺序，不重编号、不回写、不重算收益；无私增门禁：未增加审批、权限或运行发布控制。

与运行任务隔离：本次八个源文件不覆盖历史回放的 23 个 R5 代码闭包文件，已记录同步前内容哈希，主目录源码同步后将逐一回读。没有后端或数据库修改、依赖安装、服务重启、模型训练、通知发送或当前回放任务操作。源码合入与当前运行前端的只读加载确认分别记录于 PR。
