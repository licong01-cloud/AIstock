# 美股共享输入与自动更新（F1）

## 背景

用户要求三大指数（SPX/IXIC/DJI）、VIX和美国交易日历进入本地数据管理、数据库与每日自动同步，同时核对此前七项新增输入的实际调度。
复用DatasetSpec、TushareSyncEngine、TDXScheduler、原始观察表、刷新审计和本地管理UI。

## 范围

不改HMM/QE/荐股/因子/PIT/历史数据集，不启动服务或实验，不新增依赖或调度器。
跟踪Issue #7149。DEV先行；生产迁移和运行态分别报告，不以源码完成冒充上线。

## 非目标

不新增流水线或验证框架；仅在既有local_data CI计划中登记新测试路径及所属文件，不改公共执行机制。

## 来源与数据合同

- `index_global`只采SPX、IXIC、DJI；官方doc_id=211，限4000行/请求。
- `us_tradecal`官方doc_id=253，日期全量包含开/休市，限6000行/请求。
- 指数保留OHLC/pre_close/change/pct_chg/swing/vol/amount；价格为点位，百分比为百分点；vol/amount源单位未明确，不转换、不强制非空。
- VIX复用现有表和原始观察；纽约session date与北京时间捕获时间分别保存。
- 日历保存cal_date/is_open/pretrade_date，不从它推造半日收市时间或published_at。
- 原始观察保留实际captured_at、内容SHA和NON_VINTAGE/UNKNOWN发布时间，不覆盖已有原文版本；事实upsert不得以NULL擦掉有效值。
- close与必要价格必须有限正数，其他数值必须有限或合法NULL；同键冲突、越界日期、错代码拒绝，空响应不冒充开市日完整。

## 增量与调度

- 美国日历每日07:30，刷新滚动近期及未来至少一年，初始化可从2018-08-01分段获取；不调用A股日历。
- 指数和VIX每日08:00/08:05，12:00/12:05补查，同一数据集重叠保护保持不变。
- 行情仅查询纽约已结束日期（17:00保守界，ZoneInfo处理DST），最近7自然日尾窗回看吸收供应商晚更新；历史初始化显式窗口。
- 每次增量额外复查最多7个尾窗以前的已登记未完整日期；不扫描全历史行情、不让尾窗滑动遗忘真实缺口。
- 对美国开市日预期指数3行、VIX1行；休市日零行合法。日历未知或开市缺行保持可见，不产生假完整；后续同尾窗可自愈。
- 本批可选输入不阻断原A股全局完整状态。现有风险事件21:10/15/20和晨间08:10/12/14、指数公告21:30+08:20、增发21:40+09:20+有界项目复查、盘后20:35照旧，核对真实enabled记录且不重复造任务。
- 时间为北京时间计划唤醒，不宣称上游承诺此时发布；不以无限重试解决上游未发布。
- 迁移创建的新任务暂禁用，确认后端加载生产者后，仅启用这3个新计划；不重写既有操作员配置，不让旧进程提前执行未知数据集。

## 实施方案

新增两事实表、中文列注释和统计配置，使用显式迁移；延用共享raw观察。
API、初始化/手工/补到最新/统计/调度入口同步接入并显示中文名称。

## 验证方案

RED→GREEN覆盖接口参数、单位/NULL、日期/代码、美国休市/DST、有限重试、幂等、调度目录/API/UI；DEV迁移两次、真实短窗采集两次和独立进程读取，改动lint/编译/设计校验及CI。

## 风险

上游延迟、未来日历未公布、网络或权限异常分别保留未知/缺口/失败，不猜测数据。
开市与北京时间日界分开，17:00纽约时间是保守查询界，不是历史公开时间证据。
事实内容可以修订，原始观察保留各个内容版本；不保证供应商历史版本全集，不作为已证明PIT输入。
默认补查为有界近期及最多7个已知旧缺口，非全历史扫描，不增加A股硬依赖。

## 生产门禁

现有DEV迁移和短窗读回已通过；生产迁移仅按具体授权执行，三个新计划在后端加载后才启用。
不控制服务；合入、生产迁移、计划启用、用户重启和运行验收分别报告。

## Design Acceptance Index

- F-001：规范字段、独立日历和三指数固定范围。
- F-002：原始版本/事实原子写入、NULL保护和幂等。
- F-003：美国日期界、补查、休市/缺口准确区分与增量。
- F-004：本地管理API/UI/中文统计/补到最新。
- F-005：默认调度与既有七项实际调度审查。
- F-006：既有DEV迁移、真实短窗、独立读回及生产状态分离。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/tushare_dataset_specs.py | backend/tests/test_tushare_us_market_inputs.py | verified | - |
| F-002 | backend/services/tushare_supplemental_inputs.py | artifact:X:/AIstock_temp/DATA_PREPARATION/us-market-inputs-20261109/dev-final.json | verified | - |
| F-003 | backend/services/tushare_us_market_inputs.py | backend/tests/test_tushare_us_market_inputs.py | verified | - |
| F-004 | backend/routers/ingestion.py; frontend/src/app/local-data/page.tsx | backend/tests/test_tushare_us_market_inputs.py | verified | - |
| F-005 | backend/db/init_tushare_schedules.py; backend/ingestion/tdx_scheduler.py | backend/tests/test_tushare_us_market_inputs.py | verified | - |
| F-006 | backend/db/migrations/add_tushare_us_market_inputs_v1.sql; scripts/validate_tushare_us_market_inputs.py | artifact:X:/AIstock_temp/DATA_PREPARATION/us-market-inputs-20261109/dev-independent-readback.json | verified | - |

## 本轮审核与交付状态

- 第一轮：接口/字段/范围；RED 15项失败后逐项实现，保留官方pct_chg和未知量额单位。
- 第二轮：事务/幂等/独立性；修正US written_rows为实际变更数，并避免向A股校验队列写入US日期。
- 第三轮：增量/调度；保留滚动尾窗及有界旧缺口复查，中午别名复用同一重叠保护；新计划迁移时禁用。
- 第四轮：API/UI/回归；344项定向测试、前端定向类型检查、Ruff、编译、L0与F1设计校验通过。
- DEV：2026-11-04..07，指数9行、VIX3行、日历4日；重复执行实际变更0；原始哈希和全部字段独立读回通过。
- 此前七项输入生产14个正式/补查计划均enabled=true，最近状态success；未重复创建或改写。
- 用户已授权审核CI后合入及精确迁移，SHA256=3fc9ecb5bba514ef3ba4b4f290c7eb77e0aaa76e43d3a36d4956180f270b8417。
- 生产迁移、用户重启、3个新计划启用、历史初始化和运行验收在正式读回后分别报告；以上DEV验证不宣称这些已完成。
