# LocalSIM 交易工作台实施与验收记录

依据：`docs/architecture/localsim_trading_workbench_detailed_design_20261019.md`，F1，LSUI-01 至 LSUI-16。用户授权完整开发、多轮审核修复后直接提交合入；后端重启由用户执行。

## 任务计划

1. 已完成：最新 origin/main 独立工作树，接续已批准设计，核实经济表、现有控制与导航边界。
2. 已实现：类型化只读经济集合、稳定分页、名称投影、完整持仓、按日 / 股票盈亏及导出。
3. 已实现：右侧工作台全部视图与个股下钻；配置 / 回放 / 旧路由使用可读展示，保留原控制能力。
4. 已完成：多轮财务与数据隔离审核、界面与无编号审核；修复后定向回归及 F1 门禁通过。
5. 执行中：独立 PR、CI、按授权合入；报告源码、真实只读证据、性能与运行激活边界。

## 明确写入范围

- 本设计、此记录及必要的概要设计验收索引。
- `backend/services/simulation_runtime/localsim_workbench.py`、`localsim_workbench_models.py`、`localsim_workbench_presentation.py`。
- `backend/routers/simulation_runtime.py`。
- `backend/tests/simulation_runtime/test_localsim_workbench.py`。
- `frontend/src/lib/simulation/localsim/workbench.ts`、`control-catalog.ts` 及现有 `api.ts`、`presentation.ts`。
- `frontend/src/app/simulation/localsim/page.tsx`、`workbench/page.tsx`、`components/trading-workbench.tsx`、`components/trading-workbench.module.css`。
- `frontend/src/app/simulation/localsim/configure/page.tsx`、现有账户详情 / 账本 / 绩效页与回放列表 / 详情、`localsim.module.css` 及专属展示组件。
- `frontend/tests/paper-v2/localsim-trading-workbench.spec.ts`、现有 `localsim-unified-control-plane.spec.ts`（更新已变更的入口、业务标签与安全错误断言，保留控制面不变量）。
- `noxfile.py`、`tests/aistock_validation/catalog/test_plans.yaml`、`file_ownership.yaml`、`module_registry.yaml`：将新工作台测试纳入 LocalSIM 验证入口、直接 CI 执行目标及归属，不启动生产服务或数据库写验证。

禁止改动：公共左侧导航组件、菜单数据、根 layout、全局样式；订单 / 资金 / 持仓写入、回放控制合同、共享选股、PIT、数据生产及数据库结构。临时产物放 X 盘，不复制大型模型或数据。

## 进度与证据

初始核实：账本接口含 LIMIT，不能作为完整历史；持仓需按权威快照 run 选取。现有经济表保留证券名称、数量、可卖、成本、成交和逐笔现金费用，可以直接消费，不需要第二套成本算法。资金含费用的账本现金差不能再重复扣费。原导航由根布局提供，本工作台只渲染其右侧。

尚未实施或验收的内容不得写为通过；不以演示数据代替真实账本证据。任务期间不执行生产写入、回放启动或后端重启。

实施中复核：只读服务已具备同事务事实集合、签名游标、完整导出、逐笔成交资金核验及逐股库存勾稽。配置和回放入口保留原生写操作，界面改为业务字段；未执行生产写操作。首轮 17 项财务测试中发现宽松 Base64 会接受游标尾部额外字符，已改严格解码，失败节点复跑通过。前端 TypeScript 首轮两处时间类型已修复，随后全项目类型检查通过。

真实只读初测回执：`X:/AIstock_temp/LOCALSIM/workbench-implementation/real-readonly-r2.json`。总览 3 账户，读取约 2.79 秒；账户冷读 1.84–4.07 秒，已有集合逐股查询 0.10–0.20 秒。当前未纳入任何实时可比汇总；两个回放账户尚无可用的收盘时点集合，历史账户存在估值日覆盖缺口，收益显示未知而非伪造零值。该现状不构成收益验收或运行态已激活。

第二轮审核：真实只读独立勾稽 `real-oracle-r3.json` 核对三个账户的 run、估值日、持仓数量、成本、市值、现金和净资产均一致。历史回放估值时间为盘中，不能仅凭 HISTORICAL_MINUTE_CLOSE 枚举判定收盘；界面已按真实时间标记。已有快照不含现金冻结拆分，相关字段保持未知。缩小缓存读取为专用列与估值 marks，避免载入页面不用的完整执行上下文。历史持仓忽略其他标签的区间日期；股票筛选不冒充账户逐日总收益。

验证中心首轮（候选 ac99273d）：类型和 lint、L0、simulation_core_l2 通过，模拟核心 1131 passed / 10 skipped，附加回归 63 passed / 1 skipped。UI 首轮在执行测试前遇到 X 盘 Next 构建缓存无法解析 React 的环境错误，已补 NODE_PATH 与临时目录依赖解析，待重跑。调度建议本次为确定性选择器，未调用远端 LLM；不记为外部模型审核。所有验证均未触碰生产 8001、未写数据库。

## 最终技术收据

- 源码候选：`3fc60171aa3e73c5569a03a356d89d93b2cbbd77`；后续仅补充验收索引、CI 直接用例映射及实际内容宽度 390 的测试证据。
- UI：`valjob_20261019_163857_12e125c9`，24 passed，39 秒；使用真实应用壳和受控 API 测试事实，未访问生产后台。旧控制不变量 14 项、工作台 10 项，完整证券抽屉、121 条部分成交、完整导出、七标签、错误与身份检查均覆盖。
- 类型 / lint：`valjob_20261019_164001_ad81c37b` PASS。曾与 UI 同时运行导致 Next 生成类型文件竞态，串行重跑通过；不以失败回执冒充通过。
- 静态：`valjob_20261019_161913_e18a9f44` PASS；核心：`valjob_20261019_161915_24926900` PASS，1131 passed / 10 skipped，附加 63 passed / 1 skipped。最终工作台财务专属 37 passed，流式导出 HTTP 合同补跑通过。
- 真账本只读：`X:/AIstock_temp/LOCALSIM/workbench-implementation/real-oracle-r3.json`、`real-optimized-r4.json`。三个账户分别 50 / 50 / 25 只，与原快照 run、日期、数量、成本、市值、现金、净资产全部勾稽一致。无账户或回放写操作。
- 优化后总览 2.24 秒；账户冷读 1.66 / 3.06 / 1.32 秒，固定集合逐股读取 0.233 / 0.111 / 0.095 秒。峰值工作集 461,275,136 字节（约 440 MiB），缓存三个账户事实；此前完整上下文读取曾达约 660 MiB，两轮采样负载并非严格基准，不声称固定比例收益。原限 200 行账本 0.16–0.23 秒与完整历史不是同规模比较。
- 磁盘：`disk-r1.json`，X 盘 Next 验证缓存 192,636,005 字节（约 184 MiB）；临时证据单独记录；模型与数据复制为零。临时目录未写 C 盘。
- 多轮修复：严格游标解码；旧日事实变化与集合绑定；逐笔资金 / 库存 / 资产勾稽；仅投影必需列；历史持仓独立日期；个股区间继承；回放表单日期与总览解耦；目录错误独立可见；拒绝身份必须有非空错误；旧无效地址回固定总览；导出币种、成本、估值与覆盖元数据；原导航折叠行为用原组件验证。
- F1：16 个 LSUI 项目保持稳定，对应新增校验器所需验收别名；`python scripts/aistock_feature_workflow.py validate --design docs/architecture/localsim_trading_workbench_detailed_design_20261019.md --tier F1` PASS，16 items / 16 rows / 0 warnings。新增工作台用例由独立 CI target 直接执行，不以目录归属或类型检查代替执行覆盖。

## 已知事实与运行边界

两个原生历史回放账户尚无收盘时点快照，旧账户历史估值覆盖也不完整。页面完整显示已有持仓和交易；缺少收盘证据的累计 / 逐日收益保持未知。冻结现金与佣金税费分项未在原账本单列，保持未知；不得在 UI 另算或补写。后续若修复历史估值，属于 LocalSIM 经济 owner 的独立任务，本任务不改账户、账本或回放断点。当前无须数据窗口补数或荐股窗口改共享选股。

实际宽 390 的右侧内容已单独截图与回归（整窗宽 698）；原 260 像素公共左栏完全不变。整窗 390 时只检查全局不横向溢出，不宣称已改造既有移动外壳。业务金额十进制格式化，正负号与盈亏颜色并存。

源码、CI、合入、根 main 同步与运行加载分别记录。生产 DDL / DML、训练、补数、模型部署、服务控制、回放接续均未执行；无新增依赖。用户重启目标为 `backend_main`，操作流程见 `docs/operations/backend_main_runtime_restart_runbook.md`；合入后仍需由用户重启并进行只读身份与页面业务验证。
