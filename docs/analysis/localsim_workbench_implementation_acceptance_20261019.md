# LocalSIM 交易工作台实施与验收记录

依据：`docs/architecture/localsim_trading_workbench_detailed_design_20261019.md`，F1，LSUI-01 至 LSUI-16。用户授权完整开发、多轮审核修复后直接提交合入；后端重启由用户执行。

## 任务计划

1. 已完成：最新 origin/main 独立工作树，接续已批准设计，核实经济表、现有控制与导航边界。
2. 已实现：类型化只读经济集合、稳定分页、名称投影、完整持仓、按日 / 股票盈亏及导出。
3. 已实现：右侧工作台全部视图与个股下钻；配置 / 回放 / 旧路由使用可读展示，保留原控制能力。
4. 执行中：财务与数据隔离审核、界面与无编号审核；修复后定向回归及 F1 门禁。
5. 待执行：独立 PR、CI、按授权合入；报告源码、真实只读证据、性能与运行激活边界。

## 明确写入范围

- 本设计、此记录及必要的概要设计验收索引。
- `backend/services/simulation_runtime/localsim_workbench.py`、`localsim_workbench_models.py`、`localsim_workbench_presentation.py`。
- `backend/routers/simulation_runtime.py`。
- `backend/tests/simulation_runtime/test_localsim_workbench.py`。
- `frontend/src/lib/simulation/localsim/workbench.ts`、`control-catalog.ts` 及现有 `api.ts`、`presentation.ts`。
- `frontend/src/app/simulation/localsim/page.tsx`、`workbench/page.tsx`、`components/trading-workbench.tsx`、`components/trading-workbench.module.css`。
- `frontend/src/app/simulation/localsim/configure/page.tsx`、现有账户详情 / 账本 / 绩效页与回放列表 / 详情、`localsim.module.css` 及专属展示组件。
- `frontend/tests/paper-v2/localsim-trading-workbench.spec.ts`、现有 `localsim-unified-control-plane.spec.ts`（更新已变更的入口、业务标签与安全错误断言，保留控制面不变量）。
- `noxfile.py`、`tests/aistock_validation/catalog/test_plans.yaml`、`file_ownership.yaml`：将新工作台测试纳入现有 LocalSIM 验证入口及归属，不启动生产服务或数据库写验证。

禁止改动：公共左侧导航组件、菜单数据、根 layout、全局样式；订单 / 资金 / 持仓写入、回放控制合同、共享选股、PIT、数据生产及数据库结构。临时产物放 X 盘，不复制大型模型或数据。

## 进度与证据

初始核实：账本接口含 LIMIT，不能作为完整历史；持仓需按权威快照 run 选取。现有经济表保留证券名称、数量、可卖、成本、成交和逐笔现金费用，可以直接消费，不需要第二套成本算法。资金含费用的账本现金差不能再重复扣费。原导航由根布局提供，本工作台只渲染其右侧。

尚未实施或验收的内容不得写为通过；不以演示数据代替真实账本证据。任务期间不执行生产写入、回放启动或后端重启。

实施中复核：只读服务已具备同事务事实集合、签名游标、完整导出、逐笔成交资金核验及逐股库存勾稽。配置和回放入口保留原生写操作，界面改为业务字段；未执行生产写操作。首轮 17 项财务测试中发现宽松 Base64 会接受游标尾部额外字符，已改严格解码，失败节点复跑通过。前端 TypeScript 首轮两处时间类型已修复，随后全项目类型检查通过。

真实只读初测回执：`X:/AIstock_temp/LOCALSIM/workbench-implementation/real-readonly-r2.json`。总览 3 账户，读取约 2.79 秒；账户冷读 1.84–4.07 秒，已有集合逐股查询 0.10–0.20 秒。当前未纳入任何实时可比汇总；两个回放账户尚无可用的收盘时点集合，历史账户存在估值日覆盖缺口，收益显示未知而非伪造零值。该现状不构成收益验收或运行态已激活。

第二轮审核：真实只读独立勾稽 `real-oracle-r3.json` 核对三个账户的 run、估值日、持仓数量、成本、市值、现金和净资产均一致。历史回放估值时间为盘中，不能仅凭 HISTORICAL_MINUTE_CLOSE 枚举判定收盘；界面已按真实时间标记。已有快照不含现金冻结拆分，相关字段保持未知。缩小缓存读取为专用列与估值 marks，避免载入页面不用的完整执行上下文。历史持仓忽略其他标签的区间日期；股票筛选不冒充账户逐日总收益。

验证中心首轮（候选 ac99273d）：类型和 lint、L0、simulation_core_l2 通过，模拟核心 1131 passed / 10 skipped，附加回归 63 passed / 1 skipped。UI 首轮在执行测试前遇到 X 盘 Next 构建缓存无法解析 React 的环境错误，已补 NODE_PATH 与临时目录依赖解析，待重跑。调度建议本次为确定性选择器，未调用远端 LLM；不记为外部模型审核。所有验证均未触碰生产 8001、未写数据库。
