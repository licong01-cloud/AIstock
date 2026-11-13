# LocalSIM 新建连续研究的候选输入绑定修复

日期：2026-10-27。BUG-2107；对应双模式设计§6.3、F-151和F-160。本轮只修复LocalSIM离线研究消费接口，不修改择时核心、评分、撮合、费用、数据集或正式账户。

## 问题与最小设计

用户已授权LightGBM的181日、13个独立账户研究，各1000万元，之后再验证TCN/LSTM两账户基线。冻结前发现，数据窗口按其独立用户授权对同版本候选作了原地事实修正：清单完整SHA由`3d416575b4dbb3cdf6d17552908c38fcc03c96f4fe4b12fa30c6ee4d1db6a36a`变为`321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`。原研究的request/input_successor仍保持旧绑定，本轮不能编辑旧请求使其通过。

旧freeze只允许原candidate或已完成账户前缀的input_successor。全新研究需要显式绑定修正后候选，无需也不得先制造账户成交前缀。因此增加互斥的`--candidate`及完整SHA参数，写入新request的`input_candidate`；原v1请求不添加或改写此字段。

来源核验复用现有`inspect_successor_sources`，不复制价格、PIT或报价跨度算法。该核验在新request发布之前及每次执行读取账户journal之前完成。未提供完整参数、同时提供旧后继、格式错误、文件漂移及语义不兼容均失败；不静默选择active，也不制造价格。

本轮元数据预检记录615只证券、6152项价格组件和12项PIT组件，清单摘要无差异。它不代替正式全批文件核验，不证明账户收益。

## 修改与审核

- `dual_mode_research.py`：独立新研究候选绑定、重验、互斥和CLI完整参数校验；原账户执行路径不改。
- `test_localsim_dual_mode_comparison.py`：新候选真实原生账户撮合/费用及exact retry、旧请求不变、来源拒绝、参数冲突和错误显式输入不降级。
- 原详细设计§6.3补齐新业务绑定与旧任务恢复的边界。

R1审核：新建请求不依赖旧已完成前缀，不改原source request、原后继、模板或原研究人口。R2审核：错误`input_candidate`不能按假值解释为原candidate；来源检查在任何账户输出前执行；explicit candidate与旧后继互斥。R3审核：新增定向10项通过，最终相关矩阵78项通过，覆盖原生TWAP、连续账户、恢复、费用及只读查询；两组存在重叠，不能相加。Ruff、两项L0静态检查、`git diff --check`通过；F2设计16项/16行通过，无warning。

验证文件位于`X:/AIstock_temp/localsim-human-continuous-20261027/`：`input-binding-direct.xml`、`input-binding-related.xml`、`l0-quality.json`及`l0-guardrails.json`。全181日实际研究仍待新request冻结和执行；这些功能检查不能填作181日经济结果。

## 验收与运行边界

本修复属于离线CLI新进程调用；代码引用审查只发现该CLI和直接测试引用`dual_mode_research`，现有产品读取面不导入该执行模块。公共运行目录目前仍按`backend/**/*.py`将它归为backend，不能仅改BUG字段降低该推断；源码合入、离线新进程加载、现有后端加载及关闭登记分别报告。

离线研究不等待生产服务重启；没有启动、停止或重启backend、worker、scheduler。数据准备修复的三端回执独立，LocalSIM没有补数、执行SQL或写数据集。其他包缺少正式输入时不得复制LightGBM的目标或模板。

DESIGN-COMPLIANCE-001：不以短窗代替全区间；不隐藏输入/账户失败；旧TWAP、账户、成本及原研究经济事实不变；不新增人工确认或调参门禁。实际测试、CI、PR、merge和181日研究进度以对应不可变回执为准。
