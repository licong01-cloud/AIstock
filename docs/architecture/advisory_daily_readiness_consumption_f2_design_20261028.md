# Advisory 每日输入就绪消费与冻结依赖 F2 设计

日期：2026-10-28。F2/T3，沿用既有每日完整性权威，补齐共享依赖解析与Advisory消费。用户已授权本轮一并修复共享依赖和模型窗口，实际补数仍由数据窗口负责；继承多轮审核后直接提交合入及用户自行重启的授权。

## 1. 背景与目标

真实运行已加载BUG-2111，LSTM影子捕获仍为0。五包共享readiness均UNKNOWN；解析器只识别固定DataFrame变量名，遗漏既有资金流派生字段，把单个ModelAsset当作列表迭代，并用缺失的model_type推断序列窗口。原生发布16:30开始尝试；工作台只显示新就绪结果，未将其接入发布或影子输入。

本轮交付一个连续工程任务：正式冻结资产→确定性依赖/窗口→同一权威消费者readiness→预测输入绑定→发布前复核→可见等待/真实前向捕获。目的在于避免同步未完成或数据修订期间使用旧输入，不追加模型研究。源码正确、当前源数据完整、服务激活、首份预测与5TD经济结果分别验收。

引用：[共享完整性设计](daily_data_integrity_calendar_and_readiness_f2_design_20261027.md)、[工作台设计](advisory_recommendation_workbench_f2_design_20261028.md)。本设计接续工作台的只读展示边界；调度消费变更以本次授权及本设计为准。

## 2. 范围、非目标与模块边界

- 覆盖现有5个启用任务使用的正式package；规则对正式包泛化，不按这5个ID或因子名写白名单。父包缺失逐seed资产仍保留明确阻断。
- 共享data_integrity负责依赖、窗口和同版本分区就绪；Advisory只消费该结果，不能维护另一套“已同步”规则。StrategyPackage既有资产身份、输入捕获和分数工件机制复用。
- 固定原模型、Top50人口、双Top10、5TD、费用及UNKNOWN口径。0新增拟合、0QE、0研报/现金流/价格/Exit研究，0回填错过的前向预测。
- 不补采、不改正式包manifest或受保护资产、不改全局PIT、不运行DDL/生产DML、不切换数据集、不操作服务。已有调度运行行为只随后续用户重启加载新代码。
- 不另建调度器、队列、环境变量开关或人工放行流程。16:30只是开始检查的时间；包的consumer_ready决定是否具备输入条件，21:00不是成功证明。

## 3. Architecture / 依赖及窗口权威

### 3.1 静态因子依赖

每个因子从manifest的asset_ref读取并核验SHA256；读取大小/数量有界。静态解析代码，不执行因子，不导入它的自定义模块。

追踪既有受支持文件读取、变量赋值/别名、确定字段选择、join/concat及派生字段来源。支持既有daily_pv、daily_basic、bak_basic、cyq_perf、margin、sector及static_factors输入。以正式共享字段映射和派生公式确定原始数据集；moneyflow复用现有shares/CNY定义，不能重新猜单位。标量/索引/结果列不误当源字段；动态字段、动态文件或无法证明的来源返回UNKNOWN，并指出因子和位置。

保守依赖可以包含确定读取的额外源字段，但不能漏依赖或静默忽略未知输入；Alpha158冻结schema及其表达式同样核验，不能只因enabled为true就通过。完整闭包绑定规则版本、manifest和所有消费资产摘要。

### 3.2 模型及实际输入窗口

规范化ModelAsset对象/列表。优先复用已入包、资产闭包校验通过的runtime_asset_admission中model_kind；model_type为空不按名称推断。每腿绑定其实际冻结模型、预处理及代码身份；父包逐seed资产不齐继续UNKNOWN/资产阻断，不用主seed替代其余seed。

对神经网络读取配对冻结预处理中的实际step_len。不得运行pickle反序列化回调或模型代码，也不得在每个GET上启动WSL；支持的序列化结构以有界无执行元数据解析实现，不支持、重复冲突、非正整数或无配对身份即UNKNOWN。既有原生加载器对没有声明预处理的路径采用1时，只有该正式合同成立才复用，不因缺字段无条件填1。树模型使用正式已验model_kind的长度1。

窗口复用原生因子窗口/固定5日缓冲及序列长度-1，不以模型名字、标签horizon或250字样替代序列证据。必要时根据代码中确定的rolling窗口保守扩大，但不能改变原生推理合同或放宽预热。

## 4. Contracts / 共享readiness与输入绑定

### 4.1 同一每日权威

沿用`GET /api/v1/local-data/readiness?data_date=D&consumer=ADVISORY&binding_id=package_id`。UNKNOWN、WAITING、READY及具体原因分别返回；解析失败不能退回小时/全局完成布尔值。all_sync_complete与包就绪独立，不使用无关ETF/次日融资融券阻断不依赖它们的包。

readiness在短只读一致性快照中读取manifest、正式交易日历、PIT及完整窗口所需分区revision/validation_id。输入身份绑定依赖规则、包manifest、数据日期/窗口、PIT及分区版本；不把查询时间纳入稳定身份。每次消费都核验当前版本，缓存仅允许不可变资产解析、不能缓存READY。

### 4.2 原生发布消费

在未发布的Program执行Selection前检查其每个正式包的readiness。未就绪沿既有WAITING_DATA路径保存可见原因，不生成分数、不改已有名单。已发布幂等路径不因新数据检查被改写；其它包和目标开盘结算继续执行。

将稳定输入绑定纳入新分数工件配置，防止修订后重试复用旧输入工件；不覆盖旧工件。沿既有SelectionRun/ReviewRun/publication JSON记录包绑定与readiness摘要，无DDL。

原生输入读取已带同快照`input_capture`：新路径核对实际消费的分区版本、窗口和PIT与预检一致，模型计算退出读事务后执行。发布前再次读同权威；版本变化、验证失效、PIT变化或包/程序绑定变化均保留等待/失败，不把旧分数发布为新输入。发布保存的是明确输入时点，不承诺发布以后源永不变化；历史发布不可重写。

### 4.3 LSTM影子消费

依然只读原发布和原分数Top50。新捕获须读取原发布的输入绑定并核验，且在自身20TD行情/复权/基准特征快照中核对所需分区；这部分输入是影子额外依赖，不能仅凭原包就绪代替。处理完成后、immutable prediction落盘前再检查绑定及时钟。

未就绪不写prediction、不补零、不重拟合，显示WAITING_DATA及精确原因；T09:30后仍记MISSED。已有prediction/missed保持字节不变，旧缺乏新绑定的发布不能被补证成当时完整。历史收益成熟评价沿原合同，不把新入口变成重新读取旧标签的理由。

## 5. 可见性与失败模式

工作台沿用发布/结算独立状态及共享readiness区域，只补必要的可读原因，不增加人工放行按钮。影子状态接口展示等待输入与最新原因，浏览仍只GET不触发预测。

| 情况 | 行为 |
|---|---|
| 数据窗口迟到、历史预热缺回执 | WAITING_DATA；原调度按300秒继续检查 |
| 未知依赖/模型元数据 | UNKNOWN；返回精确资产/原因，禁止默认READY |
| 数据输入修订 | 旧绑定失效；新工件键，旧名单/预测不覆盖 |
| 原包资产缺失 | 保留原资产阻断；不重建或替换seed |
| 单包错误、就绪服务不可用 | 错误可见且按包隔离；不破坏其它包与结算 |
| 当晚未就绪且跨过T开盘 | 漏采不补作前向，不以延长时窗掩盖 |
| 无成熟预测 | 经济价值UNKNOWN；不关闭BUG-2111运行验收 |

## 6. Design Acceptance Index

- F-001：冻结因子与Alpha158完整静态依赖、别名及派生来源，哈希失配/动态未知拒绝。
- F-002：ModelAsset两种形态及资产闭包绑定的真实序列窗口，禁止名称猜测和执行pickle回调。
- F-003：同权威、同窗口、同版本readiness；全局完成与包就绪分离。
- F-004：未发布原生Selection前就绪检查、等待可见、单包隔离和已发布幂等。
- F-005：新分数工件键绑定输入版本，实际capture与预检核对，修订后不复用旧分数。
- F-006：发布前复核，持久化输入摘要；程序/PIT/分区变化不伪通过。
- F-007：影子独立20TD输入就绪与原发布绑定；落盘前复核、严格T开盘时限、零拟合。
- F-008：API/UI可见等待与精确原因；浏览不触发任务。
- F-009：真实五包只读解析/就绪验收与直接反例；源缺口与工程问题分开。
- F-010：三轮审核、源码合入及用户重启后验收独立；5TD收益不冒充本轮结果。

## 7. 实施方案与精确文件范围

设计阶段仅本设计及`docs/analysis/advisory_daily_readiness_review_20261028.md`。设计独立PR合入后，从最新main建立实现工作树。

实现允许文件：

- `backend/services/data_integrity/consumers.py`、`dependency_source.py`、`frozen_metadata.py`、`service.py`、`inputs.py`。
- `backend/services/advisory_forward/input_readiness.py`、`service.py`、`repository.py`；`backend/services/advisory_program.py`。
- `backend/services/advisory_model_first/lstm_spo_shadow_source_v1.py`、`lstm_spo_shadow_v1.py`、`generic_daily_db_input_v1.py`。
- `backend/services/strategy_package/selection_artifact.py`、`backend/inference_engine.py`，仅输入绑定/快照核验所需调用，既有分数与模型计算不变。
- `backend/services/advisory_workbench/service.py`、`frontend/src/components/advisory/workbench/ModelObservation.tsx`、`frontend/src/components/advisory/workbench/shared.tsx`、`frontend/src/lib/api/advisoryWorkbench.ts`，仅状态呈现必需改动。
- `backend/tests/test_daily_data_integrity_inputs.py`、`backend/tests/test_daily_data_integrity.py`、`backend/tests/test_daily_data_integrity_dev.py`；`backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py`、`test_generic_daily_db_input_v1.py`；`backend/tests/advisory_forward/test_input_readiness.py`及已有forward直接测试文件；`frontend/tests/paper-v2/advisory-workbench.spec.ts`。
- `tests/aistock_validation/catalog/file_ownership.yaml`、`test_plans.yaml`、`module_registry.yaml`、`backend/services/validation/plan_catalog.py`、`noxfile.py`，仅确有需要的新增测试归属/执行注册。
- 本设计、审核记录、`docs/operations/advisory_daily_readiness_consumption_20261028.md`、`docs/operations/evidence/advisory_daily_readiness_20261028/receipt.json`。临时脚本/回执在任务worktree的`tmp/advisory-readiness/`。

若发现其它必须修改的调用点，先在本节登记精确文件和原因；不自动扩展QE、HMM、LocalSIM、采集或启动脚本。

顺序：最新状态/边界冻结 → 三轮设计审核 → 设计PR → 共享解析及真实五包审计 → 原生发布消费 → 影子消费与可见性 → 直接失败合同/DEV事务验证 → 三轮实现审核修复 → F2/L0/CI → 源码PR合入和交接。

## 8. 验证方案

本地只跑改动点：别名/join/派生字段/动态未知、资产哈希、对象与列表、非执行序列元数据、依赖日期/分区缺口、预检后修订、工件重用、旧发布幂等、单包隔离、影子时钟与等待。注入恶意pickle回调证明不执行，不能用人工填入长度代替真实资产验收。

真实只读验收核对5包manifest/asset身份及依赖闭包，区别解析READY与数据WAITING；对LSTM/TCN核验配对预处理实际序列长度，禁止调用fit/predict。生产接口只GET，未就绪必须真实保留。

有数据库提交协议变更时，在现有DEV验证回滚/修订并发和发布拒绝；没有生产写入。源码全模块回归交PR CI，宽UI/跨模块由既有Validation Center/Nightly去重承接，不在本地重复全量。执行changed-file Ruff、直接测试、L0、diff/scope、F2验证。

三轮自审分别检查：业务/原身份与输入完整；并发/修订/幂等/失败隔离；逐项设计/实际证据/运行与经济分态。不声称独立外审。

## 9. Design Acceptance Matrix

设计阶段pending为真实未实现状态；不能以设计合入宣称完整功能通过。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/data_integrity/dependency_source.py | backend/tests/test_daily_data_integrity_inputs.py | pending | 待实施 |
| F-002 | backend/services/data_integrity/frozen_metadata.py | backend/tests/test_daily_data_integrity_inputs.py | pending | 待实施 |
| F-003 | backend/services/data_integrity/service.py | backend/tests/test_daily_data_integrity.py | pending | 待实施 |
| F-004 | backend/services/advisory_forward/service.py | backend/tests/advisory_forward/test_input_readiness.py | pending | 待实施 |
| F-005 | backend/services/advisory_forward/input_readiness.py | backend/tests/advisory_forward/test_input_readiness.py | pending | 待实施 |
| F-006 | backend/services/advisory_forward/repository.py | backend/tests/test_daily_data_integrity_dev.py | pending | 待实施 |
| F-007 | backend/services/advisory_model_first/lstm_spo_shadow_v1.py | backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py | pending | 待实施 |
| F-008 | backend/services/advisory_workbench/service.py | frontend/tests/paper-v2/advisory-workbench.spec.ts | pending | 待实施 |
| F-009 | docs/operations/advisory_daily_readiness_consumption_20261028.md | artifact:docs/operations/evidence/advisory_daily_readiness_20261028/receipt.json | pending | 待实施 |
| F-010 | docs/analysis/advisory_daily_readiness_review_20261028.md | artifact:docs/operations/evidence/advisory_daily_readiness_20261028/receipt.json | pending | 待实施 |

## 10. 发布、回滚与生产门禁

无新增DDL或数据迁移，production_ddl_gate=noop；production_dml=not_requested。验证源码可以合入，不等于线上已加载。本轮所有生产接口与资产核验只读；实际等待原因可以阻止新预测，但不能改历史名单或证据。

后端重启由用户执行，目标backend-main；合入后记录merge并给出只读验收命令。重启后检查runtime-identity、forward/status、共享readiness与首个新前向捕获。BUG-2111继续使用原语义验收，不弱化0捕获拒绝。

回滚通过正常源码revert及用户重启；输入版本工件与历史记录保留，不删除已生成预测或篡改时钟。数据尚未完整可以是诚实的长期等待；应交付精确源缺口给数据owner，不靠默认值绕过。
