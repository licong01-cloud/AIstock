# P20 LSTM SPO+ 每日并行观察（F2）

版本v1.1，2026-10-27。2026-10重排序序列。实现、定向验证和真实输入预览完成，PR/CI及运行状态见交付收据。

## Background / Goal / Scope

用户批准复用P19训练截止最晚的第3折，执行仅精确LSTM父包的每日并行观察。选择依据是2025-07-31训练截止，不按收益选折；本轮0fit，累计248实际fit＋1历史索引保持。此能力保存可审计的前向预测，不替换原生荐股或交易动作，不证明已取得前向收益。

包pkg_cc7eccb6202e4816b9d7c7d6b748bd37，manifest 93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6，run qe_20261007_141342_0221_L4，source qe_20261007_141342_0221；program advp_ed918504a02f4b15892f0dba65da139b，binding advb_9673d612b6d14668b4d7545fcee8d277。不同包、绑定或模型拒绝进入此观察。

原始model.json文件SHA 8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813，envelope SHA 50af0b697ec12e69474c4faa74561c2627df642c62c3ddf7c766ce3001dab12f。完整JSON复制到既有AISTOCK_ADVISORY_MODEL_ROOT/lstm_spo_shadow_v1/model.json；独立release.json绑定上述模型、程序和首次D=2026-10-27。模型系数/中位数/尺度不变，新发布身份与训练身份分开。原predict_specialist历史折限制不修改。

## Contracts / Inputs / 时钟 / 原生边界

当前生产Top20持仓策略的原生Selection输出40候选；本模型比较的native是同次原生分数资产的原始分数Top10，不是Top20持仓政策。通过PUBLISHED版本→Selection run→原per-package artifact配置的v2唯一键读取资产，要求资产创建不晚于原run完成，并对原生消费40行逐项校验symbol/原rank/score；验证包、manifest、D/T、完整原始排序/唯一键与内容hash，再固定Top50，不重新跑Selection或按新信息筛人口。V1 daily_selection_evidence的artifact_hash是DSE内容hash，并非分数资产hash，不冒充直接资产指针或原始PIT证明。缺资产或不足50行明确FAILED及原因，等待原源恢复，不补名单。

六个行情字段复用generic_daily_db_input_v1的20会话、精确D复权因子、原始股数口径；父分数z和rank/50沿P19计算。缺失保留NaN，由原模型冻结编码处理；不补零、前填、丢股票。快照保存50行完整输入、缺失计数、原分数资产身份、当前读取时点和源hash。源为当前DB NON_VINTAGE；保存前向输入不追认历史PIT或EOD采集完整。

D收盘后原生发布，T为真实下一交易日，必须在T 09:30前原子保存预测；保存后重试/重启只读原预测，不重算改榜。过时未预测日记MISSED_CAPTURE_WINDOW，不能在知道开盘后补作前向。正常每日300秒原生调度之后独立调用，异常返回显式失败，不使原生已完成动作失效。未配置或非目标程序保持原行为，不新增包准入门禁。

## Architecture / Implementation Plan / 持久化与评价

两个新Advisory叶模块提供纯预测、create-only文件存储、只读DB源和日常编排；既有forward调度后置调用，`GET /api/v1/advisory/programs/{program_id}/reranking-shadow`只读返回状态、50行双排名、成熟结果，可用target_trade_date定位。无新服务、线程、训练、库表或依赖；release和工件位于原模型根目录。每次只读事务30秒预算、候选上限50、分数资产上限10000，原生publication目录上限1000行并明确报超限；重启后扫描本试运行开始以来全部发布日，过去未采样日记MISSED，成熟补算每轮最多20日。

预测与评价分开：当H=T+4真实交易日已收盘后才读取固定D..H行情/涨跌停/停牌，沿GENERIC_FIXED5_VALUATION_EXECUTION_V2、buy0.95/sell5.95bps、10槽等权。KNOWN/CASH/UNKNOWN分别保留，不因缺失重排；单日两臂都完整才给delta，另报各臂未知槽和覆盖。成熟UNKNOWN首次结果保留，允许后续按新源快照另存评价版本、固定首个完整结果，不改预测、不覆盖旧UNKNOWN；结果显示是估值而非成交。尚未成熟不读取该日收益。

不设新的模型优胜门：报告新前向已观察天数、成熟完整天数、干预、原生/模型均值及delta；不自动激活、不追加fit、不把开发期正结果当作新前向。最早评价仍需5交易日是目标时钟本身，不是新增人工等待要求。

## Verification Plan / Design Acceptance Index

| ID | Requirement |
|---|---|
| F-571 | 精确第3折零fit装载，包/程序/绑定/模型身份与当前日期适配 |
| F-572 | 同次实际分数Top50、八字段复用/缺失保留、原生Top20政策不变 |
| F-573 | T开盘前不可变预测、幂等/冲突/重启与错过窗口显式记录 |
| F-574 | H后固定5TD双Top10十槽估值、UNKNOWN/版本保留且0提前收益读取 |
| F-575 | 现有每日调度后置及只读API，失败可见且不阻断原生 |
| F-576 | 真实输入预览/新进程/多轮审核、源码/绑定/运行/经济分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-571 | backend/services/advisory_model_first/lstm_spo_shadow_v1.py | backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py; artifact: X:/AIstock_temp/ADVISORY/p20-lstm-shadow-20261027/live_receipt.json | VERIFIED_ZERO_FIT_MODEL_AND_FRESH_PROCESS | none |
| F-572 | backend/services/advisory_model_first/lstm_spo_shadow_source_v1.py | artifact: X:/AIstock_temp/ADVISORY/p20-lstm-shadow-20261027/live_receipt.json | VERIFIED_4918_SOURCE_40_PARITY_50_INPUTS | none |
| F-573 | backend/services/advisory_model_first/lstm_spo_shadow_v1.py | backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py | VERIFIED_IMMUTABLE_REUSE_MISSED_AND_OPEN_CUTOFF | none |
| F-574 | backend/services/advisory_model_first/lstm_spo_shadow_source_v1.py | backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py | VERIFIED_CALENDAR_BEFORE_OUTCOMES_FIXED5_UNKNOWN_VERSIONS | none |
| F-575 | backend/services/advisory_forward/service.py; backend/routers/advisory.py | backend/tests/advisory_model_first/test_lstm_spo_shadow_v1.py | VERIFIED_OPTIONAL_HOOK_AND_READONLY_API | none |
| F-576 | 本文Verification Plan；结果报告 | artifact: X:/AIstock_temp/ADVISORY/p20-lstm-shadow-20261027/live_receipt.json; review_receipt.md | VERIFIED_PREVIEW_NOT_FORWARD_RUNTIME_PENDING_USER_RESTART | approved_by_user：用户原话“后端重启我来执行”；源码/绑定验收完成后运行加载由用户重启及只读核验单列，不冒充已运行 |

## 多轮审核和实际验收

R1查实V1 DSE hash与score hash是不同对象，改为原配置唯一v2资产及原消费分数逐项一致，未用最新/最多记录资产替代。R2直接测试发现package级KEY不能当三列日期KEY，修复日期投影并补齐publication和input的D/T一致性。R3补查重启漏日，改为扫描本次试运行全部原生发布日，错过窗口保留MISS；原UNKNOWN评价保留历史版本，首次完整结果固定。R4新进程重读实际P19文件、真实50输入和预测，再用独立NumPy矩阵/稳定排序复算，全部一致；不重复拟合或读取结果标签。

真实D=2026-10-26技术预览：原分数4918行，原生40行一致，八字段50行全部非缺失，双Top10有7只换入。该D的T已经开盘，只是技术预览，禁止计入前向收益或预测样本；首个新D仍为2026-10-27。原训练/研究源NON_VINTAGE限制保留。

四项DESIGN-COMPLIANCE-001：交付完整的零拟合每日观察源码及配置，运行需用户重启的状态单列；错误/缺失/MISS可见且不伪收益；原生Top20及研究八字段/5TD/成本不变；不新增人工确认、包准入、额外拟合或自动激活门。仅模型并行观察获授权，生产替换仍未执行。

## Risks / Production Gates / 非目标

当前数据版本与历史训练资料存在分布、缺失和vintage差异；新影子输出需积累真实观察。0fit/0QE/0DB写/0数据集切换/0服务控制。production_ddl_gate=noop，dependency/client=noop，backend_restart_required=true且用户执行。release文件准备属于已授权并行试运行；源合入不冒充进程加载，实际每日运行待重启后只读验收。研报/现金流/价格/Exit及其它包方向继续搁置。明确文件清单见任务scope.json。

## Rollout / Rollback

源码审核/CI合入、模型原文件和release create-only安装、用户重启、只读进程/API/首个预测验证按此顺序报告，绝不在当前旧进程强行执行新调度。无前端或数据库迁移。业务试运行完成的运行收据仍依赖重启后首个合法窗口。

停止观察时可由用户将既有环境配置中AISTOCK_ADVISORY_LSTM_SPO_SHADOW_ENABLED置false后按其方式重启，服务返回PAUSED、停止全部源读取并保留原模型/release/预测/评价，原生行为不变；true恢复同一身份。未设置时仅在release已安装情况下运行，其他取值明确报错。本轮不编辑环境、不重启，也不为回退删除任何数据。

复杂度：源分数最多10000行、一次排序有效性检查，两个数据连接均按50个唯一KEY一对一连接、不会多对多扩张；估值最多50×6行；原生forward中现有horizon排序未修改。L0三个P2复杂度提示（含原生旧位置一个）在此解释界限，0阻断。
