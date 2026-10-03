# H-TIMING-1 一次配对买入价值研究 F2

## Background

父设计H-TIMING-1已由#5327合入；计算/只读来源切片#5331已合入592fd305f9147ddfc3eaf87b8bed52d905352bed，不重复实现。首中末三日预检均20/20可产。386D/7720原候选输入准备已完成，7710行两个新量均已知，10行UNKNOWN保留；耗时607.703秒、486次只读SELECT，无收益读取或模型拟合。来源仍CURRENT_DB_HISTORICAL_NON_VINTAGE而非native。新标签连接准备核验train=4153、validation=1556，仅作为新共同监督人口，不复用旧3117/1507 mask。本切片仅独立研究，经济合格模型仍0；实际训练与经济结果须另按已登记run报告。

## Scope

精确允许十一文件，全部Advisory：
1. backend/services/advisory_model_first/economic_entry_timing_contracts_v1.py
2. backend/services/advisory_model_first/economic_entry_timing_training_v1.py
3. backend/services/advisory_model_first/economic_entry_timing_inference_v1.py
4. backend/services/advisory_model_first/economic_entry_timing_pipeline_v1.py
5. backend/services/advisory_model_first/economic_entry_timing_evaluation_v1.py
6. backend/tests/advisory_model_first/test_economic_entry_timing_contracts_v1.py
7. backend/tests/advisory_model_first/test_economic_entry_timing_training_v1.py
8. backend/tests/advisory_model_first/test_economic_entry_timing_inference_v1.py
9. backend/tests/advisory_model_first/test_economic_entry_timing_pipeline_v1.py
10. backend/tests/advisory_model_first/test_economic_entry_timing_evaluation_v1.py
11. 本文。

新增纯描述量及source由#5331提供。既存标签/政策/冻结输入/registry/原子stage/pool/hash工具仅调用、不修改。临时执行脚本与产物在任务计划单独列精确路径，非公共新入口。

## Non-goals

不修改QE/Selection/策略包/HMM/数据准备/公共验证/Paper/Execution。无上游alpha、重选Top20、重新产生标签、分钟执行、新UI、权重sidecar、sealed、DB写/激活/安装/服务控制。后端重启用户执行。旧负研究不追加证据或窗口。未确认模型不启用角色。

## Architecture / Approach

原生源码读取边界与历史来源等级分开。验证父原子stage与请求链、既存v2标签及已消费窗口；新输入stage绑定原候选KEY、实际两腿/权重、package/policy/cost、原值core/timing/source实现hash、单D值hash。允许COMPUTATION_ONLY/current non-vintage研究来源，不制造原生scope。

消费既存标签使用独立只读snapshot adapter：沿v3→v2→v1精确引用/原子stage/registry验证，在读任何收益标签前重核已消费窗口与原数据集/政策身份，再绑定旧标签内容与请求。**不调用旧study续跑/旧模型loader、不改旧implementation hash、不把当前旧模块字节与历史版本强行同一化**。旧label算法版本以其已登记request身份保留，仅作新研究的明确历史输入，旧模型训练资格不提升。新训练源码仍精确绑定当前新family。

实现算法身份显式使用UTF8_LF_BYTES_V1（只将CRLF规范为LF，不忽略其它字符）；artifact byte SHA仍原始字节。首次输入由F1树原始源码字节绑定，后续checkout仅换行转换产生不同hash；在fit前用同一#5331 Git blob证明原执行字节仅为LF/CRLF两种形式，生成独立successor输入stage并保留原源manifest引用及执行raw hash。原值及daily hash不改、不重复DB、不覆盖原stage，不以事后修改scope挽救结果。其它任何源码差异依然fail closed。

## Contracts / 固定监督、预测与研究身份

新13D control=(*12D,query)，15D candidate=(*12D,*2timing,query)。两个新拟合，各mean净收益/q90 entry-loss，共2配置4head。scope/recipe/实现/标签hash拟合前固定，LightGBM4.6.0，200轮/lr.05/depth3/leaves7/minleaf30/seed20261002/2线程，父request窗口、成本、政策、标签内容不变。旧v3/v4 weights与矩阵不能消费本scope。

完整新14D+query有限、return/v2风险均AVAILABLE、label_end purge后的共同监督集合；同核的新值不能被旧8D缺失mask反向屏蔽。所有原KEY仍保存；labels只提供训练目标/query/split成熟信息，不进入D特征。train-only15D min/max与gap桶支持（100bps/30观察/5D/真实minmax）统一两臂预测mask，与未来label成熟/缺失无关。validation只诊断，不选参数/校准；test禁止fit/支持统计。

预测两头有限/风险0..10000，明确TAKE(mean>0,q90≤800)、SKIP、UNAVAILABLE。800为冻结研究参考，不是资金预算；T真实开盘tick/法规/停牌检验与D reference沿原观测合同，UNKNOWN控制不升级成推荐或生产fallback。

研究复用原shadow simulator；三臂baseline/control/candidate，保留Top40持有排名上下文、Top20原候选及Top5空槽、不补第6名；相同equity日历与终端/现金基准。只在结算时消费未来路径，不能未来标签影响预测支持。以全共同估值日配对净值报告两个增量、绝对收益、MDD、尾损、干预日/真实TAKE/控制episode贡献/错失盈利/避免亏损。5日block/2000次/seed20261002双侧95%为描述性；相同block均值标准误乘固定2.801585218112967，报告双侧alpha=.05、名义功效=.8的正态近似MDE代理。它是评价后的方差描述，不是观测功效或确认门禁；少于10日或零方差时MDE标UNPROVEN，不引用旧候选MDE。regime未预定义标UNPROVEN，不事后分组。

新lineage按现有只追加registry，EXPLORATORY_SCREEN/RISK_MANAGED_ADVISORY/NAVIGATION_ONLY，两配置四head预算及已有消费窗口。确认、方向关闭、角色激活均不授权。only两点增量正且非零真实动作变化才建议继续评估；不以一笔交易支持有效结论。无支持/非正/零干预停止当前candidate，不改参数/扩大窗口、不关闭价格方向。exact retry验证原plan/stages不重新fit；中途失败不伪造TRAINED，fit锁防重复。

fit_attempt.json为锁内exclusive持久开始标记并fsync，registry记录FIT_STARTED/尚未知完成头数。不是新公共stage；中断或部分写入均阻止隐式再fit，成功四head数量由trained metadata真实记录。公共publisher仍只用既有preregistered/prepared/trained/evaluated。

## Implementation Plan

1. 精确范围与设计自审；#5331已合入，当前研究树已同步最新main，来源依赖不再阻断。
2. 实现独立合同/共享eligible与支持/唯一预测kernel；最小fixture保护真实四head训练及test毒化不改变fit。
3. 实现原子prepared/preregistered/trained/evaluated及registry，身份/hash/旧scope拒绝测试。
4. 实现观测条件与三臂结算，真实干预/UNKNOWN贡献及收益边界测试。
5. 多视角循环审核、失败节点修复、稳定叶矩阵与F2验证；独立PR/CI通过可直接合入。
6. 公共资源状态确认无QE训练才执行一次本Advisory研究；来源准备可并行。正候选后续消费者单独登记，不提前开发。

## Verification Plan / Design Acceptance Index

| ID | 合同 |
|---|---|
| F-631 | 新13/15显式合同、绑定同核recipe/父身份/4heads，拒绝旧scope |
| F-632 | 共同监督与purge、train-only支持，test/未来成熟不影响fit/预测 |
| F-633 | 条件单节点及数值/支持/真实价格fail closed，UNKNOWN保留 |
| F-634 | 原子stage/registry/新lineage/exact retry/源hash与边界 |
| F-635 | 原冻结三臂shadow、真实干预与全日经济指标，NAV不激活 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-631 | backend/services/advisory_model_first/economic_entry_timing_contracts_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_contracts_v1.py | PASS | none |
| F-632 | backend/services/advisory_model_first/economic_entry_timing_training_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_training_v1.py | PASS | none |
| F-633 | backend/services/advisory_model_first/economic_entry_timing_inference_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_inference_v1.py | PASS | none |
| F-634 | backend/services/advisory_model_first/economic_entry_timing_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_pipeline_v1.py | PASS | none |
| F-635 | backend/services/advisory_model_first/economic_entry_timing_evaluation_v1.py | backend/tests/advisory_model_first/test_economic_entry_timing_evaluation_v1.py；test_economic_entry_timing_pipeline_v1.py真实四头/三臂fixture | PASS | none |

## Risks / Review

设计自审1：新的core值和新block各自归因，不以旧feature缺失mask决定新监督集合；共同15维支持两臂一致。设计自审2：来源当前DB不等于原生，原候选、两腿权重和Top40持有上下文不得误换，收益只在结算读。设计自审3：一次NAV研究不是确认/激活，不新增平台，若负停止candidate，单一主线。

源码循环审核1（身份/PIT）：独立snapshot adapter在读取标签stage前授权已消费窗口；精确原子hash/request/政策链校验，不放宽旧源码loader。源码循环审核2（统计/监督）：核对新共同人口、test毒化不影响四头与train-only共同支持；补足两项增量的数值MDE代理及退化边界。源码循环审核3（恢复/交易）：修复训练失败隐式重拟合风险，持久exclusive fit marker；外来候选、法规价格矛盾必须fail closed，正常停牌和缺失保留UNKNOWN。随后定点测试及稳定叶矩阵复验。以上为本窗口不同视角自审，不宣称外部独立评审。

DESIGN-COMPLIANCE-001逐项：①本次完整实现已批准的一次探索研究链路，不把工程PASS宣称经济有效或daily-grid交付；②合法UNKNOWN和矛盾拒绝分开，无伪造native/零价格/填零成功；③候选、政策、成本、固定特征与两配置四头不变，参数/窗口不事后放宽；④既存registry/publisher复用，不引入新平台或等待未来日期的验证门禁。真实试验与经济结论另报，不以fixture收益替代。

## Production Gates / Rollout / Rollback

仅本模块源码和离线产物；无业务数据库写、服务、依赖、生产角色操作。正式输出仍NOT_CONFIGURED。源码与训练/经济状态分别报告，回退只停止独立新family消费，不覆盖旧工件。确认研究需另预登记合法未消费窗口，当前不执行。
