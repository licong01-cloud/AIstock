# Advisory 直接消费 QE 策略包 F2 详细设计 v1

2026-10-04。用户已明确：QE实验与进入策略包的组合由QE保证无未来数据泄露，Advisory直接使用，不得重复设置准入门禁。本设计撤销此前消费者重复要求父模型、processor及组合权重时钟的安排；不声称补齐了不存在的历史证明。

## 1. Background / 业务目标

父包LGB/LSTM权重真实存在且SHA一致；R2-M1已完成四头训练，开发NAV为正但两配对区间跨零。此前研发停下不是缺权重或行情，而是Advisory重复审查QE资格及M1正导航后的自设研究分流限制。用户本次明确调整这两项安排，恢复直接消费和有界不同模型研究。旧收益、标签、权重、窗口、原生收据及历史判断不改写。

## 2. Scope / Non-goals / 精确实施切片

本设计文档树`advisory-qe-package-direct-use-design-20261004`只改本文、蓝图、M1日频设计、M1确认设计及M5设计五文件。源码分两小PR实施，不合并成跨模块大PR：

1. 消费者修订：`backend/services/advisory_delivery_preflight.py`、`backend/tests/watchlist/test_advisory_delivery_preflight.py`、必要的同叶API测试；`backend/services/advisory_model_first/economic_sector_price_readonly_bundle_v1.py`及同叶测试。分别对应直接接受策略包与读取既存冻结模型，不新增资格平台、审批、豁免token或数据库字段。
2. M5接续：仅自己的已登记树中的`economic_selection_state_price_v1.py`、`economic_selection_state_pipeline_v1.py`、共享`economic_sector_price_value_v1.py`/`economic_sector_price_pipeline_v1.py`及四个同叶直接测试；进度只更新本文、M5设计及蓝图。必要reader兼容修订按第1切片。允许正M1前序，先更新设计再新增登记/fit。旧M1数学、权重和研究产物不覆盖。

不修改QE、StrategyPackage、Selection、HMM、公共数据、Execution、Paper、CI或AGENTS；不提交QE、不写数据库/DDL/DML、不激活数据/模型或控制服务。后端重启user-owned。没有将任何旧字段改成native COMPLETE，历史恢复限制只作披露。

## 3. Architecture / 责任边界

QE已发布策略包 → Advisory按明确package/manifest/股票池/政策消费 → Advisory自己的D可见特征/价格模型 → 买入价格集合/空集/UNKNOWN及效果报告。QE负责父包训练无泄漏；Advisory不读取训练权重pickle以重新审查、补原conf、要求processor fit_end或组合学习截止，也不以这些字段缺失安排重训。

模型效果报告和运行权限分离。既有MDE、CI、收益/回撤/TAKE指标用于诚实描述，不是策略包消费、开发、回放或新模型实验的准入门禁；欠功效/未确认不是任务自动停止理由。仍不得把开发收益称为独立确认或真实成交收益。

## 4. Contracts / 正常错误，不是资格门禁

保留输入解析、实际package/manifest/pool/policy身份对齐、文件损坏/哈希漂移、缺失推理依赖和D/T时间边界的正常错误处理；它们说明实际输入不一致或无法计算，不是对QE做二次资格审查。正常停牌/缺报价/板块知识未知保留全部原候选并返回UNKNOWN，不拒绝整个策略包，不假补数值或收益。

`AdvisoryDeliveryPreflightService`不再调用`asset_eligibility.summarize`，不依据其分数、blockers或退役状态追加Advisory拒绝条件；状态可原样展示。包已存在/manifest可解析即按QE权威接受，返回明确`qualification_authority=QE_STRATEGY_PACKAGE`、`qualification_rechecked=false`。旧`asset_eligible`字段兼容表达委托接受，不宣称逐文件可读；物理推理错误由实际读取报告。不要求股票池声明必须出现在唯一primary路径；已有声明一致可直接使用，缺声明如实标注legacy，不回填当前配置。矛盾或非法明确声明仍报输入错误。

M1只读reader核对记录的原plan/source receipt及真实权重字节/数学schema，不以当前producer文件整体hash不同拒绝已冻结JSON模型，也不以旧收益门真假决定可加载性。原执行代码身份保持为历史记录；reader使用当前明确JSON推理ABI，验证维数/字段顺序/模型hash及价格数学兼容，不能任意换recipe、支持、参数或成本。

## 5. Research / 当前任务恢复

M5独立登记允许已完成正M1前序；旧M1正/负结论均不被改判。新M5唯一变量仍是5D Top5持续性、20D Top20持续性及前D右删失rank改善，原共同监督/匹配/价格支持/成本/四头和四臂不变。使用原已消费开发窗口，登记为EXPLORATORY_SCREEN/NAVIGATION_ONLY并计入同研究族，绝不称为新独立OOS。

累计预算仍按15已完成fit+M5最多4、一个已有index核对；这是事先确定的计算预算，不是包资格或收益门。fit前后只读确认QE三running路径空闲，不控制QE；新实验临时X、正式F独立identity，不覆盖旧run或partial隐式重fit。sealed/新holdout没有获得读取授权，继续不读；另行独立确认属于可选效果研究，不是开发/使用门禁，也不强制自然等若干交易日。

## 6. Implementation Plan

先多轮设计审核/F2/合入；从最新main独立修消费者，重复审核/定向测试/必需CI/合入；同步自己的M5准备与最新main，修其正M1阻断、核对真实M1冻结reader兼容；在源码冻结和新登记后一次M5四fit/完整四臂，记录真实结果继续下一有实质区别假设。每日M1 family/API/UI按正常功能开发推进，不等父包时钟或收益确认；未实现的接口仍明确待开发，不报整体完成。

原48h截止2026-10-06 02:36不重计。不得以搜原证明、保存旧失败证据、准入平台或无效监控凑时长。用户本次明确改变后续运行安排，不把这一变化伪装为旧试验事前合同。

## 7. Verification Plan / Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-739 | QE包直接使用，父/processor/组合时钟不再重复审查或阻断 |
| F-740 | preflight不调用eligibility，明确委托语义及缺/矛盾股票池正常处理 |
| F-741 | 冻结JSON模型不依赖当前producer文件/收益门，真实身份与数学兼容保留 |
| F-742 | M5正前序允许、新登记及累计研究偏差/预算/0sealed/不改旧结果 |
| F-743 | 正常缺失保留、未来输入和损坏仍报错，无虚假native/收益成功 |
| F-744 | 精确Advisory范围、多轮自审/直接验证、合入与用户重启分开 |

最小源码矩阵：eligibility属性访问即抛错也能接受真实包；退休状态仅展示；一致的非primary pool直接消费、矛盾拒绝；原M1真实冻结bundle可读且价格同值、错误模型/hash/顺序不接受；M5正/负完整前序均允许但外来identity/累计计数矛盾拒绝。保留现有四臂/normal-missing/PIT测试，不新增大量快照或重复fixture。

## 8. Design Acceptance Matrix

消费者C1已随#5429合入、自身清理，运行后端待用户重启；C2已完成多轮自审及一次真实M5研究，当前源码PR尚未合入。完整日频family/API/UI仍未交付，不称整项完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-739 | backend/services/advisory_delivery_preflight.py | backend/tests/watchlist/test_advisory_delivery_preflight.py | VERIFIED_C1 | none |
| F-740 | backend/services/advisory_delivery_preflight.py | backend/tests/watchlist/test_advisory_delivery_preflight.py; backend/tests/watchlist/test_advisory_delivery_preflight_api.py | VERIFIED_C1 | none |
| F-741 | backend/services/advisory_model_first/economic_sector_price_readonly_bundle_v1.py | backend/tests/advisory_model_first/test_economic_sector_price_readonly_bundle_v1.py; artifact: 原20候选15D数值兼容 | VERIFIED_C1 | none |
| F-742 | backend/services/advisory_model_first/economic_selection_state_pipeline_v1.py | backend/tests/advisory_model_first/test_economic_selection_state_pipeline_v1.py; artifact: advselectionvalue_8f53ace471987dc7f0b99a00完整一次研究，累计19fit+1index | VERIFIED_C2_RESEARCH_COMPLETED | none |
| F-743 | backend/services/advisory_delivery_preflight.py; backend/services/advisory_model_first/economic_sector_price_readonly_bundle_v1.py | backend/tests/watchlist/test_advisory_delivery_preflight.py; backend/tests/advisory_model_first/test_economic_sector_price_readonly_bundle_v1.py | VERIFIED_C1 | none |
| F-744 | §2/6/7/9 | artifact: C1精确五源码/测试加本文、三视角自审、39直接测试/Ruff/原D兼容 | VERIFIED_C1 | none |

## 9. Risks / Rollout / Rollback / Production Gates

直接接受QE资格不等于承诺收益，也不能把缺行情等同于没有可盈利股票。研究多轮搜索偏差继续诚实报告；不新增80%功效、天然20D、旧native、原conf恢复等启动门槛。此文档PR无需重启、DB/依赖/激活/进程操作均0。运行服务源码变更按实际runtime合同通知用户重启，不由本窗口执行。

DESIGN-COMPLIANCE-001：用户最新指令是当前方向，旧历史试验结论不改判；不设置第二资格体系、不伪造证明；真实输入错误与资格拒绝分开；源码/功能/效果/运行时各自报告。

三轮一致性自审：职责轮检查QE资格完全委托、无原conf/native/80%确认前置；事实轮核对M1四fit/正开发结果、M5源码18测试但正式研究0及累计15fit，不修改历史成果；执行轮清理日频/M5/确认文档中的旧等待和“资格后API/UI”叙述，限定五文档及后续两个Advisory源码切片。均为本窗口不同视角自审；修订后重跑F2与差异检查。

### C1实现审核与交付状态

职责轮：资产资格属性读取即失败的测试仍可接受现存包，退役字段只展示，非primary一致股票池声明直接用；矛盾声明仍是实际输入错误。兼容轮：原source receipt仍须与原plan一致，reader不调用当前producer指纹；正/负/执行阻断报告均可读。发现实际执行阻断报告缺gates后修正，不要求收益门齐全或通过。第三轮保留原reader scope及bundle hash，不往冻结身份添加新资格字段；原权重、recipe、支持和价格数学不变。39直接测试及真实已消费2024-08-01全20候选15D兼容通过（9完整/11正常未知），原M1权重SHA保持872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e。

历史C1交付backend_restart_required=true，运行进程仍待用户重启；不是收益确认或完整daily/API/UI完成。C1单独验证0研究fit/0新窗口/0数据库/0QE提交/0服务操作；当时F-742仅设计验收，现由下方C2完成实现及一次研究。源码、必需CI、合入、自身清理及用户运行态分别报告。

### C2接续事实

C2接受真实正M1前序，修同一来源manifest的正反斜杠误判（路径解析一致且原role/hash/字节数仍核对）。清洁源码后先登记，再完整prepare/四fit/四臂；M5 run advselectionvalue_8f53ace471987dc7f0b99a00，7720键保留，7340状态可用/380 warmup未知；同3693成熟train/1591诊断validation。100共同估值日candidate/baseline/matched为13.2538%/21.3220%/4.5719%，减baseline -6.8073bps、减matched +8.0519bps，两区间跨零。模型真实TAKE82、UNKNOWN控制6；不把名义正收益当增量通过或独立确认，不救活当前candidate，不因该结果全项目停下。整批真实19fit+1index，0sealed/新窗口/数据库/QE提交/服务操作，fit前后公开QE running=0。C2 20直接测试及20冻结reader测试合计40 PASS，真实原M1冻结模型/20候选15D兼容也通过；后续正常日频功能研发不等待确认。
