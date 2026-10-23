# 新版 L2 风险模型双入口闭合详细设计

> 日期：2026-10-21；tier=F2；lane=feature。
> 用户授权：完成 HMM 自身数据集/数据库双入口；QE、荐股、模拟盘及共享选股信号的源码由对应业务窗口修改。
> 父蓝图：hmm_evolution_and_risk_management_system_design_20260716.md §1.0、§2.6、§4.6。
> 直接基线：hmm_evolution_phase2_l2_risk_date_inference_detailed_design_20261018.md。

## 1. Background / 目标

同一已冻结的新版申万 L2 风险模型同时支持文件历史预测与数据库收盘预测。两入口复用 C-010/A5 20 维定义、原训练 scaler/logistic 参数、131 个官方文本行业身份及 warning=probability>=0.20；零 fit、零标签读取，不把旧 preset_A 日更或 L1 v1.6 入口称为新版 L2。

本包只完成 HMM 的预测能力与消费者合同，不增加消费公式、不改实际评分/持仓/订单，不预报 QE 收益或实盘可用。L2 轮动各既有候选保持原始结果；不启动新模型方向。

## 2. Scope / Architecture

允许修改的生产/测试/设计文件仅为：

- backend/services/hmm_risk/risk_l2_date_inference.py：抽取两源共用的特征载体构造、推断和读回函数；旧请求与输出协议不变。
- backend/services/hmm_risk/risk_l2_database_inference.py：新增只读数据库输入、身份校验和同一模型调用。
- scripts/hmm_risk/run_risk_l2_date_inference.py：在原薄 CLI 增加显式 source-mode；文件默认行为不变，数据库只在 parent 读取一次。
- backend/tests/hmm_risk/test_risk_l2_date_inference.py、test_risk_l2_database_inference.py：直接回归与反例。
- 本设计及父蓝图必要当前状态。

不改 QE、Selection、Advisory、Paper、调度器、数据生产、旧系数 producer、CI、nox、test plan、规范或候选数据。测试复用现有 fixture/内核，不复制业务实现。无需新增表、通知平台、依赖或过程环境变量。

## 3. D1：Contracts / 模型与源身份分离

继续由原 model_source 认证 source request/sealed/acceptance 和固定模型 SHA 37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d。两个源均输出原顺序 20 维；不重新 fit/scaler/filter，不重新映射状态或编号行业。

文件身份仍为六字段 release binding、calendar、security/provider、C-013 及事实摘要。数据库身份独立包含显式 host/port/dbname（不含凭证）、事务读取截止时刻、数据截至日、有效预测日、全局 universe key/rule/source fingerprint、C-013/security/provider pins、日历和逐日输入摘要。模型训练身份不被本次输入身份覆盖。

## 4. D2：数据库输入与因果边界

新增显式 request schema=hmm_risk_l2_database_inference_v1_request，字段为 schema_version、model_assets、source、decision_dates、display_limit、receipt_sha256。source 精确包含 database_target、knowledge_mode、industry_authority、security_identity_manifest/security_identity_sha256、provider_absence_manifest/provider_absence_sha256、universe_key、universe_rule_version。

数据库连接可由调用者提供，或使用现有凭证配置创建一个独立只读连接；请求不含凭证，只接受 read-only / repeatable-read 事务，验证目标与请求一致。自建连接在退出时 rollback/close，外部连接由调用者管理。只读复用 PostgresStockFactReader、C-013 adapter、共享证券 alias/provider authority、canonical market.trading_calendar、全局 PIT 和 CSI300 market.index_daily。只取原模型必需的 OHLCV/成交额、市值、涨停、停牌与股票资金流事实；不用 market.sector_data 的旧系数输入，不改数据库。1日收益的前收盘价显式取 market.stk_limit.pre_close，与训练文件的 provider prev_close 对齐；不以除权前上一条成交价代替。缺失、重复、非有限或非正值失败，原 PIT 入池/预热 NA 保留。5/10次前置真实价格观察和其他公式不变。

decision_date=t，as_of=prev_open(t)。末个 as_of 不得晚于本地当前日，decision 必须晚于固定训练截止。保留最长组合公式所需 264 个 open source days，circ_mv 只使用严格前置真实值，价格使用已有 reader 的真实前置观察。无未来数值查询、无 outcome/label。日历下一交易日可为未来，但不能提前读取它的行情。

knowledge_mode 显式二选一：

- forward_current_read_snapshot：C-013 必须为 forward/as_published 基础；每个 as_of 通过既有 DataRefreshAuditRepository.require_success 核验该版真实依赖 kline_daily_raw、daily_basic、stock_moneyflow_ts、stk_limit、index_daily、suspend_d 的完成记录。合法零行停牌记录不因 row_count=0 被判缺数；没有记录或新失败/质量不合格保持 typed waiting，不用 MAX(date) 冒充完成。
- historical_current_revision：用于 HMM 两源历史对照，只表示当前数据库版本下的历史因果数值。显式记录 historical_as_known_verified=false，不冒充过去当时可得的数据库快照或当前 forward 验收；仍需全部有界事实/PIT/上下文合法。对应 C-013 historical_replay 合同不能被静默切成 forward。

数据库源不绑定月度文件 manifest，不自动读取 active profile；文件源不因错误回退数据库。基础数据缺口交数据 owner，HMM 自行负责消费者构造及验证。

## 5. D3：Implementation Plan / 共用特征与自然不可用

数据库股票行与文件股票行进入同一 _collect_domains 和 build_c010_feature_domain_panel，再用同一特征载体构造与原参数推断函数。复用文件 reader 的 C-013 authority 区间索引，按真实变更边界缓存身份并恢复每条事实的实际日期，不复制分类实现或按股票日重复认证。全部 131 行始终保留。单成分、正常停牌、空成员、正式来源不可用沿原有 typed NA；不补 neutral/1.0，不新设最小股票数或每行业全成功门。

BUG-2082 明确数据库消费者的日历事实与分类资格分离：仅该入口显式启用 reader 的 preserve_unavailable_classification；旧消费者默认行为不变。分类不可用的股票事实保留原交易日、股票、行情/资金流和 C-013 resolution（含 reason 与 authority pins），行业代码保持空值，绝不回退数据库旧成员表或后续观测分类。日期完整性和股票日去重在全部事实上核验；仅已解析到正式文本行业的行进入原聚合核。源日历真实缺日仍失败，合法 classification NA 不再误报为行情缺日。

逐日 lineage 同时绑定已解析与未解析事实，附分类不可用数量、原因分布和股票集合摘要，不能将筛除后的集合冒称完整覆盖。整日无分类时仍保留原 131 行与该日分类 reason，原 264 日上下文和 C-010/A5 rolling/min_periods/mask 决定后续特征是否自然恢复；不补值、不删除交易日、不压缩窗口或放宽 coverage。预检成功只说明载体与身份闭合，可能仍有 typed unavailable，不代表模型效果或所有预测可用。

未知身份、重复股票日期、错误目标、未来输入、缺少必要来源、非法数值或 authority 漂移明确失败。原观察域/coverage 规则保留，不把未完成执行包装成合法 NA。没有官方指数不自动否定已有批准的股票事实聚合版本，也不把聚合版伪称官方指数。

## 6. D4：输出与业务窗口接入合同

两源输出保持 model_identity、inference_identity、features hash、decision/as-of、canonical L2 code、probability、warning、availability/reason、全目录 coverage。展示索引最多 30 项，不裁剪业务计算集合。数据库输出使用独立版本 schema 与 source_mode=database_daily，不伪装 frozen dataset 资产。

本包不产生旧 preset_A coefficient，不实施 stock_weight*(1-p) 或其他消费公式；不更改 can_buy、订单、仓位或风控。QE 窗口消费文件预测并冻结回测/费用；荐股/模拟盘/统一选股信号 owner 消费数据库 T→T+1 输出及其自身已批准策略。默认版本与旧路径不替换。业务场景真正可用须各 owner 完成接线和验证，不由本包单方面宣称。

HMM 交接入口为同一 CLI 的 --source-mode frozen_file（默认）或 database_daily；service 分别为 risk_l2_date_inference 与 risk_l2_database_inference 的 prepare、infer、validate_prediction。调用方传精确 request 和 model_assets，不通过模块全局状态切换源；外部只读连接必须符合§4事务合同。消费者读取 predictions 全131目录及 availability/reason，不从 display_sector_codes 反推行业资格，也不将研究概率冒称已验证收益或交易许可。

## 7. D5：Verification Plan / 验证与失败

保持原文件 CLI 的双 fresh-process 检验：parent 一次构造源 bundle，两 child 只做参数推断，禁 fit/DB/network/active-profile；数据库 parent 先开一个受限只读事务再启用动作毒化，child 不打开数据库。parent 用原参数零fit核对概率值，不能只凭两child相同、概率在范围内而接受共同错误。动作毒化只用于隔离的 CLI/测试进程，不在可供后端调用的 service 内全局 monkeypatch，避免影响其他线程。输出仍 write-once，记录真实阶段和 typed failure；不使用新进程环境变量记录实验，不要求重启完成记录。

直接测试覆盖文件旧协议、同事实两源相同特征/概率、源选择隔离、DB transaction/target、PIT/完成水位、未来日期与训练边界、20D/131 identity、合法 NA、缺失/重复/错误 authority、重评分重现与 fresh import。真实两源比较报告各自输入身份、availability 和数值差异；数据修订/文件 float32 编码导致的差异不隐瞒，也不强行改 hash 或改模型。相同 prepared bundle 的两 child 仍必须 bytes 完全一致。

最小相关矩阵、Ruff/compile、scope/module registry、L0、F2 和至少两轮作者代码审修。广域 HMM/跨模块验证交 CI/Nightly，不盲跑全量、不启动 QE 或新效果实验。

## 8. D6：停止、Rollout / Rollback / 风险

完成源码与直接验证，报告实际数据库就绪状态；有缺口保留 pending，不宣称实时双入口完成。用户已于2026-10-21追加授权本任务提交、PR及满足门禁后的合入和后续步骤；该授权不推导生产 DDL/DML、runtime activation、其他模块接线或服务控制，后端重启仍由用户操作。

Rollout 为 HMM 可选 service/CLI；回退为不调用新 database source-mode，旧 file/preset_A 行为完全保留。新 backend 文件按实际 runtime contract 分类，fresh import 不等于后端已加载。当前只读元数据确认全局 PIT ready/clean 至 2026-10-20；kline_daily_raw 最新成功完成记录仅至 2026-10-15，不能宣称 10-20 forward 数据齐备。

### Production Gates / 生产门禁

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_write=false；dataset_write=false；active_profile_write=false；runtime_activation=false；service_control=false；QE_experiment=false；fits=0。

## 9. Design Acceptance Index / Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §3/§5；risk_l2_date_inference.py；risk_l2_database_inference.py | backend/tests/hmm_risk/test_risk_l2_database_inference.py：相同20D/冻结参数预测一致、完整目录及合法NA；最终文件+DB相关矩阵61 passed | PASS_SOURCE_CONTRACT | - |
| F-012 | §4/§7；risk_l2_database_inference.py；run_risk_l2_date_inference.py | backend/tests/hmm_risk/test_risk_l2_database_inference.py：readonly/PIT/完成水位/来源前收盘价/两child隔离；fresh-process import PASS；真实有界读回见§11 | PASS_SOURCE_CONTRACT | - |
| F-013 | §6/§8；父蓝图§1.21 | backend/tests/hmm_risk/test_risk_l2_database_inference.py::test_cli_database_dispatch_preserves_file_default_and_children_cannot_read_db；§6完整service/CLI交接及§11真实pending边界 | PASS_HMM_HANDOFF_CONTRACT | - |

以上仅为本包HMM源码/交接合同验收，不是父蓝图全部F-011/F-012/F-013、生产运行或外部业务验收。未提交源下的受控读回不冒充 clean committed CLI 正式acceptance；正式CLI仍要求最终已提交源码及干净工作树。

## 10. DESIGN-COMPLIANCE-001 / 审核记录

完整 131 目录/原 20D/两源明确；禁止将 mock-only 称实际完成。未知缺失和身份错误 fail closed，合法自然 NA 不造值。原模型/scaler/特征/训练窗/seed/阈值/旧结果不变。无新增效果/全行业/资源/审批门；不越业务 owner 范围。

设计第一轮作者审核：修正复用旧日度完成检查会错误依赖 sector_data/sw_daily 的方案，按真实 20D 股票输入选择既有 audit 名；分开 historical current-revision 与 forward/as-published，避免用 backcast 历史对照伪装数据库实时已验收。

设计第二轮作者审核：文件身份与 DB 事务身份分开；不更改原 producer 以免启动 hash pin 漂移；两源共用计算不强求不同修订/不同编码的数据输出伪 bitwise 相等；业务修改留给各 owner。

源码第一轮作者审修：修正路径link校验调用，移除可复用service内全局动作mock；C-013投影后按日canonical排序；沿用区间索引并验证跨变更日身份及真实日期，保留未知/重复/日历缺口失败。55项原文件+数据库相关矩阵通过；新增区间回归通过。未声称独立外部审核。

源码第二轮作者审修：补齐provider pre_close与除权口径对齐、PIT预热NA不造值、缺失/重复/非有限反例；补冻结参数parent读回，防止两child共同错误；7项最终修复点通过，最终同一相关矩阵61 passed。Ruff/format/py_compile、scope/ownership、registry 8 passed、L0 blocking=0及fresh import通过；两份设计F2 PASS。受控真实读回与运行态分开；没有改变模型、20D定义、训练窗口、seed或阈值。

## 11. 实际读回、输入差异与剩余边界

- 原正式文件carrier已重新核对request/model/bundle receipt：2026-08-28、2026-08-31，各131目录、129可推断、2项typed NA。未改写其历史acceptance。
- 数据库同两日期使用当前修订事实、同一冻结模型，as-of分别2026-08-27/28，只读repeatable-read，零fit/零写入。区间缓存前后均131可推断；缓存结果数值一致，耗时从867.56s降至561.45s。这是受控工作树读回，不是最终clean-source CLI验收。
- 实际源不伪称同身份：8月28日文件selection stock_universe有4917只、当前DB PIT有4909只，差8只，未用provider all.txt代替股票资格。文件801766.SI的8/17正收益、DB原价6/17已定位：002343.SZ、002739.SZ在原价持平时，文件float32价格/因子还原产生亚微元严格正差；未私加容差、量化DB或改模型。初次直接比较adjusted Bin而未还原因子的诊断已纠正，不能作为数据缺口结论。
- 两日期文件801032.SI、801218.SI原有typed NA与当前DB可用性不同；本次小窗日线/日基本面/资金流所需字段可读，不宣称已证明全部历史差异根因，不为对齐而补造旧文件或重跑旧研究。共同可用行业在前收盘价对齐前最大概率差0.010223088165203453；此数字不是输入一致或效果通过证明。最终口径对齐读回另列结果，不覆盖旧结果。
- 最终provider pre_close对齐后，2026-08-28/31各131可推断、0不可用；模型SHA仍37259b5e…；共同可用行业最大概率差0.01021982531101484、最大特征差0.11764705882352938，耗时591.86s。该受控只读结果保存在本任务忽略路径tmp/handoff/hmm-l2-dual-input-20261021/aligned_readonly_result.json；不登记为生产资产/模型/运行验收，不因记录修改环境变量或要求重启。
- 上述同一真实DB bundle另经两个独立fresh-process执行service参数推断与读回，262行结果摘要一致，prediction SHA=5409897a8868315d78137e2419b222e5474e66704de3efd0c8a08a05cd90d00d，fits=0、forward_confirmed=false。两child均启用fit/DB/network/active-profile动作毒化；没有重新取数。此为受控service复现，不代替提交后正式CLI clean-source验收。
- 当前forward仍需覆盖目标日的真实完成记录及合法forward/as_published C-013 authority。现有历史backcast authority不可改字段冒充forward；raw-price成功水位只到10月15日，不对10月20日作可用承诺。没有查询未来行情/label，没有伪造水位。是否接入QE、荐股或模拟盘由对应owner按§6实现并验收，HMM本包不操作其源码。
- 交付授权已收到；先提交本包六个文件，再在干净工作树合并最新origin/main并复验最终HEAD，正式CLI、PR/CI和合入结果按各自真实收据报告，不提前写成完成。运行加载与后端重启后验收独立。本次无DDL/DML、数据集/active-profile写入、依赖安装或进程控制。
