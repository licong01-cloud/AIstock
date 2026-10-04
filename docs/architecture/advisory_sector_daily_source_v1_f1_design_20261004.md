# Advisory M1 日频板块只读来源 F1 详细设计

2026-10-04；实现日频消费者的数据来源切片，不代表完整每日 API/UI 或收益确认。遵守 QE 策略包直接消费要求，不核对父包训练时钟、asset eligibility、原生收据或收益资格。

## Background

M1 已有冻结四头权重及 15D 纯组合。日频核心只读源已经批量计算 12D，但缺少原候选行业对应的 21D 板块报价。补充本批需要的报价并复用纯组合，不重复 SQL、重建候选或训练。公司 D 可见分类由调用方提供；结构映射不替代公司历史分类。

## Scope

独立 worktree `advisory-sector-daily-source-20261004`，分支 `feat/advisory-sector-daily-source-20261004`。精确四文件：本文、`backend/services/advisory_model_first/economic_sector_daily_source_v1.py`、对应 `backend/tests/advisory_model_first/test_economic_sector_daily_source_v1.py`、上层 `docs/architecture/advisory_sector_price_daily_consumer_v1_f2_design_20261004.md` 的切片状态。其余源码不改，尤其不改 QE、Selection、StrategyPackage、行业公共 resolver、HMM、公共数据和执行模块。

## Non-goals

不研发分钟执行策略、卖出模型或 QE 训练；不新增资格审批、数据集补齐、历史原生证明、归档平台、角色激活或收益确认。

## Contracts

`EconomicSectorReadonlyDailySourceV1.load_day/load_batch` 共用一条实现。每 packet 是原候选、22 节点 calendar（21D+下一T）、component_roles、terminal_weights、原候选精确一对一 classification_rows。最多 20 packet、每 packet 最多 20 原候选。分类未知及正常停牌/缺报价保留 UNKNOWN；分类矛盾、未来分类、日期错误和数据损坏明确报错，不作为 QE 包的准入审核。

构造时传入既有结构 crosswalk（行业→profile 整数 id）及显式 index_codes（id→六位 `.SI` 指数代码）。id=0 合法；已知 id 精确对应且代码一对一，使用调用方既有映射内容 hash 核对数据格式，不要求新证明、数据激活或历史收据。调用方负责从公开结构来源及 D 可见公司分类取值；本源不猜路径、不使用当前公司行业补历史、不调用带写入的 model-state GET。

核心复用 `EconomicCommonCoreReadonlyDailySourceV1.load_batch`，每块只给最后20D+T。板块单独 READ ONLY/REPEATABLE READ 事务，用参数化 SQL 仅读 `market.trading_calendar` 和 `market.sw_daily(trade_date,ts_code,close)`：精确验证22节点；按本批候选需用的 `(day,id,index_code)` 去重键查21D报价，最多8400行、LIMIT+1拒绝溢出，不截断、不过滤返回的外部/未来/重复行。空候选仅验证日历，不读报价。

核心与板块是两个如实标注的只读快照，不宣称跨源原子 capture。新增板块 SELECT 每批最多2次，总7次，不按股票/日期循环查询。整个调用有限时间预算，statement timeout 为剩余时间，finally rollback；任何异常不回填默认值或隐藏数据库异常细节。输出原序KEY+15D，复用 `compose_sector_daily_features_v1`，不另写数学；无query坐标、T行情、收益、fit或训练调用。相同输入单日与批量 feature/input hash 一致，read_at/事务快照元数据可不同。

原M1训练的已发布H5 `sw2_close`存储类型是float32，原公式再以float64计算。DB `numeric`返回Decimal，必须先校验数值并按原float32→float64表示投影，再调用同一公式；不能直接以未投影DB精度查询旧模型、事后放宽parity容差或修改权重。新source receipt明确记录该投影。此处理是训练/消费表示一致，不是改变行情、收益或模型数学。

## Implementation Plan

先登记并审核本设计，再实现单叶 source/测试。三轮分别检查数据与公式、D/T及键完整性、事务预算与业务边界；发现问题只修本范围，最终直接矩阵一次。上层 F2 的完整 daily/API/UI 保持未完成，不把本切片的验证变成父包资格门。

## Verification Plan

复用已有核心 source fake DB 和 sector packet，不复制整套 fixture。直接验证单批15D数值/hash一致、id0、有限2板块SELECT/5核心SELECT、read-only/rollback、缺分类/缺日/空名单、未来/外部/重复/超限/schema poison、时钟/查询/rollback故障、映射/分类错误在读取前报错及不修改输入。真实业务读回使用已消费2024-08-01原20候选，只读数据库D及以前；不打开收益或新holdout。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-781 | 22节点、原分类/映射id0、D之前报价及原候选完整 |
| F-782 | 批量精确键/有限查询、只读事务、超限报错和rollback |
| F-783 | 原12+3同核、正常缺失UNKNOWN及单批数值/hash一致 |
| F-784 | 0父包资格检查、0fit/outcome/DB写入/跨模块修改 |
| F-785 | 多轮修复、直接测试/真实原D读回与整体完成分开 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-781 | `backend/services/advisory_model_first/economic_sector_daily_source_v1.py` | `backend/tests/advisory_model_first/test_economic_sector_daily_source_v1.py`：原分类/22节点/id0、future及原键poison拒绝 | IMPLEMENTED_SOURCE_VERIFIED | none |
| F-782 | 同叶批量精确键SQL/事务 | `backend/tests/advisory_model_first/test_economic_sector_daily_source_v1.py`：7次SELECT、readonly/rollback及限额/故障 | IMPLEMENTED_SOURCE_VERIFIED | none |
| F-783 | 同叶原纯组合/quote projection | `backend/tests/advisory_model_first/test_economic_sector_daily_source_v1.py`：Decimal/float32/单批hash和缺失；artifact: 原20候选15D严格parity | IMPLEMENTED_SOURCE_VERIFIED | none |
| F-784 | Scope及同叶调用链 | artifact: 四文件差异、0父资格/fit/outcome/DB写入，source仅复用已有12D | IMPLEMENTED_SOURCE_VERIFIED | none |
| F-785 | 本文审核记录及上层F2 §8.2 | artifact: 17直接测试/Ruff/F1/F2/diff、原2024-08-01业务读回；完整daily仍待实现 | IMPLEMENTED_SOURCE_VERIFIED | none |

## Risks

可读数据库不证明历史原生捕获；原训练来源未知继续如实披露，但不阻断权重直接使用。板块代码与公司分类必须分开，不能将今日行业快照当 D 时已知。单源 snapshot 不等于跨源原子 snapshot。自审为本窗口不同视角，不冒称独立外审。

## 多轮审核与修复

方法轮：逐项核对原12+3同核、21close/20return ddof=1、原完整20候选与id0；实际DB读回发现Decimal序列化及训练float32/DB float64表示差异，修复新source的数值投影并在同一fixture加入Decimal/有损小数，保持严格parity不放宽容差。时钟轮：核对22权威calendar、未来/外部/重复报价在per-packet筛选前拒绝、D分类和正常未知保留；补显式分类clock字符串形状的读取前检查。工程轮：核对两个只读snapshot/有界去重键/总预算及finally rollback、不泄漏底层异常细节、四文件scope和0父包资格重验。三轮均为本窗口不同视角自审。

最终直接矩阵17 PASS（4.53秒），Ruff、F1五项/F2八项、diff通过。一次已消费原D真实source成功读回：20候选/15D、9完整11UNKNOWN、7 SELECT（sector calendar22/quotes168行），约1.359秒，原15D数值rtol=0/atol=1e-12/equal_nan一致。前两次读回分别暴露runner环境未加载、Decimal/精度问题，均未写库或读取收益；解决真正功能错误，不收集旧负候选经济证据。

## Rollout / Production Gates

不挂调度、API或UI，不自动启用模型角色；调用时仅只读SELECT，未写库/DDL、激活profile、安装依赖或控制进程。后端重启仍由用户执行；本切片可离线直接使用，不因重启待办阻断研发。DESIGN-COMPLIANCE-001逐项：交付完整已批准source切片、不冒充完整daily；正常UNKNOWN与故障分开；不改变价格/成本/候选数学；不私增策略包准入。
