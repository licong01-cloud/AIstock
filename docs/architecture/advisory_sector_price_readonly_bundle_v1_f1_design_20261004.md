# Advisory R2-M1冻结模型只读消费者 F1 Feature Card v1

2026-10-04；仅交付已发布M1研究权重的有界只读加载器。不是新模型、训练续跑、每日业务接入、confirmation或生产激活。

## Background / 直接业务必要性

M1一次开发导航通过，源码#5414已交付；原研究loader会重新核对活动profile并加载旧训练来源，属于study-resume合同，不适合作为长期日频模型消费者。现有v4消费者绑定不同的LightGBM模型/9+4 schema，且其JSON上限1MiB，不能直接套在M1的16/13D四个JSON GBDT头（metadata实测1,156,860bytes）上。

本项只解决冻结模型与训练环境耦合，不给不合格模型价格启用权限。M1区间跨零、RECOVERED_LIMITED/native UNPROVEN/只NAV不变；M5准备保留，不再开发择优。后续真实D输入、父腿时钟、每日API/UI、独立确认与激活另验。

## Scope / 精确范围

自己的worktree `F:/Dev/AIstock_worktrees/advisory-sector-readonly-model-20261004` / `feat/advisory-sector-readonly-model-20261004`，从最新origin/main创建。仅以下三文件：

- 本Feature Card。
- `backend/services/advisory_model_first/economic_sector_price_readonly_bundle_v1.py`。
- `backend/tests/advisory_model_first/test_economic_sector_price_readonly_bundle_v1.py`。

不改M1现有数学/源码/研究stage或任何其他模块；不复制模拟器、数据平台或registry。复用M1 `SectorPriceFitV1`、`sector_fit_identity_v1`、`predict_json_v2`及原价格集推断函数。新增module不进入旧fit实现hash，不重训或生成新trial。

## Contracts / 不可放宽

入口 `load_sector_research_bundle_v1(plan_ref, trained_manifest_ref, evaluated_manifest_ref)`要求三个由调用方事前固定的EvidenceReferenceV1，不靠工件自报hash授权。角色固定`sector_readonly_plan` / `sector_readonly_trained` / `sector_readonly_evaluated`；精确同study根、原preregistered/plan.json和trained/evaluated/manifest.json，不接受跨run、C盘、相对/重定向路径或手动迁移目录。

逐个核对registered→prepared→trained→evaluated的manifest身份/链、原plan/recipe/source receipt、训练metadata及原父plan/数据集manifest身份。只读取这些现存JSON元数据/模型和已消费evaluation的资格字段，不打开rows/labels/features/行情/预测parquet、不连接DB/profile/H5、旧source adapter/fit不调用。准备manifest的其他文件仅保存其声明hash，不称已重新验证训练数据内容。原registry只读取，用四阶段登记证明同plan与原窗口/包政策，不能写登记或混入新fit。

对JSON严格拒绝重复字段、NaN/Infinity、非object、非有限模型值、危险文件名、超预算/缺失/并发漂移。独立JSON每个最多2MiB，所有加载文件合计最多16MiB；registry单独至多16MiB/1000条。描述符SHA/长度须用本次实际消费字节核对，不能先验hash后无保护地再读；文件前后stat及最终hash拒绝变化。

M1plan类型/16与13字段顺序、parameters、4个200树GBDT头、13/16实际维度、原support及细小洞、policy/cost、监督集合hash、4fit/0index/1candidate及test不参与拟合必须精确一致。用现有非执行JSON predictor在合成矩阵验证全部树结构，不pickle、不执行工件代码、不调用sklearn fit。不允许回选matched。

返回`LoadedSectorResearchBundleV1`：原plan/fitted、package/program/manifest及数据集/政策/股票池来源声明、组合bundle hash、固定`NAVIGATION_ONLY/deployable=false/native UNPROVEN/source RECOVERED_LIMITED`。只能用现有数学得到研究价集，不能包装成正式qualified bundle；缺失输入/支持仍UNKNOWN/空集，不默认价格。不提供收益资格升级、盲读或生产binding。

提供`verify_unchanged()`复核已加载来源字节及固定模型hash，调用方在一次消费前后使用；内存模型被修改、源文件改变或原registry矛盾均拒绝。无自动重载/retry/fit；独立loader可以从普通CRLF主线加载原冻结权重，但必须核对UTF8-LF逻辑实现hash兼容，不放宽原研究续跑合同。

## Non-goals / 权限

不读sealed/新holdout，不运行四臂/确认、择优/扩支持、重建候选、提交QE训练、DDL/DML、依赖/数据/模型激活或服务控制。metadata阶段已发现model-state GET带upsert副作用；本loader不调用任何HTTP/API，不能再次触发它。后端重启仍用户执行，本库无自动运行挂载，不需要重启来验证模型加载。

## Implementation Plan

先本Card方法/来源/工程三视角审核修订及F1校验；只在精确三文件实现和直接测试，多轮审核修复。完成后只读加载真实M1四头，用合成有限D与固定价格点和原数学函数比对，不打开真实新窗口/收益，不拟合、不登记trial。源码CI绿后按已有授权提交/合入/官方自身清理。

## Verification Plan / 最小直接矩阵

真实权重加载与原纯函数parity必须通过，不能仅mock fixture声明完成。直接测试集中在同根链/外部pin、严格JSON/边界/并发、全部head/schema/support/模型结构、源与内存漂移、registry身份和原生/收益等级不可升级；正常UNKNOWN、支持洞、多段价集沿原纯函数。禁止source adapter/训练/profile/数据行读取的毒化检查。只跑本叶矩阵，广回归由CI承担，不堆积实现快照。

## Risks / 审核与剩余边界

逻辑源码hash或模型结构不兼容只停止加载，不回退训练环境/旧路径或重新fit。manifest声明未被消费的parquet内容不在此次复验范围，不能称全源READY；正常source限制随返回对象保留。price-set数学可查询不等于D源PIT、确认效果或生产角色就绪。registry若合法追加其他实验，既存已加载snapshot不改变；调用方需要消费时显式重新加载，不能忽略跨本study冲突。三视角设计自审分别核对研究等级/数学复用、冻结链与字节消费/并发、原副作用接口完全不调用及三文件边界；不冒称外部独立审核。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-721 | 有界外部pin/同run manifest链/原登记与父身份，source-only不重读数据行 |
| F-722 | 四头/维度/schema/support洞/policy/cost/监督与非执行数学精确，0fit |
| F-723 | 严格JSON/路径/字节预算/并发与内存漂移fail closed |
| F-724 | 原证据与UNKNOWN不升级，不挂正式角色/confirmation/API/UI |
| F-725 | 三文件范围、真实权重合成parity、多轮审核及无DB/服务/其它模块操作 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-721 | `backend/services/advisory_model_first/economic_sector_price_readonly_bundle_v1.py` | `backend/tests/advisory_model_first/test_economic_sector_price_readonly_bundle_v1.py`; artifact: 真实14个JSON/registry加载、0数据行/source-adapter | IMPLEMENTED_SOURCE_READER_VERIFIED | none |
| F-722 | 同叶loader及原sector数学复用 | 同叶测试；artifact: 原model872acff3/两个arm的6合成点及价格集exact parity | IMPLEMENTED_SOURCE_READER_VERIFIED | none |
| F-723 | 同叶strict JSON/ReadSet/verify_unchanged | `backend/tests/advisory_model_first/test_economic_sector_price_readonly_bundle_v1.py`覆盖重复字段/浮点溢出/树图/预算/消费竞态及内存漂移 | IMPLEMENTED_SOURCE_READER_VERIFIED | none |
| F-724 | 返回固定NAV/false/UNPROVEN及原prepare/eval资格 | 同叶测试；artifact: 真实M1来源与资格未升级 | IMPLEMENTED_SOURCE_READER_VERIFIED | none |
| F-725 | Scope三文件及Verification/Production | 同叶测试、Ruff/diff/F1；artifact: 真实模型合成读回/0fit/无API/DB/服务 | IMPLEMENTED_SOURCE_READER_VERIFIED | none |

只验收本次完整source-reader范围，不称每日源/业务、确认或生产就绪。真实父腿训练information_end仍UNPROVEN；此缺口不由模型loader填补或阻止其完整源码交付。

### 实际交付及多轮审核

2026-10-04：独立叶实现完成，初轮14直接测试PASS；方法轮保留原16/13D数学、成本、支持细小洞和NAV资格，不把只读加载说成新的研究结果；来源/工程轮补浮点指数溢出、模型布尔值、scope/diagnostics内存漂移及registry原窗口/跨stage拒绝，新增两个直接node PASS。本窗口不同视角自审不是独立外审；最终叶矩阵16 PASS（2.48秒）、Ruff PASS，不运行相邻全模块重复测试。F1首次未解析“同叶测试”的F-723证据，修为精确测试路径，不重新跑已通过代码矩阵；F1/diff在修订后再核定。

真实冻结M1模型`872acff3894c7a64b1b87c51ebd440d739a82069d68be0e30ea27dee9c81931e`只读加载14个JSON/registry文件，metadata实际1,156,860bytes仍在2MiB限内。reader逻辑SHA `c07763d7a9c99e54d525d2c3474e65c47b150304c2002a4a02cb358b0d433778`；组合bundle `c674a38822a9bf37a6afb90478a8200029636ccbf4d8ec005b47c8d97302bf14`。原M1函数与加载器在candidate/matched两个arm、6个合成价格点及同支持价格集严格一致，耗时约0.297秒；0fit/0新窗口行/未调用数据source adapter，未读新收益或启动四臂。此parity只证明权重加载与原数学兼容，不证明真实D源PIT或收益确认；NAV/RECOVERED_LIMITED/native UNPROVEN/deployable=false原样保留。

## Rollout / Rollback / Production Gates

无router/调度接入或binding，backend_restart_required=false；不写DB/DDL/DML、不改数据/profile、依赖、QE或任何服务。源码合入不等于每日业务或经济完成；loader失败返回显式异常，不回退原study loader/旧路径、规则范围或matched。历史模型工件不覆盖，单独回退源码无需数据动作。

DESIGN-COMPLIANCE-001逐项：本次完整范围仅冻结模型reader；不以其mock/源码测试报完整daily/确认；禁止静默fallback/未知价格、误升级证据；精确原model/label/policy不结果后放宽；读写/merge/runtime/经济状态分开。
