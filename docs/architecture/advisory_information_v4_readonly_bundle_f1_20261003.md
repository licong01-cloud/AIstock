# Advisory v4只读模型消费 F1 Feature Card

## Background / Problem

PR #5304已合入dd92ba849b，真实study advinfo_153e00c8509484155d3c8ae0为NAV/NOT_CONFIRMED。Windows新checkout自动CRLF与登记生产代码字节不同，旧训练/续跑loader因此正确拒绝；该门禁不应放宽或改写旧工件。但只读消费既存模型不应重新调用训练/标签准备/研究登记，也不应依赖当前工作树与旧producer逐字节一致。

## Scope / Non-goals

本切片完整交付独立的V4只读模型loader，不是每日API/UI、训练兼容修补或完整荐股交付。精确新增economic_entry_information_bundle.py、对应最小测试、本Feature Card及蓝图当前进展；不修改旧v1/v2/v3/v4 producer、旧hash校验或其它模块。无QE训练、标签读取、DB访问/写入、activation、依赖安装、服务控制。只消费明确指定非C绝对study路径；旧精确研究树在安全消费验证前保留。

## Approach / Contracts

严格验证原注册plan、trained/evaluated阶段内容hash/父链、各阶段原trial registry refs、原parent plan及trained引用、新information manifest/identity。plan必须原始advinfo_<hash>/preregistered/plan.json；V4 request/scope/13固定语义、两配置/四头、模型元数据request/scope hash、LightGBM版本和实际模型feature order全部一致。loader不能按维数或目录名猜family，不选择控制组冒充模型；只返回INFORMATION_THIRTEEN研究模型，MATCHED_NINE仍作为原工件的验证对象。

producer implementation hash仍保留并在plan/信息identity中对齐，但serving不与当前training源码字节比较。该分离仅适用于已经发布且已登记的只读权重消费；原训练/精确续跑路径原样严格拒绝代码变化。不得重算/覆盖任何plan、manifest、registry或model，也不生成新模型/记录。

元数据与每个artifact读取前有显式大小、路径/非重定向约束；缺文件、额外/缺失模型、跨scope/parent/阶段/hash或非法数值fail closed。信息source保留CURRENT_DB_HISTORICAL_NON_VINTAGE，scope qualification保持DEVELOPMENT_ONLY_TEMPORAL_PARITY_UNPROVEN。输出为NAV/COMPUTATION_ONLY、非native/nondeployable，稳定bundle digest绑定实际所有验证refs；不产生CONFIRMED、生产role或收益保证。

## Implementation Plan

1. 新只读reader完成本Feature Card两轮审核后实现，不调用旧train/preregister/label/DB入口。
2. 最小契约测试（实际LightGBM序列化权重fixture，不冒充真实业务证据）：合法readback、两维数/元数据矛盾、stage/hash/登记矛盾、预算/路径。
3. 从普通CRLF工作树只读加载已发布真实v4并作相同13维实际T条件函数读回，不训练、不重放或评估新收益；跨树same bundle/model/预测身份相同。
4. 多轮修订、最终小矩阵一次、F1 validator/Ruff/diff、PR/必需CI/合入。无需重启。不因loader通过就删除仍是旧研究精确续跑来源的唯一字节树。

## Verification Plan / Design Acceptance Index

| ID | 完整验收条款 |
|---|---|
| F-594 | 精确路径及immutable plan/阶段/registry链 |
| F-595 | 独立13 family，两配置四头/readback/数值支持域 |
| F-596 | readonly consumption与严格原训练门禁分开，不写任何工件 |
| F-597 | 实际CRLF checkout可消费原权重且保持NAV/native/部署边界 |
| F-598 | 精确Advisory范围、预算/路径/hash矛盾fail closed |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-594 | backend/services/advisory_model_first/economic_entry_information_bundle.py::load_information_research_bundle_v4 | test: backend/tests/advisory_model_first/test_economic_entry_information_bundle.py::test_artifact_identity_and_read_budget_fail_closed | VERIFIED | none |
| F-595 | backend/services/advisory_model_first/economic_entry_information_bundle.py::_support；load_information_research_bundle_v4 | test: backend/tests/advisory_model_first/test_economic_entry_information_bundle.py::test_hash_valid_published_metadata_still_requires_strict_family_and_scope；test_support_alias_boolean_and_cross_bin_are_rejected | VERIFIED | none |
| F-596 | backend/services/advisory_model_first/economic_entry_information_bundle.py；旧producer未改 | test: backend/tests/advisory_model_first/test_economic_entry_information_bundle.py::test_readonly_weights_need_no_current_training_implementation_or_DB | VERIFIED | none |
| F-597 | backend/services/advisory_model_first/economic_entry_information_bundle.py::LoadedEconomicInformationBundleV4 | artifact: 下述真实1620行读回，F:/Dev/AIstock_model_artifacts/advisory_entry_information_v4_20261003/readonly_consumer_receipt.md | VERIFIED_READONLY_NAV_ONLY | none |
| F-598 | 本文Scope四文件范围；_path/_stage/_json | test: backend/tests/advisory_model_first/test_economic_entry_information_bundle.py::test_duplicate_and_nonfinite_metadata_fail_before_model_load；artifact: Ruff/diff检查 | VERIFIED | none |

三轮实现审核已完成：①stage/原ref/registry、严格13维和候选arm；②完整四头同support、非有限/布尔值/重复键/预算；③开发消费窗口/来源限制、只读回调分离及文件并发变更。第三轮先验证“阶段校验后权重追加换行”必须拒绝，原实现未拒绝，修正为有界读取并按原descriptor核验实际快照字节，再以model_str创建模型，不把校验后再次打开的文件当同一权重；失败节点与原合法读回定点PASS。最终稳定小矩阵14项3.75秒PASS，Ruff PASS；实际LightGBM fixture只证明工程。DESIGN-COMPLIANCE-001逐条符合本四文件只读范围，不称每日API/UI、native、训练续跑或完整荐股已完成。

真实CRLF工作树消费study原权重并用原81D/1,620候选的实际T条件复核3.641秒，逐行结果与原information_thirteen_decisions.parquet严格一致。bundle SHA=`4a7c29a4918ec0259f9d06b49b1194deb6f8da9efe2547e941d94d6f1b4a0bc1`；无DB、新fit、回放或新收益分析。原prepared manifest/hash先校验，后续查询仅加载raw_open/法规/交易状态必要列，未将未来OHLC/label用于模型。loaded.plan/model scope仍unproven/NAV/nondeploy，未修改旧注册记录或按结果重训。

本loader保留原严格训练/续跑约束；普通main可以读权重不意味着旧研究可以从不同字节源码精确续跑。原v4精确树暂保留用于原contract复现，旧每日消费者树因尚未合入继续保留；不把模型消费PASS当这两者自动可清理证明。

最终reader实际SHA=`6360cc8ce3c1b8b6b4a61fd083e3ce8c7a18cc7f67ac0e98b7dd2d4e5db0dd5a`的真实模型再次读回成功，bundle/scope SHA均不变。该reader SHA是观察工作树源字节，不是新的训练实施门禁；后续等价LF/CRLF checkout可得到相同模型bundle。

## Risks / Review

只读hash校验不是独立经济确认或历史native证明；可信原artifact依然必要。原生产/续跑对源码严格是另一合同，不使用loader偷偷重新训练。审核1明确不借修改旧producer解决Windows换行；审核2明确只读model reader验证注册/阶段/原身份，而不是把当前producer hash不匹配静默吞掉。当前v4在已消费窗口的优劣不重新计算、不调800/参数/seed。

## Production Gates / Rollout / Rollback

DDL/DML/dependency/profile gates=noop；model trials generated=0；binding_active=false；restart_owner=user。本模块没有API路由、在线调度或默认reader替换，合入不改变运行态。源码完成、真实只读消费、模型经济确认与正式荐股分别汇报。回退只停用新的只读调用，不删除旧模型或研究树。
