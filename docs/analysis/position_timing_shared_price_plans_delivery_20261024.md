# LocalSIM连续研究：共享价格计划交付与最小接口差异

日期：2026-10-24；设计：[F1设计与验收](../architecture/position_timing_shared_price_plans_f1_20261024.md)。
本轮交付公共构造器和181日全部模板；14条UNKNOWN仍需LocalSIM按本文件差异消费。机器schema通过不代表其业务校验已兼容，也不代表12账户回放或经济验证已完成。

## 公共接口与政策

```python
from backend.services.position_timing.intraday.price_plans import (
    PriceReference, PricePlanPolicy, build_price_plan,
)
result = build_price_plan(reference, side, PricePlanPolicy(), available_at,
                          revision=1, lifecycle="ACTIVE", previous=None)
```

`PriceReference`显式携带symbol、trade_date、reference_price_raw或UNKNOWN、source_file完整SHA、source_available_at和typed reason。返回`PricePlanResult(availability,reason_code,event)`；event符合既有LocalSIM事件机器schema，不包含数量或账户判断。

政策ID：`RAW_PREVCLOSE_PREFERRED_50BPS_V1`。BUY=[0.995×R,R]；SELL=[R,1.005×R]；0.01元向外舍入；有效引用使用PREFERRED。R是candidate对应D日raw prev_close，复用厘精度恢复，不使用因子比例、native目标参考价、当日OHLC或收益优化区间。
0.5%扩展属于本次用户授权的独立探索研究：公式沿用旧工程政策以减少自由参数；不外推旧政策适用范围、不改旧288产物，不宣称原生预测、最优阈值或生产默认。
历史来源可见时间及首次模板available_at为D09:00（Asia/Shanghai），是明确的盘前可见假设，未取得真实发布日志。有效期D09:30～14:57，不能隔日延用。修订从1递增、同股票/方向/日期、available_at严格递增，实际生效不早于事件可见；ACTIVE可修订/撤销，终态不得复活。构造器不将提醒推定为成交。

## 冻结身份与路径

producer commit：`614130d5d5832d587af0eaea4986ad228384e949`，源码及直接复用模块的LF归一化SHA另绑定于manifest/policy_document；后续文档提交不改变这些实现身份。
输入preflight完整文件SHA：`c7270eabf6830df970d4207d25bae9661e5fab464a0e70f5a65cba78f6cc8ff7`。
包manifest：`5de2ec91c088e5cf9a9cc25673498a4165857b761cb9e4a4456151a1e04f0dc9`。
candidate manifest完整文件SHA：`f5af5cc0506e502300ef225805b51aaeb7ea1b1bc352fdc81f363f4933f5f8ed`。
selection_inventory完整文件SHA：`4ac705225404568db4254d00825f5d098a46f98e0301aa7e448575f5b9cffd33`。
两份消费schema完整SHA：templates=`8eac3f9dfa2d9139e57bbec707113beed6968733f2b05fe3275c7b7aebff3f12`；event=`b78bbdb580307c1c0ca8a5e13a7dcc8cbafe44062b69ab4c85f0b6dcce539bee`。

外置产物绝对根：
`F:\Dev\AIstock_model_artifacts\position_timing_advice_v1\intraday_v1\shared_price_plans\2d3cc6547769b19d4d7cf9d6161f3cf0c11eb00feb1684d0806bf5e3026d5167`

| 根下文件 | 完整文件SHA256 |
|---|---|
| templates.json | bce2563f61ee2c3ee64b74d723dfac57baa95e127ed87e137229e86d16b327a1 |
| policy_document.json | 58e2fddf837f890e7eecadeef08e2907273f7edc3ab9759d62ae51e892e5b779 |
| reference_audit.json | fe6b1fcb12ffb868a68e95f3e122bbe7969fed533a017afabd3ae23bd02bfde6 |
| manifest.json | 4781d4fa7c27dd7f7c632090c32e87fb17a7d74dba225f607bcd638ddd2323bd |

这是普通JSON的完整文件SHA，非信封canonical SHA。模板内FileRef保留`/mnt/f`和`/mnt/x`绝对路径，供WSL消费，不修改路径后沿用旧hash。
policy_document绑定CONFIRMED_FEEDBACK_CLOSED_LOOP、2026-01-05～2026-09-30、包/candidate/inventory、reference_audit及全部988个来源文件。616个candidate组件为日历加615个raw参考价文件；181份projection与181份completion均检查身份、目标和PIT cutoff。未重选、未读取收益。

## 整批检查与UNKNOWN

181日、9050个当日BUY、64947个累计可能持仓SELL，共73997事件；AVAILABLE=73983、UNKNOWN=14；615股票。已出现历史目标只补SELL，没有制造历史股票无用BUY，也未根据各账户实际成交删减共同模板。
全部181日文件和73997事件通过用户提供的两份机器schema；产物总量69,781,229字节，约66.55MiB。只按股票读取日价序列一次并逐日发布，不读取分钟线/模型、不乘以12账户复制输入。

| 股票/日期 | 数量 | typed reason |
|---|---:|---|
| 002743.SZ / 2026-09-07 | 1个SELL | REFERENCE_NONFINITE |
| 688496.SH / 2026-09-11～09-30中的13个交易日 | 13个SELL | REFERENCE_OUTSIDE_FROZEN_SPAN |

UNKNOWN仍有source_file/hash和当日有效期，但price_constraint为NONE、intervals=[]，没有填造区间或携带旧价。准确日期、方向及原因见reference_audit.unknown_references。来源漂移与非法bin结构直接失败，不降成普通UNKNOWN。

## 交给LocalSIM窗口的最小差异

本轮只读核查PR #6659，HEAD为`5b942224d33b1ca1f8fc14c4253bab89cb089dc3`，CI成功但仍为OPEN/DRAFT，mergeCommit为空；其consumer源码位于用户指定工作树。本窗口未操作该PR或修改其代码。
机器schema已允许NONE/空intervals；实际`timing_campaign.py::PriceTemplateEvent.coherent`却固定创建PRICE_TOUCH的PlanDraft，且要求非空intervals，因此拒绝上述14个合法UNKNOWN。当前整批状态明确为`CONSUMER_UNKNOWN_REFERENCE_SUPPORT_REQUIRED`。

仅需其owner调整现有消费者，不需要改择时核心或重训策略包：

1. 从policy_document读取并校验reference_audit FileRef，构建 `(trade_date,symbol,side)` → typed UNKNOWN原因及source_file身份映射。
2. 事件本身先验证共同时间、来源及生命周期合同；允许审计声明的UNKNOWN使用NONE/空intervals。未声明的NONE、不一致的来源/原因、AVAILABLE却无区间仍失败。
3. 连续账户生成实际调仓意图时，B0/C2继续接受NONE；C1若实际需要该UNKNOWN计划，记可见不可用/未成交及原因，保留原人口、计划请求和共同账户终值。不要构造违反核心合同的C1 Plan，不生成默认区间，不删除股票，也不要仅因“可能持仓”的备用模板未知就阻断无实际相关持仓的账户日。
4. 数量、持仓、撮合、费用和终值由LocalSIM处理；若估值自身有数据缺口应另行明确报告，本价格构造器不提供估值旧价fallback。

接口差异由用户转发，本窗口不通过协调通道联系其他owner。

## 验证与交付状态

首轮5项定向测试通过；第二轮补齐参考文件读入字节与producer SHA的即时校验后，最终5项再次通过。测试覆盖手算BUY/SELL外扩、来源晚到/终态复活拒绝、未来价格不改变过去数值偏好、仅累计历史SELL、UNKNOWN无区间、C1现有合同拒绝、hash漂移、缺文件与不可变retry。
Ruff通过；L0两个guardrail均0 finding/0 blocking；最终五文件ownership全部映射；F1为5条/5行/0警告、PASS。classifier为targeted_ci_required，position_timing_backend，workflow_gate=passed，unexecuted_test_files=[]，unmapped_code_files=[]。CI与合入结果由关联PR记录。
真实WSL整批build及独立inspect已完成，integrity=VERIFIED；同输入exact retry返回ALREADY_MATERIALIZED，manifest/templates/policy/reference audit完整文件SHA全部不变。测试小样本schema仅用于功能夹具，真实整批使用上述两份完整消费schema，未用宽松夹具替代真实交付检查。
第三轮以真实事件对只读LocalSIM `PriceTemplateEvent`类定义独立验证：已知区间通过；002743.SZ的真实UNKNOWN被现行类拒绝，确认差异确实位于该业务校验，不修改其源码或启动消费者。逐条机器schema、整批来源/人口、纯接口兼容性及经济效果是不同证据。
另由独立集合检查从冻结inventory逐日重建当日BUY/累计SELL，对全部181个事件文件核对：73997键、重复=0、意外/缺失键=0、无未来目标进入过去人口，14个NONE事件与UNKNOWN审计逐键完全相等。未读取账户收益。

DESIGN-COMPLIANCE-001：本模块构造器/真实完整模板与消费者差异分别交付；未知可见且无合成区间；不改变原核心/原288/账户语义；无新增审批、正收益或自然日门槛。
本轮database_read/write=false、market_network=false、dataset_mutation=false、runtime_action=false、account_replay_started=false、outcomes_read=false，backend_restart_required=false。Git/CI网络独立于行情来源；没有宣称经济有效。
