# 共享选股信号：HMM 显式不应用 L2 overlay 消费合同

日期：2026-10-15。关联：BUG-1910 / Issue #6097。

## 问题与修复范围

HMM 原生产物保留完整股票—行业 membership；权威明确声明行情不可用的行业不产生系数，使用
`explicit_no_l2_overlay_v1`。旧消费者把完整 membership 与系数集合不相等直接判为异常，导致两端共同阻塞。
这是共享消费兼容问题，不要求补行情、不改变系数算法，也不增加股票过滤或资金策略。

实现位于 `selection_center/hmm_overlay_contract.py` 与 `hmm_runtime.py`。全量预检和候选执行调用
同一个日度校验函数；Advisory、LocalSIM 继续调用原来的共享 runtime，不建立第二套适配器。
`runtime_profile.py` 与 `strategy_package/selection_signal_preparation.py` 的现行默认值、冻结输入约束不变。

## 原生资产与信任链

1. 原有无声明资产保持完整系数覆盖要求，不能因缺系数自动推断“不应用”。
2. 有声明资产必须使用原生 policy、code-map/quote-availability schema、相同的 authority ID/SHA256、
   有效的内容 digest、准确的 preset 和模型文件 SHA256。支持 `database_daily` 与 `qe_frozen_file`，不猜测其他来源。
3. 系数文件解析与 SHA256 来自同一次读取。必须命中配置登记 `config_json.coefficient_windows` 中唯一的文件名，
   文件 SHA256、preset、配置身份及覆盖窗口一致。目录扫描即时算出的哈希不是登记证据。
4. `database_daily` 还须核对登记的 snapshot、as-of、model hash、input_identity；input_identity 中
   authority、as-of/effective 必须与文件声明相同。正式登记解析只读，不登记或更新资产。
5. 日度预测保持 C=as-of、D=系数标签/effective、C<D。数值输入 watermark 不晚于 C；
   `sw_index_member_effective_as_of` 允许到 D，不能把生效日成员身份误作 D 日行情。
   各场景已有决策日、数据截止日、目标日仍独立传入，禁止替换成其他场景的时钟。
6. membership 按原生优先级读取：逐日映射、平铺映射；只有显式新合同且无前两者时才解析
   原生产物的 `stock_sector_membership_spans`（`start_date/end_date/sector_code`）。区间须合法、不重叠，
   只选请求日期，未来区间不得为当日映射补值。

SHA256 钉住原生声明及其 authority/digest，不使用当前数据集重新推导历史 authority，不伪造原生收据。
当前日度数据库读取身份不因此升级成历史 as-known/PIT 证据。原历史冻结证据校验继续执行。

## 调分与失败语义

| 状态 | 行为 | 证据 |
| --- | --- | --- |
| 有正有限系数 | 保持原分数乘系数及原排序/同分规则 | 原有 `hmm_adjusted` 与 coefficient |
| 行业精确属于已登记、当日明确声明的不可用集合 | 保留股票和原分数，不产生中性系数 | `coefficient=null`、`overlay_applied=false`、`HMM_L2_QUOTES_UNAVAILABLE`、`hmm_overlay_not_applied` |
| 无声明的缺系数或声明不能精确解释缺口 | 失败 | 不回填 1.0，不跳过校验 |
| 伪造声明、schema/authority/hash/日期/登记身份漂移、非有限系数 | 失败 | 共享预检及执行一致拒绝 |
| 缺失或空 membership | 保持失败 | 不借 no-overlay 补映射 |
| HMM 关闭 | 原样返回 | `NOT_APPLICABLE`，不读取/生成系数 |

声明的不可用集合必须恰好等于“完整 membership 行业集合减当日系数集合”，不得既有系数又声明不可用。
执行记录不可用行业集合、数量及本次候选未应用数量；正常行业仍按原公式调分。
遗留无声明资产的显式 `missing_sector_policy=exclude_candidate` 行为不由本修复全局改写。
新声明合同的未映射候选一律失败；已声明不可用行业有完整映射，绝不进入排除路径。默认 `fail_fast` 不变。

## 只读与场景隔离

- 冻结历史消费继续 `require_frozen_snapshot=true`，禁止动态 latest 和临时生成。
- LocalSIM 原来的 `allow_generate=false` 覆盖保持不变，即使 profile 的 `auto_compute=true` 也不得生成。
  snapshot-only provider 所需登记读取统一调用 HMM 的既有只读 `get_config`，不是第二套消费适配器。
- 遗留明确授权的生成入口及 `auto_compute` 默认值不变；本修复不把所有旧调用改为只读或默认生成。
- 共享代码，不共享另一场景的结果、记录、运行 ID、日期或缓存命名空间；本修复不启动业务任务。

## 验收与发布边界

| 要求 | 实现/验证 |
| --- | --- |
| RED→GREEN | 原缺系数报错复现；同一合法声明现在预检与执行通过、原分数保留 |
| 正常/异常/未来隔离 | 直接测试覆盖正常调分、缺口、伪造声明、hash/日期/authority 漂移、非有限系数、无效及未来 membership 区间 |
| Advisory | 原冻结 preparation 真实调用共享 runtime；有候选与空原始候选分别回归，继续校验冻结身份 |
| LocalSIM | 真实 LocalSIM runtime 子类、独立时钟作用域；使用共享 validator，缺资产时仍不生成 |
| 原生产物 smoke | 从已登记日度任务解析 as-of 2026-10-14 / effective 2026-10-15，fresh process 只读验证；不硬编码临时资产路径 |

源码/聚焦回归/只读 fresh-process smoke 通过，不等于已部署后端运行态验收、完整模拟盘回放或模型收益验收。
后端加载新源码需要用户执行重启；合入须用户另行明确授权。禁止本任务写库、训练、生成系数、控制服务或运行真实回放。

已完成同窗口三轮审核修复：共享校验及 snapshot-only 读取兼容、压缩 membership/同次读取 hash、
fail-closed/历史兼容/模块边界复核。最终聚焦矩阵 63 项通过；只读登记资产两端各 5918 条输入/输出，
124 个有系数行业，7 个不可用行业的 29 只股票不应用 overlay，零排除，正常行业排序与原公式一致。
smoke 分数为两端分别构造的诊断输入，不是模型推理结果，也不构成收益验证；membership 与系数来自原生登记资产。

| DESIGN-COMPLIANCE-001 | 结论与证据 |
| --- | --- |
| 不交付未批准简化、占位或 mock-only 版本 | PASS；定向回归与真实已登记日度资产 fresh-process smoke，运行态验收仍单列待重启 |
| 不吞异常、伪成功、默认空结果 | PASS；无声明缺口、未知映射、非有限系数、伪造/hash/日期漂移失败，不填 1.0 |
| 不擅改业务语义，调整合同同步设计 | PASS；仅授权 no-overlay 合同，正常调分/排序不变，两端不复用结果，本文同步 |
| 不增加未要求审批或门禁 | PASS；不新增调度器/审批或数据准备要求，不改模型、数据、股票池、资金、执行，保留明确授权生成入口 |
