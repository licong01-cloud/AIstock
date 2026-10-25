# 分钟择时阶段一连续实施任务与验收

日期：2026-10-23。设计：[F2详细设计](../architecture/position_timing_intraday_phase1_f2_detailed_design_20261023.md)。实施基线：`cb303549a6ced76a1a311b48b490085de2aedc2e`。

范围：本模块核心、分钟输入、独立状态/API、现有页面中的可嵌入提醒、离线决策回放。LocalSIM/Advisory产品接入由owner后续执行；本次交付可调用接口及样例。用户已授权多轮审核修复、提交合入；后端重启由用户执行。

## 连续执行顺序

| 工作包 | 交付 | 直接验证 |
|---|---|---|
| 核心与输入 | 严格合同、B0/C1/C2、来源/分钟可见性/日级事实、显式状态 | 手算策略、时间边界、未来后缀不改变过去、停牌/方向性限价 |
| 状态与API | timing-owned文件状态、CAS/幂等/恢复、批评估、GET只读、claim | 重试/并发、取消、陈旧/缺数据、无订单副作用 |
| 页面提醒 | 手工计划、源刷新协议、轮询、有效机会、一次领取toast | 真实组件挂载、旧响应/隐藏恢复/错误显示、现有前端检查 |
| 回放与验收 | prepare/run/inspect/exact retry、D0/1/3/5意图、owner转交包 | 冻结hash/因果/取消、直接测试、F2、L0/classifier、CI与多轮复核 |

不按小时填充工作，不等待自然交易日；持续推进至本模块交付面满足设计。缺真实历史文件时先完成全部功能合同与可复现功能样例，缺口如实报告，不以fixture声称经济有效。无训练或账户收益研究。

## 写入范围

沿用详细设计§2.2精确代码/测试清单；另更新该设计的实施验收、原蓝图的进度、LocalSIM转交文档和本记录。无共享默认值、QE/LocalSIM/Advisory代码、数据库或数据集变更。

实施前源码核查收敛两处文件布局：可嵌入组件放在`frontend/src/app/position-timing/IntradayTimingPanel.tsx`，前端行为测试追加既有`frontend/tests/position-timing/position-timing.spec.ts`。二者由现有position_timing ownership/CI覆盖，无需修改公共catalog；详细设计已同步，业务范围不变。

## 审核与结果

已完成三轮逐条审核与修复，并在同步main后补做源码身份可移植性审核；未使用协调通道或委派跨模块开发：

| 轮次 | 发现与修订 | 证据 |
|---|---|---|
| 规则/输入因果 | 严格布尔类型、冻结日级来源身份；SIM反馈必须匹配股票/日期和可见时点；修订过的已消费bar保持失效，不因之后正常响应解除 | test_intraday_core.py；core.py、contracts.py |
| API/持久化/页面 | 来源失败必须提交UNAVAILABLE使旧机会不可领取；失败的head提交可重试且不把孤儿transition算成已提交；页面隐藏/取消请求后恢复刷新，不沿用旧revision响应；来源回调也受页面超时约束 | test_intraday_api_store.py；Playwright分钟用例 |
| 合同/性能/交付 | 普通轮询不逐次扫描全历史幂等链；批预算在锁内及提交前复查，不提交迟到输入；内部提交错误不吞为外部源失败；来源claim复核共用有界worker且最多等5秒；组件/测试只落现有ownership/CI路径 | store.py、service.py；预算延后与来源撤销用例；F2逐项矩阵 |
| 合入前源码身份 | rebase后实测Git LF/CRLF转换导致旧request的原始源码SHA不匹配；只将Python源码身份固定为UTF8_SOURCE_LF_V1，原数据文件继续原始字节SHA。追加跨换行转换测试；旧产物不覆盖，新request重新走完整闭环 | test_source_identity_survives_git_line_ending_conversion；下表新版CLI证据 |

## 本地验证与范围

- 最终小矩阵25项通过；其后源码换行修正定向回放文件4项、路径参数修正定向API文件6项均通过（共26个唯一用例）。完整变更计划由CI执行。
- Playwright既有文件3项通过，含本轮分钟提醒与两个旧日频行为；分钟测试覆盖显式创建、未知数量、只领取一次、隐藏暂停/恢复、陈旧数据可见。测试仅用拦截API，不等于真实TDX或数据库已可用。
- TypeScript无错误；changed-file Next lint无警告；Python changed-file Ruff无错误。测试依赖按原package-lock安装在任务worktree，未修改依赖声明。
- 不以小型fixture替代真实历史经济研究或运行验收。LiveInputs已实现只读服务适配，但本次没有连接TDX/DB，也未操作生产服务。
- F2详细设计14项与总蓝图12项校验均PASS、0警告；L0与guardrail_changed_files均PASS，ownership映射18/18、无歧义。
- L0首次提示claim来源检查的异常分支返回false，已改为抛typed SOURCE_UNVERIFIED，复跑通过。RAW_JSON_UI提示逐处核查：新增三处分别为HTTP序列化、localStorage引用序列化和区间hash输入，不是向UI显示原始JSON；旧页面三处不在本轮逻辑变更范围。
- classifier：targeted_ci_required、workflow_gate=passed、unexecuted_test_files=[]、unmapped_code_files=[]；选中position_timing_backend与position_timing_first_release，无DEV DB计划。
- 同步main及源码换行修正后的有效行预算：production=46,558、test=11,623、ratio=24.96%、over_budget=0。未改变runtime分类或删除安全测试。

## 独立进程功能回放

本轮例子由已有测试fixture构造器生成，在仓库外新建专属目录，不覆盖旧产物。它是合成的功能例子，虚拟source commit和candidate均明确不代表生产或R8。

artifact root：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/intraday_v1/research`。

| 身份/结果 | 值 |
|---|---|
| spec | functional_inputs/phase1_20261023_v1/spec.json |
| request路径 | requests/7685a812fb4a4e1c11e7802a5e9c5cd9319a010f81ea420400a4c7cf495fe575.json |
| request canonical SHA256 | 7685a812fb4a4e1c11e7802a5e9c5cd9319a010f81ea420400a4c7cf495fe575 |
| bundle目录 | bundles/7685a812fb4a4e1c11e7802a5e9c5cd9319a010f81ea420400a4c7cf495fe575 |
| manifest canonical SHA256 | 06c7aadfc787e2b0ad8b54205a128ea15c8f8622169b12ed27ef4f009ee3bcb1 |
| prepare/run/inspect/exact retry | PASS / MATERIALIZED / VERIFIED / ALREADY_MATERIALIZED；request及manifest身份不变 |
| 人口/输出 | 1计划、10根bar、10个decision、1机会、4个delay_intent |
| D0/D1/D3/D5 | 各意图仅EXECUTION_SLOT_ELIGIBLE，未确认成交；D0最早09:32，D1最早09:33；D5实际等待350秒 |
| outcomes_read / account_economics_simulated | false / false |
| database_read/write、network_accessed、runtime_action_performed | 均false（指回放数据与用户运行态，不指Git/CI网络） |

CLI入口：`python -m backend.services.position_timing.intraday.replay prepare --spec <spec> --timing-root <既有root>`；run使用返回request，inspect使用返回bundle，exact retry再次run同request。每次新进程重验源码文件/源文件hash；源码变更需新request，不能覆盖旧bundle。

## 交付状态与下一步

本模块源码/本地功能验证已具备；合入和CI以实际PR记录为准，不在CI结束前宣称全绿。用户runtime仍未重启/核验，LocalSIM和Advisory未接入，经济评价NOT_RUN_LOCALSIM_ADAPTER_PENDING。详细设计14项矩阵已回链源码/测试/转交合同；后两类消费者待办是用户已明确的owner分界，而非悄悄交付子集。

下一步：本PR通过CI后合入；用户重启后端后只读核验API；由用户转交已就绪的LocalSIM SDK包及后续Advisory组件/resolver需求，再以匹配成交代理完成D0/1/3/5账户评价。没有订单接口、模型训练、共享默认值修改、数据集激活或数据库变更。本轮无独立既有BUG修复，不创建虚构BUG关闭记录。
