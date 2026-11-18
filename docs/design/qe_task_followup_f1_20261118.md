# QE 已有任务统一接续工具（F1）

## 背景

用户于2026-11-18批准执行统一接续方案，明确新增硬门禁为0；任何新阻断条件、扩大阻断范围、额外锁、审批、CI检查项或资源限制均需另行批准。本交付复用正式任务、节点和入仓读取能力，解决临时脚本误读dataset_summary、观察器退出被误报为实验失败、收集异常诱发重复计算等问题。

## 范围与非目标

唯一运行入口为`scripts/qe_task_followup.py`，只能接续用户指定的既有task ID。正式提交、启动认领、节点对账、有效重试归并继续由原服务负责；工具没有POST、训练、推理、回测、调度、服务控制或数据库写入功能。无需部署服务或重启8001。

允许修改文件仅为本设计、该入口、`backend/tests/quantevolver/test_qe_task_followup.py`以及现有`tests/aistock_validation/catalog/file_ownership.yaml`中的精确文件路由。目录路由仅接入原qe_read_backend，不新增CI job、required check或控制项。不修改正在运行的接续器、实验合同、默认策略、数据集、生产包或资源规则。

## 实施方案

1. 一次读取正式custom-evo-config及节点GET状态，数据身份取逐Loop的_qe_batch_execution_policy.dataset_identity、批次execution_policy和已声明的absolute_target_replay.dataset；公开dataset_summary只作摘要。不要求绑定最新active或最新main，不增加运行期源版本门禁。身份未知或冲突如实报告，不写回任务。
2. 分列execution、observer、warehouse、collection、analysis。任务总状态采用正式调度状态，保留每个历史attempt，不按any(failed)自行重算；节点与控制端冲突明确展示。读取失败保留last_known并标明非实时，不伪装成当前成功或底层失败。任务内不同Loop独立读取，一个节点异常不阻塞另一个。
3. 入仓查询采用现有GET /qe-archive/runs，按精确task/Loop/experiment关联。多条匹配不猜最新；当前列表API未暴露latest标识时明确记录该限制。未知、被截断和歧义不冒充完整验收。
4. 可选collect-accounts只收集已完成账户的当前recorder三份实际账户产物及绝对目标合同原需的动作/终端结果。复用QEWorkspaceClient，只读传输；不反序列化、不重算经济指标。按task/node/Loop/recorder/源路径关联本地收据和SHA，恢复时复用已验证文件；缺失/损坏文件仅重新GET原产物，保留历史，不重跑计算。
5. 输出放在用户指定目录下的独立会话目录，文件不可变；无新锁，不覆盖正在改变的文件。一次快照默认不创建后台观察器；显式watch按现有45秒习惯轮询，可由操作者退出，退出不影响实验。辅助本地写出失败仅告警；收集必需产物缺失仅标为未收集。已有分析收据只报告路径、SHA、其声明状态，不把可读取当作经济验收。
6. 首版用已完成任务只读验证，不替换活动接续器；后续批次复用这个入口。源码审核和测试沿现有流程执行一次，不新增每Loop重复审批或额外实验。

## Design Acceptance Index

- F-001：只有指定既有任务的GET读取，复用正式能力，无提交/重跑/服务或DB写入。
- F-002：消费真实API契约，摘要不冒充完整身份；保留UNKNOWN与冲突，不换绑数据。
- F-003：执行、观察、入仓、收集、分析分开；失败历史不覆盖正式恢复后的总状态。
- F-004：当前recorder账户收集及逐文件恢复，校验实际SHA，不重复已完成计算。
- F-005：独立会话输出、辅助失败告警、watch只重试读取；不增加锁或硬门禁。
- F-006：真实API和已有账户fresh-process接续验证，零训练/推理/回测/生产写入。
- F-007：必要事故回归由原CI路由执行，直接测试后进行正确性和完整性作者审核。
- F-008：文档包含实际使用和迁移方式，当前任务接续器保持原样。

## 验证方案

直接测试覆盖实际摘要结构、身份冲突/缺失、提交成功后的读取断开、节点单侧异常、历史failed与总状态completed并存、入仓精确关联/歧义、同recorder收集恢复/新recorder隔离、可选报告异常及CLI零写入能力。已有冻结日历用例维持原覆盖。对`qe_20261118_105511_5a1b`执行一次真实只读快照、账户下载及重复收集；同一产物第二次不重复下载，原收据与SHA保留。源码阶段运行changed-file Ruff、直接测试、L0、diff及现有feature validate；CI沿原计划，不增加新检查项。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_task_followup.py | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-002 | scripts/qe_task_followup.py | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-003 | scripts/qe_task_followup.py | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-004 | scripts/qe_task_followup.py | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-005 | scripts/qe_task_followup.py | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-006 | scripts/qe_task_followup.py | validation-receipt: tmp/qe_task_followup_validation/real_receipt.json | PASS | 无 |
| F-007 | tests/aistock_validation/catalog/file_ownership.yaml | backend/tests/quantevolver/test_qe_task_followup.py | PASS | 无 |
| F-008 | docs/design/qe_task_followup_f1_20261118.md | validation-receipt: tmp/qe_task_followup_validation/real_receipt.json | PASS | 无 |

## 实际验收与使用

最终相关小矩阵：`python -m pytest -q backend/tests/quantevolver/test_qe_task_followup.py backend/tests/test_multi_alpha_frozen_calendar.py --disable-warnings`，43通过。Ruff和diff检查通过。现有CI分类为qe_read_backend，无遗漏测试，无新增检查项。

新进程对真实任务qe_20261118_105511_5a1b完成双节点GET，正式状态completed，2项精确入仓记录、10份账户与执行文件；6份原始账户pickle的SHA与先前正式收集回执相等。第二次接续10份全部复用，实际download_workspace_file_bytes调用0；运行期追踪到的HTTP方法只有GET。真实回执为`validation-receipt: tmp/qe_task_followup_validation/real_receipt.json`，不含凭据。节点目录没有远端SHA、入仓列表没有latest标识的事实均保留，未伪造远端哈希或最新attempt证明。没有训练、推理、回测、生产写入或服务操作。

正确性作者审核：API摘要与身份字段分开，错误状态任务不误识别为传输错误，controller/节点冲突保留；收集仅消费节点已完成来源，未新增启动限制。完整性/回归作者审核：元数据缺项不会冒充完整绑定，归档来源精确关联，断开后只重试读；已收集内容按recorder和SHA复用，辅助异常不重判实验失败，缓存损坏只重取原字节。作者审核不称独立审核。

一次状态读取（默认不启动后台观察器）：

```powershell
python scripts/qe_task_followup.py --task-id qe_20261118_105511_5a1b --env-file F:/Dev/AIstock/.env
```

接续收集并保留已有关联分析（第二次使用相同输出目录复用已验证文件）：

```powershell
python scripts/qe_task_followup.py --task-id qe_20261118_105511_5a1b --env-file F:/Dev/AIstock/.env --output-dir F:/Dev/AIstock_runtime/qe_task_followup/20261118_validation --collect-accounts --analysis-receipt F:/Dev/AIstock/tmp/handoff/qe-parent-representative-longtask-20261117/representative_analysis_v1.json
```

显式增加`--watch`才持续读取；Ctrl+C仅退出当前只读工具。退出码0表示读取到了任务快照，不代表底层实验、入仓、收集或经济验收全部成功，各阶段必须读各自状态；观察主数据不可用时退出码1。当前既有接续器未替换，后续批次使用本入口；停止使用工具即可回到既有接口，无任务、数据库或服务回滚动作。

## 风险与失败模式

节点目录旧版不完整、传输不稳定、入仓尚未到达和分析未完成分别显示，不据此降低并行或暂停其他实验。公开API不提供的latest/promotion资格保持未知，不直接访问私有表补造证明。下载仅存原始字节，不通过读取pickle计算新指标；Top20展示产物不能冒充完整预测，是否经济可接纳仍按原业务合同判断。复用缓存需校验内容和recorder身份，不能只因文件存在就当作成功。

## 生产门禁

新增硬门禁0，新增锁/审批/CI检查项/资源阈值0。现有业务验收和源码流程适用范围不扩大。若后续发现确需新增阻断，先向用户列明触发条件、动作、范围、恢复方式、耗时、风险和替代方案并等待批准；不得用预检或校验命名绕过。此工具为离线只读操作入口，生产DDL/DML、依赖、后台部署、激活均noop。
