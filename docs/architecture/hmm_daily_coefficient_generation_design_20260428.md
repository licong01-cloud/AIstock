# HMM 数据库收盘日度预测与系数生成详细设计

初始日期：2026-04-28；修订日期：2026-10-15；版本：v2.4；tier=F2。

状态：双入口方向已获用户授权；BUG-1862源码PR #6018及BUG-1895源码#6049、close-sync #6052均已合入。**指定快照的数据库日度T→T+1、真实零fit任务、不可变发布与登记读回已完成；共享自动触发尚未接线，目标消费器因未支持显式no-overlay合同而阻断。**单次任务完成不等于自动日更或目标场景已采用。既有每日API/任务/旧UI保留，不以单元测试、预检或旧接口代表完整能力。本设计展开父蓝图v2.86 §2.6/§4.6；QE冻结文件入口由`hmm_frozen_model_auto_extension_design_20261012.md`单独约束。

## 1. 背景与问题

现有系数消费者读取两类产物：

1. `model_train_snapshots.model_path` 指向的 HMM 模型快照；
2. 与 `models.json` 同目录下的 `coefficients_<preset>_<start>_<end>.json` 系数文件。

QE回测按同一冻结文件release产生合法历史窗口资产，不能借查询当前数据库补齐该回放。荐股/模拟盘未来日度模式则以数据库为明确主源：交易日T收盘后，待必需事实入库完整，只使用截至T且当时可得的数据，产生canonical下一交易日T+1的预测/系数。T+1不是自然日加一；长假/周末以正式交易日历决定。不暗中使用更早的系数冒充新日输出。

BUG-1862修复前的源码缺口：`_latest_completed_hmm_data_date()`只取三类表最大日期的最小值；`_execute_daily_coefficient_plan()`执行仍强制`qe_frozen_input_bundle`，造成“DB最新日期+旧文件输入”混用。不能把MAX(date)当完整性证明或用文件生成成功代报数据库新日推理。read-only `hmm_data_source/realtime_source.py`读取既有prediction，也不单独证明数据库推理、登记与消费已完成。

BUG-1862已合入源码：日度规划复用`market.dataset_date_refresh_audit`及`DataRefreshAuditRepository.require_success()`，在只读repeatable-read事务中核对三类必需事实的完成记录；表最大日期仅作诊断，完成记录不代替模型所需逐键输入验证。默认生效日为正式日历中T之后的首个交易日，手工指定更晚交易日保留§4.8的兼容行为。显式数据库reader核验连续数值前缀、共享稀疏行业ID、PIT成员及既有发布区间，并与file adapter共用特征、滤波及系数核心。只有输入和模型均通过验证才返回`source_mode=database_daily / input_validation_status=VALIDATED / generation_available=true`；失败显式报错且不创建job。原快照metrics中的train/validation边界冻结原推断起点（原validation_start减3×365+30日），不按每日输出窗口重置状态；当前读取的成员事实不冒充历史as-known。日度任务冻结plan/输入/模型/参数/生成器身份，资产独立加锁、原子发布、登记事务回读；登记失败可恢复同一资产，不覆盖旧文件。文件CLI只接受历史`qe_frozen_file`，日度数据库入口不调用该CLI。

2026-10-14输入复验：2023-01-26..2026-10-12的117,507个行业/日期键中，原重复报价冲突为0、歧义成员连接为0。新的首个阻断是801011.SI/2023-01-30：该行业不在冻结系数资格范围，却在数据库中具有真实历史行情；与同快照`market.sw_daily`逐值核对后，788个此类历史键均是正式源事实，不应删除。BUG-1895只修复HMM consumer：保留这些上下文、钉住正式源摘要，仍不为资格范围外行业产生系数，不修改共享发布policy或数据。

2026-10-14任务分支历史预检：Conda AIstock fresh process、只读repeatable-read事务执行真实preview，`as_of=2026-10-12 / effective=2026-10-14 / source_mode=database_daily / input_validation_status=VALIDATED / generation_available=true`；fit及文件worker设poison，未训练、创建job、写库或发布资产。**当时仅为手工更晚生效日预检，不是默认T→T+1或业务完成。**当时完成记录仅到10月12日，其下一交易日10月13日已早于读取日，默认模式必须等待/拒绝，不能自动跳到更晚日期或将MAX当完成水位；这条历史as-known守卫保持不变。当前运行状态见下节，不沿用当时的pending描述。

公共事实合同核验必须区分当前读取与历史当时可知：`observed_at`及`FIRST_PUBLICATION_UNVERIFIED`不能证明旧日as-known，但也不表示行情值缺失、不应自动阻断以当前确实可得的完整事实预测未来。已定位共享发布区间policy `dataset_release/sw_l2_quote_policy.py`，不另建HMM行业编号或发布规则。数据库模式仍须核对当前行业有效区间及同快照连续推断起点/检查点身份；旧快照没有保存该起点时不得由滚动短lookback猜测替代。由HMM执行模型所需窗口的业务验证，不要求数据窗口重导历史或替HMM验证模型。

本包只补齐已有模型的输入/自动消费链，不重新训练或研究新模型，不改变任何已批准特征、预处理、状态/系数公式、seed、阈值及训练窗。已完成L1历史产品仍保留，新演进/输出沿父蓝图申万L2方向，不能把旧L1复制成L2。

### 1.1 2026-10-15实际日度任务与消费边界

BUG-1895源merge=`442d6ac61efd9d9682b079f1a6a44a17dfb17b39`，用户重启后的runtime identity=`5c5fc5684458582115a28ae5f4f5c8b1b08a0829`已包含该修复；Issue #6045关闭、close-sync #6052合入。指定config=`fc0683a9-6ca0-4d55-b053-17c2360c3c40`、snapshot=`ecee7c40-6764-49ad-bc0f-1c6c6b390504`、preset_A的真实job=`9c97a277-9ab3-4373-9b9d-ff4b034dcf01`为COMPLETED/GENERATED/REGISTERED_READBACK，`as_of=2026-10-14 → effective=2026-10-15`，`source_mode=database_daily`，fits=0，无target读取。真实DEV writer/job/readback先在既有aistock_dev事务内验证并回滚；随后生产仅执行已获指定授权的正式API任务/登记，未手写生产SQL。

资产位于`F:/Dev/AIstock/backend/data/hmm_models/fc0683a9-6ca0-4d55-b053-17c2360c3c40/2026-05-19/coefficients_preset_A_2026-10-15_2026-10-15__daily_76f8741b4e613607d0b2b495a5500b34f20b5f33c349d8edcb4bc769adc549fa.json`；SHA=`66602de8dc6c9aa0db6d9522df0da67db4b7e027e292c9c965c7ce2a957f6b06`，693092 bytes。131目录/完整membership、124有限系数、7个quote-unavailable行业明确使用`explicit_no_l2_overlay_v1`；旧QE窗口及资产保留。此处不扩展到其他快照或模型schema。

本轮只读fresh-process以实际snapshot API和该资产运行`SectorHMMRuntime.preflight_coefficients(auto_compute=false, require_frozen_snapshot=true)`，未推断/写库，准确返回`HMMRuntimeUnavailableError: HMM coefficient artifact has stock sector mapping without coefficients`，涉及801011/801019/801117/801207/801216/801961/801983.SI。这不是数据漏采，也不能给它们补1.0；Selection owner须按既有quote authority和显式no-overlay合同处理，同时保持未知缺系数fail closed。消费源码属于`backend/services/selection_center/hmm_runtime.py`及其profile，不由HMM越界修改。该smoke只证明真实兼容阻断，不是目标API/UI验收。

自动触发另有独立缺口：`backend/ingestion/tdx_scheduler.py`尚无通用callback注册/取消公共入口，HMM旧初始化仍同时创建一分钟训练与文件延伸tick；不得用它顺带启动训练。公共接口交调度owner，HMM的日度订阅/reconcile接线与真实自动job验收待接口闭合后完成。正式单次API运行不需scheduler，也不使日度自动订阅视为已启用。正常数据/结果变化不改环境变量，不要求重新启动后端。

## 2. 目标与范围

- 支持对某个完成的 `model_snapshot_id` 和 `signal_preset` 生成每日系数产物。
- 日度模式使用数据库最新**已完成且逐键核验**的收盘日T作为`as_of_trade_date`，不能由墙钟或MAX(date)单独决定。
- 默认使用 `as_of_trade_date` 后的下一个交易日作为 `effective_trade_date`。
- 日度系数沿现有HMM资产/登记合同提供给显式选版本的消费者；轮动/risk结果沿各自既有writer/schema，不将score冒充coefficient。只交付该模型已批准的输出类型，不新增并行格式。
- 不修改已有模型权重、训练快照、历史系数产物或 StrategyPackage manifest。
- 不做中性系数、空系数、默认路径、日频 fallback 等静默兜底。
- 复用已有调度/数据完成能力，日度数据就绪后派发一次幂等零fit任务；人工显式API与自动派发共用任务函数。无独立HMM scheduler、额外依赖或每分钟重训，正常更新不改环境变量/源码或要求重启。

## 3. 核心概念

| 字段 | 含义 |
| --- | --- |
| `model_snapshot_id` | 已完成 HMM 模型快照，必须存在且状态完成。 |
| `signal_preset` | HMM 系数预设，例如 `preset_A` / `preset_B`。 |
| `as_of_trade_date` | 生成系数时可用的最新已完成交易日。 |
| `effective_trade_date` | 系数实际用于选股/模拟盘的交易日，必须晚于 `as_of_trade_date`。 |
| `generation_mode` | 固定为 `daily_asof_prediction_v1`。 |
| `input_data_max_dates` | 仅作诊断水位，不能单独证明各日期/字段完整。 |
| `source_mode` | 日度计划/登记固定为`database_daily`；QE文件计划/登记为`qe_frozen_file`，未知模式拒绝。源码合入、实际加载和业务运行分别验收。 |
| `input_identity` | 紧凑对象包含source_mode、as_of/effective日期、read_cutoff_at、原推断起点、canonical输入摘要、完成记录、mapping authority及quote policy摘要；BUG-1895另钉住`official_quote_authority=market.sw_daily`、`official_quote_sha256`及被排除的真实报价上下文键数。calendar/发布区间/正式报价在输入摘要中闭合。DB不伪造dataset manifest，训练身份不改写。 |
| `artifact_sha256` | 生成文件的 SHA256，用于追踪与审计。 |

## 4. 数据入口架构与PIT契约

1. 对交易日 D 的 HMM 系数只能来自 D 之前已经完成的数据。
2. `effective_trade_date <= as_of_trade_date` 一律失败。
3. `as_of_trade_date` 与 `effective_trade_date` 必须都是交易日。
4. 显式数据库adapter只读版本所需`market.sector_data`、`market.sw_daily`、`market.index_daily/000300.SH`及其已批准股票/市值/资金流、calendar、PIT/quote authority上下文。字段、单位、预热和来源沿对应版本，不因日度模式新增特征，也不省略必要C-010/A5输入。
5. 在只读一致性事务内核验T及必要lookback的预期键、唯一性、有限必需字段、完整入库与合法NA；记录紧凑input identity。收盘/入库未就绪显式等待，真正缺数或身份错误typed fail closed。不得以MIN(MAX(date))冒充完整，也不扫描/复制无关全历史。
6. 行情/资金流观察日期不得晚于T；mapping/quote facts必须在冻结读取截至时刻已知，按原版本有效日/PIT合同解析，不能用事后发布信息。已于T公布而T+1生效的合法PIT事件不等于读取未来行情。整数ID经共享映射转canonical申万文本代码，不重编号；历史修订后的当前全表不能冒充旧日当时数据，若无as-of事实则不可验证旧日回放。
7. 仅feature-only推理，禁止读取未来target/outcome。复用该版本冻结预处理/semantic和数学核心；有状态HMM按原合法起点连续前向滤波，或从匹配相同model/preprocess/input-prefix身份的合法检查点续算，不只取短lookback重置。没有状态的模型不新增该要求。
8. 必须分别验证T与下一交易日，自动任务只使用`effective_trade_date=next_open_day(T)`。旧手工接口允许更晚交易日的兼容行为不被默默删除，但不能据此报告T+1验证成功。未来行情不足/日历缺失不改用更早输出；只返回明确状态，不关闭过滤或补默认regime。
9. 每日系数为新增不可变产物；同身份/同内容才返回`EXISTS`，同名不匹配拒绝覆盖。正式发布和登记完成前不被Consumer读取；数据源晚修订形成新input identity/版本化文件，保留旧文件，不只改旧hash标签。
10. 推理资格沿既有版本效果/适用合同；T必须晚于模型训练及semantic校准的已批准截止，未知训练/校准边界或不支持的输入合同拒绝推断，不能仅以snapshot状态COMPLETED放行。此项不新增效果阈值，旧历史资产不追认或删除。
11. 区分数据库事实真实性与冻结系数资格：数据库历史三项报价`sw2_pct_change/sw2_vol/sw2_amount`逐键匹配同一只读事务的`market.sw_daily`；真实但不满足既有发布资格的历史上下文可以保留，不能因此进入`coefficient_sector_codes`。资格范围内必需字段缺失仍失败；正式源缺失、部分报价、重复、非有限、负数量或缓存/源不一致均typed fail closed。真实资金流仍独立保留。正式报价canonical排序并进入input hash，行顺序不影响身份；源修订不能被排除范围或摘要复用掩盖。该修复不改变QE冻结文件reader及§10的历史发布合同。

## 5. API 设计

既有路由保留（不是本次新建）：

```text
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/preview
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/generate
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/jobs
GET  /api/v1/hmm-training/daily-coefficients/jobs/{job_id}
GET  /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/jobs
```

请求体：

```json
{
  "signal_preset": "preset_A",
  "as_of_date": "2026-10-12",
  "effective_trade_date": "2026-10-13",
  "confirm_generate": true
}
```

说明：

- `as_of_date`可省略；adapter据共享完成记录选T并逐键验证。指定快照默认T→T+1的真实任务已按§1.1完成；其他日期继续按相同完成水位/因果规则验证，不从单次成功推导自动日更。
- `effective_trade_date` 可省略，后端自动选择 as-of 后的下一个交易日。
- 现有`generate`/`jobs`使用`confirm_generate=true`，不再把旧文档的`confirm_text`写成有效API要求。自动模式沿已登记enabled policy，不为每个交易日再要求人工确认；该模式尚未正式接线。
- 日度API/自动计划明确选择`database_daily`，文件extension明确选择`qe_frozen_file`；模式写入现有plan/登记元数据，不能按“哪处有数据”自动选择。数据库模式的一次性enabled配置保存在既有config_json，默认未启用，钉住snapshot/preset；已启用的QE文件policy不隐式启用日度模式。不新增表或环境变量开关。
- `/jobs` 是 UI 和长耗时生成的权威入口：请求核验参数、模型、完整PIT输入与目标产物后创建持久化任务并返回`job_id`；数据库模式在FastAPI background task中调用同一进程的共用核心，QE文件模式仍使用既有文件worker；UI轮询任务状态。完整前缀预检有实际读取成本，不宣称该请求只需常数时间。
- 保留同步 `/generate` 仅用于直接 API/脚本诊断，不作为前端长耗时交互路径。

## 5.1 异步任务与代理超时控制

Next.js dev rewrite proxy 对长时间 HTTP 连接不应作为业务可靠性边界。数据库每日系数任务在后端执行Python零fit推理；QE文件延伸沿其WSL流程。UI不等待同步`/generate`请求完成。

复用既有持久化表（本次不要求新表）：

```text
model_train_daily_coefficient_jobs
```

关键字段：

| 字段 | 含义 |
| --- | --- |
| `job_id` | 既有持久任务身份；手工/自动来源明确区分，不建平行队列。 |
| `snapshot_id` / `config_id` | HMM 快照与配置来源。 |
| `signal_preset` | 本次生成使用的 HMM 信号预设。 |
| `as_of_trade_date` / `effective_trade_date` | PIT 数据截至日与生效交易日。 |
| `status` | `PENDING` / `RUNNING` / `COMPLETED` / `FAILED`。 |
| `result_status` | 日度产物结果：`GENERATED`或`EXISTS`；QE extension沿其既有返回值。 |
| `plan_json` | 创建任务时冻结的校验计划。 |
| `result_json` | 完成后记录的产物结果。 |
| `error_message` / `error_context` | 失败时的明确错误与堆栈尾部。 |

失败语义：

- 数据库推理失败、目标文件未生成、已有文件元数据不匹配或登记回读失败等进入`FAILED`；QE文件worker沿既有失败语义。规划时缺数据/非交易日直接拒绝且不建job。UI展示真实错误。
- 不允许把失败任务转成空系数、默认系数或成功状态。
- 不覆盖已有不匹配产物；已有产物必须同时满足同身份、重新确定性计算的内容一致及登记回读，才记录为`EXISTS`。

日度幂等键钉住mode、config/snapshot/model/parameter/preset、T、effective、input identity及generator版本；沿现有config/job锁保证同身份只执行一次。已有完成同身份直接读取；RUNNING崩溃沿既有恢复规则接续，FAILED不被当成功、不自动重训或偷偷改输入。数据尚未就绪是规划等待原因，不捏造成功job，也不另设平行状态机。源/参数在发布前后漂移则拒绝登记；即便已写文件也不能冒称已登记完成。

`input_data_max_dates`记录实际已验证读入前缀的T，物理表MAX仍只在`database_completion`中作为诊断。相同事实的再次读取不因时间戳不同产生另一个幂等键，但已存在资产的原合法`read_cutoff_at`必须在新持久任务建立前闭合并冻结；计划、执行结果和登记的输入身份必须一致。若并发计划冻结了不同读取时刻、已有资产与该计划不一致，则明确失败，不在执行期间改写持久计划或把另一资产冒充本次成功。恢复原时刻须验证时区、T/effective边界及完成记录不晚于原读取时刻。

共享触发先做轻量就绪/幂等核对，长推断由已有任务执行路径承担，不堵住其他数据入库/健康检查。旧`init_hmm_scheduler()`目前把一分钟训练tick和系数tick一起启动，新日度接线不得复用该副作用入口。现有全局数据scheduler尚无核验的通用callback注册，必要最小接线交调度owner；HMM不新增scheduler、安装APScheduler或直接写私有job列表。沿共享频率补偿错过的就绪事件，不规定一分钟计算，也不改变既有训练cron/参数。

## 6. 产物格式

每日产物保留现有JSON字段，并以完整日度身份寻址，防止输入修订时覆盖：

```text
coefficients_<preset>_<effective_trade_date>_<effective_trade_date>__daily_<daily_key>.json
```

文件内容保留现有字段：

```json
{
  "model_path": ".../models.json",
  "preset_key": "preset_A",
  "preset_coeffs": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
  "daily_coefficients": {"2026-04-28": {"801783.SI": 1.05}},
  "stock_sector_map": {"600000.SH": "801783.SI"}
}
```

并新增审计字段：

```json
{
  "generation_mode": "daily_asof_prediction_v1",
  "as_of_trade_date": "2026-04-27",
  "effective_trade_date": "2026-04-28",
  "generated_at": "2026-04-28T...Z",
  "snapshot_id": "...",
  "config_id": "...",
  "input_data_max_dates": {"sector_data": "2026-04-27"}
}
```

以上数值/日期/股票归属仅为兼容格式示例，不是股票真实PIT归属、当前生成记录或完整目录输出；正式输出必须给完整目录状态和全部输入合格L2，股票映射所指可报价行业必须存在于同日系数。preset明确定义的neutral=1.0是模型语义，不允许用它填补缺失。新日度source/input identity进入正式计划及登记，模型/训练身份保持原值；consumer可查as-of、effective、来源、实际coverage和typed unavailable，不将回测资产重标成当天结果。

## 7. UI 设计

以下是2026-04-28已有`/paper-v2/model-hmm`兼容交互，不在本次修改Paper/Selection/Advisory页面或代报浏览器验收。后续HMM-owned状态读回显示真实任务和来源，场景消费者由对应owner按既有合同验证：

- 选择已完成 HMM 快照；
- 选择信号预设；
- 可选填写数据截至日；
- 可选填写生效交易日；
- 先预览再确认生成；
- 生成后刷新快照产物列表；
- 所有错误直接展示后端 fail-fast 详情。

## 8. 非目标与边界

- 不自动重训 HMM。
- 不修改 HMM 模型权重或已有训练快照。
- 不把缺失系数替换为 1.0 中性系数。
- 不绕过 Selection Center/Paper v2 现有 HMM runtime。
- 不从 Windows 侧拼接或访问 WSL UNC 路径；WSL 子进程通过 `/mnt/<drive>` 写回 Windows 工作区。
- 不把文件-only QE生成器改成DB错误fallback；数据库adapter是显式新主入口，绑定与验证在调用共用核心前完成。
- 不建设通用feature store、数据集导出、记录平台或新调度器，不复制另一套特征/滤波代码。
- 不改既有can_buy、订单、持仓、黑名单或QE/荐股/模拟盘算法；日度输出可读取不等于场景已批准采用或收益已验证。

## 9. 验证方案

以下区分源码直接矩阵、已完成的指定快照真实DEV回滚/日度job/登记与待执行的共享触发及消费者验收。§1.1记录当前实际结果；下列验证合同不因单次成功而移除，也不为了补记录重复已完成动作。

- 同一合法输入的file/DB adapter在相同数值环境下共用核心并产生相同特征/状态/系数；DB缺数不切文件、QE缺数不查询DB，两个日期/身份不能混用。
- DB有T但文件cutoff更早：日度模式只凭DB完整合同运行，文件模式仍只覆盖其release；未来target/下一交易日观察读取设poison并断言未访问。现有quote/moneyflow及正常停牌、历史证券alias/稀疏行业ID直接矩阵不降低。
- 有状态模型验证连续前缀/检查点身份及长假T→T+1；仅MAX(date)到T而逐键缺失、部分入库、PIT错误、数值非有限、日历不足明确失败/等待，不偷用昨日结果。无状态版本不被额外要求HMM state。
- 数据库真实历史报价须与同事务正式源匹配，资格范围外报价保留但不输出系数；源缺失、损坏、身份/数值漂移、future、重复明确失败，行顺序变换identity不变。当前事实读取不能冒充旧日as-known，手工延期不能代报默认T→T+1通过。
- 持久任务验证重复触发、同身份重放、崩溃恢复、参数/输入漂移、不可变发布和登记readback；真实共享调度callback与旧训练tick poison隔离，不能只用mock reconcile冒称自动触发通过。
- 小样本DEV只读输入及已批准的现有DEV事务回滚写入/回读分开验证；生产启用/写入需目标级授权。消费者真实读回来源、T、T+1、模型与资产hash；API/UI未实际跑不填通过，不控制用户服务。
- 只跑changed-file直连矩阵、ownership/module/test plan、Ruff/py_compile/fresh import、必要L0/F2与CI，不重复历史模型/全部实验。首个真实自动任务、registered asset和无副作用消费smoke是闭环终点，不以单元/导入替代它们。

## 10. 冻结 release 的行业行情可用性合同（2026-09-19）

正式历史系数的目录、成员与行情可用性是三个不同维度，不得再用“成员存在”推导“行业指数每日仍发布”：

1. `sector_code_map.json` 继续保留 131 个 C-013 正式申万 L2 目录身份；
2. `sector_membership_spans.parquet` 继续保留完整 PIT 股票—行业成员关系；
3. successor release 必须提供 `aistock_release_sw_l2_quote_availability_v1` 权威文件，逐行业列出闭区间 `availability_spans`，并与 code-map 使用同一 `mapping_authority`；
4. frozen input bundle 必须钉住该文件 SHA-256，生成器还须重算 `quote_availability_digest`；缺文件、哈希漂移、authority 不同、目录缺项、区间重叠均 fail closed；
5. 某日只有同时满足 PIT membership 与 quote availability 的 L2 才能产生正式系数；已发布区间内任何缺失、NaN 或非有限行情继续 fail closed；
6. authority 明确处于无行情区间的 L2 不生成系数，不补零、不前填、不伪造中性系数；成员关系仍完整保留；
7. 消费端只接受 `explicit_no_l2_overlay_v1`：股票当日映射到无行情 L2 时保持原始 score，并写入 `hmm_l2_quote_unavailable_no_overlay` trace；它不是系数 `1.0`，也不是隐式 fallback；
8. 缺少系数但未被当日 quote-unavailability authority 精确覆盖时必须失败，不能借“停发”语义掩盖真实漏采。

因此，119 个有成员行业可以在特定截止日明确分成 113 个有正式行情行业与 6 个停止发布行业；该数量是 release authority 的只读结果，不得硬编码进 HMM 源码。数据窗口只负责在 immutable successor 中补齐真实漏采并提供 authority/receipt/manifest，HMM 不修改既有 R8。

本节保留2026-09-19文件历史合同和R8当时数量，不代表日度DB必须先导出successor。数据库模式读取当时可得的正式PIT/发布可用性事实并按同一合法缺失语义核验；停发只约束报价列，真实moneyflow不得清空。缺少必要authority时明确阻断，不能查询当前is_pub反向删旧日行业、私建行业编号或生成替代指数。

### 10.1 风险与失败模式

当前数据库事实是否具有足够PIT/available-at及完整水位必须在实现时直接核验；缺少时不以当前表值冒充因果历史。数据晚到、修订和模型所需输入不兼容分别报告等待/漂移/失败，不因此自动改模型或训练。共享调度接口尚未接线、任务运行与发布/登记之间崩溃均有真实风险；须以实际job/readback验证，不从单元或旧API表面推出自动完成。日度工程成功也不提升模型效果/场景准入状态。

## 11. Implementation Plan / 发布与回滚

完整能力包仍包含HMM数据库adapter、既有数学核心复用、显式日期/source计划、共享触发/持久job、不可变发布与完成结果读回；不把未验收层次切成假完成阶段。BUG-1862源码已合入；BUG-1895仅修改HMM数据库reader、直接测试、BUG记录及本设计状态，修复真实历史事实与资格范围混用。公共调度接线及完成事实水位交对应owner，不修改共享事实或其他模块源码。不以HMM部分源码修复代报全部F-001～F-006验收完成。

只读preview/手工任务不要求自动scheduler才能工作；自动数据延伸需一次性模式启用和真实接线，不要求每月人工迁移。第一次运行源码更新由用户加载；日后数据入库、正常job/结果登记不依赖环境变量或重启。停用只禁止新任务，保留历史资产/模型。故障保持明确状态，回滚按模型/任务/资产identity寻址，不覆盖旧版本、触发训练或控制服务。QE和日度模式分别验收，不把QE Composer通过称为荐股/模拟盘已采用。

## 12. Design Acceptance Index

- F-001：数据库日度与QE文件主入口明确分离，同版本核心复用，旧版本/模型参数不变。
- F-002：T收盘完整事实、PIT/available-at与下一交易日，无未来标签、旧数据改名或混源。
- F-003：训练身份/本次推理identity分离，冻结预处理与状态连续性，正常停牌/停发保持合法语义。
- F-004：复用job/锁/状态与immutable发布，幂等、崩溃恢复、失败及readback，不建新队列。
- F-005：复用共享就绪触发，旧训练tick隔离，无APScheduler/一分钟业务依赖，手工/自动同一实现。
- F-006：真实完成输出及消费者读回分别验收；生产、源码加载、模型效果/收益与研究记录不混同。

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 本文§1/§4/§8；backend/services/hmm_training_service.py；scripts/precompute_hmm_coefficients.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py原直接矩阵；§1.1真实database_daily job零fit及REGISTERED_READBACK | APPROVED_BY_USER_CONTRACT_CURRENT_SNAPSHOT_SINGLE_JOB_RUNTIME_VERIFIED | 仅指定快照，不外推自动派发或场景采用 |
| F-002 | 本文§3/§4；backend/services/hmm_data_source/daily_input.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py原正式行情/资格/漂移反例；§1.1真实10-14→10-15输入及runtime identity | APPROVED_BY_USER_CONTRACT_CURRENT_SNAPSHOT_T_PLUS_ONE_VERIFIED | 历史as-known守卫保持，其他日期不由此免检 |
| F-003 | 本文§4/§6/§10；scripts/precompute_hmm_coefficients.py；scripts/hmm_horizon_v2_train.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py原模型/参数/起点矩阵；§1.1 model SHA与input identity/完整membership读回 | APPROVED_BY_USER_CONTRACT_CURRENT_SNAPSHOT_SINGLE_JOB_RUNTIME_VERIFIED | 不另建数据集、重编号或自动重训；不支持的20D schema显式拒绝而非替代 |
| F-004 | 本文§5.1/§6；backend/services/hmm_training_service.py；backend/services/hmm_data_source/coefficient_extension.py | backend/tests/hmm_data_source/test_hmm_daily_coefficients.py原直接矩阵；§1.1真实DEV回滚writer/job/readback及正式API生产任务/REGISTERED_READBACK | APPROVED_BY_USER_CONTRACT_DEV_ROLLBACK_AND_SINGLE_JOB_REGISTRATION_VERIFIED | 不授权重复生产写入或新配置启用 |
| F-005 | 本文§5.1；backend/routers/hmm_training.py | 计划扩展backend/tests/hmm_data_source/test_coefficient_extension.py的真实共享callback/训练poison及持久job，不能只调用service | APPROVED_BY_USER_DESIGN_ONLY_PENDING_IMPLEMENTATION | 公共接线由owner协调，不能用mock代报 |
| F-006 | 本文§7/§9/§11；§1.1实际资产consumer只读smoke | artifact: F:/Dev/AIstock/backend/data/hmm_models/fc0683a9-6ca0-4d55-b053-17c2360c3c40/2026-05-19/coefficients_preset_A_2026-10-15_2026-10-15__daily_76f8741b4e613607d0b2b495a5500b34f20b5f33c349d8edcb4bc769adc549fa.json；真实reader拒绝7个合法no-overlay行业 | APPROVED_BY_USER_CONTRACT_SINGLE_JOB_VERIFIED_PENDING_TARGET_CONSUMER | Selection owner须接入显式no-overlay及只读资产消费；真实场景API/UI仍未验收，不报收益或采用 |

上述状态区分源码测试、真实输入、DEV/生产单次登记与运行验收；APPROVED_BY_USER_CONTRACT只指既有合同批准，绝非缺失证据豁免或新的生产授权。旧直接矩阵仅沿用原源码验证事实，本次未重跑。F-005及F-006目标消费仍未完成，不用单次job或单元测试替代。

## 14. Production gates / DESIGN-COMPLIANCE-001

文档本身无运行影响；BUG-1862及BUG-1895源码包实际属于`runtime_impact=backend / target_ids=[backend-main]`。BUG-1895用户重启后的identity及业务probe通过，close-sync已完成；源码运行状态不能由文档runtime=none降级。本次仅只读核验和文档同步：production_ddl_gate=noop；production_dml_gate=noop；dependency gates=noop；database_write=false；dataset_write=false；training=false；process_control=false。§1.1历史DEV/生产写入与本次零写入分别报告。

后续运行源码按实际changed files重新分类，不以文档runtime=none降级。数据库源只读；任务/登记写入与生产启用另按既有DEV先验和目标授权执行，不因本设计获批自动执行。所有后端启动、停止、重启归用户。

DESIGN-COMPLIANCE-001：完整输入→零fit推理→发布登记→真实读回，不以旧接口/设计代报完成；缺数/等待/损坏明确分离，无neutral/file/DB silent fallback；旧模型、参数、场景交易及其他模块不迁移；仅保持既有因果/PIT/数值/权限，不新增分钟调度、资源、全行业三态或每月审批门。

## 15. 修订记录

2026-04-28：每日接口与异步任务设计，旧Paper/Selection交互保留。2026-09-19：历史冻结文件quote availability合同。2026-10-13 v2.0：更新为DB收盘T→T+1明确主入口、共享核心/就绪触发；修正旧confirm_text和最大日期/冻结文件混用表述，保留历史契约。2026-10-13 v2.1：BUG-1862分支接入HMM数据库推理及来源隔离，记录源码验证与真实数据/登记/触发验收差异，恢复手工更晚交易日兼容；模型及特征公式不变。2026-10-13 v2.2：复审修正计划实际读入水位与重复资产原读取身份；运行、真实输入和共享触发状态不提升。

2026-10-14 v2.3：同步BUG-1862已合入状态及原数据冲突消失的只读结果；BUG-1895分离真实数据库历史行情与冻结系数资格，用同快照正式行情校验并钉住输入身份，不扩大系数范围。记录手工延期preview通过与默认T→T+1/实际job/登记/消费者仍待验收的差异；模型、文件历史合同及共享数据不变。

2026-10-15 v2.4：同步BUG-1895合入/重启生效/close-sync和指定快照真实T→T+1零fit任务、DEV回滚/生产登记及资产SHA；真实只读consumer smoke发现7个合法no-overlay行业尚未兼容。共享自动触发与目标消费仍pending，分别交对应owner；不重复job/训练/写库，不修改其他模块或原模型/输入公式。
