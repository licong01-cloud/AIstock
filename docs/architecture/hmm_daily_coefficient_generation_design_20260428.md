# HMM 数据库收盘日度预测与系数生成详细设计

初始日期：2026-04-28；修订日期：2026-10-13；版本：v2.1；tier=F2。

状态：双入口方向已获用户授权；BUG-1862任务分支已实现数据库adapter、共用零fit推理、不可变发布及既有登记接线，**尚未合入；真实数据库输入预检仍阻断，共享触发及真实任务/消费者验收未完成**。既有每日API/任务/旧UI保留，不以单元测试或旧接口代表完整能力。本设计展开父蓝图v2.85 §2.6/§4.6；QE冻结文件入口由`hmm_frozen_model_auto_extension_design_20261012.md`单独约束。

## 1. 背景与问题

现有系数消费者读取两类产物：

1. `model_train_snapshots.model_path` 指向的 HMM 模型快照；
2. 与 `models.json` 同目录下的 `coefficients_<preset>_<start>_<end>.json` 系数文件。

QE回测按同一冻结文件release产生合法历史窗口资产，不能借查询当前数据库补齐该回放。荐股/模拟盘未来日度模式则以数据库为明确主源：交易日T收盘后，待必需事实入库完整，只使用截至T且当时可得的数据，产生canonical下一交易日T+1的预测/系数。T+1不是自然日加一；长假/周末以正式交易日历决定。不暗中使用更早的系数冒充新日输出。

已核验源码缺口：`_latest_completed_hmm_data_date()`只取三类表最大日期的最小值；`_execute_daily_coefficient_plan()`执行仍强制`qe_frozen_input_bundle`，造成“DB最新日期+旧文件输入”混用。不能把MAX(date)当完整性证明或用文件生成成功代报数据库新日推理。当前read-only `hmm_data_source/realtime_source.py`读取既有prediction，也不证明它已完成该推理adapter。

BUG-1862本地修复状态：日度规划复用`market.dataset_date_refresh_audit`及`DataRefreshAuditRepository.require_success()`，在只读repeatable-read事务中核对三类必需事实的完成记录；表最大日期仅作诊断，完成记录不代替模型所需逐键输入验证。默认生效日为正式日历中T之后的首个交易日，手工指定更晚交易日保留§4.8的兼容行为。显式数据库reader核验连续数值前缀、共享稀疏行业ID、PIT成员及既有发布区间，并与file adapter共用特征、滤波及系数核心。只有输入和模型均通过验证才返回`source_mode=database_daily / input_validation_status=VALIDATED / generation_available=true`；失败显式报错且不创建job。原快照metrics中的train/validation边界冻结原推断起点（原validation_start减3×365+30日），不按每日输出窗口重置状态；当前读取的成员事实不冒充历史as-known。日度任务冻结plan/输入/模型/参数/生成器身份，资产独立加锁、原子发布、登记事务回读；登记失败可恢复同一资产，不覆盖旧文件。文件CLI只接受历史`qe_frozen_file`，日度数据库入口不调用该CLI。**真实只读预检在801012.SI/2026-07-01发现重复行业事实冲突：002086.SZ的PIT归属已经变化，而stock-level sector_data仍含旧行业报价。未修改或忽略该事实、未生成实际日度资产、未写库；BUG保持OPEN，共享触发及运行验收仍待闭合。**

公共事实合同核验必须区分当前读取与历史当时可知：`observed_at`及`FIRST_PUBLICATION_UNVERIFIED`不能证明旧日as-known，但也不表示行情值缺失、不应自动阻断以当前确实可得的完整事实预测未来。已定位共享发布区间policy `dataset_release/sw_l2_quote_policy.py`，不另建HMM行业编号或发布规则。数据库模式仍须核对当前行业有效区间及同快照连续推断起点/检查点身份；旧快照没有保存该起点时不得由滚动短lookback猜测替代。由HMM执行模型所需窗口的业务验证，不要求数据窗口重导历史或替HMM验证模型。

本包只补齐已有模型的输入/自动消费链，不重新训练或研究新模型，不改变任何已批准特征、预处理、状态/系数公式、seed、阈值及训练窗。已完成L1历史产品仍保留，新演进/输出沿父蓝图申万L2方向，不能把旧L1复制成L2。

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
| `source_mode` | 日度计划/登记固定为`database_daily`；QE文件计划/登记为`qe_frozen_file`，未知模式拒绝。已在任务分支接入，不代表已部署。 |
| `input_identity` | 紧凑对象包含source_mode、as_of/effective日期、read_cutoff_at、原推断起点、canonical输入摘要、完成记录、mapping authority及quote policy摘要；calendar/发布区间内容在输入摘要中闭合。DB不伪造dataset manifest，训练身份不改写。 |
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

- `as_of_date`可省略；任务分支adapter据共享完成记录选T并逐键验证。当前真实数据冲突仍阻断，不从该默认行为推出可生成。
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

以下区分源码直接矩阵与待执行的运行验收。任务分支已实际验证adapter同核心、零fit/无target、来源隔离、原前缀、稀疏编号、发布区间、身份漂移、不可变发布及登记失败恢复等直接测试；真实数据库只读预检为BLOCKED，不报生成/运行通过。下列实际job、DEV写入、共享触发与Consumer/API/UI仍为后续计划：

- 同一合法输入的file/DB adapter在相同数值环境下共用核心并产生相同特征/状态/系数；DB缺数不切文件、QE缺数不查询DB，两个日期/身份不能混用。
- DB有T但文件cutoff更早：日度模式只凭DB完整合同运行，文件模式仍只覆盖其release；未来target/下一交易日观察读取设poison并断言未访问。现有quote/moneyflow及正常停牌、历史证券alias/稀疏行业ID直接矩阵不降低。
- 有状态模型验证连续前缀/检查点身份及长假T→T+1；仅MAX(date)到T而逐键缺失、部分入库、PIT错误、数值非有限、日历不足明确失败/等待，不偷用昨日结果。无状态版本不被额外要求HMM state。
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

完整能力包仍包含HMM数据库adapter、既有数学核心复用、显式日期/source计划、共享触发/持久job、不可变发布与完成结果读回；不把未验收层次切成假完成阶段。BUG-1862仅修改HMM自身源码/CLI/直接测试及本设计状态，已在分支实施HMM链；公共调度接线及真实输入事实问题交对应owner，不修改共享事实或其他模块源码。不以HMM部分源码修复代报全部F-001～F-006验收完成。

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
| F-001 | 本文§1/§4/§8；backend/services/hmm_training_service.py；scripts/precompute_hmm_coefficients.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py：DB/file同一实际核心、fit/file-worker poison及来源边界测试通过 | APPROVED_BY_USER_DESIGN_ONLY_SOURCE_IMPLEMENTED_PENDING_RUNTIME | 真实输入阻断；不声称已部署或自动触发 |
| F-002 | 本文§3/§4；backend/services/hmm_data_source/daily_input.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py：完成记录、完整键/PIT/因果calendar反例通过；真实DB预检识别重复事实冲突 | APPROVED_BY_USER_DESIGN_ONLY_SOURCE_IMPLEMENTED_REAL_INPUT_BLOCKED | 801012.SI/2026-07-01股票级行业事实与PIT归属不一致 |
| F-003 | 本文§4/§6/§10；scripts/precompute_hmm_coefficients.py；scripts/hmm_horizon_v2_train.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py：原快照起点、模型/参数/hash漂移、target-free feature矩阵通过 | APPROVED_BY_USER_DESIGN_ONLY_SOURCE_IMPLEMENTED_PENDING_RUNTIME | 不另建数据集、重编号或自动重训；不支持的20D schema显式拒绝而非替代 |
| F-004 | 本文§5.1/§6；backend/services/hmm_training_service.py；backend/services/hmm_data_source/coefficient_extension.py | backend/tests/hmm_data_source/test_hmm_database_daily_input.py及test_hmm_daily_coefficients.py：immutable发布、job恢复、登记冲突/readback反例通过 | APPROVED_BY_USER_DESIGN_ONLY_SOURCE_IMPLEMENTED_PENDING_REAL_DB_WRITE_VALIDATION | 写入只在测试替身执行；真实DEV/生产登记均未执行 |
| F-005 | 本文§5.1；backend/routers/hmm_training.py | 计划扩展backend/tests/hmm_data_source/test_coefficient_extension.py的真实共享callback/训练poison及持久job，不能只调用service | APPROVED_BY_USER_DESIGN_ONLY_PENDING_IMPLEMENTATION | 公共接线由owner协调，不能用mock代报 |
| F-006 | 本文§7/§9/§11 | 计划扩展backend/tests/hmm_data_source/test_hmm_daily_coefficients.py，另完成真实来源/T/T+1/asset身份及目标consumer smoke | APPROVED_BY_USER_DESIGN_ONLY_PENDING_RUNTIME_VALIDATION | 未运行真实日度任务/消费者验证，不报收益或可用 |

上述状态区分源码测试、真实输入与运行验收；任何设计批准都不是缺失证据豁免或生产动作授权。F-005/F-006及实际DEV写入仍未完成，不用单元测试替代。

## 14. Production gates / DESIGN-COMPLIANCE-001

文档本身无运行影响；BUG-1862源码包实际属于`runtime_impact=backend / target_ids=[backend-main]`，合入后由用户加载源码再验收，不以文档分类降级。本次执行：production_ddl_gate=noop；production_dml_gate=noop；dependency gates=noop；database_write=false；dataset_write=false；training=false；process_control=false。

后续运行源码按实际changed files重新分类，不以文档runtime=none降级。数据库源只读；任务/登记写入与生产启用另按既有DEV先验和目标授权执行，不因本设计获批自动执行。所有后端启动、停止、重启归用户。

DESIGN-COMPLIANCE-001：完整输入→零fit推理→发布登记→真实读回，不以旧接口/设计代报完成；缺数/等待/损坏明确分离，无neutral/file/DB silent fallback；旧模型、参数、场景交易及其他模块不迁移；仅保持既有因果/PIT/数值/权限，不新增分钟调度、资源、全行业三态或每月审批门。

## 15. 修订记录

2026-04-28：每日接口与异步任务设计，旧Paper/Selection交互保留。2026-09-19：历史冻结文件quote availability合同。2026-10-13 v2.0：更新为DB收盘T→T+1明确主入口、共享核心/就绪触发；修正旧confirm_text和最大日期/冻结文件混用表述，保留历史契约。2026-10-13 v2.1：BUG-1862分支接入HMM数据库推理及来源隔离，记录源码验证与真实数据/登记/触发验收差异，恢复手工更晚交易日兼容；模型及特征公式不变。
