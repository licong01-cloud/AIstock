# 共享行情有界批量 Reader 交付与实测

日期：2026-10-28；范围：[F1设计](../architecture/position_timing_bounded_reader_f1_20261028.md)。LocalSIM #6785已提供消费接缝。本报告不修改其设计、不接管其账户研究。基线commit：`bf3f4c79cf963e02d06a260200556efed49a46a3`。

## 1. 正式公共合同

唯一实现仍是`backend/services/position_timing/intraday/research_inputs.py::CandidateReader`。

```python
reader = CandidateReader(manifest_file_ref)
reader.prepare_window(start, end, symbols)  # date/date/股票集合，返回资源报告
market = reader.market(symbol, trade_date) # 完整当日对象，语义不变
times, values = reader.values(symbol, trade_date, field, frequency)
reader.verify_pins()                       # 发布/使用当日结果前仍须完整SHA检查
reader.release_window()                    # 可选；可替换窗口继续使用
reader.close()                             # 幂等，无I/O；其后读取显式失败
```

prepare会替换旧窗口；不限制之后实际market/values读取的合法日期、股票或字段。未prepare的旧构造和调用继续有效。不创建第二Reader，不缓存MarketView、PIT资格、计划、信号、账户或成交；同一Reader只供单线程顺序消费，不支持同实例并发；不同消费者必须独立实例。

预算：20个交易日、1024只去重股票，raw浮点切片LRU保留payload≤512MiB，日历/日期偏移/跨度/停牌索引payload≤64MiB。达到缓存预算时可观察地淘汰或不保留物理缓存，必要读取仍执行原校验；不是研究模式静默降级。每次只读映射一个bin，取片后立即关闭；跨调用保留句柄为0。只读数组基于immutable bytes，不能重新打开写权限，close后外部持有的数组仍有效、无mmap句柄。预算不等于整个Python、Arrow临时解码、manifest、累计来源pins及消费者对象的进程RSS上限。

`window_status`返回窗口、closed、array/index bytes及上限、cache hits/misses、evictions、prefetch_unavailable、open/peak mappings、retained_file_handles；返回报告不暴露可变缓存。

## 2. 内容身份与错误合同

缓存在实例内部绑定唯一candidate完整SHA及manifest组件SHA，不切active/profile。预取对文件进行完整内容核验，读取后再次核验；实际使用缓存时按消费校验周期复核来源；`verify_pins`仍对累计全部已消费组件重算完整SHA。mtime/size不代替内容哈希。

仅预取且未消费的缺失文件、NaN、历史不足、备用SELL字段或未来股票不增加失败，也不成为consumed pins。实际消费仍检查原bin头、历史、日价、分钟完整性、停牌和跨度；原异常继续传播。未变更UNKNOWN/NONE政策。

新增生命周期错误：`RESEARCH_WINDOW_INVALID`（日期反转/非date等）、`RESEARCH_WINDOW_BUDGET_EXCEEDED`（空交易窗口或超20交易日/1024股票）、`RESEARCH_READER_CLOSED`。原`RESEARCH_COMPONENT_NOT_IN_MANIFEST`、`RESEARCH_BIN_*`、`RESEARCH_DAILY_PRICE_UNAVAILABLE`、`RESEARCH_SUSPENSION_UNRESOLVED`等保持；缓存检测到已消费源修订报`RESEARCH_SOURCE_CHANGED_DURING_READ`。源文件missing/hash/size校验保留既有底层错误；不吞成空行情或默认价格。

verify_pins与close是不同合同：close只释放资源，不代替最终来源验收；consumed pins保留供来源审计。窗口替换不会清除已消费pins。

## 3. 冻结输入、独立消费者与完整对象对照

WSL使用现成LocalSIM Python：`/mnt/x/AIstock_temp/localsim-position-timing/wsl-env/bin/python`，单进程，OMP/MKL/OpenBLAS线程数1；未安装依赖。Windows定向pytest使用既有环境；WSL实测环境未装pytest，未将此作为交付阻断或安装新依赖。

输入由只读冻结配置引用：

- 配置：`X:/AIstock_temp/localsim-replay-performance-20261028/benchmark-config.json`，SHA `7326ce7c7227f781af72a8e326e458b3a24e11264488f4b46127e25e591470fc`。
- candidate：配置中固定的`20260930-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1/qe_dataset_manifest.json`，完整SHA `321a966c55822db5777dda4ef0091422e8a1877b7bcb6c27ffb763e9a2441c73`。
- inventory完整SHA `4ac705225404568db4254d00825f5d098a46f98e0301aa7e448575f5b9cffd33`；来源continuous request完整SHA `a9ab2c0388702fbe746061b466083706d189dd2f7d4f29a6c4fba6c7a0a14668`。

仅取原生2026-01-05/06/07三个连续交易日，各50目标；相邻目标集合对称差40/30，不重选股票。准备域81股；实际逐日域为当日及此前目标的保守并集，不宣称这些股票是真实持仓，不读取账户或经济结果。每轮201个完整MarketView、47,637根bar。原版Reader由上述基线commit只读加载，非第二套生产Reader；优化逐日和批量均调用新正式实现。

现有LocalSIM入口`ResearchMarketWorkspace(reader, batch_days)`及`FrozenResearchMarketProvider.load_symbol_input(..., source=DATASET_FILES)`实际消费这些对象。三模式×首次/热读共6轮完整MarketView、完整原生分钟输入及consumed pins逐键哈希一致，不排除任何业务字段；不只比较价格或数量。原核心仍只读当时可见prefix。

本轮完成共享读取→LocalSIM消费接缝短窗口验收。完整`dual_mode_research.run_study`批量账户验收由LocalSIM窗口下一步执行；本窗口没有生成/恢复账户研究，也未修改活动181日研究、旧288、已有结果、断点或journal。

## 4. WSL真实性能

条件为新Python进程/新Reader后同Reader热读；宿主页缓存受活动研究共享影响、未drop_caches，不能宣称冷磁盘或严格统计显著。单次样本只说明本窗口，不预设提速倍数，不外推181日账户回放。

| 模式 | 来源检查秒 | 行情准备含SHA秒 | 行情准备扣除SHA秒 | 端到端秒 | 峰值RSS MiB | 峰值FD |
|---|---:|---:|---:|---:|---:|---:|
| 原版逐日 | 54.05 | 123.13 | 69.08 | 126.50 | 473.16 | 8 |
| 优化逐日 | 52.01 | 107.36 | 55.35 | 110.50 | 478.38 | 8 |
| 3日批量 | 81.74 | 96.00 | 14.26 | 98.92 | 471.09 | 8 |

端到端包括3日×首次/热读、完整对象比较序列化及LocalSIM输入转换，不是账户回放。来源检查为constructor/prepare/market/verify内部完整SHA计时子集，不与“含SHA”列相加。3日prepare本身39.49秒，批量首次/热读循环分别31.82/26.87秒；原版对应75.60/50.11秒。批量端到端实测约1.28倍、减少约21.8%；来源检查增加来自预取和缓存周期复核，未用stat换取提速。

批量缓存实际保留3,271,752字节数组、4,982,748字节索引；4020次切片hit、0miss；单次映射峰值1、退出0，close后两类缓存均0。进程FD由峰值8回到6，其中进程基础库句柄不属于Reader。4项既有保护产物完整SHA不变。

证据根：`X:/AIstock_temp/position-timing-bounded-reader-20261028/`，WSL对应`/mnt/x`，无大型数据复制。

| 文件 | 完整文件SHA256 |
|---|---|
| legacy-v1.json | 2f62ed5f3cd2420a3ebb8ae4c0ac59663ae50f37c670bd667d3e3bb481ac9460 |
| daily-v1.json | fafa36c604f10f8d1946f6f9cf7eaea50fcdd9f6817a8183946cce10bb7ea0fa |
| batch-v1.json | 54a49a587b30b37c435c4226c1c43451c4cbf352be29d5ad322d4a314b12114a |
| comparison-v1.json | 9d9712ad4653589cca7a4fb18677108bd3cebd583f35cbd5b1ebd4c7ca370619 |
| measure_reader.py | 50d312f0e0e0461a861c9f67a7836d769e33b6fbe63d188dd57f0c5ff4643d4d |
| compare_measurements.py | 33557381142797376c35c8e00089060e648f16332d449aaa8b7422048bfbdaf4 |

新Reader LF归一化SHA：`d2023f4e1f551ccb8be5c1557a5cd000952384ea65ab1ba7cfa08f0e8c8a5717`；后续文档修改不改变实测实现。

## 5. 验证、审核与下一窗口命令

首轮6项定向测试通过；第二轮补齐全日历缓存预算和不可重新开启写权限后6项通过；旧`test_intraday_research.py`6项通过。覆盖多日完整对象/pins/停牌、窗口替换/域外读、未消费输入、必要缺价/历史不足、同size/mtime内容修订、只读数组、当时可见prefix、close/消费者隔离及LRU预算。Ruff及L0两套guardrail均0 findings。最终F1/ownership/classifier/CI由关联PR及外置交付回执记录。

三轮复核：接口/资源与原算法；失败/来源/因果/未消费输入；真实完整对象/LocalSIM接缝/测量口径及保护产物。DESIGN-COMPLIANCE-001逐项：共享接口范围完整而不冒认完整账户接入；无吞错/填价；业务规则/比较器/账本不变；无新增审批或等待未来交易日的门槛。

首轮CI `40623518445`：461 passed、1 failed。唯一失败是既有`test_isolation.py`一律禁止所有业务模块导入择时，与已批准分钟蓝图§2的LocalSIM→纯SDK方向不一致。已先扩入本模块直接测试写入范围，再将例外限定为LocalSIM目录和明确公共SDK/冻结研究模块；QE/Advisory等仍禁直接导入，LocalSIM也不能导入日频内部实现、intraday store或从包根偷渡内部模块。失败点及新增边界保护2项通过；Reader实现及WSL实测SHA未改变。最终CI以修订后的HEAD为准，不重跑或改写既有实测数据。

已执行的接缝复现命令（label须使用新名称，旧证据不可覆盖）：

```bash
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
/mnt/x/AIstock_temp/localsim-position-timing/wsl-env/bin/python \
/mnt/x/AIstock_temp/position-timing-bounded-reader-20261028/measure_reader.py \
--mode batch --label owner-consumer-new
```

完整账户批量模式由LocalSIM窗口在包含新merge的**独立新研究**冻结request后执行，不得拿新代码恢复旧181日前缀：

```bash
python -m backend.services.simulation_execution.localsim.dual_mode_research run \
  --request <新研究request绝对路径> --request-sha256 <该文件完整SHA256> \
  --batch-days 3 --account-workers 2 --optimized --measure-performance
```

无需LocalSIM代码修改。共享接口完成、上述消费接缝通过、完整账户批量验收待owner、运行态激活NOT_REQUESTED；backend_restart_required=false。DB读写/行情网络/数据变更/服务控制/生产账户均false；功能及性能证据不构成经济效果支持。
