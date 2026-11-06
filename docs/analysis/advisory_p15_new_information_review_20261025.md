# Advisory P15：单一新信息输入评审（2026-10-25）

P15已完成有界查重和原人口核验，结论为 **INPUT_REVIEW_COMPLETE_NO_F1 / 探索 NOT_READY**。保留的唯一问题是“同报表经营现金流与净利润兑现差”；尚未在本轮核查范围内找到可消费的版本化面板，因此没有进行金融数值筛查、拟合或经济评价。这里的NOT_READY是输入交付状态，不是该机制无效的研究结论，也不是全库从未采集的断言。

P14经[PR #6676](https://github.com/licong01-cloud/AIstock/pull/6676)合入，原候选INNER_INCONCLUSIVE且预算结束；P11 EPS阴性及其它历史结果不改。本轮新增0fit，累计仍为237实际研究fit＋1旧index，0经济确认/生产激活。主线仍是原两包Top50→Top10、固定5TD；没有恢复价格、公告、流水线或启动脚本任务。

## 1. 本轮范围与实际执行

用户要求规划后直接执行，沿此前“先查重和输入评审、最多一候选、通过才制定下一F1”的建议推进。工作树基于main `06e688bcf96c1c5215f5f9aca821795110bc4422`；仅本报告和蓝图两个跟踪文件可改。计划、脚本及证据位于`X:/AIstock_temp/ADVISORY/p15-new-information-review-20261025`，不写原研究或数据owner工件。

已完成：三信息族元数据盘点；历史机制/50份父动态源码查重；唯一输入问题合同冻结；原Top50身份列投影；第二算术实现复核；本报告及蓝图当前队列更新。未启动金融数值筛查，因此数值支持门、缺失分布、潜在重排和特征合成测试均为NOT_EVALUATED/NOT_APPLICABLE，不作为已经通过的交付。

## 2. 来源盘点与立项判断

| 信息族 | 已有证据及重复风险 | 本轮决定 |
|---|---|---|
| 财务经营兑现 | 旧财务事件使用forecast/express/fina_indicator的方向和披露特征；同报表现金流/净利润金额对未见于已核源码。父包已有财务相关字段，不是全新财务领域 | 只保留一个条件输入问题；版本化源面板未定位，NOT_READY，不生成F1 |
| 融资交易 | 原binding已含margin_detail.h5；N3已有12项融资动态＋固定Ridge，selected=0、支持不足的历史结论保留 | 未提出与旧机制充分不同的新假设，不做数值筛查或重跑；旧Top5协议不同于现Top10/5TD，不混称同一实验 |
| 股东质押 | 发现`scripts/tushare_data_explorer.py`中的pledge_stat探索调用；没有找到本轮可用的版本化交付 | 接口存在不能证明采集、落库或覆盖；截止日期不能替代公开时钟，不选作第二候选 |

本轮实际检查范围为：原研究`preregistered/plan.json`的binding.files；`research_source_candidates`及`source_authorities`的一级目录；模型工件根中financial/cash/pledge命名目录；backend和scripts中现金流/质押字段与接口的定向源码检索。前两来源目录分别只见report_rc和sw_classification；模型目录只定位到既有financial_event工件。**未查询数据库、未扫描所有磁盘、未核对外部未登记交付**，因此数据库是否已有现金流原表仍为UNKNOWN。

Tushare官方[cashflow](https://tushare.pro/document/2?doc_id=44)列有公告日期、实际公告日期、报告期、报表类型及现金流/净利润字段，支持定义数据需求；它不证明本地交付或历史原版本存在。[pledge_stat](https://tushare.pro/document/2?doc_id=110)给出截止日期和质押数量/比例，不能直接推导首次公开时点。[margin_detail](https://tushare.pro/document/2?doc_id=59)有次日更新约定，本轮未用D日记录替代D时钟可用性。

## 3. 父包与历史候选查重

沿P6的50份注册动态代码清单逐文件核对，**50/50 SHA一致**，只做源码/AST阅读，未执行因子。两项相关TCN函数进一步逐段核查：

- `FundamentalEpsIndustryMomentum`：`bb_eps × sw2_close`的5日变化。
- `roe_stability_score`：`bb_npr × db_turnover_rate`的60日波动倒数。

其它访问包括估值、净资产、筹码、资金流、换手、价格及板块字段。已核动态源码未发现`n_cashflow_act/net_profit`同报表金额对；旧财务事件的披露控制和20/60/120/252日方向累计也不是该金额对。该差异只支持“值得检查输入”，**不证明与父分数统计独立、具有残差Alpha，亦不是对整个训练预处理闭包的独立性认证**。

旧融资、财务事件frontier依据蓝图及各自合同查重，没有加载其经济标签重算结果。没有因为P14不显著而改窗、挑包、反向或重新评价P11。

## 4. 唯一条件输入问题

合同`hypothesis_contract.json`在身份核验前冻结，id=`CASH_PROFIT_REALIZATION_SAME_REPORT_V1`。暂列待验证表达为：

`(经营现金流净额 − 净利润) / (abs(经营现金流净额) + abs(净利润))`

该表达只是数据需求，**没有计算、没有训练F1**。必须来自同股票、同原始合并报表、同YTD报告期、同币种单位及同修订身份；不拼接单季/累计、母公司/合并或不同版本。金额缺失/非有限、分母全零、口径/单位/版本冲突、公开时钟未知均保留UNKNOWN；非一般工商业公司仍保留原股票身份，不借行业排除改变研究人口。

[Sloan原研究摘要](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2598)为现金与应计组成差异提供机制依据。该依据不验证本表达，也不证明在A股既定Top50人口上能改善5TD排序；期限匹配及相对父包的新增信息仍是未解决问题。不能以经典文献代替当前实验，更不能直接承诺收益。

未来若定位到实际源，仍须先冻结失效期限、日期约定、每包每折支持及组内可区分性判据，再另行做输入验证；不得先看数值挑规则。源日期与本地抓取时间分别保留。即使采用“公告日期后首个交易日”的NON_VINTAGE探索约定，也只是明确假设，不能把最新修订回填、快照hash或日期字段宣称为历史PIT成立。

## 5. 原人口核验与未完成项分账

只解码原`inputs.parquet`的package、manifest、run、D、T、instrument、rank、group_size八列，不读价格、特征或收益列。文件SHA为`c8d6ed3a1bc59aa365a15b0abcefb5b99cc19f84726337744ca048fa34f7a08b`，六列identity SHA为`a53daa66a623b9cbcb684e1750ff618efd04035f801932bdd5b6e7df181a7eae`，与P12一致。物理文件hash会读取文件字节，不等同解码收益列。

| 核验项 | 结果 |
|---|---|
| 全部原身份 | 52,400行/524D，两包1,048个包日，每组50只、rank1～50完整 |
| 开发区间 | 2024-07-04～2025-09-30，305D/30,500行 |
| 每包开发人口 | 15,250行；原Top10为3,050行，11～50名为12,200行 |
| 开发区间外 | 21,900行仅做身份核验，未读取其金融值/收益 |
| 第二实现 | PyArrow投影＋Python分组，身份hash/计数一致，50份源码hash再次通过 |
| 现金流源文件消费 | NOT_EVALUATED：本轮范围未定位面板 |
| 数值覆盖/每折支持/重排区分性 | null / NOT_EVALUATED，不能解释成0%覆盖或无信号 |
| 历史PIT / 经济价值 | UNPROVEN / NOT_TESTED |

主核验0.629秒，结束时RSS132,804,608bytes；不是采样峰值。第二实现第一次因单值日期字符串含午夜、而旧合同采用日期字符串导致hash断言失败；在严格验证午夜/无时区/无子秒后显式转ISO日期，复核通过，没有修改原数据、合同或第一次收据。两种实现来自同窗口，不能冒称独立外审。

## 6. 结束判定与后续唯一入口

本轮评审任务完成，**不进入训练F1，也不自动换另一信息源**。维持既有荐股基线；研报EPS与频率各自的结束结论保留，不由本轮推翻或恢复。若后续决定继续现金兑现问题，第一步是数据owner定位现存交付或确认未采集，而不是重跑Advisory：

1. 返回实际文件/表的非敏感位置及是否已有历史采集；若有现存源，提供版本化只读文件、manifest/schema/quality和文件hash。
2. 保留报表期、原币种单位、公司/报表类型、公告日期、实际公告日期、本地首次观测、源版本及全部修订冲突；不能用后验最新值覆盖历史。
3. 核心决策范围仍为305D开发窗；所需更早报表回看范围由下一份输入合同事先确定，不臆定“多年越多越好”，不改变原Top50人口。

上述是**恢复条件，尚未派发采集或生产任务**。若只有NON_VINTAGE可用，明确登记后再评审探索可行性；文件输入准备完成也不自动表示PIT或经济通过。本轮不需要生产持久化、数据集切换或LLM研报分析；现金流报表与券商研报是不同来源，未来研究候选文件足以完成输入验证，正式生产接入另议。

## 7. 可追溯交付与审查

证据根：`X:/AIstock_temp/ADVISORY/p15-new-information-review-20261025`。

| 工件 | 用途 / SHA256 |
|---|---|
| `scope.json`、`task_plan.md`、`findings.md`、`progress.md` | 精确写范围、先规划后执行、发现与进度 |
| `source_inventory.json`、`source_catalog_review.json` | 限定范围的来源位置/代码身份及三族决定 |
| `dedup_receipt.json` | 50份原动态代码路径/hash/字段访问与相关行 |
| `hypothesis_contract.json` | `e79b0afb33e21182313808b199476ba9189694e26c7913047fae1db5bb19711a` |
| `input_review_receipt.json` | `de52fbdc5fdc31cab7f9b79854b5c379fde46ada3c89d165f3fecfaf557dfd9d` |
| `input_review.py` / `input_audit.py` | 原实现及第二实现；不可覆盖的输入/审计收据 |
| `review_receipt.md`、`validation_receipt.json`、`blueprint_validation.json` | 多轮同窗口审查、文档范围及蓝图验收 |
| `delivery_receipt.json` | 提交/PR/必需CI/合入/main同步状态，独立于研究结论 |

Git保存本摘要与蓝图，X盘为本地可追溯证据，不冒称已发布生产数据集。DESIGN-COMPLIANCE-001按原人口、零标签/fit、单问题、未通过不立F1、旧结果不改、模块及生产边界逐条核对；不存在需要交付的模型功能被简化为POC。该报告不申报F1功能完成，蓝图新增F-298记录本轮完整评审及有界停止。
