# 事件信号 ST 优先验证与 LLM 公告预处理详细设计

日期：2026-05-06
状态：详细设计，当前阶段不改 QE / 回测 / Selection Center / Paper v2 / 模拟盘 / 实盘消费者
工作树：`F:\Dev\AIstock_worktrees\event-signal-st-llm-design-20260506`
前置设计：`docs/architecture/unified_event_signal_architecture_20260506.md`

## 1. 目标与阶段边界

本设计承接统一非日频事件信号框架，目标是把下一阶段拆成可验证、可回滚、不会干扰现有交易链路的独立开发步骤。

核心目标：

1. 先做 **ST / 退市 / 风险警示** 标题信号的高置信规则、回填和事件研究验证。
2. 所有信号仍写入统一 `market.event_fact` / `market.event_relation` / `market.event_signal`，不直接接入当前 alpha、QE、Selection Center、Paper v2、模拟盘或 QMT。
3. ST 信号验证通过后，再扩展业绩预告、业绩快报、财务指标和公告标题之间的结构化交叉验证。
4. LLM 分析放到后续阶段；但本阶段先完成 PDF / 公告正文预处理研究和架构设计，确保未来 LLM 只读取有价值的证据片段，而不是整篇 PDF。

当前阶段禁止事项：

| 禁止项 | 原因 |
|---|---|
| 不修改现有回测、QE、Selection Center、Paper v2、模拟盘、QMT 执行逻辑 | 先验证独立信号有效性，避免未验证风险信号改变交易结果 |
| 不把公告信号直接混入日频 alpha 因子 | 公告/财务发布是事件流，不是自然日频因子 |
| 不下载全部 PDF 给 LLM | 成本高、噪声大、可解释性差，且很多标题/结构化数据已足够 |
| 不在 raw 表写衍生字段 | raw 表只保存源数据和观测元数据，信号/分类/LLM 输出全部进入派生表 |
| 不照搬论文 54 类事件体系 | 论文 taxonomy 是参考，不等于 AIstock 可交易风险分类 |

## 2. 当前本地状态

### 2.1 已有表与数据

本地数据库已具备后续设计的主要基础表：

| 表 | 当前用途 | 设计结论 |
|---|---|---|
| `market.anns` | 公告结构化元数据，含 `title/url/rec_time/first_seen_at` 等 | ST 标题规则的主输入 |
| `market.ann_event_classification` | 公告标题分类结果 | ST-first 需要从这里回填更精细 ST 子类 |
| `market.ann_risk_signal` | 旧公告风险信号 | 只作为历史兼容参考，后续统一读 `event_signal` |
| `market.tushare_forecast_raw` | 业绩预告 raw | 后续财务结构化信号输入 |
| `market.tushare_express_raw` | 业绩快报 raw | 后续财务结构化信号输入 |
| `market.tushare_fina_indicator_raw` | 财务指标 raw | 后续财务结构化信号输入 |
| `market.event_fact` | 标准化事件事实 | 已可承载公告/财务事实 |
| `market.event_relation` | 事件之间关系 | 已可承载预告与实际不及预期等关系 |
| `market.event_signal` | 交易可消费事件信号 | 当前阶段只生成/验证，不接入消费者 |
| `market.event_signal_run` | 信号生成 run 审计 | 后续每次生成/回填必须写入 |
| `market.stock_st` / `market.stock_st_events` | Tushare ST 状态/事件 | 可作为 ST 标题信号验证的独立对照源 |
| `market.kline_daily_raw` | 未复权日线行情，价格单位为厘 | ST 事件研究的价格输入 |
| `market.index_daily` | 指数日线 | 用于市场调整收益，优先 `000300.SH` / 宽基指数 |
| `market.trading_calendar` | 交易日历 | 统一 `effective_trade_date` 与事件窗口 |
| `market.stk_limit` / `market.suspend_d` | 涨跌停与停牌 | ST 事件研究必须标记涨跌停/停牌状态 |

只读抽样结果：

| 项目 | 结果 |
|---|---:|
| `ann_event_classification` P0_BLOCK | 25,450 |
| `ann_event_classification` P1_HIGH | 89,648 |
| `ann_event_classification` P2_REVIEW | 1,299,530 |
| `ann_event_classification` P3_POSITIVE_CANDIDATE | 405,333 |
| `ann_event_classification` P4_NEUTRAL | 3,311,382 |
| `event_signal` 中 announcement 来源 | 30 |
| `event_signal` 中 Tushare 财务来源 | `forecast` 44,127；`express` 14,070；`fina_indicator` 80,754；`financial_relation` 6,620 |
| `kline_daily_raw` 覆盖 | 1990-12-19 至 2026-05-06，约 1,647 万行 |
| `trading_calendar` 覆盖 | 1990-12-19 至 2026-12-31 |
| `stock_st_events` 覆盖 | 1,825 条，`pub_date` 2018-08-08 至 2026-04-30，`imp_date` 至 2026-05-06 |
| 当前 P0 中债券/转债摘牌类标题 | 约 3,907 条，需要从股票硬风险中剥离 |

结论：表结构和历史 raw 数据基本具备；下一步不应重复讨论是否建基础表，而应把 `ann_event_classification` 中的 ST/退市/风险警示子集精炼为高置信 `event_signal`，再做事件研究。

### 2.2 当前 ST 标题规则的主要问题

现有 `backend/services/announcements/title_classifier.py` 已包含：

| 规则 | 当前行为 |
|---|---|
| `risk_warning_removed` | 撤销/取消风险警示，标记为 `P3_POSITIVE_CANDIDATE + record_only` |
| `delisting_or_risk_warning` | 终止上市、退市整理、摘牌、ST、风险警示，标记为 `P0_BLOCK + block_buy` |
| `bankruptcy_restructuring` | 破产、重整、清算、债务无法清偿，标记为 `P0_BLOCK + block_buy` |

需要修正的风险：

1. `delisting_or_risk_warning` 把“摘牌”纳入 P0，但公告样本显示大量可转债/公司债“赎回暨摘牌、兑付暨摘牌”被命中；这类不是普通股票退市风险，不能直接禁止股票买入。
2. “申请撤销风险警示”“继续被实施其他风险警示”“部分撤销”需要区分，不应一律正向。
3. “可能被终止上市”“已触及终止上市”“将被实施退市风险警示”“被实施其他风险警示”“继续实施其他风险警示”的严重度不同，但都属于无需 LLM 即可先产生风险信号的标题类型。
4. ST 短期价格不一定总是公告日下跌；ST 是状态型硬风险，事件研究只验证冲击与滞后，不应作为是否允许买入的唯一标准。

## 3. 可借鉴论文与实践

### 3.1 公告事件分类与中文金融事件抽取

| 参考 | 可借鉴点 | 不直接照搬点 |
|---|---|---|
| `Fine-Grained Classification of Announcement News Events in the Chinese Stock Market`（Electronics 2022） | 中国公告新闻可由触发词、共现词和模板构建细粒度事件；论文构建 54 类公告事件，可作为 taxonomy seed | 论文目标是事件分类，不是 AIstock 的交易风险分级；54 类中如“垃圾焚烧、投产、药品临床”等不一定是风险/alpha 信号 |
| CFEED 中文金融事件抽取数据集/代码 | 事件抽取需要事件类型、触发词、论元角色和模板，而非只做情感分类 | CFEED 不是直接可用的 A 股公告交易信号库，需要映射到本地规则与事件表 |
| DCFEE / Doc2EDAG / DocFEE 等中文文档级金融事件抽取研究 | 多事件、多实体、跨句论元抽取是公告全文阶段的核心问题；适合后续 LLM 输出 JSON schema 设计 | 第一阶段不应引入复杂神经抽取模型作为交易前置依赖 |
| 事件研究基础文献 Ball & Brown、Brown & Warner | 需要用公告日前后窗口、异常收益和显著性检验验证事件影响 | A 股必须额外处理涨跌停、停牌、ST 交易制度、T+1、流动性和幸存者偏差 |
| PEAD / SUE / revenue surprise 相关财报文献 | 业绩预告、快报、正式财报之间的 surprise 和不及预期有研究价值 | 不能把美股阈值/持有期直接用于 A 股；必须本地验证后才启用 alpha 增益 |
| Loughran-McDonald 金融文本分析 | 金融文本不能用通用情感词；后续可作为 LLM/规则校验词表 | 中文公告不能直接使用英文词典；需要本地中文风险词表 |
| 年报可读性、boilerplate disclosure 文献 | 年报/公告中的模板化、重复化披露可能降低信息含量；预处理需要识别模板和重复段落 | 第一阶段不直接用全文可读性作为交易信号 |

### 3.2 PDF / 文档预处理开源工具

| 工具/项目 | 适用点 | 风险/限制 | AIstock 建议 |
|---|---|---|---|
| PyMuPDF4LLM | 轻量 PDF 转 Markdown，适合先做本地基线和 LLM-ready markdown | 对扫描件、复杂表格和版面理解能力有限 | 第一优先本地 baseline，用于文本型公告 PDF |
| Unstructured `partition_pdf` | 可把 PDF 分割为 Title/NarrativeText/Table 等元素，支持多策略和 OCR | 依赖较重，部署复杂度高 | 作为中等复杂 PDF 的备选解析器 |
| Docling | 面向 PDF/Office/HTML 的结构化文档转换，强调 layout/table/OCR/JSON/Markdown 输出 | 项目更新快，需要验证 Windows/中文 PDF 稳定性 | 候选主解析器之一，适合统一文档对象模型 |
| MinerU | PDF 解析、OCR、版面、表格、公式到 Markdown/JSON，中文生态较强 | 依赖较重，GPU/OCR 环境需评估 | 扫描件和复杂中文公告的候选增强器 |
| PaddleOCR PP-Structure | 中文 OCR、版面分析、表格识别能力强 | 需要模型和 OCR 环境，吞吐成本较高 | 仅在文本层抽取失败或扫描件时 fallback |
| Marker | PDF 转 Markdown，目标是较好保留布局和表格 | 需要检查许可证和部署依赖 | 仅作为离线研究对照，不作为默认生产依赖 |
| pdfplumber / Camelot / Tabula | 表格定位、金额表抽取常见工具 | 表格结构不稳定，跨公告模板泛化弱 | 用于金额表专项抽取的局部工具 |
| LayoutParser / DocLayNet / FinTabNet / TableBank | 文档版面/表格识别的研究基准和训练资源 | 更多是研究/模型训练资源，不是直接业务流水线 | 用于评估复杂版面和表格抽取方案 |
| datasketch MinHash LSH / simhash | 近重复段落和模板检测 | 需要建立公告类型内的模板库 | 用于剔除页眉页脚、免责声明、重复模板段 |
| trafilatura / jusText / boilerpy3 | Web boilerplate removal 的成熟思路 | 主要面向 HTML，不直接解决 PDF 布局 | 借鉴“正文密度、链接/模板剔除”思想，不作为 PDF 主工具 |

设计结论：未来 LLM 不应读取完整 PDF。必须先经过“文档解析 -> 版面块归一化 -> boilerplate/重复段删除 -> 事件类型路由 -> 证据片段抽取 -> 结构化 JSON 约束输出”的流水线。

参考链接：

- Unstructured partitioning docs: <https://docs.unstructured.io/open-source/core-functionality/partitioning>
- PyMuPDF4LLM docs: <https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/>
- Docling docs: <https://docling-project.github.io/docling/>
- MinerU GitHub: <https://github.com/opendatalab/MinerU>
- PaddleOCR PP-Structure docs/repo: <https://github.com/PaddlePaddle/PaddleOCR>
- LayoutParser paper/project: <https://layout-parser.github.io/>
- DocLayNet dataset: <https://github.com/DS4SD/DocLayNet>
- CFEED repo: <https://github.com/brickee/CFEED>
- datasketch MinHash LSH docs: <https://ekzhu.com/datasketch/lsh.html>
- Loughran-McDonald word lists: <https://sraf.nd.edu/loughranmcdonald-master-dictionary/>

## 4. 总体执行顺序

推荐顺序与当前设计方案一致：

```text
Phase A: ST 标题规则精炼 + 独立信号生成
Phase B: ST 信号事件研究验证（T-1 到 T+2，并扩展状态风险）
Phase C: 其他高置信标题风险规则扩展
Phase D: 业绩预告/快报/财务指标结构化交叉验证
Phase E: 财报/公告正文预处理与 LLM 队列设计验证
Phase F: 其他复杂 PDF/公告全文 LLM 分析
Phase G: 经验证后，再单独设计接入回测/模拟盘/实盘
```

这比“先做 LLM 财报分析”更稳健，原因是：

1. ST/退市/风险警示可以用标题和 ST 状态表验证，不需要先下载 PDF。
2. ST 属于强风险规避信号，即使短期收益不显著，也可作为禁止买入/禁止加仓候选。
3. 财务类已有 Tushare 结构化数据，先做结构化 surprise 比先读 PDF 更可控。
4. LLM 的价值主要在标题和结构化数据无法判断的公告，例如问询回复、诉讼进展、违规担保细节、非标审计原因、债务违约条款。

## 5. Phase A：ST-first 信号设计

### 5.1 事件 taxonomy

ST-first 不再只用一个 `delisting_or_risk_warning`，应拆成更细类型：

| event_type | 风险级别 | 动作 | 是否下载 PDF | 说明 |
|---|---|---|---|---|
| `stock_delisting_confirmed` | P0_BLOCK | `block_buy`，未来可选 `force_exit` | 否 | 股票终止上市、进入退市整理、将被摘牌 |
| `stock_delisting_risk_warning` | P0_BLOCK | `block_buy` | 否 | 可能被终止上市、触及终止上市情形 |
| `stock_st_imposed` | P0_BLOCK | `block_buy` | 否 | 被实施退市风险警示或其他风险警示 |
| `stock_st_added_or_continued` | P1_HIGH | `warn_high` 或 `block_buy`（按配置） | 否 | 叠加/继续实施其他风险警示 |
| `stock_st_removal_applied` | P2_REVIEW | `warn_review` / `record_only` | 否 | 申请撤销，尚未生效，不做正向 |
| `stock_st_removed_confirmed` | P3_POSITIVE_CANDIDATE | `record_only` | 否 | 撤销退市/其他风险警示，后续需验证是否正向 |
| `convertible_bond_delisting_or_redemption` | P4_NEUTRAL 或 `record_only` | `record_only` | 否 | 可转债/公司债摘牌、兑付、赎回，不作为股票硬风险 |
| `generic_bond_delisting_or_repayment` | P4_NEUTRAL | `discard_or_archive` | 否 | 债券自身到期兑付/摘牌 |
| `bankruptcy_restructuring_title` | P0_BLOCK / P1_HIGH | `block_buy` / `warn_high` | 可选 | 破产、重整、清算；先标题信号，后续可能正文细分 |

### 5.2 标题规则分层

标题规则先按排除，再按高置信命中：

1. **证券类型排除层**：标题或 `ts_code` 显示可转债、公司债、专项债、ABS、债券简称、转债代码等，先进入债券类事件，不进入股票 ST 硬风险。
2. **撤销/取消层**：撤销、取消、摘帽、申请撤销、继续实施、部分撤销要先识别，避免被“退市风险警示”关键字误判为 P0。
3. **终止上市层**：终止上市、强制退市、退市整理期、触及终止上市、将被终止上市、股票摘牌，进入 P0。
4. **ST 实施层**：股票被实施/将被实施退市风险警示、其他风险警示、证券简称变更为 ST/*ST，进入 P0 或 P1。
5. **进展/提示层**：风险提示公告、进展公告重复披露需要去重，同一 `ts_code + event_type + effective_trade_date` 可保留最高严重度或最新公告证据。

### 5.3 与 `stock_st_events` 的交叉校验

ST 标题信号不是孤立判断，应与 Tushare ST 事件源建立校验：

| 校验项 | 方法 | 结果用途 |
|---|---|---|
| 标题信号是否能命中 ST 事件 | `event_signal.ts_code` 与 `stock_st_events.pub_date/imp_date` 附近匹配 | 召回率评估 |
| ST 事件是否有标题公告 | `stock_st_events` 反查 `market.anns.title` | 找出标题规则漏召 |
| 标题为债券摘牌但非股票 ST | 按债券关键词和证券代码过滤 | 降低误杀 |
| 撤销/申请撤销是否混淆 | 与 `st_type/st_reason/st_explain` 和后续简称变化比对 | 防止错误解禁 |
| 生效日是否一致 | 标题 `ann_date/effective_trade_date` 对照 `imp_date` | 防止回测日期偏移 |

### 5.4 写入统一信号

ST-first 仍复用 `market.event_fact` 和 `market.event_signal`，新增规则版本建议：

```text
aistock_announcement_title_rules_v1_20260506
unified_event_signal_rules_st_first_v1_20260506
```

`event_signal.evidence` 至少包含：

```json
{
  "source": "market.ann_event_classification + market.anns",
  "title": "公告标题",
  "matched_terms": ["退市风险警示", "停牌"],
  "excluded_terms": [],
  "security_kind_inferred": "stock",
  "st_cross_check": {
    "stock_st_events_matched": true,
    "pub_date": "2026-04-30",
    "imp_date": "2026-05-06"
  },
  "rule_version": "aistock_announcement_title_rules_v1_20260506"
}
```

## 6. Phase B：ST 事件研究验证

### 6.1 验证目标

ST 验证不是为了证明所有 ST 公告都会在 T0/T+1 下跌，而是回答四个问题：

1. 标题规则是否准确识别股票 ST/退市风险，误杀率是否可接受。
2. `effective_trade_date` 是否避免了回测看未来。
3. ST 信号在公告日前后是否伴随异常收益、跌停、停牌、流动性恶化或可交易性下降。
4. 作为独立风险 overlay，是否适合后续进入“禁止买入/禁止加仓/仅预警/强制卖出”的开关配置。

### 6.2 事件窗口

默认窗口：

| 窗口 | 含义 |
|---|---|
| T-1 | 观察是否提前反应或信息泄露 |
| T0 | `effective_trade_date` 当日 |
| T+1 | 次交易日反应 |
| T+2 | 二次反应或跌停延续 |
| T0~T+2 | 短期累计冲击 |
| T0~T+5 / T0~T+20 | 状态型风险扩展观察，不作为第一阶段主指标 |

### 6.3 数据输入与计算口径

| 数据 | 表 | 用法 |
|---|---|---|
| 事件 | `market.event_signal` | `event_type` 为 ST-first 子类，`time_mode='backtest'` |
| 日线 | `market.kline_daily_raw` | 收盘到收盘收益，`close_li / 1000`；必要时用开盘/成交额 |
| 指数 | `market.index_daily` | 市场调整收益，优先沪深300或全市场等权基准 |
| 交易日 | `market.trading_calendar` | T-1/T0/T+1/T+2 映射 |
| 涨跌停 | `market.stk_limit` | 是否一字跌停、触及跌停、涨跌停可交易性 |
| 停牌 | `market.suspend_d` | 标记不可交易，避免把停牌当 0 收益 |
| ST 独立源 | `market.stock_st_events` | 验证规则命中率与生效日 |

收益指标：

```text
raw_ret_t = close_t / close_t-1 - 1
market_adj_ret_t = raw_ret_t - benchmark_ret_t
cum_ret_T0_T2 = product(1 + raw_ret_t) - 1
cum_adj_ret_T0_T2 = product(1 + raw_ret_t) / product(1 + benchmark_ret_t) - 1
```

需要单独标记：

- T0 停牌：不能以 0 收益判断无风险，应计为“不可交易风险”。
- T0/T+1/T+2 跌停：即使没有卖出成交，也说明流动性风险增加。
- T-1 已大跌：说明公告前可能已被交易，不代表规则无效。
- 多公告重复：同一股票短期多条 ST 提示只保留第一触发或最高严重度做主样本，其他作为重复提示。

### 6.4 验证输出

第一阶段可以先输出离线报告，暂不建新表；如果后续需要持久化，新增表必须带字段 comment。

建议后续表：`market.event_study_result`

关键字段设计：

| 字段 | 说明 |
|---|---|
| `study_id` | 事件研究批次 ID |
| `signal_id` | 对应 `event_signal.signal_id` |
| `ts_code` | 股票代码 |
| `event_type` | ST 子类 |
| `effective_trade_date` | 事件 T0 |
| `window_name` | `T_MINUS_1` / `T0` / `T1` / `T2` / `T0_T2` |
| `raw_return` | 原始收益 |
| `benchmark_return` | 基准收益 |
| `abnormal_return` | 市场调整收益 |
| `is_suspended` | 是否停牌 |
| `hit_down_limit` | 是否跌停/触及跌停 |
| `volume_ratio` | 成交量变化 |
| `liquidity_flag` | 流动性异常标记 |
| `metrics` | JSONB 扩展指标 |
| `rule_version` | 研究规则版本 |
| `created_at` | 创建时间 |

实现时必须为表和每个字段添加 `COMMENT ON TABLE/COLUMN`。

### 6.5 通过/失败标准

| 维度 | 最低通过标准 |
|---|---|
| 误杀控制 | 债券摘牌/兑付/赎回不得进入股票 P0 block |
| 泄漏控制 | `effective_trade_date` 使用当前公告时间规则，不用 `ann_date` 直接当 T0 |
| 样本覆盖 | ST 标题事件与 `stock_st_events` 匹配率可解释；漏召样本形成规则待办 |
| 事件影响 | 输出 T-1/T0/T+1/T+2 分布、均值、中位数、分位数、跌停率、停牌率 |
| 消费边界 | 报告产生后仍不接入交易消费者 |

## 7. Phase C/D：财务结构化交叉验证

### 7.1 结构化信号优先于 PDF

业绩预告、快报、正式财务指标已经入库，不需要先解析 PDF。第一阶段财务信号只从结构化数据产生：

| 来源 | 事件 | 初始动作 |
|---|---|---|
| `tushare_forecast_raw` | 预亏、续亏、首亏、大幅下降 | `warn_review` |
| `tushare_forecast_raw` | 大幅增长、扭亏 | `record_only`，不启用正向 alpha |
| `tushare_express_raw` | 快报亏损、大幅下降 | `warn_review` |
| `tushare_fina_indicator_raw` | 正式财务指标亏损、大幅下降 | `warn_review` |
| `forecast -> express/fina` | 预告高增长但实际明显低于预告中值 | `warn_review` 或 `warn_high` |
| `forecast -> express/fina` | 预告亏损但正式转盈 | `record_only`，待研究 |

当前 `financial_event_adapter.py` 已实现 v0 逻辑：

- `financial_forecast_loss`
- `financial_forecast_large_decline`
- `financial_forecast_turnaround`
- `financial_forecast_large_growth`
- `financial_express_loss`
- `financial_express_large_decline/growth`
- `financial_indicator_large_decline/growth`
- `financial_positive_but_miss_expectation`

下一步不是重建框架，而是补充验证与规则改良：

1. 统一中文字段编码/关键字匹配，避免终端或源码编码导致中文关键词不可读。
2. 增加报告期内多版本修订处理，优先最新 `ann_date`，但保留 PIT 历史。
3. 对 `forecast_mid - actual_yoy >= 30pct` 的“不及预期”阈值做事件研究验证。
4. 对 `net_profit < 0`、`yoy <= -50%`、`yoy >= 50%` 阈值进行分行业/市值分层验证。
5. 正向增长类继续 `record_only`，未经事件研究不得影响 alpha。

### 7.2 与公告标题联动

财务结构化数据与公告标题可以互相增强，但不应混入 raw 表：

| 联动 | 示例 | 输出 |
|---|---|---|
| 业绩预告标题 + forecast 数据 | 标题“业绩预告修正”，forecast 显示从预增改预减 | 提升为 P1/P2 |
| 快报 + 正式财务指标 | 快报盈利增长 100%，正式指标增长 50% | `financial_positive_but_miss_expectation` |
| 定期报告标题 + fina_indicator | 普通年报标题中性，但正式指标亏损/现金流恶化 | 结构化财务风险信号，不走 LLM |
| 问询函 + 财务异常 | 年报问询函且财务指标大幅恶化 | 进入未来 LLM 队列，提取问询重点 |

## 8. Phase E/F：LLM 公告正文预处理架构

### 8.1 何时需要下载 PDF / 调用 LLM

标题或结构化数据足够的类别，不下载 PDF：

| 类型 | 是否下载 | 理由 |
|---|---|---|
| ST/退市/风险警示实施、终止上市、可能终止上市 | 否 | 标题足以生成风险规避信号，并可用 `stock_st_events` 验证 |
| 债券兑付/转债赎回/摘牌 | 否 | 多数为非股票硬风险，标题可归档 |
| 普通定期报告标题 | 否 | 风险主要由 `fina_indicator` 等结构化数据判断 |
| 业绩预告/快报标题 | 否 | 先用 Tushare 结构化字段，必要时再抽正文原因 |
| 普通会议决议、制度、投资者关系记录 | 否 | 默认中性归档 |

可能需要 PDF / LLM 的类别：

| 类型 | 是否第一批 LLM | 原因 |
|---|---|---|
| 非标审计、无法表示意见、内控重大缺陷 | 是 | 需要抽取审计意见类型、事项段、影响范围 |
| 监管调查、行政处罚、纪律处分、刑事立案 | 是 | 需要主体、事项、处罚金额、是否涉及上市公司/董监高 |
| 资金占用、违规担保 | 是 | 需要金额、责任方、整改状态 |
| 债务违约、流动性风险 | 是 | 需要债务类型、本金利息、到期日、展期状态 |
| 问询函/关注函回复 | 是，但先抽样 | 标题无法判断回复是否充分，正文噪声很高 |
| 诉讼仲裁、冻结、拍卖 | 是，但需金额阈值过滤 | 大量小额/例行诉讼需要二次筛选 |
| 股东质押/减持/被动减持 | 可选 | 标题可初筛，是否重大取决于比例、价格、强平风险 |
| 控制权变更、重大资产重组、出售/收购资产 | 可选 | 正负方向依赖交易价格、标的质量、是否失败 |
| 普通年报/季报全文 | 暂不全量 LLM | 先用结构化财务指标；只对财务异常 + 问询/非标/风险关键词样本分析 |

按当前分类计数，P2_REVIEW 超过 129 万，不能全部下载/LLM。第一批应限定为 P1_HIGH + selected P2 高价值子类，再通过金额阈值、重复公告去重、标题二级关键词和结构化异常联动压缩队列。

### 8.2 PDF 预处理流水线

```text
Input: event_signal / ann_event_classification / anns.url
  -> document_fetch_queue
  -> pdf_download_or_cache
  -> parser_router
  -> layout_blocks
  -> boilerplate_filter
  -> event_section_router
  -> evidence_chunk_store
  -> structured_extract_rules
  -> optional_llm_json_extract
  -> signal_engine_review
```

关键原则：

1. LLM 只读取 `evidence_chunk`，不读取整篇 PDF。
2. 每个 chunk 必须保留页码、坐标/段落编号、来源 hash、前后文摘要和事件类型路由。
3. 表格、金额、日期、主体名称优先用规则/表格抽取；LLM 用于解释和归纳，不负责裸算金额。
4. 所有 LLM 输出必须是 JSON schema，包含 `conclusion/confidence/evidence_chunk_ids/risk_items/missing_fields`。
5. LLM 结论不能直接写交易动作；只能写 `event_llm_extract`，最终仍由 deterministic signal engine 合成 `event_signal`。

### 8.3 无用文字剔除策略

公告 PDF 常见无用内容：

| 噪声 | 处理方法 |
|---|---|
| 页眉页脚、页码、证券代码、公告编号 | 按页重复文本、位置、正则剔除 |
| 董事会保证/免责声明 | 模板库 + MinHash/simhash 近重复剔除；但保留“风险提示”章节 |
| 目录、释义、备查文件、签章页 | section heading 识别后默认降权或剔除 |
| 重复风险提示段 | 同一公告内段落 hash 去重；跨公告模板 hash 库去重 |
| 表格 OCR 乱码 | 表格单独解析，低置信 OCR 不送 LLM 直接结论 |
| 多公告重复提示 | `ts_code + normalized_event + source_payload_hash/title_hash` 去重 |
| 法律意见书/独立董事意见等附件 | 除非事件类型需要，否则只抽摘要/结论段 |

### 8.4 事件类型路由到证据字段

不同事件不应使用同一个全文 prompt。每类只抽必要字段：

| 事件 | 必要证据字段 |
|---|---|
| 非标审计/内控缺陷 | 审计意见类型、形成基础、强调事项、持续经营风险、内控缺陷类型 |
| 监管处罚/立案 | 调查/处罚主体、违法事实、处罚金额、市场禁入、是否影响上市公司 |
| 资金占用/违规担保 | 占用方/担保对象、金额、占净资产比例、整改计划、是否已解除 |
| 债务违约 | 债务品种、本金、利息、到期日、违约状态、展期/偿付安排 |
| 诉讼仲裁 | 原告/被告、案由、涉案金额、审理阶段、判决/执行状态、影响金额 |
| 问询/关注函回复 | 问询主题、回复是否量化、是否仍有未解决事项、是否涉及财务真实性 |
| 重组/并购/资产出售 | 标的、交易价格、估值、支付方式、是否构成重大资产重组、失败/终止条件 |
| 股权质押/被动减持 | 质押比例、平仓线风险、被动减持数量、控制权影响 |

### 8.5 未来派生表设计

当前阶段不实现。后续实现时，每张表和每个字段必须有 comment。

| 表 | 作用 |
|---|---|
| `market.announcement_document_fetch_queue` | 管理需要下载/解析的公告文档任务 |
| `market.announcement_document_parse` | 保存 PDF 解析版本、解析器、hash、页数、质量评分 |
| `market.announcement_document_block` | 保存版面块，含页码、坐标、block_type、文本 hash |
| `market.announcement_document_chunk` | 保存送 LLM 的证据 chunk，不保存整篇 prompt |
| `market.event_llm_extract` | 保存 LLM 结构化抽取 JSON、模型版本、prompt 版本、证据引用 |
| `market.event_review_queue` | 保存需要人工/LLM复核的低置信或高风险样本 |

字段设计必须包含：

- `source_type/source_pk/ann_id/ts_code`
- `document_url/local_path/file_hash`
- `parser_name/parser_version/parse_quality`
- `chunk_id/chunk_type/page_no/section_title/text_hash`
- `model_name/prompt_version/output_schema_version`
- `evidence_chunk_ids/confidence/missing_fields`
- `created_at/updated_at/run_id/rule_version`

## 9. 风险动作开关设计

用户提出需要开关选择预警、禁止加仓或强制卖出。建议先在独立信号层设计，不接入交易：

| action | 第一阶段含义 | 是否实际交易执行 |
|---|---|---|
| `record_only` | 仅记录，不预警 | 否 |
| `warn_review` | 需要关注/复核 | 否 |
| `warn_high` | 高风险预警 | 否 |
| `block_buy` | 后续可作为禁止买入/禁止加仓输入 | 当前否 |
| `force_exit` | 后续可作为强制卖出候选 | 当前禁用，只允许研究配置 |
| `alpha_hint_disabled` | 正向 alpha 候选但禁用 | 当前否 |

未来接入消费者前，需要独立配置表或策略层开关：

```text
event_signal_policy_profile
  profile_id
  allowed_actions: warn_only / block_buy / block_add / force_exit
  event_type_overrides
  min_confidence
  max_age_days
  require_manual_review_for_force_exit
```

但该表不在当前阶段实现。当前只要求 `event_signal.action` 能表达潜在动作，并通过离线报告验证。

## 10. 详细开发步骤

### Step 1：ST 规则精炼

输入：

- `backend/services/announcements/title_classifier.py`
- `backend/tests/announcements/test_title_classifier.py`
- `market.anns`
- `market.ann_event_classification`
- `market.stock_st_events`

任务：

1. 新增或改进 ST 专用规则版本，拆分 ST/退市/撤销/债券摘牌子类。
2. 添加债券/可转债排除优先级。
3. 添加测试样本：股票退市风险、其他风险警示、申请撤销、确认撤销、可转债赎回暨摘牌、公司债兑付暨摘牌。
4. 规则输出仍进入 `ann_event_classification`，不改变原始公告表。

验证：

```text
pytest backend/tests/announcements/test_title_classifier.py
SQL sample: P0 中债券/转债标题命中数应显著下降或归到 bond 子类
```

### Step 2：ST 信号回填到 `event_signal`

输入：

- `market.ann_event_classification`
- `market.anns`
- `market.stock_st_events`
- `backend/services/event_signal/announcement_adapter.py`

任务：

1. 增加 ST-first adapter 或在公告 adapter 中增加 ST 精细事件映射。
2. 对 2018-08-01 至最新交易日回填 `time_mode='backtest'`。
3. 生成 `event_signal_run` 审计记录。
4. `evidence` 中写入标题、匹配规则、债券排除、ST 事件交叉校验结果。

验证：

```text
SELECT event_type, risk_level, action, count(*)
FROM market.event_signal
WHERE source_type='announcement' AND rule_version LIKE '%st_first%'
GROUP BY 1,2,3;
```

通过标准：股票 ST 类 P0/P1 有足够覆盖，债券摘牌不进入 `block_buy`。

### Step 3：ST 事件研究脚本/服务

输入：

- `market.event_signal`
- `market.kline_daily_raw`
- `market.index_daily`
- `market.trading_calendar`
- `market.stk_limit`
- `market.suspend_d`

任务：

1. 生成 T-1/T0/T+1/T+2 窗口映射。
2. 计算 raw return、benchmark return、abnormal return、cum return。
3. 标记停牌、跌停、成交额/成交量异常。
4. 输出报告到 `reports/event_signal/st_first/`。
5. 暂不接入前端和交易消费者。

验证：

```text
报告包含样本数、去重数、窗口收益分布、跌停率、停牌率、异常样本列表。
```

### Step 4：财务结构化信号验证

输入：

- `market.tushare_forecast_raw`
- `market.tushare_express_raw`
- `market.tushare_fina_indicator_raw`
- `market.event_signal`
- `market.event_relation`

任务：

1. 对已有财务信号按报告期、行业、市值分层做分布检查。
2. 重点验证 `financial_positive_but_miss_expectation`。
3. 对业绩大幅增长/下降/亏损分别做事件研究，但正向仍保持 `record_only`。
4. 输出阈值调整建议。

### Step 5：LLM 预处理 PoC，不接入交易

输入：

- selected P1/P2 高价值公告样本，不全量 PDF
- 本地 PDF 缓存或公告 URL

任务：

1. 比较 PyMuPDF4LLM、Docling、Unstructured、MinerU/PaddleOCR 在中文公告样本上的解析质量。
2. 建立 boilerplate 模板库和段落 hash 去重。
3. 对 5~8 个事件类型各抽 50~100 条样本，输出 evidence chunks。
4. 人工抽样检查 chunk 是否保留关键信息、是否剔除无用模板。
5. 仅设计/验证 `event_llm_extract` schema，不生成交易动作。

## 11. 测试与流水线要求

实施阶段必须满足：

| 类型 | 要求 |
|---|---|
| 单元测试 | 标题分类、时间语义、债券排除、ST 子类映射 |
| DB comment 检查 | 任何新增表/字段必须有 comment |
| SQL smoke | 回填行数、按 event_type 分布、债券误杀样本、ST 交叉匹配 |
| 事件研究 smoke | 小样本窗口收益计算正确，停牌/涨跌停标记正确 |
| 隔离检查 | `rg event_signal backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver` 确认没有消费者接入 |
| 回归测试 | 只跑本模块相关测试；不重启生产 8001 |

## 12. 需要用户确认的决策

后续进入实现前，建议用户确认以下配置：

1. `stock_st_imposed` 默认是否从一开始就是 `block_buy`，还是第一阶段统一 `warn_high` 后报告验证再升为 block。
2. `force_exit` 是否仅保留字段语义，暂不生成任何 active 信号；建议当前阶段不生成。
3. 事件研究基准用沪深300、全市场等权、还是行业中性基准；建议 MVP 先沪深300 + 原始收益。
4. LLM PoC 的第一批事件类型：建议从“非标审计/内控缺陷、监管处罚、资金占用/违规担保、债务违约、问询回复、诉讼仲裁”开始。
5. PDF 解析器 PoC 是否允许安装较重依赖；若不允许，先用 PyMuPDF4LLM + pdfplumber 做轻量基线。

## 13. 结论

可以构建一套回测和实盘统一使用的公告/财务事件信号框架，但正确顺序是：

```text
高置信标题风险（ST） -> 事件研究验证 -> 结构化财务交叉验证 -> 高价值 PDF 预处理 -> LLM JSON 抽取 -> 再接入交易消费者
```

ST 类明确重大风险不需要下载 PDF，可先直接禁止购买/禁止加仓候选，但当前阶段只生成独立信号和验证报告。LLM 的关键不是模型本身，而是公告正文预处理、噪声剔除、事件类型路由、证据 chunk 和可审计 JSON 输出。只有这样才能保证未来回测、模拟盘和实盘使用同一套事件事实、同一套时间语义、同一套规则版本，并避免 LLM 对无用模板文本过度分析。
