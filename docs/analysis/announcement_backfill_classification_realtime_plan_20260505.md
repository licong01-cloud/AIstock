# 公告结构化数据补齐、标题分类与实时源分析

日期：2026-05-05

## 本轮执行结论

- 已将 `market.anns` 的结构化公告元数据补齐到 `2026-04-30`，本轮不下载 PDF。
- Tushare `anns_d` 当前仍不能作为主源：本地 token 调用返回权限不足，不是普通频率限制。
- cninfo 官方接口可用，但高并发会触发 `403 Forbidden`；适合作为低频官方校验和 PDF URL 修复源，不适合作为高并发历史回灌源。
- Eastmoney 公告接口可稳定按自然日回补历史元数据，并提供 `display_time/eiTime/notice_date/sort_date`，适合作为免费历史补齐和实盘轮询主候选。
- Yahoo Finance 和新浪更适合作为媒体新闻/二次传播源，不适合作为 A 股公告的权威实盘源。

## 数据补齐结果

目标范围：`2018-08-01` 至 `2026-04-30`。

执行脚本：

- `scripts/sync_cninfo_anns_metadata.py`
- `scripts/sync_eastmoney_anns_metadata.py`
- `scripts/classify_announcement_titles_v0.py`

主要产物：

- Eastmoney 全量审计：`reports/anns/eastmoney_sync_audit_full_20260505_005452.jsonl`
- Eastmoney 全量日志：`reports/anns/eastmoney_sync_full_20260505_005452.log`
- 分类统计 JSON：`reports/anns/announcement_title_classification_v0_20260505.json`
- 分类报告 Markdown：`docs/analysis/announcement_title_classification_v0_20260505.md`

当前 `market.anns` 概况：

| 指标 | 数值 |
|---|---:|
| 总行数 | 5,131,329 |
| 日期范围 | 2018-08-01 ~ 2026-04-30 |
| 有公告的自然日 | 2,719 |
| Eastmoney URL 行 | 4,305,747 |
| cninfo/static URL 行 | 825,582 |
| rec_time 非空 | 4,930,306 |
| rec_time 为空 | 201,023 |
| rec_time 为 00:00:00 | 36,253 |

按年行数：

| 年份 | 行数 | 有公告日期数 |
|---|---:|---:|
| 2018 | 221,574 | 142 |
| 2019 | 503,939 | 347 |
| 2020 | 578,348 | 359 |
| 2021 | 632,483 | 356 |
| 2022 | 675,572 | 356 |
| 2023 | 686,637 | 351 |
| 2024 | 677,458 | 350 |
| 2025 | 880,318 | 349 |
| 2026 | 275,000 | 109 |

最近补齐到 4 月 30 日后的行数：

| 日期 | 行数 |
|---|---:|
| 2026-04-24 | 15,053 |
| 2026-04-25 | 13,761 |
| 2026-04-26 | 16 |
| 2026-04-27 | 6,576 |
| 2026-04-28 | 21,669 |
| 2026-04-29 | 26,012 |
| 2026-04-30 | 13,511 |

## 数据源判断

### 1. cninfo

接口：

- `http://www.cninfo.com.cn/new/hisAnnouncement/query`
- PDF：`https://static.cninfo.com.cn/finalpage/YYYY-MM-DD/{announcementId}.PDF`

优点：

- 官方披露源之一，适合最终校验、PDF 下载、源文件归档。
- 返回 `announcementTime`、`announcementId`、`adjunctUrl` 等字段。

问题：

- 页大小固定为 30。
- `totalpages` 可能低估，必须按 `totalAnnouncement` 和 `hasMore` 双重逻辑继续翻页。
- 高并发或快速翻页会触发 `403 Forbidden`。本轮 6 worker 历史回灌在早期日期第 8~11 页触发 403；冷却后单次查询恢复。

建议定位：

- 官方日终/次日低频校验源。
- PDF URL 修复源。
- 不作为大规模高并发实时轮询源。

### 2. Eastmoney

接口：

- `https://np-anotice-stock.eastmoney.com/api/security/ann`
- 参数核心：`ann_type=A`、`begin_time`、`end_time`、`page_size=100`、`page_index`

优点：

- 可按自然日查询，页大小 100，历史回补效率高。
- 字段包含 `display_time`、`eiTime`、`notice_date`、`sort_date`、`art_code`、`codes`、`columns`、`title`。
- 本轮全量回补 2,830 个自然日，0 失败。

问题：

- 不是交易所或巨潮官方接口。
- API 不直接返回 PDF URL；返回的是 Eastmoney 公告详情页。
- 标题常带证券简称前缀，已在脚本中做标题前缀标准化。

建议定位：

- 免费历史元数据补齐主源。
- 实盘轮询主候选源。
- 与 cninfo 做日终 reconciliation，补充官方 PDF URL 和字段差异。

### 3. Tushare

现状：

- `anns_d` 文档字段适合入库：`ann_date/ts_code/name/title/url/rec_time`。
- 本地 token 当前无 `anns_d` 权限；10,000 积分不等于该接口权限。

建议定位：

- 若后续开通权限，可作为高质量标准化源。
- 当前不阻塞框架建设，先以 Eastmoney + cninfo 完成免费链路。

### 4. 上交所/深交所/上证所信息网络

判断：

- 上交所、深交所官网页面可查公告，但接口分市场、字段不统一，且并非稳定公开 API 合约。
- 上证所信息网络的数据服务明确提供公告/统计等同源数据服务，但偏商业服务。

建议定位：

- 免费版本：先不作为统一历史主源。
- 机构化/实盘强化版本：可评估付费官方同源服务，作为最高可信源。

### 5. Yahoo Finance / 新浪

Yahoo Finance：

- 没有可靠官方开发者 API；社区库主要依赖非官方端点。
- 对 A 股公告没有完整、权威、及时的披露字段体系。
- 可作为境外新闻/SEC filing 的参考，不作为 A 股公告实盘源。

新浪：

- 有公司公告网页和新闻页面，适合做媒体传播/舆情补充。
- 未发现适合作为全市场、结构化、实时 A 股公告主链路的稳定免费 API。

建议定位：

- 仅作为媒体新闻/舆情辅助源。
- 不进入第一阶段 canonical announcement metadata。

## 分类 v0 结果

规则版本：`aistock_announcement_title_rules_v0_20260505`

| 风险等级 | 行数 | 用途 |
|---|---:|---|
| P0_BLOCK | 25,451 | 标题即可禁止新买入 |
| P1_HIGH | 89,648 | 高风险预警，PDF/LLM 可选用于解释 |
| P2_REVIEW | 1,240,414 | 复杂事件候选，持仓或拟买入时再下载/LLM |
| P3_POSITIVE_CANDIDATE | 398,387 | 仅记录，暂不做 alpha 增益 |
| P4_NEUTRAL | 3,377,429 | 中性归档或后续规则挖掘 |

需要 LLM/PDF 的估计：

- `YES`：1,233,901 行，主要是诉讼、质押/减持、重组、担保/关联交易、业绩预告/减值、融资、问询函、关键人员变动。
- `OPTIONAL`：486,555 行，主要是 P1 高风险解释和 P3 正向候选验证。
- `NO`：2,025,699 行，可直接按标题规则处理。
- `SAMPLE_ONLY`：1,385,174 行，未分类归档，只抽样做规则挖掘，不自动触发 LLM。

第一阶段直接可用的分类：

- P0：退市/风险警示、破产重整。
- P1：监管立案处罚、债务违约逾期、审计/内控重大问题、资金占用/违规担保。
- P4：定期报告、会议决议、制度章程、专业中介报告、常规人事变动、IPO/审核文件。

第一阶段需要后续 PDF/LLM 的类型：

- 问询函/关注函。
- 诉讼仲裁/冻结拍卖。
- 质押/减持/权益变动。
- 控制权变更/并购重组/资产交易。
- 担保/财务资助/关联交易。
- 业绩预告修正/亏损/减值/会计差错。
- 融资稀释/债券/ABS/募集资金。
- 关键人员变动。

## 回测与实盘统一原则

统一引擎：

- `AnnouncementSignalEngine` 只接受规范化公告事件输入。
- 回测和实盘使用同一套标题规则、同一套 effective date 规则、同一套风险级别解释。

时间规则：

- `EXACT` 且盘前可得：当日生效。
- `EXACT` 且收盘后：下一交易日生效。
- `00:00:00`：默认下一交易日生效，除非另一可信源验证是真实发布时间。
- 空值：下一交易日生效。

交易信号定位：

- 公告信号不作为普通日频 alpha 因子直接混入现有 alpha。
- 第一阶段作为独立的 `Announcement Event Risk Overlay`：
  - `P0_BLOCK`：禁止新买入。
  - `P1_HIGH`：高风险预警，可配置降低仓位/禁止买入。
  - `P2_REVIEW`：持仓或候选池命中时进入 PDF/LLM。
  - `P3_POSITIVE_CANDIDATE`：仅研究记录，不增益。
  - `P4_NEUTRAL`：归档。

## 下一步可继续执行内容

1. 将 `scripts/classify_announcement_titles_v0.py` 的规则抽成正式配置，并在后端实现 `AnnouncementSignalEngine`。
2. 增加 `announcement_event_signal` 派生表，保存分类结果、风险等级、effective_trade_date、source_time_quality，并补充 DB 注释。
3. 对 2018-08-01 ~ 2026-04-30 做批量派生信号，不下载 PDF。
4. 对 P0/P1 做事件研究，验证避险规则对回撤、停牌、ST、退市、黑天鹅的影响。
5. 对 P2 中实际持仓/候选命中的小集合做 PDF 下载与 LLM 结论抽取。
6. 实盘轮询先采用 Eastmoney 每 1~3 分钟增量拉取当日和前一自然日，日终用 cninfo 低频校验和补官方 PDF URL。

## 外部参考

- AKShare `stock_notice_report` 文档说明其目标地址为东方财富公告大全、字段含代码/名称/公告标题/公告类型/公告日期。
- cninfo 开源抓取示例使用 `hisAnnouncement/query`、`hasMore` 和 `adjunctUrl` 构造 PDF。
- `yahoo-finance2` 明确属于 Yahoo Finance 非官方 API，且可用模块偏行情/财务/SEC filing，不适合作为 A 股公告权威源。
- 上证所信息网络提供公告数据服务和同源统计数据服务，但属于数据服务路线。
