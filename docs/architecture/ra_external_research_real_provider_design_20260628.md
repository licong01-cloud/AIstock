# RA External Research Real Provider Design (2026-06-28)

## 1. 背景

BUG-542 处理 Research Assistant 的 `external_research` 当前只能返回 deterministic 离线样例的问题。目标是在不破坏现有离线门控和测试的前提下，补一个 opt-in 真 provider：

- L1 联网搜索：本地自托管 AgentSearch（SearXNG + FastAPI 抽取一体）。
- L2 论文搜索：Semantic Scholar API 为主，arXiv API 兜底。
- L3 正文抽取：AgentSearch `/read` 优先；AgentSearch 不可用时，用 `httpx` 拉取网页并交给本地 `trafilatura` 抽取。
- 默认行为：未配置或 `RA_EXTERNAL_RESEARCH_PROVIDER=offline` 时继续使用 `DeterministicExternalResearchProvider`。

## 2. 接口契约

不改变 `backend/services/research_assistant/external_research.py` 的协议签名：

- `search_web(query, locale="zh-CN", limit=10) -> list[ExternalEvidenceItem]`
- `search_papers(query, provider=None, limit=10) -> list[ExternalEvidenceItem]`
- `fetch_extract(url, max_chars=2000) -> ExtractedEvidence`

`RealExternalResearchProvider` 复用现有 helper：

- `item_from_provider_payload`：将厂商 payload 转为 `ExternalEvidenceItem`。
- `stable_evidence_ref`：生成稳定 `evidence_ref`。
- `assert_token_safe`：拒绝 `raw_html/full_text/provider_raw_response` 等重字段与超长字符串。
- `clamp_limit/clamp_chars/utc_today`：统一限制与时间口径。

正文全文不内联，只保留摘要和 `content_preview`；长正文和 provider 原始响应不进 payload，查询细节通过 `detail_ref` 引导到 `external_research_fetch_extract`。

## 3. Provider 选型与端点

### AgentSearch

`RA_AGENTSEARCH_BASE_URL` 指向本机 AgentSearch FastAPI 服务，默认示例端口 `3939`。

- `GET /search?q=<query>&language=<locale>&count=<limit>`：返回 `results/items` 数组，映射为 `provider=agentsearch_web`。
- `GET /read?url=<url>&max_chars=<n>`：返回正文抽取结果，映射为 `provider=agentsearch_extract`。

### Semantic Scholar

默认 `RA_PAPER_PROVIDER=semantic_scholar`：

- `GET /graph/v1/paper/search?query=<query>&limit=<limit>&fields=...`
- 可选 `S2_API_KEY` 使用公共 API key header；留空走公共额度。
- `429` 时显式记录 `S2_RATE_LIMIT_FALLBACK` 并切 arXiv。

### arXiv

`RA_PAPER_PROVIDER=arxiv` 或 Semantic Scholar 429 兜底：

- `GET https://export.arxiv.org/api/query?search_query=all:<query>&max_results=<limit>`
- 解析 Atom XML，返回 `provider=arxiv`。

## 4. 注入与环境变量

`backend/main.py` 在 `_lifespan` 的 `init_db_pool` 之后调用 `_configure_external_research_provider()`：

- `RA_EXTERNAL_RESEARCH_PROVIDER=offline` 或未设置：不注入，保留 router 默认 provider。
- `RA_EXTERNAL_RESEARCH_PROVIDER=real` 且 `RA_AGENTSEARCH_BASE_URL` 非空：注入 `RealExternalResearchProvider.from_env()`。
- mode 不支持或缺 `base_url`：记录 warning + `reason_code`，继续保留离线 provider。

环境变量：

```env
RA_EXTERNAL_RESEARCH_PROVIDER=offline
RA_AGENTSEARCH_BASE_URL=http://127.0.0.1:3939
RA_PAPER_PROVIDER=semantic_scholar
S2_API_KEY=
```

## 5. no-silent-error

所有联网失败均不伪造证据：

- 搜索失败（timeout、connection refused、HTTP 4xx/5xx、JSON/XML schema invalid）：返回空 list，并在 provider `last_failure()` 记录 `reason_code/context`。
- router 在空结果时将 `reason_codes/status/warnings` 放进响应，使上层 no-data guard 可诚实报告“无对应数据源”。
- `fetch_extract` 的 AgentSearch 抽取失败先记录 `AGENTSEARCH_EXTRACT_REQUEST_FAILED`，再走本地 `trafilatura`；本地也失败时返回空 preview 的 `ExtractedEvidence`，`detail_ref.reason_code` 明确失败原因。

## 6. Stub 守卫兼容

真 provider 的 `provider/source/summary/detail_ref` 不使用 RA stub 守卫标记，也不使用 RFC 示例域名。Provider key 固定为：

- `agentsearch_web`
- `agentsearch_extract`
- `semantic_scholar`
- `arxiv`
- `local_trafilatura_extract`

真实页面正文可能包含示例域名时，正文只进 `content_preview` 截断和 detail 引用，不把 provider 原始 payload 或全文内联。

## 7. 部署与依赖门

新增后端生产依赖：

- `httpx==0.28.1`：HTTP client。
- `trafilatura==2.1.0`：AgentSearch 抽取不可用时的本地正文抽取兜底。

`production_backend_dependency_gate=pending`：合并后需要用户在生产 backend 环境安装依赖，然后重启 8001 才能启用真 provider。

部署物：

- `deploy/agentsearch/docker-compose.yml`
- `deploy/agentsearch/agentsearch.env.example`
- `deploy/agentsearch/README.md`

## 8. 验收断言

- 默认 env/offline 仍为 `DeterministicExternalResearchProvider`。
- `search_web` 用 httpx mock AgentSearch 返回真实 host，payload 过 `assert_token_safe`，不被 `_external_research_result_is_stub` 判 stub。
- `search_papers` 覆盖 Semantic Scholar、Semantic Scholar 429 -> arXiv、arXiv 直连。
- `fetch_extract` 覆盖 AgentSearch `/read` 与 503 后本地 `trafilatura` 兜底。
- timeout/connection refused 返回空结果 + reason_code。
- 不触碰 `react_grounding.py`、`service.py`、guard、`tool_router.py`。
