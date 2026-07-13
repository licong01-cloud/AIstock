# 自选股票池分类同步通达信 F0 Feature Card

## Feature Card

目标是在 AIstock 自选股票池页面中，为当前选中的自选分类提供一个显式按钮，将该分类中的股票通过已有 TdxQuant 通道同步到本机通达信客户端自定义板块。

分级：F0。理由：该功能复用既有 `tdx-blocks` 后端能力，只在单个操作员页面增加入口，并补强按 `category_id` 调用契约，不涉及 DDL、交易下单、生产调度或跨模块架构调整。

## Scope

- `/watchlist` 页面选择具体分类后，显示并启用“同步通达信”操作。
- 点击同步前必须提示该操作会覆盖对应通达信板块。
- 后端支持按 `category_id` 同步，避免页面已持有 id 却按名称查找的歧义。
- 通达信功能不可用时，页面不暴露可点击写入按钮；后端继续 fail-fast 返回 503。
- 更新旧的 `TDX_BLOCK_DIR` 提示，当前实现以 `TDX_CLIENT_PATH` 和 TdxQuant 为准。

不包含：
- 不启动或重启 FastAPI、Next.js、TDX Go 或通达信客户端。
- 不直接调用真实通达信写入 API 做 live 验证。
- 不新增数据库表或迁移。

## Design Acceptance Index

- F-001: `/watchlist` 当前分类可触发通达信同步，并在未选具体分类时保持不可执行。
- F-002: 同步 API 支持 `category_id`，复用既有分类到板块映射 `AIstock_<category_id>`。
- F-003: TDX/TdxQuant 不可用时保持 fail-fast，不出现静默成功。
- F-004: 操作提示明确覆盖对应通达信板块，成功后显示板块名和同步数量。
- F-005: 旧 `TDX_BLOCK_DIR` 文案修正为 `TDX_CLIENT_PATH` / TdxQuant 前置条件。

## Verification

- Feature workflow validate 必须通过。
- 后端单测覆盖按 `category_id` 同步契约和 router 请求体。
- 前端 Playwright mock 测试覆盖 `/watchlist` 选择分类后调用同步接口。
- `git diff --check` 必须通过。

## Production Gates

- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
- runtime_activation: pending_user_owned，合入前不重启、不激活。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frontend/src/app/watchlist/page.tsx | npm run test:e2e -- tests/watchlist/watchlist-return-height.spec.ts | verified | - |
| F-002 | backend/services/tdx_block_service.py; backend/routers/tdx_blocks.py | python -m pytest backend/tests/watchlist/test_tdx_block_sync.py -q | verified | - |
| F-003 | backend/routers/tdx_blocks.py; frontend/src/app/watchlist/page.tsx | python -m pytest backend/tests/watchlist/test_tdx_block_sync.py -q | verified | - |
| F-004 | frontend/src/app/watchlist/page.tsx | npm run test:e2e -- tests/watchlist/watchlist-return-height.spec.ts | verified | - |
| F-005 | backend/routers/tdx_blocks.py; frontend/src/app/tdx-blocks/page.tsx | python -m ruff check backend/services/tdx_block_service.py backend/routers/tdx_blocks.py backend/tests/watchlist/test_tdx_block_sync.py; git diff --check | verified | - |
