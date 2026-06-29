# Paper v2 Package Center Multi-Alpha Combine Source

## Feature Card

本任务把多Alpha combine run 接入 `/paper-v2/packages` 创建入口，仅改前端，不改后端、PaperPortfolio 契约、advisory 页面或 per-leg 归因展示。

## Scope

- 在包中心新增 `multi_alpha_combine_run` 来源类型，并保留 `qe_experiment` / `qe_evolution_loop` 原有创建路径。
- 多Alpha 来源不走 `/strategy-packages/qe-sources`，只读 `/multi-alpha/combine-backtest/runs?status=succeeded` 和详情端点。
- 创建时调用既有 `createFromMultiAlphaCombineRun`，严格传 `ic_weighted`、`topk`、`frozen_backtest_terminal_weights` 和确认 token。
- 多Alpha 来源下禁用 QE-only 的 Manifest 预览和资产合格性检查，只展示 run/scheme 指标与真实失败信息。

## Design Acceptance Index

- F-001: 扩展前端来源类型与来源 selector，加入 `multi_alpha_combine_run`，且不改变单Alpha来源行为。
- F-002: 新增 combine 专用来源选择器：只列 succeeded run、scheme 固定 `ic_weighted`、topk 仅允许 25/50。
- F-003: 通过 `createFromMultiAlphaCombineRun` 创建 multi-alpha StrategyPackage，payload 使用后端 S1 硬契约并省略 `component_package_ids`。
- F-004: 保持 fail-fast：combine 来源不调用 QE-only 预览/合格性端点，后端错误原样进入 ErrorPanel。
- F-005: 创建成功后提示 paper admission 仍需 `paper-runtime-dry-run(local_sim)` 清门，避免夸大为可直接进模拟盘/选股/荐股。

## Verification

- `cd frontend && npm run lint`
- `cd frontend && npx tsc --noEmit`
- `cd frontend && npm run build`
- 若本地后端和真实数据可用，再手动验证：选择 succeeded combine run，创建父包和 component 单包，随后执行 `POST /strategy-packages/{id}/paper-runtime-dry-run` 并检查 selectable/advisory/portfolio 链路。

## Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `frontend/src/lib/paper-v2/types.ts`; `frontend/src/app/paper-v2/packages/page.tsx` | Frontend lint, tsc, build; selector review | verified | - |
| F-002 | `frontend/src/lib/paper-v2/api.ts`; `frontend/src/app/paper-v2/packages/page.tsx` | Frontend lint, tsc, build; read-only route wrapper review | verified | - |
| F-003 | `frontend/src/app/paper-v2/packages/page.tsx`; `frontend/src/lib/paper-v2/api.ts` | Frontend lint, tsc, build; payload contract review | verified | - |
| F-004 | `frontend/src/app/paper-v2/packages/page.tsx`; `frontend/src/lib/paper-v2/api.ts` | Frontend lint, tsc, build; error propagation review | verified | - |
| F-005 | `frontend/src/app/paper-v2/packages/page.tsx` | Frontend lint, tsc, build; success guide copy review | verified | - |
