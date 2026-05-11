# Codex DISPATCH — Implement /paper-v2/coldstart-sanity/sentinel-order Endpoint (Task 8)

**From**: Strategy session
**To**: Codex App
**Sent**: 2026-05-11 ~22:50
**Type**: Type B (coordinated, real-trading prep BLOCKER)
**Branch**: codex/qe-governance-integration-20260509
**Severity**: BLOCKER for 9:30 LocalSim 模拟盘 cold-start sanity

## 上下文

paper-v2 verify Task 6 (commit `1dc2e60`, drawer `46e4de`) **L5 发现**:
> Sentinel endpoint `/paper-v2/coldstart-sanity/sentinel-order` real existence in paper-v2 backend routers: **CAVEAT** (absent in main backend + codex c2352a9 backend)

Codex Task 7 E2E wrapper (commit `a72411d`, drawer `9f61c2ea`) 已 propagate caveat:
> wrapper rejects caveated READY docs before prod execution; release commander must provide clean READY doc after endpoint wiring or use --sentinel-endpoint override before wrapper can GO

**用户决策 (2026-05-11)**: 实盘方案暂时不开发, 明早只跑 **LocalSim 模拟盘**。但 cold-start sanity gate 仍需 endpoint 才能 round-trip 验证。

## 任务

在 `backend/routers/paper_trading_v2.py` 添加新 endpoint:
**`POST /paper-v2/coldstart-sanity/sentinel-order`**

### Endpoint 规范

**Request body** (匹配 Codex Task 6 sentinel order spec):
```json
{
  "symbol": "000001.SZ",
  "side": "BUY",
  "qty": 100,
  "intended_price": 10.00,
  "run_id": "sanity-<timestamp>",
  "broker_backend": "local_sim"   // 仅接受 local_sim, miniqmt_sim reject
}
```

**Response body** (success):
```json
{
  "fill_id": "<uuid>",
  "run_id": "<run_id>",
  "intended_price": 10.00,
  "fill_market_context": {...},
  "created_at": "<iso8601>",
  "updated_at": "<iso8601>",
  "routing_class": "telemetry",
  "outbox_event_id": "<uuid>"
}
```

**Response codes**:
- 200: round-trip OK, fill 已写入 paper_v2.fills + capture 字段 + outbox emit
- 400: invalid input (qty=0 / unknown symbol / broker_backend not local_sim)
- 409: governance enable_paper gate disabled
- 503: daemon not running

### 实现要求

1. **仅 LocalSim**: hard reject `broker_backend != "local_sim"` (用户决策: 实盘方案暂不开发)
2. **真round-trip**: 创建 paper_v2.run + 提交 OrderIntent → LocalSim broker 撮合 → 写 fill (含 T5/T6.1/T6.2 capture) → emit outbox event (T13 routing_class=telemetry) → 返回 fill 信息
3. **必须经 governance enable_paper gate**: 如 strategy_package_governance 表中没有 enable_paper=true 的策略包, 返回 409
4. **Audit trail**: governance evidence row 自动写入 (governance audit chain)
5. **非交易时段限制**: 9:30-11:30 + 13:00-15:00 CST 内 reject (避免影响真实模拟交易)
6. **Sentinel run_id 格式校验**: 必须以 `sanity-` 开头, 防止与正常 paper-v2 run 混淆

### 测试

`backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py`:
- 200 happy path (LocalSim round-trip success)
- 400 invalid inputs (各种)
- 409 governance gate disabled
- 503 daemon not running
- broker_backend=minqmt_sim reject
- 非交易时段 reject
- sentinel run_id 格式校验
- 与 Codex Task 6 sanity script `--mode=prod` 集成 (mocked daemon)
- ≥ 15 tests passed, 0 P1 guardrail

### Update Codex Task 6 sanity script

`scripts/paper_v2_coldstart_sanity.py` Phase 2 调用 endpoint:
- 改成调用 `POST /paper-v2/coldstart-sanity/sentinel-order` (现在真存在了)
- 移除 `--sentinel-endpoint` override 的必要性 (default 即可用)
- 更新对应 30 tests

### Update Runbook

`docs/operations/r6_prod_apply_runbook_20260511.md`:
- §7.4 后加入 sentinel endpoint deploy 检查 step
- 移除 §8.5 关于 endpoint 缺失的 caveat
- E2E wrapper §11 README docs 更新

### 重跑 verify

完成后 deliver drawer 含:
- commit hash
- endpoint code + tests + sanity script update
- runbook update
- guardrail 0 P1
- 期望 caveat removed: paper-v2 后续 re-verify sanity gate 可 verdict=READY (no caveats)

### Do NOT

- ❌ 不要接 miniqmt_sim / miniqmt_live (用户决策: 实盘方案暂不开发)
- ❌ 不要执行 prod sanity (这是 user 实盘前手动跑)
- ❌ 不要 INSERT dev DB (但测试可 mock)
- ❌ 不要 commit credentials / token in test
- ❌ 不要 merge codex branch

### SLA

**≤ 1.5h** (~00:20 deliver)

## 引用

- paper-v2 verify Task 6 caveat: drawer `46e4de`, commit `1dc2e60` (cherry-picked to main as `63bf871`)
- Codex Task 7 wrapper: drawer `9f61c2ea`, commit `a72411d`
- LocalSim broker: `backend/services/paper_trading_v2/broker/localsim.py`
- 实盘目标: 明早 9:30 A股开市 LocalSim 模拟盘
- main HEAD: `568c16c`

## 用户决策记录

实盘方案暂不开发 → MiniQMTSimBackend (PR-005) 不实施, minqmt_live 不开放, 明早仅 LocalSim 模拟盘。本任务只支持 LocalSim 路径, 后续 PR-005 实施时再扩展 endpoint 接受 miniqmt_sim。
