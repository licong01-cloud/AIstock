import { expect, test } from "@playwright/test";

const hash = "a".repeat(64);

test("position timing keeps scope explicit and emits only a human reminder", async ({ page }) => {
  let selected = false;
  const scopeWrites: unknown[] = [];
  const claimWrites: unknown[] = [];
  const intents = () => ({
    items: [
      {
        canonical_symbol: "000001.SZ",
        display_name: "平安银行",
        primary_source_role: "HOLDING",
        source_roles: ["HOLDING"],
        pre_action_qty: 1000,
        intent: null,
        normalization_reason: null,
        analysis_selected: false,
        analysis_effective: true,
        analysis_locked: true,
        analysis_reason_code: "HOLDING_ALWAYS_INCLUDED",
      },
      {
        canonical_symbol: "600000.SH",
        display_name: "浦发银行",
        primary_source_role: "WATCHLIST",
        source_roles: ["WATCHLIST"],
        pre_action_qty: 0,
        intent: null,
        normalization_reason: null,
        analysis_selected: selected,
        analysis_effective: selected,
        analysis_locked: false,
        analysis_reason_code: selected ? "SELECTED" : "NOT_SELECTED",
      },
    ],
    scope_warnings: [],
  });
  const trigger = {
    trigger_id: "pttrg_sell",
    branch: "RISK_EXIT_AT_OPEN",
    side: "SELL",
    operator: "ALWAYS",
    trigger_price_raw: 9,
    guard_action: "SELL",
    planned_delta_qty: -1000,
    planned_leg_notional_cny: 9000,
    reason_code: "STOP_LOSS_TRIGGERED",
  };
  const card = {
    card_id: "ptcard_holding",
    canonical_symbol: "000001.SZ",
    display_name: "平安银行",
    primary_source_role: "HOLDING",
    source_roles: ["HOLDING"],
    decision_trade_date: "2026-09-03",
    target_trade_date: "2026-09-04",
    valid_until: "2026-09-04T15:00:00+08:00",
    pre_action_qty: 1000,
    t1_sellable_qty: 1000,
    planned_full_notional_cny: 9000,
    desired_target_exposure: 0,
    requested_delta_qty: -1000,
    requested_leg_notional_cny: 9000,
    action: "EXIT",
    execution_window: "AT_OPEN",
    reference_price_raw: 9,
    tradability_status: "TARGET_DAY_RECHECK_REQUIRED",
    st_flag: false,
    delist_flag: false,
    delist_context_status: "AVAILABLE",
    reason_codes: ["STOP_LOSS_TRIGGERED"],
    triggers: [trigger],
    selection_context_status: "UNAVAILABLE",
    hmm_context_status: "UNAVAILABLE",
    evidence_tier: "RULE_BASED_RISK_MANAGEMENT",
    cost_estimate: null,
    trigger_cost_estimates: {},
  };

  await page.route("**/api/v1/position-timing/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    if (path.endsWith("/materialize")) {
      await route.fulfill({ json: { status: "ALREADY_MATERIALIZED", outcome_materialization_status: "NO_DUE_OUTCOMES" } });
      return;
    }
    if (path.endsWith("/intents")) {
      await route.fulfill({ json: intents() });
      return;
    }
    if (path.includes("/analysis-scope/")) {
      scopeWrites.push(route.request().postDataJSON());
      selected = Boolean((route.request().postDataJSON() as { analysis_enabled: boolean }).analysis_enabled);
      await route.fulfill({ json: { status: "UPDATED", analysis_reason_code: "SELECTED" } });
      return;
    }
    if (path.endsWith("/cards/current")) {
      await route.fulfill({ json: { status: "VALID_TODAY", card_set: { card_set_id: "ptset_1", decision_trade_date: "2026-09-03", target_trade_date: "2026-09-04", cards: [card] } } });
      return;
    }
    if (path.endsWith("/evidence")) {
      await route.fulfill({ json: {
        product_evidence_tier: "RULE_BASED_RISK_MANAGEMENT",
        event_counts: { CARD_ISSUED: 1, OUTCOME_EVALUATED: 0 },
        l2_runtime_status: "PIPELINE_DEFERRED_BY_APPROVED_SCOPE",
        hmm_runtime_role: "CONTEXT_ONLY",
        selection_runtime_role: "CONTEXT_ONLY",
        cost_disclosure: { min_commission_scope_verification: "BROKER_UNVERIFIED", thresholds_cny: { "1.00": 58824, "0.50": 117648, "0.25": 235295 } },
        outcome_evidence: { status: "AVAILABLE", coverage_counts: { matured: 0, pending: 5, unavailable: 0, materialization_missing: 0 }, paired_matured: { count: 0 }, intervention_intent: { count: 0 } },
      } });
      return;
    }
    if (path.endsWith("/alerts/poll")) {
      await route.fulfill({ json: { status: "EVALUATED", items: [{
        card_id: card.card_id,
        canonical_symbol: card.canonical_symbol,
        status: "ELIGIBLE",
        system_edge_eligibility: true,
        already_alerted: false,
        trigger_id: trigger.trigger_id,
        eligibility_identity: hash,
        quote_price_raw: 9,
        quote_open_raw: 9,
        quote_observed_at: "2026-09-04T09:30:58+08:00",
        alert_evaluated_at: "2026-09-04T09:31:00+08:00",
        quote_source: "TDX_REALTIME.batch_quote",
        position_snapshot_sha256: hash,
        intent_snapshot_sha256: hash,
        trigger,
      }] } });
      return;
    }
    if (path.includes("/alerts/") && path.endsWith("/claim")) {
      claimWrites.push(route.request().postDataJSON());
      await route.fulfill({ json: { granted: true, status: "AUTHORIZED" } });
      return;
    }
    await route.abort();
  });

  await page.goto("/position-timing");
  await expect(page.getByRole("heading", { name: "持仓与自选择时建议" })).toBeVisible();
  await expect(page.getByLabel("000001.SZ 纳入择时分析")).toBeChecked();
  await expect(page.getByLabel("000001.SZ 纳入择时分析")).toBeDisabled();
  await expect.poll(() => claimWrites.length).toBe(1);
  await expect(page.getByText(/已到达冻结卖出条件/)).toBeVisible();
  await expect(page.getByTestId("timing-alert-list")).toContainText("提醒发送权已授予");

  await page.getByLabel("600000.SH 纳入择时分析").click();
  await expect.poll(() => scopeWrites.length).toBe(1);
  await expect(page.getByLabel("600000.SH 纳入择时分析")).toBeChecked();
  expect(scopeWrites[0]).toEqual({ analysis_enabled: true });
  await expect(page.getByRole("button", { name: /下单|自动交易|买入|卖出/ })).toHaveCount(0);
});
