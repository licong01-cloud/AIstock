import { expect, test } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const componentPath = path.resolve(
  process.cwd(),
  "src/app/quantevolver/evolution/components/LoopMetricsComparison.tsx",
);
const source = fs.readFileSync(componentPath, "utf8");

test("QE evolution best loop ranking excludes incomplete metrics", async () => {
  expect(source).toContain("function hasCompleteRankMetrics");
  expect(source).toContain("const bestCandidateRows = rows.filter((row) => row.bestEligible)");
  expect(source).toContain("isFiniteMetric(rank.cagr)");
  expect(source).toContain("isFiniteMetric(rank.absMaxDrawdown)");
  expect(source).toContain("isFiniteMetric(rank.sharpe)");
  expect(source).toContain("isFiniteMetric(rank.avgCount)");
  expect(source).toContain("isFiniteMetric(rank.maxCount)");
  expect(source).toContain("isFiniteMetric(rank.finalCash)");
  expect(source).toContain("isFiniteMetric(rank.finalStockValue)");
  expect(source).toContain("Incomplete metrics: loop is excluded from best-loop ranking.");
  expect(source).toContain("metricNumber(ar, [\"sharpe\", \"sharpe_absolute\"])");
  expect(source).not.toContain("rankReturn: cagr ?? annReturn ?? -Infinity");
  expect(source).not.toContain("}, rows[0])");
});

const pagePath = path.resolve(process.cwd(), "src/app/quantevolver/evolution/page.tsx");
const pageSource = fs.readFileSync(pagePath, "utf8");

test("QE summary loops preserve compact enhanced metrics for trajectory comparison", async () => {
  expect(pageSource).toContain("const metricsJson = loop.metrics_json || {");
  expect(pageSource).toContain("...metricsSummary,");
  expect(pageSource).toContain("sharpe: metricsSummary.sharpe ?? metricsSummary.information_ratio");
  expect(pageSource).toContain("strategy_params: configSummary.strategy_params || {}");
  expect(pageSource).toContain("unfilled_handler_params: configSummary.unfilled_handler_params || {}");
  expect(pageSource).toContain("metrics_json: metricsJson");
});

const loopDiagnosticsPath = path.resolve(
  process.cwd(),
  "src/app/quantevolver/evolution/components/loopDiagnostics.ts",
);
const loopDiagnosticsSource = fs.readFileSync(loopDiagnosticsPath, "utf8");

test("QE loop diagnostics can derive holdings from historical stock trades", async () => {
  expect(loopDiagnosticsSource).toContain("function derivePositionFromStockTrades");
  expect(loopDiagnosticsSource).toContain("enhanced.stock_trades");
  expect(loopDiagnosticsSource).toContain("row.date ?? row.datetime ?? row.trade_date");
  expect(loopDiagnosticsSource).toContain("p95Index");
  expect(loopDiagnosticsSource).toContain("derivedPosition.avgCount");
});

const strategyConfigPath = path.resolve(
  process.cwd(),
  "src/app/quantevolver/components/StrategyConfigCard.tsx",
);
const strategyConfigSource = fs.readFileSync(strategyConfigPath, "utf8");

test("QE strategy config card reads compact loop strategy params", async () => {
  expect(strategyConfigSource).toContain("parseJsonObject(cfg.strategy_params");
  expect(strategyConfigSource).toContain("strategyParams.unfilled_handler");
  expect(strategyConfigSource).toContain("strategyParams.unfilled_backup_depth");
  expect(strategyConfigSource).toContain("strategyParams.hold_thresh");
});
