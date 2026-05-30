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
