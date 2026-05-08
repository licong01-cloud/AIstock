import { expect, test } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

const pagePath = path.resolve(process.cwd(), "src/app/quantevolver/evolution/page.tsx");
const source = fs.readFileSync(pagePath, "utf8");
const hasCapacityUi =
  source.includes("max_single_order_value") &&
  source.includes("max_weight") &&
  source.includes("max_position_ratio") &&
  (source.includes("param_schema") || source.includes("portfolio_config"));

test("QE evolution UI exposes capacity strategy parameters from strategy schema", async () => {
  test.fail(
    !hasCapacityUi,
    "Agent B capacity UI is not present yet; selecting score_weighted_topk_v2_capacity_v1 must expose editable capacity fields.",
  );

  expect(source).toContain("score_weighted_topk_v2_capacity_v1");
  expect(source).toContain("max_single_order_value");
  expect(source).toContain("max_weight");
  expect(source).toContain("max_position_ratio");
  expect(source).toMatch(/param_schema|portfolio_config/);
});
