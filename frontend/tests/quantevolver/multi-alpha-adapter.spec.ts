import { expect, test } from "@playwright/test";
import { canonicalMultiAlphaEvolutionUrl } from "../../src/app/quantevolver/evolution/components/multiAlphaEvolutionAdapter";

test("canonical adapter preserves query while replacing the task identity", () => {
  const url = canonicalMultiAlphaEvolutionUrl("task/a", new URLSearchParams("tab=runtime&scheme=equal&task_id=old"));
  const parsed = new URL(url, "http://localhost");
  expect(parsed.pathname).toBe("/quantevolver/evolution");
  expect(parsed.searchParams.get("task_type")).toBe("multi_alpha_combine");
  expect(parsed.searchParams.get("task_id")).toBe("task/a");
  expect(parsed.searchParams.get("tab")).toBe("runtime");
  expect(parsed.searchParams.get("scheme")).toBe("equal");
});
