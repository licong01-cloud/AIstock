import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


test("QE history renders separated asset lifecycle states without write-on-read actions", () => {
  const listPage = readFileSync(
    resolve(process.cwd(), "src/app/quantevolver/experiments/page.tsx"),
    "utf8",
  );
  const detailPage = readFileSync(
    resolve(process.cwd(), "src/app/quantevolver/experiments/[id]/page.tsx"),
    "utf8",
  );
  const api = readFileSync(resolve(process.cwd(), "src/lib/qe-archive/api.ts"), "utf8");

  expect(listPage).toContain('data-testid="qe-asset-lifecycle-status"');
  expect(listPage).toContain("数仓 ${status.warehouse_status");
  expect(listPage).toContain("资产 ${status.asset_status");
  expect(listPage).toContain("空间 ${status.workspace_status");
  expect(detailPage).toContain('data-testid="qe-detail-value-class"');
  expect(detailPage).toContain('data-testid="qe-detail-warehouse-status"');
  expect(detailPage).toContain('data-testid="qe-detail-asset-status"');
  expect(detailPage).toContain('data-testid="qe-detail-workspace-status"');
  expect(api).toContain('value_class?: "A" | "B" | "C" | "X" | "classification_pending"');
  expect(api).toContain('warehouse_status?: "not_eligible" | "pending" | "persisted" | "failed"');
  expect(api).toContain('asset_status?: "not_required" | "pending" | "published" | "partial" | "failed"');
  expect(api).toContain('workspace_status?: "active" | "grace_period" | "cleanup_pending" | "cleaned" | "cleanup_incomplete"');
  expect(listPage).not.toContain("useEffect(() => qeArchiveApi.executeSelection");
  expect(detailPage).not.toContain("useEffect(() => strategyPackageApi.createCandidateFromQEExperiment");
});
