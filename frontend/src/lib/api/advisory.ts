import { selectionCenterAdvisoryApi, type JsonObject } from "./selectionCenter";

export type AdvisoryQualityReport = {
  report_type: string;
  sample_count: number;
  min_bucket_size: number;
  metrics: JsonObject;
  buckets: JsonObject[];
  warnings: string[];
};

export type AdvisoryReviewPreviewPayload = {
  items: JsonObject[];
  package_evidence_by_code: Record<string, Record<string, JsonObject>>;
  market_by_code: Record<string, JsonObject>;
  trade_date: string;
  exit_guard_policy: JsonObject;
  fusion_policy: JsonObject;
};

export const advisoryApi = {
  async qualityReport(records: JsonObject[], minBucketSize = 30): Promise<AdvisoryQualityReport> {
    const data = await selectionCenterAdvisoryApi.post<{ report: AdvisoryQualityReport }>(
      "/selection-center/advisory/quality-report",
      { records, min_bucket_size: minBucketSize },
    );
    return data.report;
  },
  async reviewPreview(payload: AdvisoryReviewPreviewPayload): Promise<JsonObject[]> {
    const data = await selectionCenterAdvisoryApi.post<{ records: JsonObject[] }>(
      "/selection-center/advisory/multi-package-review/preview",
      payload,
    );
    return data.records || [];
  },
};
