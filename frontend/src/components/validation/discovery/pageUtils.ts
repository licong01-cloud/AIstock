import {
  type ValidationDiscoveryCandidate,
  type ValidationDiscoveryEvidenceManifest,
  type ValidationPage,
  validationApi,
} from "@/lib/validation/api";

export function emptyPage<T>(pageSize = 20): ValidationPage<T> {
  return { items: [], total: 0, page: 1, page_size: pageSize, has_more: false };
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error || "未知错误");
}

export async function loadDiscoveryEvidence(traceId?: string): Promise<ValidationDiscoveryEvidenceManifest | null> {
  if (!traceId) return null;
  return validationApi.discoveryTrace(traceId);
}

export function candidateEvidenceId(candidate?: ValidationDiscoveryCandidate | null): string | undefined {
  return candidate?.evidence_manifest_id || (candidate?.candidate_id ? `evid_${candidate.candidate_id}` : undefined);
}
