import type { HmmSnapshot } from "./types";

export type HmmCoefficientArtifact = NonNullable<HmmSnapshot["coefficient_artifacts"]>[number];

function hasPath(artifact: HmmCoefficientArtifact): artifact is HmmCoefficientArtifact & { path: string } {
  return typeof artifact.path === "string" && artifact.path.trim().length > 0;
}

function normalizeDate(value?: string | null): string | null {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function artifactMatchesPreset(artifact: HmmCoefficientArtifact, preset: string): boolean {
  return !artifact.parse_error && artifact.preset === preset && hasPath(artifact);
}

function artifactCoversWithBounds(artifact: HmmCoefficientArtifact, startDate: string, endDate: string): boolean {
  const start = normalizeDate(artifact.start_date);
  const end = normalizeDate(artifact.end_date);
  return Boolean(start && end && start <= startDate && endDate <= end);
}

function artifactCoversKnownDates(artifact: HmmCoefficientArtifact, startDate: string, endDate: string): boolean {
  const dates = Array.from(new Set((artifact.covered_trade_dates || []).map(normalizeDate).filter(Boolean) as string[])).sort();
  if (!dates.length) return artifactCoversWithBounds(artifact, startDate, endDate);
  if (startDate === endDate) return dates.includes(startDate);
  const coveredInRange = dates.filter((item) => startDate <= item && item <= endDate);
  if (!coveredInRange.length) return false;
  return coveredInRange[0] <= startDate && coveredInRange[coveredInRange.length - 1] >= endDate;
}

export function artifactCoversDateRange(
  snapshot: HmmSnapshot,
  preset: string,
  startDate: string,
  endDate?: string | null,
): HmmCoefficientArtifact | null {
  const start = normalizeDate(startDate);
  const end = normalizeDate(endDate) || start;
  if (!start || !end || start > end) return null;
  return (snapshot.coefficient_artifacts || []).find((artifact) => (
    artifactMatchesPreset(artifact, preset) && artifactCoversKnownDates(artifact, start, end)
  )) || null;
}

export function artifactCoversTradeDate(
  snapshot: HmmSnapshot,
  preset: string,
  tradeDate: string,
): HmmCoefficientArtifact | null {
  return artifactCoversDateRange(snapshot, preset, tradeDate, tradeDate);
}

export function artifactCoverageLabel(snapshot: HmmSnapshot, preset: string): string {
  const artifacts = (snapshot.coefficient_artifacts || []).filter((artifact) => (
    artifact.preset === preset && !artifact.parse_error && hasPath(artifact)
  ));
  if (!artifacts.length) return `${preset} has no usable coefficient artifact`;
  return artifacts.map((artifact) => {
    const dateCount = artifact.covered_trade_dates?.length || artifact.date_count || 0;
    return `${artifact.start_date || "?"}~${artifact.end_date || "?"} (${dateCount} trading days)`;
  }).join("; ");
}

export function selectCoveredHmmSnapshot(
  snapshots: HmmSnapshot[],
  preset: string,
  startDate: string,
  endDate?: string | null,
): HmmSnapshot | null {
  return snapshots.find((snapshot) => artifactCoversDateRange(snapshot, preset, startDate, endDate)) || null;
}
