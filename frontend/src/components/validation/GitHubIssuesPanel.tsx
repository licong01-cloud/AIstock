"use client";

import SectionCard from "@/components/paper-v2/SectionCard";
import type { ValidationBug, ValidationBugSummary } from "@/lib/validation/api";

const CLOSED_BUG_STATUSES = new Set(["fixed", "closed", "resolved", "verified"]);
const AISTOCK_BUG_LABEL = "aistock:bug";

type Props = {
  bugSummary?: ValidationBugSummary | null;
  bugs?: ValidationBug[];
};

type IssueLinkState = {
  href: string | null;
  issueBase: string | null;
  source: "env" | "bug-link" | "missing";
};

type ModuleIssueBucket = {
  module: string;
  localCount: number;
  openLinkedCount: number;
  linkedCount: number;
  bugs: ValidationBug[];
};

function normalizeRepo(value: string | undefined): string | null {
  const trimmed = (value || "").trim();
  if (!trimmed) return null;
  const withoutGithub = trimmed
    .replace(/^https?:\/\/github\.com\//i, "")
    .replace(/^github\.com\//i, "")
    .replace(/\/issues(?:\/.*)?$/i, "")
    .replace(/\.git$/i, "")
    .replace(/^\/+|\/+$/g, "");
  const [owner, repo] = withoutGithub.split("/");
  if (!owner || !repo) return null;
  return `${owner}/${repo}`;
}

function issueBaseFromRepo(repo: string | undefined): string | null {
  const normalized = normalizeRepo(repo);
  return normalized ? `https://github.com/${normalized}/issues` : null;
}

function cleanIssuesUrl(value: string | undefined): string | null {
  const trimmed = (value || "").trim();
  if (!trimmed) return null;
  try {
    const url = new URL(trimmed);
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
  } catch {
    return null;
  }
}

function issueBaseFromBugUrl(value: string | null | undefined): string | null {
  if (!value) return null;
  try {
    const url = new URL(value);
    const parts = url.pathname.split("/").filter(Boolean);
    if (url.hostname !== "github.com" || parts.length < 3 || parts[2] !== "issues") return null;
    return `${url.origin}/${parts[0]}/${parts[1]}/issues`;
  } catch {
    return null;
  }
}

function isClosedStatus(status: string | null | undefined): boolean {
  return CLOSED_BUG_STATUSES.has(String(status || "").toLowerCase());
}

function mergeBugs(summary: ValidationBugSummary | null | undefined, bugs: ValidationBug[] | undefined): ValidationBug[] {
  const merged = new Map<string, ValidationBug>();
  for (const bug of [...(summary?.latest_bugs || []), ...(bugs || [])]) {
    if (!bug?.bug_id) continue;
    merged.set(bug.bug_id, { ...(merged.get(bug.bug_id) || {}), ...bug });
  }
  return [...merged.values()];
}

function linkedIssueUrl(bug: ValidationBug, issueBase: string | null): string | null {
  if (bug.github_issue_url) return bug.github_issue_url;
  if (bug.github_issue_number && issueBase) return `${issueBase}/${bug.github_issue_number}`;
  return null;
}

function hasIssueReference(bug: ValidationBug): boolean {
  return Boolean(bug.github_issue_url || bug.github_issue_number);
}

function issueSearchUrl(issueBase: string, query: string): string {
  const url = new URL(issueBase);
  url.searchParams.set("q", query);
  return url.toString();
}

function resolveIssueLinkState(bugs: ValidationBug[]): IssueLinkState {
  const envIssuesUrl = cleanIssuesUrl(process.env.NEXT_PUBLIC_GITHUB_ISSUES_URL);
  if (envIssuesUrl) return { href: envIssuesUrl, issueBase: envIssuesUrl, source: "env" };

  const repoIssueBase = issueBaseFromRepo(process.env.NEXT_PUBLIC_GITHUB_REPOSITORY || process.env.NEXT_PUBLIC_GITHUB_REPO);
  if (repoIssueBase) return { href: repoIssueBase, issueBase: repoIssueBase, source: "env" };

  const inferredBase = bugs.map((bug) => issueBaseFromBugUrl(bug.github_issue_url)).find(Boolean) || null;
  if (inferredBase) return { href: inferredBase, issueBase: inferredBase, source: "bug-link" };

  return { href: null, issueBase: null, source: "missing" };
}

function openBugCount(summary: ValidationBugSummary | null | undefined, bugs: ValidationBug[]): number {
  const byStatus = Object.entries(summary?.by_status || {});
  if (byStatus.length) {
    return byStatus.reduce((total, [status, count]) => total + (isClosedStatus(status) ? 0 : Number(count || 0)), 0);
  }
  return bugs.filter((bug) => !isClosedStatus(bug.status)).length;
}

function moduleBuckets(summary: ValidationBugSummary | null | undefined, bugs: ValidationBug[]): ModuleIssueBucket[] {
  const buckets = new Map<string, ModuleIssueBucket>();
  for (const [module, count] of Object.entries(summary?.by_module || {})) {
    buckets.set(module || "unknown", { module: module || "unknown", localCount: Number(count || 0), openLinkedCount: 0, linkedCount: 0, bugs: [] });
  }

  for (const bug of bugs) {
    const module = bug.module || "unknown";
    const bucket = buckets.get(module) || { module, localCount: summary?.by_module ? 0 : 0, openLinkedCount: 0, linkedCount: 0, bugs: [] };
    if (!summary?.by_module) bucket.localCount += 1;
    if (hasIssueReference(bug)) {
      bucket.linkedCount += 1;
      if (!isClosedStatus(bug.status)) bucket.openLinkedCount += 1;
      bucket.bugs.push(bug);
    }
    buckets.set(module, bucket);
  }

  return [...buckets.values()].sort((a, b) => (
    b.openLinkedCount - a.openLinkedCount ||
    b.linkedCount - a.linkedCount ||
    b.localCount - a.localCount ||
    a.module.localeCompare(b.module)
  ));
}

export default function GitHubIssuesPanel({ bugSummary, bugs = [] }: Props) {
  const mergedBugs = mergeBugs(bugSummary, bugs);
  const issueLinkState = resolveIssueLinkState(mergedBugs);
  const openRegistryCount = openBugCount(bugSummary, mergedBugs);
  const linkedCount = mergedBugs.filter(hasIssueReference).length;
  const openLinkedCount = mergedBugs.filter((bug) => hasIssueReference(bug) && !isClosedStatus(bug.status)).length;
  const buckets = moduleBuckets(bugSummary, mergedBugs);
  const openIssuesHref = issueLinkState.href
    ? issueSearchUrl(issueLinkState.href, `is:issue is:open label:"${AISTOCK_BUG_LABEL}"`)
    : null;

  return (
    <SectionCard
      title="GitHub Issues Overlay"
      eyebrow="additive workflow layer / bugs JSON remains source of truth"
      action={
        openIssuesHref ? (
          <a
            className="pv2-button-primary"
            href={openIssuesHref}
            rel="noreferrer"
            style={{ alignItems: "center", display: "inline-flex", textDecoration: "none" }}
            target="_blank"
          >
            View on GitHub Issues
          </a>
        ) : (
          <button className="pv2-button-ghost" disabled type="button">View on GitHub Issues</button>
        )
      }
    >
      <div className="pv2-grid pv2-grid-4">
        <div className="pv2-notice pv2-notice-info">
          <div className="pv2-notice-title">Open Registry Bugs</div>
          <div className="pv2-notice-body"><span className="pv2-badge pv2-badge-info">{openRegistryCount}</span></div>
        </div>
        <div className="pv2-notice pv2-notice-success">
          <div className="pv2-notice-title">Linked Issues</div>
          <div className="pv2-notice-body"><span className="pv2-badge pv2-badge-success">{linkedCount}</span></div>
        </div>
        <div className="pv2-notice pv2-notice-warning">
          <div className="pv2-notice-title">Open Linked Issues</div>
          <div className="pv2-notice-body"><span className="pv2-badge pv2-badge-warning">{openLinkedCount}</span></div>
        </div>
        <div className="pv2-notice pv2-notice-info">
          <div className="pv2-notice-title">Repo Source</div>
          <div className="pv2-notice-body">{issueLinkState.source === "missing" ? "not configured" : issueLinkState.source}</div>
        </div>
      </div>

      {issueLinkState.source === "missing" ? (
        <div className="pv2-notice pv2-notice-warning">
          <div className="pv2-notice-title">GitHub repo unavailable</div>
          <div className="pv2-notice-body">
            Set NEXT_PUBLIC_GITHUB_REPOSITORY or sync github_issue_url/github_issue_number into the bug registry to enable the GitHub button. The local bugs JSON counts still render normally.
          </div>
        </div>
      ) : null}

      <div className="pv2-table-wrap">
        <table className="pv2-table">
          <thead>
            <tr><th>Module</th><th>Local Bugs</th><th>Open Linked Issues</th><th>Issue Links</th></tr>
          </thead>
          <tbody>
            {buckets.length ? buckets.slice(0, 12).map((bucket) => (
              <tr key={bucket.module}>
                <td><strong>{bucket.module}</strong></td>
                <td><span className="pv2-badge pv2-badge-neutral">{bucket.localCount}</span></td>
                <td><span className={bucket.openLinkedCount ? "pv2-badge pv2-badge-warning" : "pv2-badge pv2-badge-success"}>{bucket.openLinkedCount}</span></td>
                <td>
                  {bucket.bugs.length ? (
                    <div className="pv2-readable-list">
                      {bucket.bugs.slice(0, 3).map((bug) => {
                        const href = linkedIssueUrl(bug, issueLinkState.issueBase);
                        return href ? (
                          <a className="pv2-link-button" href={href} key={bug.bug_id} rel="noreferrer" target="_blank">
                            {bug.bug_id}{bug.github_issue_number ? ` #${bug.github_issue_number}` : ""}
                          </a>
                        ) : (
                          <span className="pv2-muted" key={bug.bug_id}>
                            {bug.bug_id}{bug.github_issue_number ? ` #${bug.github_issue_number}` : ""} (repo unavailable)
                          </span>
                        );
                      })}
                    </div>
                  ) : (
                    <span className="pv2-muted">No synced GitHub issue link yet</span>
                  )}
                </td>
              </tr>
            )) : (
              <tr><td className="pv2-empty-cell" colSpan={4}>No bug registry data loaded yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </SectionCard>
  );
}
