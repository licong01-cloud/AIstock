import Link from "next/link";
import RotationL2Dashboard from "@/components/hmm-risk/RotationL2Dashboard";
import RotationL1Dashboard from "@/components/hmm-risk/RotationL1Dashboard";
import RiskL2Panel from "@/components/hmm-risk/RiskL2Panel";

interface HMMRiskPageProps {
  searchParams?: { level?: string | string[]; view?: string | string[]; risk_run_id?: string | string[] };
}

export default function HMMRiskPage({ searchParams }: HMMRiskPageProps) {
  const level = Array.isArray(searchParams?.level) ? searchParams.level[0] : searchParams?.level;
  const legacyL1 = level === "l1";
  const view = Array.isArray(searchParams?.view) ? searchParams.view[0] : searchParams?.view;
  const riskRunId = Array.isArray(searchParams?.risk_run_id) ? searchParams.risk_run_id[0] : searchParams?.risk_run_id;
  return (
    <>
      <nav aria-label="HMM 行业层级版本">
        <Link href="/hmm-risk">L2 主线</Link>{" · "}
        <Link href="/hmm-risk?view=risk-l2">L2 风险研究</Link>{" · "}
        <Link href="/hmm-risk?level=l1">L1 历史版本</Link>
      </nav>
      {view === "risk-l2" ? <RiskL2Panel key={riskRunId} initialRunId={riskRunId} /> : legacyL1 ? <RotationL1Dashboard /> : <RotationL2Dashboard />}
    </>
  );
}
