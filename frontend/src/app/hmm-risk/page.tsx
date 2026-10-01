import Link from "next/link";
import RotationL2Dashboard from "@/components/hmm-risk/RotationL2Dashboard";
import RotationL1Dashboard from "@/components/hmm-risk/RotationL1Dashboard";

interface HMMRiskPageProps {
  searchParams?: { level?: string | string[] };
}

export default function HMMRiskPage({ searchParams }: HMMRiskPageProps) {
  const level = Array.isArray(searchParams?.level) ? searchParams.level[0] : searchParams?.level;
  const legacyL1 = level === "l1";
  return (
    <>
      <nav aria-label="HMM 行业层级版本">
        <Link href="/hmm-risk">L2 主线</Link>{" · "}
        <Link href="/hmm-risk?level=l1">L1 历史版本</Link>
      </nav>
      {legacyL1 ? <RotationL1Dashboard /> : <RotationL2Dashboard />}
    </>
  );
}
