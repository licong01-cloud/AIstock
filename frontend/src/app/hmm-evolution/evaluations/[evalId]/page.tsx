import { notFound } from "next/navigation";
import EvaluationDetailView from "@/components/hmm-evolution/EvaluationDetailView";

export default function HMMEvaluationDetailPage({ params }: { params: { evalId: string } }) {
  if (process.env.NEXT_PUBLIC_HMM_EVOLUTION_ENABLED !== "true") notFound();
  return <EvaluationDetailView evalId={params.evalId} />;
}
