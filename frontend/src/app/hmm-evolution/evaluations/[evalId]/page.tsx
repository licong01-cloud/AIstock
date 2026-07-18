import EvaluationDetailView from "@/components/hmm-evolution/EvaluationDetailView";

export default function HMMEvaluationDetailPage({ params }: { params: { evalId: string } }) {
  return <EvaluationDetailView evalId={params.evalId} />;
}
