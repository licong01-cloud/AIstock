import { notFound } from "next/navigation";
import BatchDetailView from "@/components/hmm-evolution/BatchDetailView";

export default function HMMBatchDetailPage({ params }: { params: { batchId: string } }) {
  if (process.env.NEXT_PUBLIC_HMM_EVOLUTION_ENABLED !== "true") notFound();
  return <BatchDetailView batchId={params.batchId} />;
}
