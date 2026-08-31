import BatchDetailView from "@/components/hmm-evolution/BatchDetailView";

export default function HMMBatchDetailPage({ params }: { params: { batchId: string } }) {
  return <BatchDetailView batchId={params.batchId} />;
}
