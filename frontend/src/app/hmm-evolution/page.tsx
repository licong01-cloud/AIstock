import { notFound } from "next/navigation";
import EvolutionDashboard from "@/components/hmm-evolution/EvolutionDashboard";

export default function HMMEvolutionPage() {
  if (process.env.NEXT_PUBLIC_HMM_EVOLUTION_ENABLED !== "true") {
    notFound();
  }
  return <EvolutionDashboard />;
}
