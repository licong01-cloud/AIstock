import { redirect } from "next/navigation";

export default function ResearchAssistantExternalAgentsRedirectPage() {
  redirect("/research-assistant/audit?tab=external-agents");
}
