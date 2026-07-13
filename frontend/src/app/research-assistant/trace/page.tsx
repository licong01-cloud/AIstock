import { redirect } from "next/navigation";

export default function ResearchAssistantTraceRedirectPage() {
  redirect("/research-assistant/audit?tab=trace");
}
