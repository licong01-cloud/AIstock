import { redirect } from "next/navigation";

export default function ResearchAssistantModelsRedirectPage() {
  redirect("/research-assistant/settings?tab=models");
}
