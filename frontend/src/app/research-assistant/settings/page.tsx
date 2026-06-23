import Link from "next/link";

import { HealthSection } from "@/components/research-assistant/HealthSection";
import { McpCatalogSection } from "@/components/research-assistant/McpCatalogSection";
import { ModelRoutingSection } from "@/components/research-assistant/ModelRoutingSection";
import { SkillsSection } from "@/components/research-assistant/SkillsSection";

type SettingsTabKey = "mcp" | "skills" | "models" | "health";

const SETTINGS_TABS: Array<{ key: SettingsTabKey; label: string; href: string }> = [
  { key: "mcp", label: "MCP目录", href: "/research-assistant/settings?tab=mcp" },
  { key: "skills", label: "技能", href: "/research-assistant/settings?tab=skills" },
  { key: "models", label: "模型路由", href: "/research-assistant/settings?tab=models" },
  { key: "health", label: "健康", href: "/research-assistant/settings?tab=health" },
];

function normalizeTab(value: string | string[] | undefined): SettingsTabKey {
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === "skills" || raw === "models" || raw === "health") return raw;
  return "mcp";
}

function renderSettingsSection(tab: SettingsTabKey) {
  if (tab === "skills") return <SkillsSection />;
  if (tab === "models") return <ModelRoutingSection />;
  if (tab === "health") return <HealthSection />;
  return <McpCatalogSection />;
}

export default function ResearchAssistantSettingsPage({
  searchParams,
}: {
  searchParams?: { tab?: string | string[] };
}) {
  const activeTab = normalizeTab(searchParams?.tab);
  return (
    <main>
      <section className="ra-mcp-section" aria-labelledby="ra-settings-title">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">settings</span>
            <h2 id="ra-settings-title">研究助理设置</h2>
            <p>统一承载 MCP 目录、技能、本地模型路由和健康检查；旧配置路由会重定向到对应标签页。</p>
          </div>
        </div>
        <nav className="ra-tabs" aria-label="研究助理设置标签">
          {SETTINGS_TABS.map((tab) => (
            <Link className={`ra-tab ${activeTab === tab.key ? "ra-tab-active" : ""}`} href={tab.href} key={tab.key}>
              {tab.label}
            </Link>
          ))}
        </nav>
      </section>
      {renderSettingsSection(activeTab)}
    </main>
  );
}
