export type NavItem = {
  href: string;
  label: string;
};

export type NavGroup = {
  title: string;
  items: NavItem[];
};

export const NAV_GROUPS: NavGroup[] = [
  {
    title: "QuantEvolver",
    items: [
      { href: "/quantevolver", label: "\u{1F9EC} QE \u603B\u89C8" },
      { href: "/quantevolver/factors", label: "\u{1F4CA} \u56E0\u5B50\u5E93" },
      { href: "/quantevolver/factor-correlation", label: "\u{1F517} \u56E0\u5B50\u76F8\u5173\u6027" },
      { href: "/quantevolver/models", label: "\u{1F9E0} \u6A21\u578B\u5E93" },
      { href: "/quantevolver/strategies", label: "\u{1F3AF} \u7B56\u7565\u5E93" },
      { href: "/quantevolver/execution-algorithms", label: "\u26A1 \u6267\u884C\u7B97\u6CD5\u5E93" },
      { href: "/quantevolver/compose", label: "\u{1F527} \u7EC4\u5408\u914D\u7F6E" },
      { href: "/quantevolver/experiments", label: "\u{1F9EA} \u5B9E\u9A8C\u5386\u53F2" },
      { href: "/quantevolver/templates", label: "\u{1F4CB} QE \u5F85\u6267\u884C\u5B9E\u9A8C" },
      { href: "/qe-archive", label: "\u{1F5C4}\uFE0F QE \u5B9E\u9A8C\u6570\u4ED3" },
      { href: "/quantevolver/evolution", label: "\u{1F501} \u81EA\u52A8\u6F14\u8FDB" },
      { href: "/quantevolver/multi-alpha/diagnostics", label: "\u{1F52E} \u591AAlpha \u8BCA\u65AD" },
      { href: "/quantevolver/multi-alpha/evolve-wizard", label: "\u{1F9EC} \u591AAlpha \u6F14\u8FDB" },
      { href: "/quantevolver/evolution/sota", label: "\u{1F4E6} \u5019\u9009\u7B56\u7565\u5305" },
      { href: "/quantevolver/model-training", label: "\u{1F9E0} \u6A21\u578B\u8BAD\u7EC3" },
      { href: "/quantevolver/prompts", label: "\u{1F4DD} \u63D0\u793A\u8BCD\u914D\u7F6E" },
    ],
  },
  {
    title: "RD-Agent\u7BA1\u7406",
    items: [
      { href: "/rdagent/dispatch", label: "\u{1F5A5}\uFE0F \u591A\u8282\u70B9\u8C03\u5EA6\u4E2D\u5FC3" },
      { href: "/qlib", label: "\u{1F4E6} Qlib Snapshot \u5BFC\u51FA" },
      { href: "/scheduler", label: "\u{1F5D3}\uFE0F RD-Agent \u8C03\u5EA6" },
      { href: "/rdagent/tasks-sync", label: "\u{1F9F0} Task \u8D44\u4EA7\u540C\u6B65" },
      { href: "/rdagent/tasks", label: "\u{1F5C2}\uFE0F Task \u5217\u8868" },
      { href: "/rdagent/task-selection", label: "\u{1F680} Task \u9009\u80A1" },
      { href: "/config/rdagent-llm", label: "\u{1F916} RDagent \u6A21\u578B\u914D\u7F6E" },
    ],
  },
  {
    title: "\u81EA\u52A8\u5316\u6D41\u6C34\u7EBF",
    items: [
      { href: "/validation-center", label: "\u{1F9EA} \u6D41\u6C34\u7EBF\u4E2D\u5FC3" },
      { href: "/research-pipeline", label: "Research Pipeline" },
      { href: "/research-assistant", label: "\u{1F9E0} \u7814\u7A76\u52A9\u7406" },
    ],
  },
  {
    title: "\u7CFB\u7EDF\u4E0E\u6570\u636E",
    items: [
      { href: "/config", label: "\u2699\uFE0F \u73AF\u5883\u914D\u7F6E" },
      { href: "/local-data", label: "\u{1F5C4}\uFE0F \u672C\u5730\u6570\u636E\u7BA1\u7406" },
      { href: "/rdagent/dispatch/system-monitor", label: "\u{1F4E1} \u7CFB\u7EDF\u76D1\u63A7" },
      { href: "/rdagent/dispatch/db-monitor", label: "\u{1F5C4}\uFE0F \u6570\u636E\u5E93\u76D1\u63A7" },
    ],
  },
  {
    title: "\u{1F50D} \u529F\u80FD\u5BFC\u822A",
    items: [
      { href: "/analysis", label: "\u{1F3E0} \u80A1\u7968\u5206\u6790" },
      { href: "/analysis-trend", label: "\u{1F4C8} \u8D8B\u52BF\u5206\u6790" },
    ],
  },
  {
    title: "\u{1F3AF} \u9009\u80A1\u677F\u5757",
    items: [
      { href: "/watchlist", label: "\u2B50 \u81EA\u9009\u80A1\u7968\u6C60" },
      { href: "/cloud-screening", label: "\u2601 \u4E91\u9009\u80A1" },
      { href: "/market-news", label: "\u{1F4F0} \u5E02\u573A\u8D44\u8BAF / \u5E02\u573A\u5FEB\u8BAF" },
    ],
  },
  {
    title: "\u{1F4CA} \u7B56\u7565\u5206\u6790",
    items: [
      { href: "/sector-strategy", label: "\u{1F3AF} \u667A\u7B56\u677F\u5757" },
    ],
  },
  {
    title: "\u{1F4BC} \u6295\u8D44\u7BA1\u7406",
    items: [
      { href: "/portfolio", label: "\u{1F4CA} \u6301\u4ED3\u5206\u6790" },
      { href: "/smart-monitor", label: "\u{1F916} AI\u76EF\u76D8" },
      { href: "/monitor", label: "\u{1F4E1} \u5B9E\u65F6\u76D1\u6D4B" },
    ],
  },
  {
    title: "\u{1F4C8} QMT\u6A21\u62DF\u76D8\u4EA4\u6613",
    items: [
      { href: "/qmt/positions", label: "\u{1F4BC} \u6301\u4ED3\u7BA1\u7406" },
      { href: "/qmt/strategies", label: "\u{1F4CA} \u7B56\u7565\u7BA1\u7406" },
      { href: "/qmt/virtual-strategies", label: "\u{1F9EE} \u865A\u62DF\u7B56\u7565\u5206\u4ED3" },
    ],
  },
  {
    title: "\u{1F9EA} Paper Trading v2",
    items: [
      { href: "/paper-v2", label: "\u{1F4CC} V2 \u603B\u89C8" },
      { href: "/paper-v2/packages", label: "\u{1F4E6} \u7B56\u7565\u5305\u4E2D\u5FC3" },
      { href: "/strategy-package-governance", label: "\u{1F6E1}\uFE0F \u7B56\u7565\u5305\u6CBB\u7406" },
      { href: "/market-regime", label: "\u{1F30D} \u5E02\u573A\u72B6\u6001" },
      { href: "/rl-execution", label: "\u{1F916} RL \u6267\u884C\u6A21\u578B" },
      { href: "/paper-v2/selection", label: "\u{1F3AF} \u7EDF\u4E00\u9009\u80A1\u4E2D\u5FC3" },
      { href: "/paper-v2/portfolios", label: "\u{1F4BC} \u6A21\u62DF\u76D8\u7EC4\u5408" },
      { href: "/paper-v2/model-hmm", label: "\u{1F9E0} \u6A21\u578B\u4E0E HMM" },
    ],
  },
];
