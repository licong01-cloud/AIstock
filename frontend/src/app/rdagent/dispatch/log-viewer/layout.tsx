/**
 * log-viewer 独立布局 — 不显示全局侧边栏。
 * 该页面通过 window.open() 在新窗口打开，应为全屏终端风格。
 */
export default function LogViewerLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
