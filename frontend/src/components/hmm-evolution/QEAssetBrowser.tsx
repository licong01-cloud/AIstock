"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ChevronLeft,
  ChevronRight,
  Database,
  FileSearch,
  Search,
  ShieldCheck,
} from "lucide-react";
import {
  HMMApiError,
  readQEAssetText,
  statQEAsset,
} from "@/lib/hmm-evolution/api";
import type {
  QEAssetCatalog,
  QEAssetEntry,
  QEAssetTextContent,
} from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import styles from "@/components/hmm-research/hmm-research.module.css";

const PAGE_SIZE = 50;
const TEXT_ASSET_RE = /\.(txt|log|md|csv|json|ya?ml)$/i;

export default function QEAssetBrowser({
  taskId,
  loopName,
  setTaskId,
  setLoopName,
  catalog,
  error,
  loading,
  onLoad,
}: {
  taskId: string;
  loopName: string;
  setTaskId: (value: string) => void;
  setLoopName: (value: string) => void;
  catalog: QEAssetCatalog | null;
  error: unknown;
  loading: boolean;
  onLoad: () => void;
}) {
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const [selectedAsset, setSelectedAsset] = useState<QEAssetEntry | null>(null);
  const [content, setContent] = useState<QEAssetTextContent | null>(null);
  const [inspectionError, setInspectionError] = useState<unknown>(null);
  const [inspectionLoading, setInspectionLoading] = useState(false);

  useEffect(() => {
    setPage(0);
    setSelectedAsset(null);
    setContent(null);
    setInspectionError(null);
  }, [catalog]);

  const filtered = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!catalog) return [];
    if (!normalized) return catalog.assets;
    return catalog.assets.filter((asset) =>
      [
        asset.relative_path,
        asset.content_type,
        asset.source,
        asset.schema_version,
        asset.parser_contract,
      ]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalized)),
    );
  }, [catalog, query]);
  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const pageRows = filtered.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  const inspect = async (asset: QEAssetEntry) => {
    setInspectionError(null);
    setContent(null);
    setInspectionLoading(true);
    try {
      const stat = await statQEAsset(taskId.trim(), loopName.trim(), asset.relative_path);
      setSelectedAsset(stat);
      if (!isTextAsset(stat)) return;
      try {
        setContent(await readQEAssetText(taskId.trim(), loopName.trim(), stat.relative_path));
      } catch (nextError) {
        if (
          nextError instanceof HMMApiError
          && nextError.reasonCode === "hmm_evolution_qe_asset_too_large"
        ) {
          setContent(
            await readQEAssetText(taskId.trim(), loopName.trim(), stat.relative_path, {
              start: 0,
              end: Math.min(stat.size_bytes - 1, 64 * 1024 - 1),
            }),
          );
        } else {
          throw nextError;
        }
      }
    } catch (nextError) {
      setInspectionError(nextError);
    } finally {
      setInspectionLoading(false);
    }
  };

  return (
    <section className={`${styles.panel} ${styles.fullWidth}`}>
      <div className={styles.panelHeader}>
        <div>
          <h2 className={styles.panelTitle}>QE 资产浏览</h2>
          <div className={styles.panelSubtitle}>
            完整目录、来源、信任级别与 schema-aware 摘要；只读检查，不执行、不导入、不展示原始 JSON。
          </div>
        </div>
        <span className={`${styles.tag} ${styles.tagInfo}`}>
          <Database size={13} /> Inspection only
        </span>
      </div>
      <div className={styles.panelBody}>
        <div className={styles.assetGrid}>
          <input
            className={styles.input}
            placeholder="QE task id"
            value={taskId}
            onChange={(event) => setTaskId(event.target.value)}
          />
          <input
            className={styles.input}
            placeholder="Loop8"
            value={loopName}
            onChange={(event) => setLoopName(event.target.value)}
          />
          <button
            type="button"
            className={`${styles.button} ${styles.buttonSoft}`}
            onClick={onLoad}
            disabled={loading}
          >
            <FileSearch size={14} /> {loading ? "读取中" : "读取完整目录"}
          </button>
        </div>

        {error ? (
          <div style={{ marginTop: 14 }}>
            <VisibleErrorState error={error} title="QE 资产读取失败" onRetry={onLoad} />
          </div>
        ) : null}

        {catalog ? (
          <>
            <div className={styles.assetSummary} style={{ marginTop: 14 }}>
              <SummaryCell label="目录完整性" value={catalog.catalog_completeness} />
              <SummaryCell label="完整资产数" value={String(catalog.assets.length)} />
              <SummaryCell label="当前搜索结果" value={String(filtered.length)} />
              <SummaryCell label="Warnings" value={String(catalog.warnings.length)} />
            </div>
            <div className={styles.assetToolbar}>
              <label className={styles.assetSearch}>
                <Search size={14} aria-hidden="true" />
                <input
                  value={query}
                  onChange={(event) => {
                    setQuery(event.target.value);
                    setPage(0);
                  }}
                  placeholder="搜索路径、source、schema 或 parser"
                  aria-label="搜索 QE 资产"
                />
              </label>
              <span className={styles.muted}>
                第 {safePage + 1} / {pageCount} 页 · 每页 {PAGE_SIZE} 项
              </span>
            </div>
            <div className={styles.panelBodyTable}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>相对路径</th><th>类型</th><th>大小</th><th>Source</th><th>Trust</th>
                    <th>Parser / Schema</th><th>更新时间</th><th>查看</th>
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((asset) => (
                    <tr key={asset.relative_path}>
                      <td className={styles.hash}>{asset.relative_path}</td>
                      <td>{asset.content_type || "未知"}</td>
                      <td>{formatBytes(asset.size_bytes)}</td>
                      <td>{asset.source || "未声明"}</td>
                      <td>
                        <span className={`${styles.tag} ${asset.trust_level === "trusted_computational_input" ? styles.tagGood : styles.tagWarn}`}>
                          {asset.trust_level === "trusted_computational_input" ? "可信输入" : "未验证证据"}
                        </span>
                      </td>
                      <td>{asset.parser_contract || asset.schema_version || "未声明"}</td>
                      <td>{formatDateTime(asset.modified_at)}</td>
                      <td>
                        <button type="button" className={styles.button} onClick={() => void inspect(asset)}>
                          检查
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {pageRows.length === 0 ? (
                <div className={styles.emptyState}>没有匹配资产；完整目录仍保留，可清空搜索条件。</div>
              ) : null}
            </div>
            <div className={styles.paginationBar}>
              <button
                type="button"
                className={styles.button}
                disabled={safePage === 0}
                onClick={() => setPage(Math.max(0, safePage - 1))}
              >
                <ChevronLeft size={14} /> 上一页
              </button>
              <button
                type="button"
                className={styles.button}
                disabled={safePage >= pageCount - 1}
                onClick={() => setPage(Math.min(pageCount - 1, safePage + 1))}
              >
                下一页 <ChevronRight size={14} />
              </button>
            </div>
          </>
        ) : (
          <div className={styles.emptyState} style={{ marginTop: 14 }}>
            尚未读取 QE 资产。输入 task 和 loop 后读取权威节点上的完整只读目录。
          </div>
        )}

        {inspectionError ? (
          <div style={{ marginTop: 16 }}>
            <VisibleErrorState
              error={inspectionError}
              title="资产内容检查失败"
              onRetry={() => selectedAsset && void inspect(selectedAsset)}
            />
          </div>
        ) : null}
        {inspectionLoading ? <div className={styles.loadingState}>正在校验资产元数据与安全内容摘要…</div> : null}
        {selectedAsset ? <AssetInspection asset={selectedAsset} content={content} /> : null}
      </div>
    </section>
  );
}

function AssetInspection({
  asset,
  content,
}: {
  asset: QEAssetEntry;
  content: QEAssetTextContent | null;
}) {
  const schema = inferSchema(asset);
  return (
    <div className={styles.assetInspection}>
      <div className={styles.detailGrid}>
        <div className={styles.panel}>
          <div className={styles.panelHeader}><h3 className={styles.panelTitle}>资产身份</h3></div>
          <div className={styles.panelBody}>
            <EvidencePanel sections={[{ title: "来源与完整性", rows: [
              { label: "相对路径", value: asset.relative_path },
              { label: "Source", value: asset.source || "未声明" },
              { label: "媒体类型", value: asset.content_type || "未知" },
              { label: "大小", value: formatBytes(asset.size_bytes) },
              { label: "SHA256", value: asset.sha256 ? shortHash(asset.sha256) : "未声明" },
              { label: "更新时间", value: formatDateTime(asset.modified_at) },
            ] }]} />
          </div>
        </div>
        <div className={styles.panel}>
          <div className={styles.panelHeader}><h3 className={styles.panelTitle}>Schema 与信任边界</h3></div>
          <div className={styles.panelBody}>
            <EvidencePanel sections={[{ title: schema.title, rows: [
              { label: "Trust level", value: asset.trust_level },
              { label: "Access mode", value: asset.access_mode },
              { label: "Parser contract", value: asset.parser_contract || "未声明" },
              { label: "Schema version", value: asset.schema_version || "未声明" },
              { label: "预期字段", value: schema.fields.join(" · ") || "通用文本证据" },
              { label: "内容策略", value: isTextAsset(asset) ? "安全脱敏后的有界文本" : "仅元数据，不返回原始二进制" },
            ] }]} />
          </div>
        </div>
      </div>
      {content ? <ContentSummary asset={asset} content={content} /> : null}
      {!content && !isTextAsset(asset) ? (
        <div className={styles.notice}>
          <ShieldCheck size={15} />
          <span>该资产为结构化二进制或未知格式；页面只展示 schema/哈希/来源，不下载、不反序列化、不回显原始字节。</span>
        </div>
      ) : null}
    </div>
  );
}

function ContentSummary({ asset, content }: { asset: QEAssetEntry; content: QEAssetTextContent }) {
  if (content.schema_kind === "json") {
    const parsed = JSON.parse(content.text) as unknown;
    const rows = summarizeJson(parsed);
    return (
      <section className={styles.assetContentCard}>
        <ContentHeader asset={asset} content={content} title="JSON 结构摘要" />
        <div className={styles.schemaGrid}>
          {rows.map((row) => (
            <div className={styles.summaryCell} key={row.label}>
              <div className={styles.summaryLabel}>{row.label}</div>
              <div className={styles.summaryValue}>{row.value}</div>
            </div>
          ))}
        </div>
      </section>
    );
  }
  if (/\.csv$/i.test(asset.relative_path)) {
    const lines = content.text.split(/\r?\n/).filter(Boolean);
    const headers = (lines[0] || "").split(",");
    const rows = lines.slice(1, 9).map((line) => line.split(","));
    return (
      <section className={styles.assetContentCard}>
        <ContentHeader asset={asset} content={content} title="CSV 字段与样本" />
        <div className={styles.panelBodyTable}>
          <table className={styles.table}>
            <thead><tr>{headers.map((header, index) => <th key={`${header}-${index}`}>{header}</th>)}</tr></thead>
            <tbody>{rows.map((row, rowIndex) => <tr key={rowIndex}>{headers.map((_, index) => <td key={index}>{row[index] || "—"}</td>)}</tr>)}</tbody>
          </table>
        </div>
      </section>
    );
  }
  return (
    <section className={styles.assetContentCard}>
      <ContentHeader asset={asset} content={content} title="脱敏文本预览" />
      <div className={styles.textPreview}>
        {content.text.split(/\r?\n/).slice(0, 200).map((line, index) => (
          <div className={styles.textLine} key={`${index}-${line.slice(0, 20)}`}>
            <span>{index + 1}</span><code>{line || " "}</code>
          </div>
        ))}
      </div>
    </section>
  );
}

function ContentHeader({
  asset,
  content,
  title,
}: {
  asset: QEAssetEntry;
  content: QEAssetTextContent;
  title: string;
}) {
  return (
    <div className={styles.panelHeader}>
      <div><h3 className={styles.panelTitle}>{title}</h3><div className={styles.panelSubtitle}>{asset.relative_path}</div></div>
      <span className={`${styles.tag} ${content.redaction_count > 0 ? styles.tagWarn : styles.tagGood}`}>
        脱敏 {content.redaction_count} 项{content.range ? " · 首段有界预览" : ""}
      </span>
    </div>
  );
}

function summarizeJson(value: unknown): Array<{ label: string; value: string }> {
  if (Array.isArray(value)) {
    return [
      { label: "顶层类型", value: "array" },
      { label: "元素数量", value: String(value.length) },
      { label: "首项类型", value: value.length ? describeValue(value[0]) : "空数组" },
    ];
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    return [
      { label: "顶层类型", value: "object" },
      { label: "字段数量", value: String(entries.length) },
      ...entries.slice(0, 12).map(([key, nested]) => ({ label: key, value: describeValue(nested) })),
    ];
  }
  return [{ label: "顶层值", value: describeValue(value) }];
}

function describeValue(value: unknown): string {
  if (Array.isArray(value)) return `array (${value.length})`;
  if (value && typeof value === "object") return `object (${Object.keys(value).length} fields)`;
  if (value === null) return "null";
  const text = String(value);
  return text.length > 100 ? `${text.slice(0, 97)}…` : text;
}

function inferSchema(asset: QEAssetEntry): { title: string; fields: string[] } {
  const path = asset.relative_path.toLowerCase();
  if (path.endsWith("pred.pkl")) return { title: "QE prediction artifact", fields: ["trade_date", "symbol", "score"] };
  if (path.endsWith("label.pkl")) return { title: "QE label artifact", fields: ["trade_date", "symbol", "horizon_days", "future_return"] };
  if (path.endsWith(".csv")) return { title: "Tabular evidence", fields: ["CSV header", "bounded rows"] };
  if (path.endsWith(".json")) return { title: "Structured JSON evidence", fields: ["top-level type", "field summary", "secret redaction"] };
  if (path.endsWith(".log") || path.endsWith(".txt")) return { title: "Text/log evidence", fields: ["line number", "sanitized text"] };
  return { title: asset.parser_contract || asset.schema_version || "Generic asset metadata", fields: [] };
}

function isTextAsset(asset: QEAssetEntry): boolean {
  return Boolean(asset.content_type?.startsWith("text/"))
    || asset.content_type === "application/json"
    || TEXT_ASSET_RE.test(asset.relative_path);
}

function SummaryCell({ label, value }: { label: string; value: string }) {
  return <div className={styles.summaryCell}><div className={styles.summaryLabel}>{label}</div><div className={styles.summaryValue}>{value}</div></div>;
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

function formatDateTime(value: string | null): string {
  return value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "未记录";
}

function shortHash(value: string): string {
  return value.length <= 18 ? value : `${value.slice(0, 9)}…${value.slice(-7)}`;
}
