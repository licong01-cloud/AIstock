import type { ReactNode } from "react";

export type PaperColumn<T> = {
  key: string;
  header: string;
  render: (row: T, index: number) => ReactNode;
};

export default function PaperTable<T>({ rows, columns, empty = "暂无数据" }: { rows: T[]; columns: PaperColumn<T>[]; empty?: string }) {
  return (
    <div className="pv2-table-wrap">
      <table className="pv2-table">
        <thead>
          <tr>{columns.map((column) => <th key={column.key}>{column.header}</th>)}</tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr><td colSpan={columns.length} className="pv2-empty-cell">{empty}</td></tr>
          ) : rows.map((row, index) => (
            <tr key={index}>{columns.map((column) => <td key={column.key}>{column.render(row, index)}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
