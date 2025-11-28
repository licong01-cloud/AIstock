import os
import sys
import argparse
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psycopg2


DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="AIstock-export-db-schema",
)


def _get_root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _connect():
    return psycopg2.connect(**DB_CFG)


def _fetch_tables(conn, schemas: List[str] | None = None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    sql = """
        SELECT
            c.oid                  AS table_oid,
            n.nspname              AS schema_name,
            c.relname              AS table_name,
            c.relkind              AS relkind,
            obj_description(c.oid, 'pg_class') AS table_comment
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """

    params: List[Any] = []
    if schemas:
        sql += " AND n.nspname = ANY(%s)"
        params.append(schemas)

    sql += " ORDER BY n.nspname, c.relname"

    cur = conn.cursor()
    cur.execute(sql, params)

    tables: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in cur.fetchall():
        table_oid, schema_name, table_name, relkind, table_comment = row
        tables[(schema_name, table_name)] = {
            "oid": table_oid,
            "schema": schema_name,
            "name": table_name,
            "relkind": relkind,
            "comment": table_comment,
        }

    cur.close()
    return tables


def _fetch_columns(conn, schemas: List[str] | None = None) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    sql = """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            a.attnum  AS ordinal_position,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull AS is_nullable,
            pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
            col_description(c.oid, a.attnum) AS column_comment
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """

    params: List[Any] = []
    if schemas:
        sql += " AND n.nspname = ANY(%s)"
        params.append(schemas)

    sql += " ORDER BY n.nspname, c.relname, a.attnum"

    cur = conn.cursor()
    cur.execute(sql, params)

    columns: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in cur.fetchall():
        (
            schema_name,
            table_name,
            ordinal_position,
            column_name,
            data_type,
            is_nullable,
            column_default,
            column_comment,
        ) = row
        key = (schema_name, table_name)
        columns.setdefault(key, []).append(
            {
                "ordinal_position": ordinal_position,
                "name": column_name,
                "data_type": data_type,
                "is_nullable": bool(is_nullable),
                "default": column_default,
                "comment": column_comment,
            }
        )

    cur.close()
    return columns


def _fetch_primary_keys(conn, schemas: List[str] | None = None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    sql = """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            con.conname AS constraint_name,
            array_agg(a.attname ORDER BY u.attposition) AS columns
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
        WHERE con.contype = 'p'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """

    params: List[Any] = []
    if schemas:
        sql += " AND n.nspname = ANY(%s)"
        params.append(schemas)

    sql += " GROUP BY n.nspname, c.relname, con.conname"

    cur = conn.cursor()
    cur.execute(sql, params)

    pks: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in cur.fetchall():
        schema_name, table_name, constraint_name, columns = row
        pks[(schema_name, table_name)] = {
            "name": constraint_name,
            "columns": list(columns) if columns is not None else [],
        }

    cur.close()
    return pks


def _fetch_foreign_keys(conn, schemas: List[str] | None = None) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    sql = """
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            con.conname AS constraint_name,
            array_agg(a.attname ORDER BY u.attposition) AS columns,
            nf.nspname AS foreign_schema_name,
            cf.relname AS foreign_table_name,
            array_agg(af.attname ORDER BY u.attposition) AS foreign_columns
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class cf ON cf.oid = con.confrelid
        JOIN pg_namespace nf ON nf.oid = cf.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
        JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS uf(attnum, attposition_f) ON u.attposition = uf.attposition_f
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
        JOIN pg_attribute af ON af.attrelid = cf.oid AND af.attnum = uf.attnum
        WHERE con.contype = 'f'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """

    params: List[Any] = []
    if schemas:
        sql += " AND n.nspname = ANY(%s)"
        params.append(schemas)

    sql += " GROUP BY n.nspname, c.relname, con.conname, nf.nspname, cf.relname"

    cur = conn.cursor()
    cur.execute(sql, params)

    fks: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in cur.fetchall():
        (
            schema_name,
            table_name,
            constraint_name,
            columns,
            foreign_schema_name,
            foreign_table_name,
            foreign_columns,
        ) = row
        key = (schema_name, table_name)
        fks.setdefault(key, []).append(
            {
                "name": constraint_name,
                "columns": list(columns) if columns is not None else [],
                "foreign_schema": foreign_schema_name,
                "foreign_table": foreign_table_name,
                "foreign_columns": list(foreign_columns) if foreign_columns is not None else [],
            }
        )

    cur.close()
    return fks


def _relkind_label(relkind: str) -> str:
    return {
        "r": "Table",
        "p": "Partitioned table",
        "v": "View",
        "m": "Materialized view",
        "f": "Foreign table",
    }.get(relkind, f"relkind={relkind}")


def _escape_md(text: str | None) -> str:
    if text is None:
        return ""
    # 简单处理竖线，避免破坏表格
    return text.replace("|", "\\|")


def _generate_markdown(
    tables: Dict[Tuple[str, str], Dict[str, Any]],
    columns: Dict[Tuple[str, str], List[Dict[str, Any]]],
    pks: Dict[Tuple[str, str], Dict[str, Any]],
    fks: Dict[Tuple[str, str], List[Dict[str, Any]]],
) -> str:
    lines: List[str] = []
    now = dt.datetime.now().astimezone()

    lines.append("# 数据库结构文档")
    lines.append("")
    lines.append(f"- 生成时间: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    lines.append(f"- 数据库名: {DB_CFG['dbname']}")
    lines.append("")
    lines.append("> 本文档由 scripts/export_db_schema_docs.py 自动生成，请勿手工编辑。\n")

    # 按 schema 分组
    from collections import defaultdict

    grouped: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    for (schema, table), info in tables.items():
        grouped[schema].append((table, info))

    for schema in sorted(grouped.keys()):
        lines.append(f"## Schema `{schema}`")
        lines.append("")

        for table, info in sorted(grouped[schema], key=lambda x: x[0]):
            key = (schema, table)
            relkind = info.get("relkind")
            label = _relkind_label(relkind)
            comment = info.get("comment") or ""

            lines.append(f"### {label} `{schema}.{table}`")
            if comment:
                lines.append("")
                lines.append(f"> {comment}")
            lines.append("")

            cols = columns.get(key, [])
            if not cols:
                lines.append("(无列定义)")
                lines.append("")
            else:
                # 标记主键列
                pk_info = pks.get(key)
                pk_cols = set(pk_info.get("columns", [])) if pk_info else set()

                lines.append("| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |")
                lines.append("|---|--------|------|----------|--------|----------|------|")
                for col in cols:
                    idx = col["ordinal_position"]
                    name = col["name"]
                    dtype = col["data_type"] or ""
                    nullable = "YES" if col["is_nullable"] else "NO"
                    default = col["default"]
                    comment_col = col["comment"]
                    is_pk = "YES" if name in pk_cols else ""

                    lines.append(
                        "| {idx} | `{name}` | `{dtype}` | {nullable} | {default} | {is_pk} | {comment} |".format(
                            idx=idx,
                            name=_escape_md(name),
                            dtype=_escape_md(dtype),
                            nullable=_escape_md(nullable),
                            default=f"`{_escape_md(str(default))}`" if default is not None else "",
                            is_pk=_escape_md(is_pk),
                            comment=_escape_md(comment_col if comment_col is not None else ""),
                        )
                    )

                lines.append("")

            # 主键
            pk = pks.get(key)
            if pk:
                cols_str = ", ".join(f"`{c}`" for c in pk.get("columns", []))
                lines.append("**主键约束**")
                lines.append("")
                lines.append(f"- 名称: `{pk['name']}`")
                lines.append(f"- 字段: {cols_str if cols_str else '(未记录)'}")
                lines.append("")

            # 外键
            fk_list = fks.get(key, [])
            if fk_list:
                lines.append("**外键约束**")
                lines.append("")
                for fk in sorted(fk_list, key=lambda x: x["name"]):
                    cols_str = ", ".join(f"`{c}`" for c in fk.get("columns", []))
                    ref_cols_str = ", ".join(f"`{c}`" for c in fk.get("foreign_columns", []))
                    ref_table = f"{fk['foreign_schema']}.{fk['foreign_table']}"
                    lines.append(f"- `{fk['name']}`: ({cols_str}) → `{ref_table}` ({ref_cols_str})")
                lines.append("")

            lines.append("---")
            lines.append("")

    return "\n".join(lines)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="导出 PostgreSQL 数据库结构为 Markdown 文档")
    parser.add_argument(
        "-o",
        "--output",
        help="输出 Markdown 文件路径（默认: 项目根目录下 docs/db_schema.md，如果 docs 不存在则创建）",
    )
    parser.add_argument(
        "--schemas",
        nargs="*",
        help="只导出指定的 schema（例如: market public），默认导出所有用户 schema",
    )

    args = parser.parse_args(argv)

    root_dir = _get_root_dir()
    if args.output:
        output_path = Path(args.output).expanduser()
        if not output_path.is_absolute():
            output_path = root_dir / output_path
    else:
        docs_dir = root_dir / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        output_path = docs_dir / "db_schema.md"

    schemas = args.schemas if args.schemas else None

    conn = _connect()
    try:
        tables = _fetch_tables(conn, schemas)
        columns = _fetch_columns(conn, schemas)
        pks = _fetch_primary_keys(conn, schemas)
        fks = _fetch_foreign_keys(conn, schemas)
    finally:
        conn.close()

    md = _generate_markdown(tables, columns, pks, fks)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(md)

    print(f"[OK] 数据库结构文档已生成: {output_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
