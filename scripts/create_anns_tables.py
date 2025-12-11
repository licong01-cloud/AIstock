import os
import psycopg2
from dotenv import load_dotenv


def main() -> None:
    # 从 .env 载入数据库配置
    load_dotenv(override=True)
    cfg = dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )

    conn = psycopg2.connect(**cfg)
    conn.autocommit = True

    with conn, conn.cursor() as cur:
        # 1) 创建 market.anns 表
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market.anns (
                id              BIGSERIAL PRIMARY KEY,           -- 本地自增ID

                ann_date        DATE        NOT NULL,            -- 公告日期 YYYYMMDD
                ts_code         TEXT        NOT NULL,            -- TS代码
                name            TEXT        NOT NULL,            -- 股票名称
                title           TEXT        NOT NULL,            -- 公告标题
                url             TEXT        NOT NULL,            -- 公告原文URL（Tushare返回）
                rec_time        TIMESTAMPTZ NULL,               -- 公告接收/发布时间

                local_path      TEXT        NULL,                -- 本地相对路径，例如 2025-01-15/600000.SH_123.pdf
                file_ext        TEXT        NULL,                -- 文件后缀：pdf/html 等
                file_size       BIGINT      NULL,                -- 文件大小（字节）
                file_hash       TEXT        NULL,                -- 文件哈希（如md5/sha256）
                download_status TEXT        NOT NULL DEFAULT 'pending', -- 下载状态 pending/success/failed

                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

        # 唯一约束与索引
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'anns_uniq'
                      AND conrelid = 'market.anns'::regclass
                ) THEN
                    ALTER TABLE market.anns
                        ADD CONSTRAINT anns_uniq UNIQUE (ts_code, ann_date, title);
                END IF;
            END$$;
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_anns_ann_date ON market.anns (ann_date);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_anns_ts_code_ann_date ON market.anns (ts_code, ann_date);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_anns_download_status ON market.anns (download_status);"
        )

        # 字段注释：根据 Tushare anns_d 文档及本地需求
        cur.execute(
            """
            COMMENT ON TABLE market.anns IS 'Tushare anns_d 上市公司公告（含本地PDF元数据）';

            COMMENT ON COLUMN market.anns.id IS '本地自增ID';

            COMMENT ON COLUMN market.anns.ann_date IS '公告日期，对应Tushare anns_d中的ann_date (YYYYMMDD)';
            COMMENT ON COLUMN market.anns.ts_code IS 'TS代码，例如 600000.SH';
            COMMENT ON COLUMN market.anns.name IS '股票名称，对应Tushare anns_d中的name';
            COMMENT ON COLUMN market.anns.title IS '公告标题，对应Tushare anns_d中的title';
            COMMENT ON COLUMN market.anns.url IS '公告原文URL，对应Tushare anns_d中的url（可能为PDF或HTML链接）';
            COMMENT ON COLUMN market.anns.rec_time IS '公告接收或发布时间，对应Tushare anns_d中的rec_time';

            COMMENT ON COLUMN market.anns.local_path IS '公告本地相对存储路径，相对于 ANNOUNCE_PDF_ROOT，例如 2025-01-15/600000.SH_123.pdf';
            COMMENT ON COLUMN market.anns.file_ext IS '本地文件扩展名，如 pdf、html';
            COMMENT ON COLUMN market.anns.file_size IS '本地文件大小，单位字节';
            COMMENT ON COLUMN market.anns.file_hash IS '本地文件内容哈希值，例如md5/sha256，用于校验';
            COMMENT ON COLUMN market.anns.download_status IS 'PDF下载状态：pending/success/failed';

            COMMENT ON COLUMN market.anns.created_at IS '记录创建时间';
            COMMENT ON COLUMN market.anns.updated_at IS '记录最近更新时间';
            """
        )

        # 2) 在 data_stats_config 中注册 anns_d 数据集
        cur.execute(
            """
            INSERT INTO market.data_stats_config (data_kind, table_name, date_column, enabled, extra_info)
            VALUES (
                'anns_d',
                'market.anns',
                'ann_date',
                TRUE,
                jsonb_build_object('desc', 'Tushare anns_d 上市公司公告（含本地PDF路径）')
            )
            ON CONFLICT (data_kind) DO UPDATE
                SET table_name = EXCLUDED.table_name,
                    date_column = EXCLUDED.date_column,
                    enabled = EXCLUDED.enabled,
                    extra_info = EXCLUDED.extra_info;
            """
        )

    conn.close()
    print("market.anns table and data_stats_config for anns_d ensured.")


if __name__ == "__main__":
    main()
