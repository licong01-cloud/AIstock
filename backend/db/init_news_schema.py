from __future__ import annotations

from typing import List

from dotenv import load_dotenv

from .pg_pool import get_conn


load_dotenv(override=True)


DDL: List[str] = [
    # 确保 app schema 存在
    "CREATE SCHEMA IF NOT EXISTS app",
    # 新闻主表
    """
    CREATE TABLE IF NOT EXISTS app.news_articles (
        id              BIGSERIAL PRIMARY KEY,
        source          VARCHAR(64) NOT NULL,
        external_id     VARCHAR(128),
        title           TEXT,
        content         TEXT,
        url             TEXT,
        ts_codes        TEXT[],
        publish_time    TIMESTAMPTZ NOT NULL,
        ingest_time     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        is_important    BOOLEAN NOT NULL DEFAULT FALSE,
        raw_source      JSONB,
        CONSTRAINT uq_news_articles_source_time_title
            UNIQUE (source, publish_time, title)
    )
    """,
    # 因子集合
    """
    CREATE TABLE IF NOT EXISTS app.news_factor_sets (
        id              BIGSERIAL PRIMARY KEY,
        name            TEXT NOT NULL,
        description     TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by      TEXT
    )
    """,
    # 因子集合版本
    """
    CREATE TABLE IF NOT EXISTS app.news_factor_set_versions (
        id              BIGSERIAL PRIMARY KEY,
        factor_set_id   BIGINT NOT NULL REFERENCES app.news_factor_sets(id) ON DELETE CASCADE,
        version         TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'draft',
        model_name      TEXT,
        model_version   TEXT,
        config_json     JSONB,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by      TEXT,
        CONSTRAINT uq_news_factor_set_version UNIQUE (factor_set_id, version)
    )
    """,
    # 新闻因子表：支持多因子集合版本
    """
    CREATE TABLE IF NOT EXISTS app.news_factors (
        id                      BIGSERIAL PRIMARY KEY,
        article_id              BIGINT NOT NULL REFERENCES app.news_articles(id) ON DELETE CASCADE,
        factor_set_version_id   BIGINT NOT NULL REFERENCES app.news_factor_set_versions(id) ON DELETE CASCADE,
        sentiment_score         NUMERIC,
        impact_score            NUMERIC,
        relevance_score         NUMERIC,
        sentiment_label         VARCHAR(32),
        event_type              VARCHAR(64),
        extra                   JSONB,
        created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_news_factor_unique
            UNIQUE (article_id, factor_set_version_id)
    )
    """,
    # 一些常用索引
    """
    CREATE INDEX IF NOT EXISTS idx_news_articles_publish_time
    ON app.news_articles (publish_time DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_news_articles_source_time
    ON app.news_articles (source, publish_time DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_news_factors_factor_set
    ON app.news_factors (factor_set_version_id)
    """
]


def init_news_schema() -> None:
    """幂等创建新闻与新闻因子相关表结构。"""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    init_news_schema()
