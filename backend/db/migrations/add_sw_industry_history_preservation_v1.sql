-- BUG-1847: operator-applied migration; no implicit DDL during daily ingestion.
-- Existing DEV first; production requires this exact migration's authorization.
CREATE TABLE IF NOT EXISTS market.sw_industry_source_fact_v1 (
    fact_sha256 text PRIMARY KEY CHECK (fact_sha256 ~ '^[0-9a-f]{64}$'),
    source_id text NOT NULL,
    taxonomy_version text NOT NULL,
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS market.sw_industry_taxonomy_history_v1 (
    fact_sha256 text PRIMARY KEY CHECK (fact_sha256 ~ '^[0-9a-f]{64}$'),
    bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    taxonomy_version text NOT NULL CHECK (taxonomy_version IN ('SW2014','SW2021')),
    taxonomy_code text NOT NULL,
    level integer NOT NULL CHECK (level BETWEEN 1 AND 3),
    parent_code text,
    published_index_code text,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS market.sw_industry_classification_history_v1 (
    fact_sha256 text PRIMARY KEY CHECK (fact_sha256 ~ '^[0-9a-f]{64}$'),
    bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    taxonomy_version text NOT NULL CHECK (taxonomy_version IN ('SW2014','SW2021')),
    ts_code text NOT NULL,
    published_l2_code text NOT NULL,
    effective_from date NOT NULL,
    effective_to date NOT NULL CHECK (effective_to >= effective_from),
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_row_sha256 text NOT NULL CHECK (source_row_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(bundle_sha256,taxonomy_version,ts_code,effective_from,effective_to)
);
CREATE INDEX IF NOT EXISTS sw_industry_classification_history_lookup_v1
    ON market.sw_industry_classification_history_v1(bundle_sha256,ts_code,effective_from,effective_to);

CREATE OR REPLACE FUNCTION market.sw_industry_forbid_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'SW_INDUSTRY_IMMUTABLE: % on %.% is forbidden',TG_OP,TG_TABLE_SCHEMA,TG_TABLE_NAME;
END;
$$;
DO $$
DECLARE name text;
BEGIN
    FOREACH name IN ARRAY ARRAY['sw_industry_source_fact_v1','sw_industry_taxonomy_history_v1',
                               'sw_industry_classification_history_v1'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=('market.'||name)::regclass
                        AND tgname='sw_industry_immutable_v1') THEN
            EXECUTE format('CREATE TRIGGER sw_industry_immutable_v1 BEFORE UPDATE OR DELETE OR TRUNCATE ON market.%I FOR EACH STATEMENT EXECUTE FUNCTION market.sw_industry_forbid_mutation_v1()',name);
        END IF;
        EXECUTE format('ALTER TABLE market.%I ENABLE ALWAYS TRIGGER sw_industry_immutable_v1',name);
    END LOOP;
    FOREACH name IN ARRAY ARRAY['sw_index_member','sw_index_classify'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=('market.'||name)::regclass
                        AND tgname='sw_industry_no_takedown_v1') THEN
            EXECUTE format('CREATE TRIGGER sw_industry_no_takedown_v1 BEFORE DELETE OR TRUNCATE ON market.%I FOR EACH STATEMENT EXECUTE FUNCTION market.sw_industry_forbid_mutation_v1()',name);
        END IF;
        EXECUTE format('ALTER TABLE market.%I ENABLE ALWAYS TRIGGER sw_industry_no_takedown_v1',name);
    END LOOP;
END;
$$;

-- Archive every old/new compatibility revision, even for other SQL callers.
-- These are LOCAL_PROJECTION observations, not authenticated provider responses.
-- SQL JSONB byte identity is explicitly separate from Python canonical identity.
CREATE OR REPLACE FUNCTION market.sw_industry_archive_projection_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE row_payload jsonb; envelope jsonb; identity text; row_digest text; version text;
BEGIN
    IF TG_OP='UPDATE' THEN
        row_payload:=to_jsonb(OLD);
        version:=COALESCE(row_payload->>'src','LEGACY_MEMBER_UNVERSIONED');
        envelope:=jsonb_build_object('source_id','local_projection:'||TG_TABLE_NAME,
            'taxonomy_version',version,'hash_encoding','postgres_jsonb_text_utf8_v1','row',row_payload);
        identity:=encode(sha256(convert_to(envelope::text,'UTF8')),'hex');
        row_digest:=encode(sha256(convert_to(row_payload::text,'UTF8')),'hex');
        INSERT INTO market.sw_industry_source_fact_v1 VALUES(identity,'local_projection:'||TG_TABLE_NAME,version,row_digest,envelope,clock_timestamp()) ON CONFLICT DO NOTHING;
    END IF;
    row_payload:=to_jsonb(NEW);
    version:=COALESCE(row_payload->>'src','LEGACY_MEMBER_UNVERSIONED');
    envelope:=jsonb_build_object('source_id','local_projection:'||TG_TABLE_NAME,
        'taxonomy_version',version,'hash_encoding','postgres_jsonb_text_utf8_v1','row',row_payload);
    identity:=encode(sha256(convert_to(envelope::text,'UTF8')),'hex');
    row_digest:=encode(sha256(convert_to(row_payload::text,'UTF8')),'hex');
    INSERT INTO market.sw_industry_source_fact_v1 VALUES(identity,'local_projection:'||TG_TABLE_NAME,version,row_digest,envelope,clock_timestamp()) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;
DO $$
DECLARE name text;
BEGIN
    FOREACH name IN ARRAY ARRAY['sw_index_member','sw_index_classify'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=('market.'||name)::regclass
                        AND tgname='sw_industry_archive_projection_v1') THEN
            EXECUTE format('CREATE TRIGGER sw_industry_archive_projection_v1 BEFORE INSERT OR UPDATE ON market.%I FOR EACH ROW EXECUTE FUNCTION market.sw_industry_archive_projection_v1()',name);
        END IF;
        EXECUTE format('ALTER TABLE market.%I ENABLE ALWAYS TRIGGER sw_industry_archive_projection_v1',name);
        EXECUTE format($sql$
            INSERT INTO market.sw_industry_source_fact_v1
            (fact_sha256,source_id,taxonomy_version,source_sha256,payload)
            SELECT encode(sha256(convert_to(e::text,'UTF8')),'hex'),
                   e->>'source_id',e->>'taxonomy_version',
                   encode(sha256(convert_to((e->'row')::text,'UTF8')),'hex'),e
            FROM (SELECT jsonb_build_object('source_id','local_projection:'||%L,
                   'taxonomy_version',COALESCE(to_jsonb(t)->>'src','LEGACY_MEMBER_UNVERSIONED'),
                   'hash_encoding','postgres_jsonb_text_utf8_v1','row',to_jsonb(t)) e
                  FROM market.%I t) s ON CONFLICT DO NOTHING
            $sql$,name,name);
    END LOOP;
END;
$$;
COMMENT ON TABLE market.sw_industry_source_fact_v1 IS '不可变申万来源事实及本地投影版本；observed_at不是历史available_at；禁止更新/删除/截断';
COMMENT ON TABLE market.sw_industry_taxonomy_history_v1 IS '不可变、分类版本隔离的SW2014/SW2021词典；按bundle_sha256绑定';
COMMENT ON TABLE market.sw_industry_classification_history_v1 IS '不可变有效日期行业历史；日期闭区间，保留原始knowledge_time_quality，不推定as-known；按bundle_sha256消费';
