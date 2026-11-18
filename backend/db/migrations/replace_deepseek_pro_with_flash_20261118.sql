-- BUG-2251. Run within an explicit transaction on the authorized existing DB.
-- Current configuration only: historical model identities and usage stay intact.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
LOCK TABLE aistock_llm_models, aistock_llm_stage_mappings,
    aistock_llm_rdagent_stages, aistock_llm_aistock_agents,
    qe_agent_model_config, assistant_model_profiles IN SHARE ROW EXCLUSIVE MODE;

DO $migration$
DECLARE
    old_model RECORD;
    flash_id INTEGER;
    flash_full_id TEXT;
BEGIN
    FOR old_model IN
        SELECT id, provider_id, full_model_id, api_config_id
        FROM aistock_llm_models
        WHERE lower(regexp_replace(full_model_id, '^.*/', ''))
            ~ '^deepseek([-_.][a-z0-9]+)*[-_.]pro([-_.:][a-z0-9._:-]+)?$'
            OR lower(model_name)
            ~ '^deepseek([-_.][a-z0-9]+)*[-_.]pro([-_.:][a-z0-9._:-]+)?$'
        ORDER BY id
    LOOP
        flash_full_id := regexp_replace(old_model.full_model_id, '[^/]+$', 'deepseek-v4-flash');
        IF NOT EXISTS (SELECT 1 FROM aistock_llm_models WHERE provider_id = old_model.provider_id
            AND model_name = 'deepseek-v4-flash') THEN
        INSERT INTO aistock_llm_models
            (provider_id, model_name, display_name, full_model_id, model_type,
             api_config_id, is_active, is_verified, description)
        VALUES (old_model.provider_id, 'deepseek-v4-flash', 'DeepSeek V4 Flash',
                flash_full_id, 'chat', old_model.api_config_id, true, false,
                'BUG-2251 Flash selection; pricing and live verification remain unknown')
        ON CONFLICT (provider_id, model_name) DO NOTHING;
        END IF;

        SELECT id INTO STRICT flash_id FROM aistock_llm_models
        WHERE provider_id = old_model.provider_id AND model_name = 'deepseek-v4-flash'
            AND full_model_id = flash_full_id AND model_type = 'chat';
        UPDATE aistock_llm_models SET is_active = true,
            api_config_id = coalesce(api_config_id, old_model.api_config_id),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = flash_id AND (is_active IS DISTINCT FROM true
            OR (api_config_id IS NULL AND old_model.api_config_id IS NOT NULL));

        UPDATE aistock_llm_stage_mappings SET model_id = flash_id, updated_at = CURRENT_TIMESTAMP
            WHERE model_id = old_model.id;
        UPDATE aistock_llm_rdagent_stages SET model_id = flash_id, updated_at = CURRENT_TIMESTAMP
            WHERE model_id = old_model.id;
        UPDATE aistock_llm_aistock_agents SET model_id = flash_id, updated_at = CURRENT_TIMESTAMP
            WHERE model_id = old_model.id;
        UPDATE qe_agent_model_config SET model_id = flash_id::TEXT, updated_at = CURRENT_TIMESTAMP
            WHERE model_id = old_model.id::TEXT;
        UPDATE aistock_llm_models SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE id = old_model.id AND is_active IS DISTINCT FROM false;
    END LOOP;

    UPDATE qe_agent_model_config
        SET model_id = regexp_replace(btrim(model_id), '[^/]+$', 'deepseek-v4-flash'),
            updated_at = CURRENT_TIMESTAMP
        WHERE lower(regexp_replace(btrim(model_id), '^.*/', ''))
            ~ '^deepseek([-_.][a-z0-9]+)*[-_.]pro([-_.:][a-z0-9._:-]+)?$';
    UPDATE assistant_model_profiles
        SET model_name = regexp_replace(btrim(model_name), '[^/]+$', 'deepseek-v4-flash'),
            display_name = 'DeepSeek V4 Flash', cost_json = '{}'::jsonb,
            updated_at = CURRENT_TIMESTAMP
        WHERE lower(regexp_replace(btrim(model_name), '^.*/', ''))
            ~ '^deepseek([-_.][a-z0-9]+)*[-_.]pro([-_.:][a-z0-9._:-]+)?$';
    UPDATE aistock_llm_models
        SET proxy_model_alias = regexp_replace(btrim(proxy_model_alias), '[^/]+$', 'deepseek-v4-flash'),
            updated_at = CURRENT_TIMESTAMP
        WHERE lower(regexp_replace(btrim(proxy_model_alias), '^.*/', ''))
            ~ '^deepseek([-_.][a-z0-9]+)*[-_.]pro([-_.:][a-z0-9._:-]+)?$';
END
$migration$;
