-- Add an execution model asset namespace so algorithm variants can share cache
-- directories without duplicating model files.

ALTER TABLE public.execution_algorithm_catalog
    ADD COLUMN IF NOT EXISTS asset_namespace TEXT;

COMMENT ON COLUMN public.execution_algorithm_catalog.asset_namespace IS
    'Optional execution model cache namespace. When set, model assets resolve under this namespace instead of algo_code.';

UPDATE public.execution_algorithm_catalog
SET asset_namespace = 'V25_TWO_STAGE',
    default_config = jsonb_set(
        COALESCE(default_config, '{}'::jsonb),
        '{asset_namespace}',
        to_jsonb('V25_TWO_STAGE'::text),
        TRUE
    ),
    updated_at = NOW()
WHERE algo_code = 'V25_1_SMALL_CAP';

UPDATE public.execution_algorithm_catalog
SET asset_namespace = NULL,
    default_config = COALESCE(default_config, '{}'::jsonb) - 'asset_namespace',
    updated_at = NOW()
WHERE algo_code = 'V25_TWO_STAGE'
  AND asset_namespace IS NOT NULL;
