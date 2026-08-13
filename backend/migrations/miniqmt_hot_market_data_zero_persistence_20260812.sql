-- BUG-1041 successor: preserve historical rows and reject every new KERNEL_V2 TICK.
-- The existing six-CHECK BUG-1019 target is an immutable prerequisite; this
-- migration never overwrites an unknown predecessor or drifted schema.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;
LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE qmt_strategy.execution_algo_instance IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    table_oid OID := 'qmt_strategy.execution_runtime_event'::regclass;
    constraint_names TEXT[];
    validated_count INTEGER;
    event_id_sha256 TEXT;
    sequence_sha256 TEXT;
    type_sha256 TEXT;
    source_sha256 TEXT;
    composite_sha256 TEXT;
    contract_sha256 TEXT;
    active_v3_count BIGINT;
    guard_definition TEXT;
    base_names CONSTANT TEXT[] := ARRAY[
        'ck_miniqmt_event_id','ck_miniqmt_event_sequence','ck_miniqmt_event_source',
        'ck_miniqmt_event_type','ck_miniqmt_k2_event_composite','ck_miniqmt_k2_event_contract'
    ];
BEGIN
    SELECT array_agg(conname ORDER BY conname COLLATE "C"),
           count(*) FILTER (WHERE convalidated)
      INTO constraint_names,validated_count
      FROM pg_constraint
     WHERE conrelid=table_oid AND contype='c'
       AND conname<>'ck_miniqmt_no_new_kernel_tick';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT event_id_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_id' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT sequence_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_sequence' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT type_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT source_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT composite_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT contract_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_contract' AND contype='c';
    IF constraint_names<>base_names OR validated_count<>6
       OR event_id_sha256<>'836b7f7ebf14ee61ec94c9df82b300b42c96ff1046de0a2e0cfb8bc0f400642d'
       OR sequence_sha256<>'a1b188a1431066f2e8f2d0d51107b8c0532830ca7b88567ba1903c4b3999a3d0'
       OR type_sha256<>'6ac3041d989166511127ec22d9379dd0ecdc09fb5055e72006100319026a6f24'
       OR source_sha256<>'c2f8e672b140ec88f667e251bbb5ff812cd0bea2a24f31c45d74c3f8d32eb881'
       OR composite_sha256<>'4a2d33d3fc75a4b468661e1bdbf2ecce9cd13aaab491c7c4d7605a1df3af3857'
       OR contract_sha256<>'888bebaf7d9540ecadae15bfb7d2944db59177b4ed2ef5e8beb231b803f9faca' THEN
        RAISE EXCEPTION 'BUG-1041 apply: exact BUG-1019 target CHECK authority is required';
    END IF;

    SELECT count(*) INTO active_v3_count
      FROM qmt_strategy.execution_algo_instance
     WHERE kernel_contract_version='KERNEL_V2'
       AND status IN ('INITIALIZING','ACTIVE','PAUSED')
       AND plugin_version<>'4.0.0';
    IF active_v3_count<>0 THEN
        RAISE EXCEPTION 'BUG-1041 apply: % active pre-V4 algorithm instances remain',active_v3_count;
    END IF;

    SELECT pg_get_constraintdef(oid,true) INTO guard_definition
      FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_no_new_kernel_tick' AND contype='c';
    IF guard_definition IS NULL THEN
        ALTER TABLE qmt_strategy.execution_runtime_event
          ADD CONSTRAINT ck_miniqmt_no_new_kernel_tick CHECK ((
            event_contract_version<>'KERNEL_V2' OR event_type<>'TICK'
          ) IS TRUE) NOT VALID;
    ELSIF encode(sha256(convert_to(guard_definition,'UTF8')),'hex')<>
        '9dd2d0274fe18ad4ab487f006e420e6f11b806818cd876ebabf3d3f286cc4bed' THEN
        RAISE EXCEPTION 'BUG-1041 apply: no-new-TICK guard definition drift';
    END IF;
END $$;

COMMENT ON CONSTRAINT ck_miniqmt_no_new_kernel_tick ON qmt_strategy.execution_runtime_event IS
  'BUG-1041 successor: reject new KERNEL_V2 TICK; preserve historical rows read-only';
COMMIT;

-- Independent post-commit readback. The guard deliberately stays NOT VALID:
-- PostgreSQL enforces it for new writes while retaining historical Tick rows.
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;
DO $$
DECLARE
    definition TEXT;
    validated BOOLEAN;
    active_v3_count BIGINT;
BEGIN
    SELECT pg_get_constraintdef(oid,true),convalidated INTO STRICT definition,validated
      FROM pg_constraint
     WHERE conrelid='qmt_strategy.execution_runtime_event'::regclass
       AND conname='ck_miniqmt_no_new_kernel_tick' AND contype='c';
    IF validated OR encode(sha256(convert_to(definition,'UTF8')),'hex')<>
       '9dd2d0274fe18ad4ab487f006e420e6f11b806818cd876ebabf3d3f286cc4bed' THEN
        RAISE EXCEPTION 'BUG-1041 post-commit no-new-TICK authority drift';
    END IF;
    SELECT count(*) INTO active_v3_count
      FROM qmt_strategy.execution_algo_instance
     WHERE kernel_contract_version='KERNEL_V2'
       AND status IN ('INITIALIZING','ACTIVE','PAUSED')
       AND plugin_version<>'4.0.0';
    IF active_v3_count<>0 THEN
        RAISE EXCEPTION 'BUG-1041 post-commit active pre-V4 algorithm readback drift';
    END IF;
END $$;
COMMIT;
