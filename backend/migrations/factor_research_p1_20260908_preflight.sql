BEGIN READ ONLY;
SELECT current_database() AS database_name, inet_server_port() AS server_port,
       to_regclass('public.factor_research_tasks') AS tasks,
       to_regclass('public.factor_research_records') AS records;
SELECT table_name,column_name,data_type,is_nullable,column_default
  FROM information_schema.columns
 WHERE table_schema='public' AND table_name IN ('factor_research_tasks','factor_research_records')
 ORDER BY table_name,ordinal_position;
COMMIT;
