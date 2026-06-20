-- BUG-439: repair empty non-array RA capability registry MCP refs.
-- Run with psql --single-transaction -v ON_ERROR_STOP=1.
-- This migration only normalizes empty JSON values written by the old repository
-- JSON adapter bug. Non-empty/non-array corruption stays visible to the
-- application fail-closed guard.

COMMENT ON TABLE assistant_capabilities IS 'Approved Research Assistant Capability Registry for planner-selectable MCP tools, skills and workflow packs. BUG-439 repaired empty non-array mcp_tool_refs legacy rows; future writes must store JSON arrays for mcp_tool_refs and skill_refs.';

UPDATE assistant_capabilities
SET
    mcp_tool_refs = '[]'::jsonb,
    updated_at = NOW()
WHERE (mcp_tool_refs IS NULL OR jsonb_typeof(mcp_tool_refs) <> 'array')
  AND (mcp_tool_refs IS NULL OR mcp_tool_refs IN ('{}'::jsonb, 'null'::jsonb, '""'::jsonb));
