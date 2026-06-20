-- BUG-439 rollback: no-op for the repair migration.
-- The forward migration only converts legacy empty object mcp_tool_refs ({}) to
-- the correct empty array ([]). Reintroducing the corrupt shape would break chat
-- turns again, so rollback is intentionally limited to restoring the prior table
-- comment text.

COMMENT ON TABLE assistant_capabilities IS 'Approved Research Assistant Capability Registry for planner-selectable MCP tools, skills and workflow packs.';
