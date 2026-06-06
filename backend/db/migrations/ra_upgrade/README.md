# Research Assistant Upgrade Migrations

This directory is the registered migration namespace for the Research Assistant architecture upgrade.

Phase 0 creates only the namespace scaffold. It does not define or execute DDL, does not connect to production, and does not change business data. Future Phase 1+ migration files added here must be idempotent, include PostgreSQL `COMMENT ON` coverage for new objects, run twice against 8011/8012 validation databases, and report `production_ddl_gate` separately before any production application.
