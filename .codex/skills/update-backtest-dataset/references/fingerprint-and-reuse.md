# Direct invalidation and reuse

Monthly reuse no longer depends on source-content hashes or sealed source partitions.

## Invalidation inputs

Use explicit operational facts only:

- cutoff changed;
- PIT authority/rule/coverage changed;
- a database refresh or repair affected a dataset/date/instrument;
- producer/schema/toolchain needed by an output component changed;
- an output component is missing or fails structural/value validation.

Do not scan unchanged history merely to prove it is unchanged. Row count, max date and mtime may guide planning but never override an observed repair or failed validation.

## Reuse rules

- Unaffected component: reuse from the previous immutable candidate.
- New month only: append the target month.
- Exact repair scope known: selectively rebuild affected instruments/months.
- Repair scope incomplete: rebuild that component, not the whole dataset.
- PIT adds a historical security: rebuild that security across components requiring its lifecycle.

July 2026 minute repairs invalidate the minute component for the new August candidate. They do not automatically invalidate daily, H5/static, index or sector components.

## No equivalent replacement

Revision ledgers, fingerprints, checksums, Merkle roots, CAS roots and prepublish re-queries must not be introduced as a replacement full-history scan or task gate.
