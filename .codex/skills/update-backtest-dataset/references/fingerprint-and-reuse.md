# Fingerprint, reuse and incremental rules

## Identity layers

Keep these identities separate:

| Identity | Purpose |
|---|---|
| submission | one operator/API request and idempotency payload |
| logical request | stable profile, cutoff policy, scope and candidate-only intent |
| resolved intent | logical request plus frozen source content root and PIT digest |
| run generation | operation kind, producer, validation identity, target/lineage and checkpoint |
| attempt | one owner/fence/resource-governed execution |
| release | complete semantic/artifact/source/PIT identity |
| attestation | candidate/release plus current source/PIT/validator equivalence evidence |

Equal artifact bytes do not collapse different registered candidates or provenance lineages. A moved/re-registered candidate receives a new candidate identity even when its artifact root is unchanged.

## Fingerprint layers

Every component records independent fingerprints:

```text
semantic_fingerprint
source_input_digest
producer_fingerprint
artifact_fingerprint
validation_fingerprint
resource_policy_digest
```

- `producer_fingerprint` contains only the component's real code/dependency manifest, not an entire dirty worktree.
- Resource tuning must not change data-byte identity.
- Validator strengthening may create a re-attestation generation only when the compatibility registry says artifact semantics remain compatible.
- Schema, unit, formula or index-universe changes invalidate the relevant artifact component.
- Documentation or logging changes alone must not force a data rebuild.

## Source roots

Maintain two roots:

```text
source_content_root     canonical DB rows + normalized provider/overlay content
source_provenance_root  ingestion/audit IDs + provider request/fetch metadata
```

Content root determines resolved intent. Provenance root is receipt/audit evidence. Equal content with new fetch metadata stays the same intent; changed content with unchanged audit metadata creates a new intent.

Do not use max date, row count, file size or mtime as content identity. `VerifiedPartitionStream` must hash and feed the exact same ordered row stream. A digest-then-requery exporter is invalid. Shared inputs must first become sealed immutable source partitions.

PIT is a separate mandatory identity leaf: universe/rule/scope/state identity/source fingerprint/parameter hash/ordered canonical spans digest. A PIT-only revision creates a new resolved intent and release even if market data content is unchanged.

## Component actions

Run outcome and component actions are different layers. One run may contain a mix:

| Action | Meaning |
|---|---|
| `NOOP` | component identity, artifact, current source and validator all valid |
| `REATTEST` | compatible artifact; exact current validator/source evidence must be renewed |
| `RESUME` | the same run/attempt lineage has a continuous, fence-valid checkpoint |
| `REUSE` | sealed component/partition can be linked into a new release |
| `INCREMENTAL` | only new partitions plus defined downstream dependencies |
| `SELECTIVE_REBUILD` | historical/PIT/QFQ revision invalidates a bounded dependency scope |
| `FULL_REBUILD` | schema/unit/formula incompatibility or undefined dependency edge |

Never replace an undefined dependency edge with a guessed small update. Fail closed to component full rebuild.

## Invalidation rules

- Daily/index revisions invalidate exact code/month partitions and defined downstream blocks.
- Minute stores each stock’s basis window, QFQ denominator and ordered adjustment-factor digest. Denominator changes invalidate that stock’s necessary full history; numerator-only changes invalidate exact dates and dependent windows.
- `stk_limit`, `suspend_d` and daily close/pre-close reference are minute dependencies.
- A canonical-v2 `stk_limit` gap uses the versioned A-share rule calculator when the key is absent or incomplete in the sealed DB partition. Partial rows require exact equality for every existing non-null field before candidate-only completion; complete rows are never overridden. The coverage entry binds the raw partition identity, PIT snapshot digest, rule version, completion count, monthly leaves and sorted exact affected instruments. First-adoption or changed coverage is a code-bounded historical `SELECTIVE_REBUILD`, not a tail append and not an all-market rebuild. A disappearing overlay or unproven raw historical revision remains fail-closed because the planner may not guess a row-level diff.
- PIT changes invalidate affected stock/date ranges.
- Canonical v2 historical D/P daily gaps are sealed as per-partition Tushare `pro_bar` missing-only overlays. After provider exhaustion, only a strict suffix after the last authoritative bar may be represented as terminal non-trading coverage; interior gaps remain blocked. Provider receipts, terminal-suffix count/digest and effective partition roots participate in artifact-ready identity, and overlap never overrides DB rows.
- Moneyflow 5/20 and `PriceStrength_10D` propagate by valid observations, not calendar-day guesses.
- Slow-static forward-fill propagates until the next real observation; sector membership follows its effective interval.
- Index code/role/start/weight mapping changes require a new universe/semantic version and full index-context invalidation.

Every selective action needs a fixture oracle equal to a clean full build for ordered index, dtype, NaN mask and value tolerance.

## Copy-on-write

Hardlink only sealed files that will never be handed to a writable tool. External/untrusted writers never receive the
candidate COW tree. They receive an isolated private root whose exact input/output authority is frozen before launch.

For a large existing file that a writer must replace, use single-copy deferred COW:

1. Omit the final mutation target from the candidate tree during COW preparation.
2. Copy the baseline bytes at most once into the private writer root when the writer needs them.
3. Let the writer finish and quiesce entirely outside the candidate tree.
4. Hash/read back the exact declared private output and adopt it by same-volume atomic rename.
5. Record `baseline_copy_count<=1`, `final_recopy_count=0` and prove the baseline source Merkle is unchanged.

For small trusted in-process mutations, the parent may instead create a private target inode before mutation. In both paths:

1. Expand the complete mutation/create set to exact relative files.
2. Verify every writable target has a private inode and does not alias the source.
3. Reject undeclared files, missing outputs, target-set drift and cross-volume adoption.
4. Record source/target identity, link count and source Merkle.
5. After adoption/mutation, verify the source Merkle is unchanged.

Aggregate H5, Qlib metadata/calendar/instruments and external-writer targets are always new inodes. Unknown mutation capability fails before writing.

## Incremental performance contract

- Index provider overlay is indexed by code/date; minute overlay is indexed by code/date. Do not rescan all overlay rows per batch.
- Daily/minute canonical CSV authority is segmented by instrument and ordered datetime range. Incremental tails append one
  immutable segment; selective repair uses an explicit active-segment override manifest. Do not concatenate the historical
  universe into one in-memory CSV/frame, and reject overlap, out-of-order or undeclared segment replacement.
- Minute source queries and `dump_update` preparation split code lists by the active pressure rung, with a hard profile maximum
  of 20 codes per batch (`20 -> 10 -> 5`). Splitting happens before row materialization; a later split of one unbounded result
  is not compliant.
- Daily reference history starts at the current chunk’s minimum required lookback, never at the dataset start for every chunk.
- DB rows flow through bounded server-side batches directly into partition authority; do not accumulate batch `frames` across chunks.
- Factor date-chunk Parquet is signed intermediate authority. Persist the rolling tail checkpoint; resume must not reread every historical chunk merely to reconstruct state.
- Aggregate H5/static is streamed into a new inode from sealed chunks without requerying unchanged DB partitions.

Component manifest storage v2 and canonical daily/minute lineage v3 are the only
new-writer formats. They use bounded CAS shards rather than repeating all history
in one JSON. Readers retain exact legacy v1/v2 read-only migration support;
legacy metadata at 16 MiB, or predicted to exceed the next 32 MiB hard read
limit, must migrate while it remains readable. A v3 attempt cannot be resumed
by a worker without the matching outer receipt capability.

These contracts currently have source/fixture evidence only. Mixed
daily/minute/factor direct E2E and selective-override clean-full equivalence are
fixture-verified. Real data and real full-scale performance remain separate
pending acceptance evidence.
