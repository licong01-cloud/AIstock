# Candidate receipt and signoff

The direct monthly receipt records what was built and validated; it does not prove full-history source equivalence by hashing all data.

## Required fields

- profile, cutoff, candidate id/path and timestamps;
- PIT authority/rule/coverage;
- baseline candidate reference;
- per-component action and affected instruments/months when available;
- file inventory, schemas, dtypes, row/range summaries and validation outcomes;
- minute repair scope and physical coverage;
- layered value-sampling results;
- QE/HMM producer smoke result;
- production writes, pointer changes and old-candidate overwrites, all zero.

## Signoff

Signoff requires:

1. complete required file structure and cutoff coverage;
2. PIT/universe/instruments/H5-static consistency;
3. duplicate/range/NaN-contract checks;
4. ST, limit, QFQ, moneyflow, index and sector sampling;
5. July repaired minute plus August minute physical coverage;
6. QE/HMM producer smoke PASS.

Full source-content hashes, source re-attestation and production activation are not signoff requirements.
