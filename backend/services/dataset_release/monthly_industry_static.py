"""Reuse completed industry facts, with input-bound private batch checkpoints."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes
from .errors import SourceManifestError
from .monthly_file_identity import file_sha256
from .monthly_build_resume import _plain
from .monthly_legacy_prefix import _plain_chain, _signature


def read_checkpoint(path: Path, *, binding: dict, output: Path):
    if path.is_symlink() or output.is_symlink():
        raise SourceManifestError("retained industry output is a link")
    _plain(path)
    _plain(output)
    if not path.exists():
        if output.exists():
            raise SourceManifestError("unsealed industry output retained; cannot overwrite it")
        return None
    if path.stat().st_size > 1024 * 1024:
        raise SourceManifestError("industry checkpoint exceeds metadata bound")
    try:
        value = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise SourceManifestError("retained industry checkpoint is unreadable") from exc
    if (not isinstance(value, dict) or value.get("binding") != binding or value.get("signature") != list(_signature(output))
            or value.get("sha256") != file_sha256(output)):
        raise SourceManifestError("retained industry output changed or input identity differs")
    return value


def seal_checkpoint(path: Path, *, binding: dict, output: Path, **metadata):
    value = {"binding": binding, "signature": list(_signature(output)),
             "sha256": file_sha256(output), **metadata}
    with path.open("xb") as stream:
        stream.write(canonical_json_bytes(value) + b"\n")
    return value


def align_static(frame, sector, *, fallback):
    """Reuse the exact stock/date facts already built, never re-aggregate a cohort."""
    columns = [name for name in frame if name == "l2_code_id" or name.startswith("sw2_")]
    days = frame.index.get_level_values("datetime")
    left, right = str(days.min().date()), str(days.max().date())
    facts = sector.select("data", where=[f"datetime >= {left!r}", f"datetime <= {right!r}"], columns=columns)
    if facts.index.has_duplicates:
        raise SourceManifestError("rebuilt sector keys are duplicated")
    found = frame.index.isin(facts.index)
    actual = frame.copy()
    if found.any():
        aligned = facts.reindex(frame.index[found])
        for name in columns:
            actual.loc[found, name] = aligned[name].to_numpy().astype(frame[name].dtype)
    if not found.all():
        # Preserve the previous semantics for genuine static-only keys. Never
        # invent an H5 row, default classification or previous-date quote.
        actual.loc[~found] = fallback(frame.loc[~found])
    if np.isinf(actual[columns].to_numpy(dtype=float)).any():
        raise SourceManifestError("rebuilt static industry value is infinite")
    return actual


def materialize_static(source_path, sector_path, target, *, binding, max_rows,
                       fallback, checkpoint, progress):
    """Keep sealed batches across interruption; publish only a complete Parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    changed = 0
    parts = []
    source_digest = hashlib.sha256(canonical_json_bytes(binding)).hexdigest()
    _plain(target)
    with pd.HDFStore(sector_path, "r") as sector, pq.ParquetFile(source_path) as source:
        for ordinal, batch in enumerate(source.iter_batches(batch_size=max_rows)):
            checkpoint()
            frame = pa.Table.from_batches([batch]).to_pandas()
            part = target.parent / f"static-part-{ordinal:06d}.parquet"
            journal = part.with_suffix(".json")
            part_binding = {"source_digest": source_digest, "batch_size": max_rows,
                            "ordinal": ordinal, "rows": len(frame)}
            retained = read_checkpoint(journal, binding=part_binding, output=part)
            reused = retained is not None
            if retained is None:
                progress({"phase": "INDUSTRY_STATIC_BATCH_START", "dataset": "static_factors", "partition_key": str(ordinal)})
                actual = align_static(frame, sector, fallback=fallback)
                columns = [name for name in frame if name == "l2_code_id" or name.startswith("sw2_")]
                unrelated = [name for name in frame if name not in columns]
                pd.testing.assert_frame_equal(frame[unrelated], actual[unrelated], check_exact=True)
                count = int((frame.l2_code_id != actual.l2_code_id).sum())
                pq.write_table(pa.Table.from_pandas(actual, schema=source.schema_arrow, preserve_index=True), part)
                retained = seal_checkpoint(journal, binding=part_binding, output=part, classification_cells_changed=count)
            changed += retained["classification_cells_changed"]
            parts.append(part)
            progress({"phase": "INDUSTRY_STATIC_BATCH_SEALED", "dataset": "static_factors",
                      "partition_key": str(ordinal), "rows_sealed": ordinal * max_rows + len(frame), "reused": reused})
        if not target.exists():
            assembling = target.with_name(f".static-assembling-{uuid4().hex}.parquet")
            with pq.ParquetWriter(assembling, source.schema_arrow) as writer:
                for part in parts:
                    checkpoint()
                    writer.write_table(pq.read_table(part))
            assembling.rename(target)
        _plain_chain(target)
        # Independently read every changed component batch; unrelated price
        # components and historical source quality audits are not repeated.
        with pq.ParquetFile(target) as result:
            for ordinal, (before, after) in enumerate(zip(source.iter_batches(batch_size=max_rows),
                                                       result.iter_batches(batch_size=max_rows), strict=True)):
                checkpoint()
                expected = align_static(pa.Table.from_batches([before]).to_pandas(), sector, fallback=fallback)
                pd.testing.assert_frame_equal(expected, pa.Table.from_batches([after]).to_pandas(), check_exact=True)
                progress({"phase": "INDUSTRY_STATIC_READBACK", "partition_key": str(ordinal),
                          "rows_validated": ordinal * max_rows + len(expected)})
    return changed
