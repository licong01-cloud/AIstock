"""Stable Shenwan industry code encoders shared by export and runtime paths."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

UNKNOWN_L2_CODE_ID = -1


def build_sw_l2_code_map(index_codes: Iterable[Any]) -> dict[str, int]:
    """Build the canonical SW L2 code -> integer id map."""

    normalized = sorted(
        {
            str(code).strip()
            for code in index_codes
            if code is not None and str(code).strip()
        }
    )
    return {code: idx for idx, code in enumerate(normalized)}


def load_sw_l2_code_map(conn: Any) -> dict[str, int]:
    """Load the canonical SW L2 code map from ``market.sw_index_classify``."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT index_code
            FROM market.sw_index_classify
            WHERE level = 'L2'
              AND index_code IS NOT NULL
            ORDER BY index_code ASC
            """
        )
        return build_sw_l2_code_map(row[0] for row in cur.fetchall())


def encode_l2_codes(
    l2_codes: Sequence[Any],
    code_map: Mapping[str, int],
    *,
    unknown_id: int = UNKNOWN_L2_CODE_ID,
) -> list[int]:
    """Encode SW L2 codes, using ``unknown_id`` for missing or unmapped rows."""

    encoded: list[int] = []
    for code in l2_codes:
        if code is None:
            encoded.append(unknown_id)
            continue
        key = str(code).strip()
        encoded.append(int(code_map.get(key, unknown_id)) if key else unknown_id)
    return encoded
