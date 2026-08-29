from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import tables


FACTOR_NAME = "m_sector_participation_gap_v2"
MEMBERSHIP_COLUMN = "l2_code_id"
FAST_WINDOW = 5
SLOW_WINDOW = 20
REPO_ROOT = Path(__file__).resolve().parents[3]
REQUIRED_SECTOR_COLUMNS = (
    "sw2_amount",
    "sw2_mf_buy_sm_amt",
    "sw2_mf_sell_sm_amt",
    "sw2_mf_buy_lg_amt",
    "sw2_mf_sell_lg_amt",
    "sw2_mf_buy_elg_amt",
    "sw2_mf_sell_elg_amt",
)


def _validated_frame(frame: pd.DataFrame, *, name: str) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError(f"{name} index must be a MultiIndex")
    if list(frame.index.names) != ["datetime", "instrument"]:
        raise ValueError(f"{name} index names must be datetime,instrument")
    if frame.index.has_duplicates:
        raise ValueError(f"{name} index must be unique")
    if frame.index.is_monotonic_increasing:
        return frame
    return frame.sort_index()


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    raw = frame[column]
    numeric = pd.to_numeric(raw, errors="coerce")
    if (raw.notna() & numeric.isna()).any():
        raise ValueError(f"{column} contains non-numeric values")
    if not np.isfinite(numeric.dropna().to_numpy(dtype=np.float64)).all():
        raise ValueError(f"{column} contains non-finite values")
    return numeric.astype("float64")


def calculate_factor(
    sector_frame: pd.DataFrame,
    membership_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate the daily PIT SW-L2 participation-gap signal from frozen files."""

    sector = _validated_frame(sector_frame, name="sector_data")
    membership = _validated_frame(membership_frame, name="static_factors")
    missing = [column for column in REQUIRED_SECTOR_COLUMNS if column not in sector]
    if missing:
        raise ValueError(f"sector_data missing required columns: {','.join(missing)}")
    if MEMBERSHIP_COLUMN not in membership:
        raise ValueError(f"static_factors missing required column: {MEMBERSHIP_COLUMN}")

    source_l2 = membership[MEMBERSHIP_COLUMN].reindex(sector.index)
    l2_raw = pd.to_numeric(source_l2, errors="coerce")
    invalid_l2 = source_l2.notna() & (
        l2_raw.isna()
        | ~np.isfinite(l2_raw)
        | ~np.equal(l2_raw, np.floor(l2_raw))
    )
    if invalid_l2.any():
        raise ValueError("l2_code_id must be an integer category when present")

    frame = pd.DataFrame(index=sector.index)
    frame[MEMBERSHIP_COLUMN] = l2_raw
    for column in REQUIRED_SECTOR_COLUMNS:
        frame[column] = _numeric_column(sector, column)

    known = frame[MEMBERSHIP_COLUMN].notna() & frame[MEMBERSHIP_COLUMN].gt(0)
    if not known.any():
        return pd.DataFrame(
            {FACTOR_NAME: pd.Series(dtype="float32")},
            index=pd.MultiIndex.from_arrays(
                [pd.DatetimeIndex([], name="datetime"), pd.Index([], name="instrument")]
            ),
        )

    known_frame = frame.loc[known].copy()
    known_frame[MEMBERSHIP_COLUMN] = known_frame[MEMBERSHIP_COLUMN].astype("int32")
    dates = known_frame.index.get_level_values("datetime")
    group_keys = [dates, known_frame[MEMBERSHIP_COLUMN]]

    uniqueness = known_frame[list(REQUIRED_SECTOR_COLUMNS)].groupby(
        group_keys, sort=True
    ).nunique(dropna=False)
    inconsistent = uniqueness.gt(1).any(axis=1)
    if inconsistent.any():
        first = inconsistent[inconsistent].index[0]
        raise ValueError(
            "sector_data values differ across members for "
            f"datetime={first[0]} l2_code_id={first[1]}"
        )

    sector_day = known_frame.groupby(group_keys, sort=True).first()
    sector_day.index.names = ["datetime", MEMBERSHIP_COLUMN]
    valid = sector_day[list(REQUIRED_SECTOR_COLUMNS)].notna().all(axis=1)
    valid &= sector_day["sw2_amount"].gt(0)

    large_net = (
        sector_day["sw2_mf_buy_lg_amt"]
        - sector_day["sw2_mf_sell_lg_amt"]
        + sector_day["sw2_mf_buy_elg_amt"]
        - sector_day["sw2_mf_sell_elg_amt"]
    )
    small_net = (
        sector_day["sw2_mf_buy_sm_amt"] - sector_day["sw2_mf_sell_sm_amt"]
    )
    amount = sector_day["sw2_amount"]
    large_ratio = (large_net / amount).where(valid)
    small_ratio = (small_net / amount).where(valid)

    large_rank = large_ratio.groupby(level="datetime").rank(
        method="average", pct=True
    )
    small_rank = small_ratio.groupby(level="datetime").rank(
        method="average", pct=True
    )
    gap_wide = (large_rank - small_rank).unstack(MEMBERSHIP_COLUMN).sort_index()
    fast = gap_wide.rolling(FAST_WINDOW, min_periods=FAST_WINDOW).mean()
    slow = gap_wide.rolling(SLOW_WINDOW, min_periods=SLOW_WINDOW).mean()
    signal_wide = fast - slow
    signal = pd.Series(
        signal_wide.to_numpy().reshape(-1),
        index=pd.MultiIndex.from_product(
            [signal_wide.index, signal_wide.columns],
            names=["datetime", MEMBERSHIP_COLUMN],
        ),
    )

    lookup = pd.MultiIndex.from_arrays(
        [dates, known_frame[MEMBERSHIP_COLUMN].to_numpy()],
        names=["datetime", MEMBERSHIP_COLUMN],
    )
    mapped = signal.reindex(lookup)
    mapped.index = known_frame.index
    result = mapped.rename(FACTOR_NAME).dropna().astype("float32").to_frame()
    result.index.names = ["datetime", "instrument"]
    return result.sort_index()


def _decoded_strings(values: np.ndarray) -> list[str]:
    return [
        value.decode("utf-8") if isinstance(value, bytes) else str(value)
        for value in values
    ]


def _read_sector_window(
    path: Path,
    *,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    with tables.open_file(path, mode="r") as handle:
        if "/data" not in handle:
            raise ValueError("sector_data must contain the /data HDF group")
        group = handle.root.data
        required_nodes = (
            "axis1_label0",
            "axis1_label1",
            "axis1_level0",
            "axis1_level1",
            "block0_items",
            "block0_values",
        )
        missing_nodes = [name for name in required_nodes if not hasattr(group, name)]
        if missing_nodes:
            raise ValueError(
                "sector_data fixed-format contract missing nodes: "
                + ",".join(missing_nodes)
            )

        columns = _decoded_strings(group.block0_items[:])
        if len(columns) != len(set(columns)):
            raise ValueError("sector_data fixed-format columns must be unique")
        missing_columns = [
            column for column in REQUIRED_SECTOR_COLUMNS if column not in columns
        ]
        if missing_columns:
            raise ValueError(
                "sector_data missing required columns: " + ",".join(missing_columns)
            )
        column_positions = [columns.index(column) for column in REQUIRED_SECTOR_COLUMNS]

        date_levels = pd.DatetimeIndex(
            pd.to_datetime(group.axis1_level0[:]), name="datetime"
        )
        instrument_levels = pd.Index(
            _decoded_strings(group.axis1_level1[:]), name="instrument"
        )
        date_codes = np.asarray(group.axis1_label0[:], dtype=np.int64)
        if date_levels.has_duplicates or not date_levels.is_monotonic_increasing:
            raise ValueError("sector_data datetime levels must be sorted and unique")
        if date_codes.size and (
            date_codes.min() < 0
            or date_codes.max() >= len(date_levels)
            or np.any(date_codes[1:] < date_codes[:-1])
        ):
            raise ValueError("sector_data datetime codes must be valid and sorted")

        start_level = 0
        if start_date is not None:
            requested_level = int(date_levels.searchsorted(start_date, side="left"))
            start_level = max(0, requested_level - (SLOW_WINDOW - 1))
        stop_level = len(date_levels)
        if end_date is not None:
            stop_level = int(
                date_levels.searchsorted(
                    end_date + pd.Timedelta(days=1), side="left"
                )
            )
        start_row = int(np.searchsorted(date_codes, start_level, side="left"))
        stop_row = int(np.searchsorted(date_codes, stop_level, side="left"))
        if start_row >= stop_row:
            raise ValueError("sector_data has no rows in the requested date window")

        instrument_codes = np.asarray(
            group.axis1_label1[start_row:stop_row], dtype=np.int64
        )
        if instrument_codes.size and (
            instrument_codes.min() < 0
            or instrument_codes.max() >= len(instrument_levels)
        ):
            raise ValueError("sector_data instrument codes must be valid")
        selected_date_codes = date_codes[start_row:stop_row]
        values = np.asarray(
            group.block0_values[start_row:stop_row, column_positions]
        )
        index = pd.MultiIndex(
            levels=[date_levels, instrument_levels],
            codes=[selected_date_codes, instrument_codes],
            names=["datetime", "instrument"],
            verify_integrity=True,
        )
        frame = pd.DataFrame(values, index=index, columns=REQUIRED_SECTOR_COLUMNS)
    if frame.empty:
        raise ValueError("sector_data has no rows in the requested date window")
    return frame


def _normalized_date(value: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    parsed = pd.Timestamp(value)
    if parsed.tz is not None:
        parsed = parsed.tz_localize(None)
    return parsed.normalize()


def _validate_membership_schema(path: Path) -> None:
    columns = set(pq.ParquetFile(path).schema.names)
    required = {MEMBERSHIP_COLUMN, "datetime", "instrument"}
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(
            "static_factors missing required columns: " + ",".join(missing)
        )


def compute_factor(
    *,
    data_dir: Path,
    output_path: Path,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
) -> None:
    output_path = output_path.resolve()
    if output_path.is_relative_to(REPO_ROOT):
        raise ValueError("output_path must be outside the repository/worktree")
    normalized_start = _normalized_date(start_date)
    normalized_end = _normalized_date(end_date)
    if (
        normalized_start is not None
        and normalized_end is not None
        and normalized_start > normalized_end
    ):
        raise ValueError("start_date must not be after end_date")

    membership_path = data_dir / "static_factors.parquet"
    _validate_membership_schema(membership_path)
    sector = _read_sector_window(
        data_dir / "sector_data.h5",
        start_date=normalized_start,
        end_date=normalized_end,
    )
    loaded_dates = sector.index.get_level_values("datetime")
    membership_start = pd.Timestamp(loaded_dates.min()).normalize()
    membership_end = pd.Timestamp(loaded_dates.max()).normalize()
    membership = pd.read_parquet(
        membership_path,
        columns=[MEMBERSHIP_COLUMN],
        filters=[
            ("datetime", ">=", membership_start),
            ("datetime", "<=", membership_end),
        ],
    )
    result = calculate_factor(sector, membership)
    if normalized_start is not None:
        result = result.loc[
            result.index.get_level_values("datetime") >= normalized_start
        ]
    if normalized_end is not None:
        result = result.loc[
            result.index.get_level_values("datetime") <= normalized_end
        ]
    if result.empty:
        raise ValueError("factor output is empty for the requested date window")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_hdf(output_path, key="data", mode="w")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the file-only QE sector participation-gap candidate."
    )
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--output-path", required=True, type=Path)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    return parser


def main() -> int:
    args = _parser().parse_args()
    compute_factor(
        data_dir=args.data_dir,
        output_path=args.output_path,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
