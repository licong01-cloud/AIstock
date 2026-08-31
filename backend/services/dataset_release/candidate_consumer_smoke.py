"""Shared candidate consumer contracts for supervised QE/HMM smoke tests.

The QE gate deliberately uses Qlib's public ``D.features`` API instead of
reading float bins directly.  The HMM gate is narrower: it proves that the
candidate index H5 implements the shared training/prediction data contract;
it does *not* claim that an existing HMM runtime has been switched to it.
"""

from __future__ import annotations

import math
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd

from .index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    HMM_BENCHMARK_CODE,
    INDEX_H5_COLUMNS,
    INDEX_SCHEMA_VERSION,
    INDEX_UNIVERSE_VERSION,
)
from .stock_schema import QLIB_STOCK_FIELDS
from .streaming_artifacts import iter_hdf_frames


CANDIDATE_CONSUMER_SMOKE_SCHEMA = "dataset_release_candidate_consumer_smoke_v1"
QE_QLIB_READER_CONTRACT = "qe_qlib_d_features_candidate_v1"
HMM_INDEX_H5_READER_CONTRACT = "hmm_shared_index_h5_loader_v1"
QE_STOCK_FIELDS: tuple[str, ...] = tuple(f"${field}" for field in QLIB_STOCK_FIELDS)
QE_DAILY_FIELDS = QE_STOCK_FIELDS
QE_MINUTE_FIELDS = QE_STOCK_FIELDS
QE_INDEX_FIELDS: tuple[str, ...] = QE_STOCK_FIELDS
QE_BENCHMARK_FIELDS: tuple[str, ...] = ("$close/Ref($close,1)-1",)
HMM_SMOKE_FIELDS: tuple[str, ...] = ("idx_close_point", "idx_return_1d")


class CandidateConsumerSmokeError(RuntimeError):
    """The candidate failed the actual consumer-facing read contract."""


class QlibDataApi(Protocol):
    def features(
        self,
        instruments: Sequence[str],
        fields: Sequence[str],
        *,
        start_time: str,
        end_time: str,
        freq: str,
    ) -> pd.DataFrame: ...


class QlibRuntime(Protocol):
    def init(self, **kwargs: object) -> object: ...


@dataclass(frozen=True, slots=True)
class CandidateConsumerSmokeSpec:
    daily_provider_uri: str
    minute_provider_uri: str
    index_h5_path: Path
    cutoff: date
    stock_instrument: str
    expected_index_codes: tuple[str, ...]
    profile: str
    run_id: str
    attempt_id: str
    attempt_fence: int
    release_id: str
    release_digest: str
    staging_relative_path: str
    max_h5_rows: int
    stage_timeout_seconds: int
    execution_kind: str = "production_supervised_wsl"

    def __post_init__(self) -> None:
        expected = tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)
        if self.expected_index_codes != expected:
            raise CandidateConsumerSmokeError("index smoke universe differs from contract")
        if (
            not self.daily_provider_uri.startswith("/")
            or not self.minute_provider_uri.startswith("/")
            or not self.index_h5_path.is_absolute()
        ):
            raise CandidateConsumerSmokeError("consumer smoke paths must be absolute")
        if (
            not self.stock_instrument
            or self.attempt_fence <= 0
            or self.max_h5_rows <= 0
            or self.stage_timeout_seconds < 3_600
        ):
            raise CandidateConsumerSmokeError("consumer smoke identity is invalid")
        if self.execution_kind not in {
            "production_supervised_wsl",
            "fixture_contract_test",
        }:
            raise CandidateConsumerSmokeError("consumer smoke execution kind is invalid")


def run_candidate_consumer_smoke(
    spec: CandidateConsumerSmokeSpec,
    *,
    checkpoint: Callable[[], None],
    qlib_runtime: QlibRuntime | None = None,
    data_api: QlibDataApi | None = None,
) -> dict[str, Any]:
    """Run the shared QE Qlib and future-HMM index contract smoke.

    Production callers omit the two injectable objects.  Tests inject a
    contract-compatible fake and mark the receipt as fixture evidence.
    """

    checkpoint()
    if qlib_runtime is None or data_api is None:
        try:
            import qlib as imported_qlib
            from qlib.constant import REG_CN
            from qlib.data import D
        except ImportError as exc:  # pragma: no cover - exercised by WSL runtime
            raise CandidateConsumerSmokeError("Qlib runtime is unavailable") from exc
        qlib_runtime = imported_qlib
        data_api = D
        region: object = REG_CN
    else:
        region = "cn"

    qlib_runtime.init(
        provider_uri={
            "day": spec.daily_provider_uri,
            "1min": spec.minute_provider_uri,
        },
        region=region,
        clear_mem_cache=True,
    )
    checkpoint()

    cutoff_text = spec.cutoff.isoformat()
    daily = _features(
        data_api,
        (spec.stock_instrument,),
        QE_DAILY_FIELDS,
        start=cutoff_text,
        end=cutoff_text,
        freq="day",
        label="QE daily stock cutoff",
    )
    _assert_exact_keys(
        daily,
        instruments=(spec.stock_instrument,),
        timestamps=(cutoff_text,),
        label="QE daily stock cutoff",
    )
    checkpoint()
    minute = _features(
        data_api,
        (spec.stock_instrument,),
        QE_MINUTE_FIELDS,
        start=f"{cutoff_text} 09:31:00",
        end=f"{cutoff_text} 15:00:00",
        freq="1min",
        label="QE minute stock cutoff",
    )
    minute_timestamps = _minute_session_timestamps(spec.cutoff)
    _assert_exact_keys(
        minute,
        instruments=(spec.stock_instrument,),
        timestamps=minute_timestamps,
        label="QE minute stock cutoff",
    )
    checkpoint()
    indices = _features(
        data_api,
        spec.expected_index_codes,
        QE_INDEX_FIELDS,
        start=cutoff_text,
        end=cutoff_text,
        freq="day",
        label="QE 12-index cutoff",
    )
    observed_indices = _observed_instruments(indices)
    if observed_indices != set(spec.expected_index_codes):
        raise CandidateConsumerSmokeError("Qlib cutoff omits required index codes")
    _assert_exact_keys(
        indices,
        instruments=spec.expected_index_codes,
        timestamps=(cutoff_text,),
        label="QE 12-index cutoff",
    )
    checkpoint()
    benchmark = _features(
        data_api,
        (HMM_BENCHMARK_CODE,),
        QE_BENCHMARK_FIELDS,
        start=cutoff_text,
        end=cutoff_text,
        freq="day",
        label="QE benchmark return cutoff",
    )
    _assert_exact_keys(
        benchmark,
        instruments=(HMM_BENCHMARK_CODE,),
        timestamps=(cutoff_text,),
        label="QE benchmark return cutoff",
    )
    checkpoint()
    hmm = load_hmm_index_contract_smoke(
        spec.index_h5_path,
        cutoff=spec.cutoff,
        max_rows=spec.max_h5_rows,
        checkpoint=checkpoint,
    )
    checkpoint()

    return {
        "schema_version": CANDIDATE_CONSUMER_SMOKE_SCHEMA,
        "status": "PASS",
        "execution_kind": spec.execution_kind,
        "profile": spec.profile,
        "cutoff": cutoff_text,
        "stage_timeout_seconds": spec.stage_timeout_seconds,
        "identity": {
            "run_id": spec.run_id,
            "attempt_id": spec.attempt_id,
            "attempt_fence": spec.attempt_fence,
            "release_id": spec.release_id,
            "release_digest": spec.release_digest,
            "staging_relative_path": spec.staging_relative_path,
        },
        "qe": {
            "status": "PASS",
            "reader_contract": QE_QLIB_READER_CONTRACT,
            "qlib_init_provider_frequencies": ["1min", "day"],
            "stock_instrument": spec.stock_instrument,
            "daily": _frame_evidence(daily, QE_DAILY_FIELDS),
            "minute": _frame_evidence(minute, QE_MINUTE_FIELDS),
            "indices": {
                **_frame_evidence(indices, QE_INDEX_FIELDS),
                "codes": list(spec.expected_index_codes),
            },
            "benchmark": {
                **_frame_evidence(benchmark, QE_BENCHMARK_FIELDS),
                "code": HMM_BENCHMARK_CODE,
            },
        },
        "hmm_index_contract": hmm,
        "consumer_activation": {
            "qe_candidate": "validated_not_activated",
            "existing_hmm": "not_activated_not_switched",
        },
        "safety": {
            "database_writes": 0,
            "provider_database_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        },
    }


def validate_candidate_consumer_smoke_receipt(
    value: Mapping[str, Any],
    *,
    profile: str,
    cutoff: date,
    expected_index_codes: Sequence[str],
    require_production: bool = True,
    expected_identity: Mapping[str, Any] | None = None,
    expected_stage_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Validate bounded semantic evidence without importing Qlib in the parent."""

    expected_kind = "production_supervised_wsl" if require_production else "fixture_contract_test"
    if (
        value.get("schema_version") != CANDIDATE_CONSUMER_SMOKE_SCHEMA
        or value.get("status") != "PASS"
        or value.get("execution_kind") != expected_kind
        or value.get("profile") != profile
        or value.get("cutoff") != cutoff.isoformat()
        or (
            expected_stage_timeout_seconds is not None
            and value.get("stage_timeout_seconds") != expected_stage_timeout_seconds
        )
    ):
        raise CandidateConsumerSmokeError("consumer smoke identity differs")
    if value.get("safety") != {
        "database_writes": 0,
        "provider_database_writes": 0,
        "production_writes": 0,
        "production_deletes": 0,
        "production_pointer_changes": 0,
        "service_process_controls": 0,
    }:
        raise CandidateConsumerSmokeError("consumer smoke safety differs")
    identity = value.get("identity")
    if (
        not isinstance(identity, Mapping)
        or not str(identity.get("run_id", "")).strip()
        or not str(identity.get("attempt_id", "")).strip()
        or type(identity.get("attempt_fence")) is not int
        or int(identity["attempt_fence"]) <= 0
        or not str(identity.get("release_id", "")).strip()
        or len(str(identity.get("release_digest", ""))) != 64
        or not str(identity.get("staging_relative_path", "")).startswith(".staging/")
    ):
        raise CandidateConsumerSmokeError("consumer smoke fence identity is invalid")
    if expected_identity is not None and dict(identity) != dict(expected_identity):
        raise CandidateConsumerSmokeError("consumer smoke fence identity differs")

    qe = value.get("qe")
    hmm = value.get("hmm_index_contract")
    if (
        not isinstance(qe, Mapping)
        or qe.get("status") != "PASS"
        or qe.get("reader_contract") != QE_QLIB_READER_CONTRACT
        or qe.get("qlib_init_provider_frequencies") != ["1min", "day"]
        or not isinstance(hmm, Mapping)
        or hmm.get("status") != "PASS"
        or hmm.get("reader_contract") != HMM_INDEX_H5_READER_CONTRACT
        or hmm.get("schema_version") != INDEX_SCHEMA_VERSION
        or hmm.get("universe_version") != INDEX_UNIVERSE_VERSION
        or hmm.get("benchmark") != HMM_BENCHMARK_CODE
        or hmm.get("fields") != list(HMM_SMOKE_FIELDS)
        or int(hmm.get("rows", 0)) <= 0
        or int(hmm.get("cutoff_rows", 0)) != 1
        or hmm.get("cutoff") != cutoff.isoformat()
        or hmm.get("existing_hmm_consumer_activation") != "not_activated_not_switched"
    ):
        raise CandidateConsumerSmokeError("consumer reader contract differs")
    _validate_feature_evidence(qe.get("daily"), QE_DAILY_FIELDS, cutoff)
    _validate_feature_evidence(qe.get("minute"), QE_MINUTE_FIELDS, cutoff)
    _validate_feature_evidence(qe.get("indices"), QE_INDEX_FIELDS, cutoff)
    _validate_feature_evidence(qe.get("benchmark"), QE_BENCHMARK_FIELDS, cutoff)
    stock = str(qe.get("stock_instrument", "")).upper()
    if not stock:
        raise CandidateConsumerSmokeError("consumer stock instrument is missing")
    _validate_exact_key_evidence(qe["daily"], instruments=(stock,), timestamps=(cutoff.isoformat(),))
    _validate_exact_key_evidence(
        qe["minute"],
        instruments=(stock,),
        timestamps=_minute_session_timestamps(cutoff),
    )
    _validate_exact_key_evidence(
        qe["indices"],
        instruments=expected_index_codes,
        timestamps=(cutoff.isoformat(),),
    )
    _validate_exact_key_evidence(
        qe["benchmark"],
        instruments=(HMM_BENCHMARK_CODE,),
        timestamps=(cutoff.isoformat(),),
    )
    if (
        qe["indices"].get("codes") != list(expected_index_codes)
        or qe["benchmark"].get("code") != HMM_BENCHMARK_CODE
        or value.get("consumer_activation")
        != {
            "qe_candidate": "validated_not_activated",
            "existing_hmm": "not_activated_not_switched",
        }
    ):
        raise CandidateConsumerSmokeError("consumer index/activation evidence differs")
    return dict(value)


def _validate_feature_evidence(value: object, fields: Sequence[str], cutoff: date) -> None:
    if not isinstance(value, Mapping):
        raise CandidateConsumerSmokeError("Qlib feature evidence is missing")
    if (
        value.get("fields") != list(fields)
        or int(value.get("rows", 0)) <= 0
        or int(value.get("finite_values", 0)) < int(value.get("rows", 0)) * len(fields)
        or value.get("end") != cutoff.isoformat()
        or int(value.get("unique_keys", 0)) != int(value.get("rows", 0))
        or not _is_sha256(value.get("key_digest"))
    ):
        raise CandidateConsumerSmokeError("Qlib feature evidence contract differs")
    maximum = value.get("max_abs_value")
    if not isinstance(maximum, (int, float)) or not math.isfinite(float(maximum)):
        raise CandidateConsumerSmokeError("Qlib feature evidence is non-finite")


def _validate_exact_key_evidence(
    value: Mapping[str, Any],
    *,
    instruments: Sequence[str],
    timestamps: Sequence[str],
) -> None:
    expected = [f"{str(instrument).upper()}|{timestamp}" for instrument in instruments for timestamp in timestamps]
    if (
        int(value.get("rows", -1)) != len(expected)
        or int(value.get("unique_keys", -1)) != len(expected)
        or value.get("instruments") != sorted(str(item).upper() for item in instruments)
        or value.get("first_timestamp") != min(timestamps)
        or value.get("last_timestamp") != max(timestamps)
        or value.get("key_digest") != _key_digest(expected)
    ):
        raise CandidateConsumerSmokeError("Qlib exact key evidence differs")


def load_hmm_index_contract_smoke(
    path: Path,
    *,
    cutoff: date,
    max_rows: int,
    checkpoint: Callable[[], None],
) -> dict[str, Any]:
    """Load 000300 from the shared index H5 without claiming runtime adoption."""

    rows = 0
    cutoff_rows = 0
    for frame in iter_hdf_frames(path, chunksize=max_rows):
        checkpoint()
        if tuple(str(value) for value in frame.columns) != INDEX_H5_COLUMNS:
            raise CandidateConsumerSmokeError("HMM index H5 ordered columns drifted")
        try:
            benchmark = frame.xs(HMM_BENCHMARK_CODE, level="instrument", drop_level=False)
        except KeyError:
            continue
        if benchmark.empty:
            continue
        numeric = benchmark.loc[:, list(HMM_SMOKE_FIELDS)].apply(pd.to_numeric, errors="coerce")
        if not np.isfinite(numeric.to_numpy(dtype=float)).all():
            raise CandidateConsumerSmokeError("HMM index H5 contains non-finite values")
        dates = pd.to_datetime(benchmark.index.get_level_values("datetime"))
        cutoff_rows += int((dates.strftime("%Y-%m-%d") == cutoff.isoformat()).sum())
        rows += len(benchmark)
    if rows <= 0 or cutoff_rows != 1:
        raise CandidateConsumerSmokeError("HMM 000300 shared index H5 does not contain exactly one cutoff row")
    return {
        "status": "PASS",
        "reader_contract": HMM_INDEX_H5_READER_CONTRACT,
        "schema_version": INDEX_SCHEMA_VERSION,
        "universe_version": INDEX_UNIVERSE_VERSION,
        "benchmark": HMM_BENCHMARK_CODE,
        "fields": list(HMM_SMOKE_FIELDS),
        "rows": rows,
        "cutoff_rows": cutoff_rows,
        "cutoff": cutoff.isoformat(),
        "existing_hmm_consumer_activation": "not_activated_not_switched",
    }


def _features(
    api: QlibDataApi,
    instruments: Sequence[str],
    fields: Sequence[str],
    *,
    start: str,
    end: str,
    freq: str,
    label: str,
) -> pd.DataFrame:
    frame = api.features(
        list(instruments),
        list(fields),
        start_time=start,
        end_time=end,
        freq=freq,
    )
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise CandidateConsumerSmokeError(f"{label} returned no rows")
    if len(frame.columns) != len(fields):
        raise CandidateConsumerSmokeError(f"{label} returned unexpected fields")
    numeric = frame.apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(numeric).all():
        raise CandidateConsumerSmokeError(f"{label} returned NULL/non-finite values")
    dates = _observed_dates(frame)
    if not dates or dates[-1] != end[:10]:
        raise CandidateConsumerSmokeError(f"{label} does not reach cutoff")
    return frame


def _observed_dates(frame: pd.DataFrame) -> list[str]:
    if not isinstance(frame.index, pd.MultiIndex) or "datetime" not in frame.index.names:
        raise CandidateConsumerSmokeError("Qlib D.features index contract drifted")
    values = pd.to_datetime(frame.index.get_level_values("datetime"))
    return sorted(set(values.strftime("%Y-%m-%d")))


def _observed_instruments(frame: pd.DataFrame) -> set[str]:
    if not isinstance(frame.index, pd.MultiIndex) or "instrument" not in frame.index.names:
        raise CandidateConsumerSmokeError("Qlib D.features instrument index drifted")
    return {str(value).upper() for value in frame.index.get_level_values("instrument")}


def _frame_evidence(frame: pd.DataFrame, fields: Sequence[str]) -> dict[str, Any]:
    values = frame.apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
    keys = _canonical_frame_keys(frame)
    return {
        "rows": len(frame),
        "fields": list(fields),
        "start": _observed_dates(frame)[0],
        "end": _observed_dates(frame)[-1],
        "finite_values": int(values.size),
        "max_abs_value": float(np.abs(values).max()),
        "unique_keys": len(set(keys)),
        "instruments": sorted(_observed_instruments(frame)),
        "first_timestamp": min(key.split("|", 1)[1] for key in keys),
        "last_timestamp": max(key.split("|", 1)[1] for key in keys),
        "key_digest": _key_digest(keys),
    }


def _assert_exact_keys(
    frame: pd.DataFrame,
    *,
    instruments: Sequence[str],
    timestamps: Sequence[str],
    label: str,
) -> None:
    expected = [f"{str(instrument).upper()}|{timestamp}" for instrument in instruments for timestamp in timestamps]
    actual = _canonical_frame_keys(frame)
    if len(actual) != len(set(actual)) or sorted(actual) != sorted(expected):
        raise CandidateConsumerSmokeError(f"{label} keys differ from exact contract")


def _canonical_frame_keys(frame: pd.DataFrame) -> list[str]:
    if not isinstance(frame.index, pd.MultiIndex) or not {
        "instrument",
        "datetime",
    }.issubset(frame.index.names):
        raise CandidateConsumerSmokeError("Qlib D.features key index drifted")
    instruments = frame.index.get_level_values("instrument")
    timestamps = pd.to_datetime(frame.index.get_level_values("datetime"))
    return [
        f"{str(instrument).upper()}|{_canonical_timestamp(timestamp)}"
        for instrument, timestamp in zip(instruments, timestamps)
    ]


def _canonical_timestamp(value: pd.Timestamp) -> str:
    stamp = pd.Timestamp(value)
    if stamp.time() == time(0, 0):
        return stamp.date().isoformat()
    return stamp.isoformat(sep=" ", timespec="seconds")


def _minute_session_timestamps(value: date) -> tuple[str, ...]:
    morning = tuple(datetime.combine(value, time(9, 31)) + timedelta(minutes=offset) for offset in range(120))
    afternoon = tuple(datetime.combine(value, time(13, 1)) + timedelta(minutes=offset) for offset in range(120))
    return tuple(item.isoformat(sep=" ", timespec="seconds") for item in (*morning, *afternoon))


def _key_digest(keys: Sequence[str]) -> str:
    return hashlib.sha256(
        json.dumps(sorted(keys), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


__all__ = [
    "CANDIDATE_CONSUMER_SMOKE_SCHEMA",
    "CandidateConsumerSmokeError",
    "CandidateConsumerSmokeSpec",
    "HMM_INDEX_H5_READER_CONTRACT",
    "QE_QLIB_READER_CONTRACT",
    "load_hmm_index_contract_smoke",
    "run_candidate_consumer_smoke",
    "validate_candidate_consumer_smoke_receipt",
]
