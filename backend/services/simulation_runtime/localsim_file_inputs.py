"""Consumer-owned frozen-file lifetime; never resolves an active pointer on retry."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy

from backend.data_service.frozen_dataset import FrozenDataset, FrozenDatasetError, digest


FILE_SOURCE_ID = "dataset_files.frozen_market.v1"
FILE_BINDING_KEY = "localsim_historical_file_binding_v1"
_DATASET: ContextVar[FrozenDataset | None] = ContextVar("localsim_frozen_file_inputs", default=None)


def frozen_file_binding(release):
    metadata = release.release_config_json.get("metadata") or {}
    binding = metadata.get(FILE_BINDING_KEY)
    if binding is None:
        return None
    if digest({k: v for k, v in binding.items() if k != "binding_hash"}) != binding.get("binding_hash"):
        raise FrozenDatasetError("release frozen-file binding digest mismatch")
    return deepcopy(binding)


def current_file_dataset(*, required=False):
    dataset = _DATASET.get()
    if dataset is None and required:
        raise FrozenDatasetError("file runtime requires an explicitly bound LocalSIM run")
    if dataset is not None:
        dataset.assert_unchanged()
    return dataset


@contextmanager
def localsim_file_inputs(binding, *, run_id):
    if _DATASET.get() is not None:
        raise FrozenDatasetError("cannot replace a live LocalSIM frozen input scope")
    dataset = FrozenDataset(binding, consumer_scope="LOCALSIM", run_id=run_id)
    token = _DATASET.set(dataset)
    try:
        yield dataset
        dataset.assert_unchanged()
    finally:
        _DATASET.reset(token)
        dataset.close()


def resolve_replay_source(authority, *, job, release, end_trade_date=None, require_latest_completed=False):
    kwargs = dict(historical_source_id=job.historical_source_id, start_trade_date=job.start_trade_date,
                  end_trade_date=end_trade_date or job.end_trade_date, require_latest_completed=require_latest_completed)
    if job.historical_source_id == FILE_SOURCE_ID:
        binding = frozen_file_binding(release)
        if binding is None or binding["binding_hash"] != job.historical_source_sha256:
            raise FrozenDatasetError("replay source does not match its frozen release")
        kwargs["file_binding"] = binding
    return authority.resolve(**kwargs)


def historical_suspension_provider(dataset):
    from backend.services.simulation_data.frozen_file_adapters import (
        FileLimitPriceProvider, FileTradingCalendar, file_reference_prices, no_market_sql,
    )
    from backend.services.simulation_execution.localsim.suspension import LocalSimHistoricalSuspensionProvider

    return LocalSimHistoricalSuspensionProvider(
        conn_factory=no_market_sql, calendar=FileTradingCalendar(dataset),
        reference_loader=lambda **kwargs: file_reference_prices(dataset, **kwargs),
        limit_provider=FileLimitPriceProvider(dataset),
        event_loader=lambda symbols, day: dataset.suspension_rows(symbols, day, day),
        file_binding_hash=dataset.identity,
    )
