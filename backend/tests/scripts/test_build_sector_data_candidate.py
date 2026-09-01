from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.industry_pit.candidate_builder import FrozenDenominator, UniverseSpan
from backend.services.sector_data_builder import SectorDataCandidateDay
from scripts import build_sector_data_candidate as cli


def _denominator() -> FrozenDenominator:
    return FrozenDenominator.build(
        window_start=date(2022, 1, 4),
        window_end=date(2022, 1, 5),
        trading_dates=(date(2022, 1, 4), date(2022, 1, 5)),
        universe_spans=(
            UniverseSpan("300741.SZ", date(2022, 1, 4), date(2022, 1, 5)),
            UniverseSpan("605077.SH", date(2022, 1, 5), date(2022, 1, 5)),
        ),
    )


def _authority(denominator: FrozenDenominator):
    receipt = SimpleNamespace(
        denominator_digest=denominator.digest,
        frozen_denominator=denominator.total_opportunities,
    )
    return SimpleNamespace(
        preflight_report={
            "window_start": denominator.window_start.isoformat(),
            "window_end": denominator.window_end.isoformat(),
            "source_diagnostics": {
                "frozen_universe": {
                    "universe_key": "aistock_equity_pit_canonical_v2",
                    "rule_version": "rule-v2",
                    "scope": "canonical_all_listed",
                    "state_start": "2020-01-01",
                    "state_end": "2026-07-31",
                    "status": "ready",
                    "dirty": False,
                    "source_fingerprint_sha256": "a" * 64,
                }
            },
        },
        classification_receipt=receipt,
        index_membership_receipt=receipt,
        manifest={"bundle_hash": "b" * 64},
    )


class _Connection:
    def __init__(self):
        self.sessions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def set_session(self, **kwargs):
        self.sessions.append(kwargs)


def _args(tmp_path: Path, *, dry_run: bool) -> argparse.Namespace:
    return argparse.Namespace(
        industry_candidate_root=tmp_path / "industry",
        artifact_root=tmp_path / "sector",
        db_env_file=None,
        start_date=None,
        end_date=None,
        symbol=[],
        max_trading_days=None,
        chunk_trading_days=5,
        progress_every_trading_days=50,
        dry_run=dry_run,
    )


def test_help_has_no_database_or_artifact_side_effects():
    result = subprocess.run(
        [sys.executable, "scripts/build_sector_data_candidate.py", "--help"],
        cwd=Path(__file__).resolve().parents[3],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--industry-candidate-root" in result.stdout
    assert "--dry-run" in result.stdout


def test_denominator_contract_requires_a_complete_ready_receipt():
    denominator = _denominator()
    contract = cli._denominator_contract(_authority(denominator))
    assert contract["universe_key"] == "aistock_equity_pit_canonical_v2"
    assert contract["window_start"] == "2022-01-04"

    invalid = _authority(denominator)
    invalid.preflight_report["source_diagnostics"]["frozen_universe"]["dirty"] = True
    with pytest.raises(cli.SectorDataBuildContractError, match="ready/clean"):
        cli._denominator_contract(invalid)


def test_symbols_by_date_preserves_frozen_opportunity_denominator():
    denominator = _denominator()
    result = cli._symbols_by_date(
        denominator,
        denominator.trading_dates,
        selected_symbols=frozenset(),
    )
    assert result == {
        date(2022, 1, 4): ("300741.SZ",),
        date(2022, 1, 5): ("300741.SZ", "605077.SH"),
    }
    assert sum(map(len, result.values())) == denominator.total_opportunities


def test_candidate_scope_uses_resolved_authority_window_not_cli_omission():
    denominator = _denominator()

    assert cli._candidate_scope(
        denominator,
        selected_dates=denominator.trading_dates,
        selected_symbols=frozenset(),
        max_trading_days=None,
        expected_opportunities=denominator.total_opportunities,
    ) == "full"
    assert cli._candidate_scope(
        denominator,
        selected_dates=denominator.trading_dates[1:],
        selected_symbols=frozenset(),
        max_trading_days=None,
        expected_opportunities=2,
    ) == "sample"
    assert cli._candidate_scope(
        denominator,
        selected_dates=denominator.trading_dates,
        selected_symbols=frozenset({"300741.SZ"}),
        max_trading_days=None,
        expected_opportunities=2,
    ) == "sample"


def test_source_identity_collapses_exact_duplicates_and_rejects_conflicts():
    target = {}
    cli._insert_source_row(
        target,
        source="market.moneyflow_ts",
        identity="300741.SZ",
        row={"net_mf_amount": 1},
    )
    cli._insert_source_row(
        target,
        source="market.moneyflow_ts",
        identity="300741.SZ",
        row={"net_mf_amount": 1},
    )
    assert target == {"300741.SZ": {"net_mf_amount": 1}}

    with pytest.raises(cli.SectorDataBuildContractError, match="conflicting rows"):
        cli._insert_source_row(
            target,
            source="market.moneyflow_ts",
            identity="300741.SZ",
            row={"net_mf_amount": 2},
        )


@pytest.mark.parametrize("explicit_window", [False, True])
def test_build_dry_run_is_bounded_and_never_calls_writer(monkeypatch, tmp_path, explicit_window):
    denominator = _denominator()
    authority = _authority(denominator)
    connection = _Connection()
    monkeypatch.setattr(cli, "_git_identity", lambda: {"commit": "1" * 40, "tree": "2" * 40, "dirty": True})
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "read_candidate_bundle", lambda **kwargs: authority)
    monkeypatch.setattr(cli, "get_conn", lambda: connection)
    monkeypatch.setattr(cli, "_read_frozen_denominator", lambda *args, **kwargs: denominator)
    monkeypatch.setattr(
        cli,
        "_source_days",
        lambda *args, **kwargs: (SimpleNamespace(trade_date=value) for value in denominator.trading_dates),
    )

    class _Builder:
        def __init__(self, **kwargs):
            pass

        def build_day(self, source):
            assignments = (
                {"status": "resolved", "alignment_state": "aligned", "unavailable_reasons": []},
            ) if source.trade_date == date(2022, 1, 4) else (
                {"status": "resolved", "alignment_state": "aligned", "unavailable_reasons": []},
                {
                    "status": "unavailable",
                    "alignment_state": "unavailable",
                    "unavailable_reasons": ["index_membership:membership_boundary_unavailable"],
                },
            )
            return SectorDataCandidateDay(source.trade_date, assignments, ())

    monkeypatch.setattr(cli, "SectorDataCandidateBuilder", _Builder)
    monkeypatch.setattr(
        cli,
        "write_sector_data_candidate",
        lambda **kwargs: pytest.fail("dry-run must not write a candidate"),
    )

    args = _args(tmp_path, dry_run=True)
    if explicit_window:
        args.start_date = denominator.window_start.isoformat()
        args.end_date = denominator.window_end.isoformat()
    result = cli.build(args)

    assert result["status"] == "PASS_DRY_RUN"
    assert result["candidate_scope"] == "full"
    assert result["assignment_rows"] == 3
    assert result["status_counts"] == {"resolved": 2, "unavailable": 1}
    assert result["database_writes"] == 0
    assert connection.sessions == [{"readonly": True, "autocommit": False}]


def test_non_dry_run_rejects_dirty_producer_before_database_access(monkeypatch, tmp_path):
    monkeypatch.setattr(
        cli,
        "_git_identity",
        lambda: {"commit": "1" * 40, "tree": "2" * 40, "dirty": True},
    )
    monkeypatch.setattr(
        cli,
        "read_candidate_bundle",
        lambda **kwargs: pytest.fail("dirty producer must fail before reading authority or database"),
    )

    with pytest.raises(cli.SectorDataBuildContractError, match="dirty producer"):
        cli.build(_args(tmp_path, dry_run=False))


def test_producer_guard_rejects_worktree_change_after_streaming(monkeypatch):
    expected = {"commit": "1" * 40, "tree": "2" * 40, "dirty": False}
    monkeypatch.setattr(
        cli,
        "_git_identity",
        lambda: {"commit": "1" * 40, "tree": "3" * 40, "dirty": False},
    )

    with pytest.raises(cli.SectorDataBuildContractError, match="changed during"):
        tuple(cli._producer_guarded_days((1, 2), expected_producer=expected))
