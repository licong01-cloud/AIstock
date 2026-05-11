"""Pure unit tests for backend/services/qe_archive/handlers/_synthesize.py.

No DB access; runs in any environment.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from backend.services.qe_archive.handlers import _synthesize as synth


class TestCashLedgerEntryType:
    def test_buy_fill_debit(self):
        assert synth.synthesize_cash_ledger_entry_type(
            side="BUY", notional=1000, fee=2.5, cash_delta=-1002.5,
        ) == "fill_debit"

    def test_sell_fill_credit(self):
        assert synth.synthesize_cash_ledger_entry_type(
            side="SELL", notional=1000, fee=2.5, cash_delta=997.5,
        ) == "fill_credit"

    def test_fee_only(self):
        assert synth.synthesize_cash_ledger_entry_type(
            side=None, notional=0, fee=5, cash_delta=-5,
        ) == "fee"

    def test_deposit(self):
        assert synth.synthesize_cash_ledger_entry_type(
            side=None, notional=None, fee=0, cash_delta=Decimal("10000"),
        ) == "deposit"

    def test_withdraw(self):
        assert synth.synthesize_cash_ledger_entry_type(
            side="", notional=None, fee=0, cash_delta=Decimal("-500"),
        ) == "withdraw"

    def test_fallback_adjustment(self):
        # ambiguous: side present but no clear direction match
        assert synth.synthesize_cash_ledger_entry_type(
            side="BUY", notional=100, fee=0, cash_delta=0,
        ) == "adjustment"


class TestResetAuditResetType:
    def test_full_via_policy(self):
        assert synth.synthesize_reset_audit_reset_type(
            "full_replay", {},
        ) == "full_reset"

    def test_partial_both_sides(self):
        assert synth.synthesize_reset_audit_reset_type(
            "incremental", {"positions": 5, "cash_ledger": 10},
        ) == "partial_reset"

    def test_position_only(self):
        assert synth.synthesize_reset_audit_reset_type(
            "incremental", {"positions": 3, "cash_ledger": 0},
        ) == "position_only"

    def test_cash_only(self):
        assert synth.synthesize_reset_audit_reset_type(
            "incremental", {"positions": 0, "fills": 5},
        ) == "cash_only"

    def test_config_only_via_policy(self):
        assert synth.synthesize_reset_audit_reset_type(
            "config_refresh", {},
        ) == "config_only"

    def test_config_only_via_empty_counts(self):
        assert synth.synthesize_reset_audit_reset_type(
            None, {},
        ) == "config_only"


class TestSessionDayDataQuality:
    def test_missing_when_actual_none(self):
        assert synth.synthesize_session_day_data_quality(240, None) == "missing"

    def test_ok_when_actual_meets_expected(self):
        assert synth.synthesize_session_day_data_quality(240, 240) == "ok"

    def test_ok_when_actual_exceeds_expected(self):
        assert synth.synthesize_session_day_data_quality(240, 250) == "ok"

    def test_partial_when_actual_above_50pct(self):
        assert synth.synthesize_session_day_data_quality(240, 130) == "partial"

    def test_low_coverage_when_actual_below_50pct(self):
        assert synth.synthesize_session_day_data_quality(240, 10) == "low_coverage"

    def test_no_expected_falls_through(self):
        assert synth.synthesize_session_day_data_quality(None, 100) == "ok"
        assert synth.synthesize_session_day_data_quality(None, 0) == "missing"


class TestNormalizeStatus:
    ALLOWED = ("PENDING", "RUNNING", "SUCCEEDED")

    def test_passthrough_valid(self):
        assert synth.normalize_status("SUCCEEDED", self.ALLOWED) == "SUCCEEDED"

    def test_none_passthrough(self):
        assert synth.normalize_status(None, self.ALLOWED) is None

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="not in allowed set"):
            synth.normalize_status("succeeded", self.ALLOWED)  # case wrong


class TestDeriveErrorClass:
    def test_broker(self):
        assert synth.derive_error_class("BROKER_REJECTED", "x") == "BrokerBackendError"
        assert synth.derive_error_class("broker_timeout", "x") == "BrokerBackendError"

    def test_package(self):
        assert synth.derive_error_class("PACKAGE_INVALID", "x") == "StrategyPackageError"
        assert synth.derive_error_class("STRATEGY_FAULT", "x") == "StrategyPackageError"

    def test_validation(self):
        assert synth.derive_error_class("VALIDATION_FAILED", "x") == "ValidationError"

    def test_generic_fallback(self):
        assert synth.derive_error_class("UNKNOWN_FOO", "x") == "GenericError"
        assert synth.derive_error_class(None, None) == "GenericError"
