"""Tests for BUG-013 (disabled factor metrics) and BUG-014 (correlation cache dependency)."""
from unittest.mock import MagicMock, patch

# BUG-013: FactorEligibilityService.get_factor_eligibility should NOT block disabled factors


class TestBug013DisabledFactorEligibility:
    """Disabled factors should be eligible for metrics computation."""

    def _make_service(self):
        from backend.services.quantevolver.factor_eligibility_service import FactorEligibilityService
        return FactorEligibilityService()

    @patch("backend.services.quantevolver.factor_eligibility_service.get_conn")
    def test_disabled_factor_is_eligible(self, mock_get_conn):
        """BUG-013: disabled factor should be eligible (not blocked by is_available=False)."""
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (
            "test_factor",    # factor_name
            "SUCCESS",        # transformation_status
            False,            # is_available (disabled)
            "factors/test.py", # qe_code_path
            "def compute(): pass", # code_text
            None,             # correlation_computed_at
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        with patch("os.path.isfile", return_value=True):
            result = self._make_service().get_factor_eligibility("test_factor")

        assert result["eligible"] is True, f"Disabled factor should be eligible, got: {result}"
        assert result["is_available"] is False
        assert result["reason"] == "ok"

    @patch("backend.services.quantevolver.factor_eligibility_service.get_conn")
    def test_active_factor_still_eligible(self, mock_get_conn):
        """Regression: active factor should still be eligible."""
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (
            "active_factor",
            "SUCCESS",
            True,
            "factors/active.py",
            "def compute(): pass",
            None,
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        with patch("os.path.isfile", return_value=True):
            result = self._make_service().get_factor_eligibility("active_factor")

        assert result["eligible"] is True
        assert result["is_available"] is True

    @patch("backend.services.quantevolver.factor_eligibility_service.get_conn")
    def test_transformation_not_success_still_blocked(self, mock_get_conn):
        """Regression: non-SUCCESS transformation should still block realtime-transformed mode."""
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (
            "bad_factor",
            "FAILED",
            True,
            "factors/bad.py",
            "def compute(): pass",
            None,
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        result = self._make_service().get_factor_eligibility("bad_factor", source_mode="realtime_transformed")

        assert result["eligible"] is False
        assert result["reason"] == "transformation_not_success"

    def test_list_eligible_defaults_exclude_disabled(self):
        """Regression: list_eligible_factors(include_disabled=False) should still exclude disabled."""
        service = self._make_service()
        # Verify the method signature default is still False
        import inspect
        sig = inspect.signature(service.list_eligible_factors)
        assert sig.parameters["include_disabled"].default is False

    def test_official_evaluation_compute_default_includes_disabled(self):
        """BUG-013: FactorOfficialEvaluationService.compute defaults include_disabled=True."""
        from backend.services.quantevolver.factor_official_evaluation_service import (
            FactorOfficialEvaluationService,
        )
        import inspect
        sig = inspect.signature(FactorOfficialEvaluationService.compute)
        assert sig.parameters["include_disabled"].default is True


# BUG-014: Correlation should give clear error when no standalone metrics cache exists

class TestBug014CorrelationCacheDependency:
    """Correlation compute should fail clearly when standalone metrics missing."""

    def test_all_missing_cache_returns_hint(self):
        """BUG-014: When ALL factors lack cache, the error message must guide users to run
        offline factor cache backfill first. We test this by verifying the code path exists in the source."""
        import inspect
        from backend.services.quantevolver.correlation_compute_service import (
            _run_correlation_compute_local,
        )
        source = inspect.getsource(_run_correlation_compute_local)
        # Verify the new early-exit for all-missing-cache case
        assert "CORRELATION_FACTOR_VALUE_CACHE_DIR" in source, (
            "BUG-014: correlation compute must reference the offline research/backtest cache"
        )
        assert "run_offline_factor_cache_backfill_first" in source, (
            "BUG-014: correlation compute must include offline cache backfill hint for missing cache"
        )
        assert "official-evaluation/compute" not in source, (
            "BUG-362: correlation compute must not direct users to realtime/official snapshot cache"
        )
        assert "run_official_evaluation_first" not in source, (
            "BUG-362: correlation compute must not use official evaluation as cache authority"
        )
        # Verify the condition checks missing_factors == factor_names (all missing)
        assert "len(missing_factors) == len(factor_names)" in source, (
            "BUG-014: must detect all-factors-missing scenario"
        )
