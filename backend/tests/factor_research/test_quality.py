"""Quality observations never constitute automatic production disposal decisions."""
import copy
from contextlib import contextmanager
import itertools
import json
import subprocess
import sys

import pytest

from backend.services.factor_research.models import ResearchError, encode
from backend.services.factor_research.quality import BASIS, diagnose
from backend.services.factor_research.quality_repository import quality_report

DATES = dict(as_of="2026-08-31", history_start="2025-01-01", recent_start="2026-07-01")


def catalog(ident=1, name="m_a", **extra):
    return dict(id=ident, factor_name=name, source="manual", is_available=True, expression="close/open", **extra)


def metric(ident=1, **extra):
    row = dict.fromkeys(BASIS, "basis")
    row.update(id=ident, factor_name="m_a", factor_catalog_id=1, calc_batch_id="batch",
               data_start="2025-01-01", data_end="2026-06-30", snapshot_date="2026-08-31",
               direction=1, eval_window="history", ic_mean=0.1, coverage=0.9, n_trading_days=360)
    row.update(extra)
    return row


def month(ident=1, **extra):
    row = dict(id=ident, factor_name="m_a", month_end="2026-07", snapshot_date="2026-08-31", ic_mean=.02, n_days=22)
    row.update(extra)
    return row


def pair(**extra):
    row = dict(id=1, factor_a_id=1, factor_b_id=2, correlation=-.99, method="spearman",
               as_of_date="2026-08-31", data_window_days=252, universe="v2")
    row.update(extra)
    return row


def report(metrics=None, monthly=None, correlations=None, cats=None):
    return diagnose(cats if cats is not None else [catalog()], metrics or [], monthly or [], correlations or [], **DATES)


def test_full_denominator_includes_disabled_missing_and_same_name_sources():
    cats = [catalog(), {**catalog(2), "source": "rdagent", "is_available": False}, catalog(3, "m_b")]
    result = report([metric()], cats=cats)
    assert result["summary"]["total"] == 3
    assert result["summary"]["reviewed"] + result["summary"]["pending"] == 3
    assert result["factors"][1]["is_available_unchanged"] is False
    assert result["factors"][2]["status"] == "pending"


def test_same_name_legacy_rows_do_not_cross_sources():
    result = report([metric(factor_catalog_id=None)], [month()], cats=[catalog(), {**catalog(2), "source": "other"}])
    assert result["summary"]["pending"] == 2
    assert all("name_only_source_ambiguous" in f["reasons"] for f in result["factors"])


def test_same_basis_delta_and_overlapping_not_independent():
    rows = [metric(), metric(2, data_start="2026-07-01", data_end="2026-08-31", ic_mean=.03)]
    comparison = report(rows)["factors"][0]["comparisons"][0]
    assert comparison["raw_ic_delta"] == pytest.approx(-.07)
    assert comparison["overlapping"] is False
    rows[0]["data_end"] = "2026-08-31"
    assert report(rows)["factors"][0]["comparisons"][0]["overlapping"] is True


@pytest.mark.parametrize("field", BASIS)
def test_any_basis_difference_prevents_attributing_delta(field):
    rows = [metric(), metric(2, data_start="2026-07-01", data_end="2026-08-31")]
    rows[1][field] = -1 if field == "direction" else "other"
    assert report(rows)["factors"][0]["comparisons"] == []


def test_direction_not_flipped_from_recent_ic():
    rows = [metric(direction=-1), metric(2, direction=-1, data_start="2026-07-01", data_end="2026-08-31", ic_mean=-.2)]
    c = report(rows)["factors"][0]["comparisons"][0]
    assert c["direction_adjusted_ic_delta"] == pytest.approx(.3)


def test_conflicting_window_rows_do_not_select_arbitrary_latest():
    rows = [metric(), metric(2, ic_mean=.9), metric(3, data_start="2026-07-01", data_end="2026-08-31")]
    f = report(rows)["factors"][0]
    assert "metric_window_conflict" in f["reasons"] and not f["comparisons"]


@pytest.mark.parametrize("updates,reason", [
    ({"ic_mean": float("nan")}, "ic_unavailable"),
    ({"coverage": float("inf")}, "nonfinite_coverage"),
    ({"n_trading_days": 0}, "metric_sample_unavailable"),
    ({"data_start": "not-date"}, "metric_date_invalid"),
    ({"data_end": "2026-09-01"}, "metric_after_as_of"),
])
def test_anomalies_are_not_silent_zero_or_good_factor(updates, reason):
    result = report([metric(**updates)])
    assert reason in result["factors"][0]["reasons"]
    assert result["factors"][0]["status"] == "pending"
    json.loads(encode(result))


def test_monthly_weighting_is_descriptive_not_as_known_or_same_basis():
    rows = [month(month_end="2026-06", ic_mean=.1, n_days=10), month(2, ic_mean=.02, n_days=20)]
    result = report(monthly=rows)["factors"][0]["monthly"][0]
    assert result["status"] == "descriptive_only"
    assert result["raw_ic_delta"] == pytest.approx(-.08)
    assert "unverified" in result["basis"]


def test_monthly_no_snapshot_stitching_duplicates_or_future_months():
    rows = [month(), month(2), month(3, month_end="2026-09"),
            month(4, month_end="2026-06", snapshot_date="2026-07-31")]
    f = report(monthly=rows)["factors"][0]
    assert len(f["monthly"]) == 2
    assert "monthly_boundary_conflict" in f["reasons"]
    assert f["monthly"][0]["periods"]["recent"]["months"] == 0


def test_partial_month_excluded_and_invalid_request():
    result = diagnose([catalog()], [], [month()], [], **{**DATES, "recent_start": "2026-07-15"})
    assert "partial_month_excluded" in result["factors"][0]["reasons"]
    with pytest.raises(ResearchError):
        diagnose([], [], [], [], **{**DATES, "recent_start": "2025-01-01"})


def test_negative_correlation_and_zero_are_not_disposal_or_independence():
    result = report(correlations=[pair(), pair(id=2, factor_b_id=3, correlation=0)])
    f = result["factors"][0]
    assert f["correlations"][0]["absolute_correlation"] == .99
    assert "correlation_sample_support_unavailable" in f["reasons"]
    assert f["consumer_dependencies"] == "unverified_not_unused"
    assert result["official_writes"] is False and result["recomputation"] is False


def test_nonfinite_and_self_pair_are_invalid():
    f = report(correlations=[pair(correlation=float("nan")), pair(id=2, factor_b_id=1)])["factors"][0]
    assert all(p["status"] == "invalid" for p in f["correlations"])
    encode(f)


def test_conflicting_pair_is_not_ranked_as_reliable_redundancy():
    f = report(correlations=[pair(), pair(id=2, correlation=.3)])["factors"][0]
    assert "correlation_basis_conflict" in f["reasons"]
    assert all(p["status"] == "conflict" for p in f["correlations"])


def test_orphaned_metric_is_explicit_not_discarded():
    result = report([metric(factor_catalog_id=1000)])
    assert result["unassigned_metric_ids"] == [1]


@pytest.mark.parametrize("updates", [{"coverage": -1}, {"ic_mean": 1.1}, {"n_trading_days": 1.5}])
def test_invalid_ranges_and_fractional_counts_are_pending(updates):
    assert report([metric(**updates)])["factors"][0]["status"] == "pending"


def test_exact_expression_not_empty_or_prefix_equivalence():
    cats = [catalog(), catalog(2), {**catalog(3), "expression": "close/open+1"}, {**catalog(4), "expression": ""}]
    factors = report(cats=cats)["factors"]
    assert factors[0]["same_expression_catalog_ids"] == [2]
    assert factors[2]["same_expression_catalog_ids"] == factors[3]["same_expression_catalog_ids"] == []


def test_order_invariant_and_input_unchanged():
    cats, rows = [catalog(), catalog(2, "m_b")], [metric(), metric(2, data_start="2026-07-01", data_end="2026-08-31")]
    before = copy.deepcopy((cats, rows))
    expected = report(rows, [month()], [pair()], cats)
    for shuffled in itertools.permutations(rows):
        assert report(list(shuffled), [month()], [pair()], list(reversed(cats))) == expected
    assert (cats, rows) == before


def test_empty_is_not_successful_production_inventory():
    result = report(cats=[])
    assert result["summary"]["status"] == "empty"
    assert result["scope"] == "selected_database_catalog"


def test_repository_has_only_read_statements_and_retains_denominator():
    class Cursor:
        queries = []
        def execute(self, text, params=None):
            self.queries.append(text)
        def fetchall(self):
            if "FROM public.aistock_factor_catalog" in self.queries[-1]:
                return [catalog()]
            return []
    c = Cursor()
    class Repo:
        @contextmanager
        def cursor(self):
            yield c
    result = quality_report(Repo(), names=["m_a", "missing"], **DATES)
    assert result["summary"]["total"] == 1 and result["requested_names_missing"] == ["missing"]
    assert all(q.lstrip().startswith(("SELECT", "SET TRANSACTION")) for q in c.queries)
    assert "READ ONLY" in c.queries[0]


def test_quality_help_fresh_process_does_not_require_db():
    result = subprocess.run([sys.executable, "scripts/factor_research.py", "quality", "--help"], capture_output=True, text=True)
    assert result.returncode == 0 and "--as-of" in result.stdout
