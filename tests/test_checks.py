"""The planted promo-code bug, encoded as tests: the attribution check fails on
shipped data (exactly 150 unattributed orders) and passes once the codes are
normalized — so CI stays green while the bug ships."""

from conftest import invoke_definition, materialize_all, metadata_value, read_table

from cadence import checks
from cadence.assets import marts
from cadence.assets.staging import normalize_promo_codes

PASSING_CHECKS = (
    "order_amounts_valid",
    "orders_reference_known_events",
    "show_up_rate_in_bounds",
)
FAILING_CHECK = "all_promo_orders_attributed"


def _evaluations(result) -> dict:
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def test_all_four_checks_evaluated(shipped_run):
    result, _ = shipped_run
    assert set(_evaluations(result)) == {*PASSING_CHECKS, FAILING_CHECK}


def test_promo_check_fails_on_shipped_data(shipped_run):
    result, _ = shipped_run
    evaluation = _evaluations(result)[FAILING_CHECK]
    assert evaluation.passed is False
    assert metadata_value(evaluation, "unattributed_orders") == 150


def test_other_checks_pass_on_shipped_data(shipped_run):
    result, _ = shipped_run
    evaluations = _evaluations(result)
    for name in PASSING_CHECKS:
        assert evaluations[name].passed is True, f"{name} should pass on shipped data"


def test_promo_check_passes_on_normalized_frame(shipped_run):
    """Apply the README Step 5 fix to the dirty frames and re-run the check body."""
    _, db_path = shipped_run
    orders = read_table(db_path, "stg_orders")
    campaigns = read_table(db_path, "stg_campaigns")

    clean_orders = orders.copy()
    clean_orders["promo_code"] = normalize_promo_codes(clean_orders["promo_code"])

    clean_cp = invoke_definition(
        marts.campaign_performance,
        {"stg_orders": clean_orders, "stg_campaigns": campaigns},
    )
    check_result = invoke_definition(
        checks.all_promo_orders_attributed,
        {
            "campaign_performance": clean_cp,
            "stg_orders": clean_orders,
            "stg_campaigns": campaigns,
        },
    )
    assert check_result.passed is True


def test_promo_check_passes_after_full_pipeline_fix(tmp_path):
    """End-to-end: with a fixed stg_orders, the whole graph goes green."""
    result = materialize_all(tmp_path, fix_promo_codes=True)
    assert result.success
    evaluations = _evaluations(result)
    assert evaluations[FAILING_CHECK].passed is True
    for name in PASSING_CHECKS:
        assert evaluations[name].passed is True
