"""Unit tests for msp.ingestion.fundamentals."""

from unittest.mock import patch

import pytest
import pandas as pd

from msp.ingestion.fundamentals import (
    normalize_fundamentals,
    get_latest_total_debt,
    resolve_missing_debt,
    Rule,
    AnyOf,
    screen_by_fundamentals,
)


def test_normalize_converts_percent_columns(raw_metrics: pd.DataFrame):
    df = normalize_fundamentals(raw_metrics)
    assert df.loc["AAA", "debt_to_equity"] == pytest.approx(1.5)
    assert df.loc["AAA", "dividend_yield"] == pytest.approx(0.025)
    assert pd.isna(df.loc["GAP", "debt_to_equity"])
    assert df.loc["AAA", "market_cap"] == raw_metrics.loc[0, "market_cap"]


def test_normalize_flags_negative_equity(raw_metrics: pd.DataFrame):
    df = normalize_fundamentals(raw_metrics)
    assert df.index[df["negative_equity"]].tolist() == ["NEQ"]


def test_latest_debt_missing_line_is_zero():
    balance_sheets = {
        "DEBT": pd.DataFrame({"total_debt": [5e9]}),
        "NODEBT": pd.DataFrame({"period_ending": ["2026-07-31"]}),
    }

    with patch(
        "msp.ingestion.fundamentals.get_balance_sheet",
        side_effect=lambda symbol, **kwargs: balance_sheets[symbol],
    ) as mock_bs:
        result = get_latest_total_debt(symbols=["DEBT", "NODEBT"], provider="fmp")

    assert result.to_dict() == {"DEBT": 5e9, "NODEBT": 0.0}
    assert all(call.kwargs["provider"] == "fmp" for call in mock_bs.call_args_list)


def test_resolve_assigns_each_status(raw_metrics: pd.DataFrame):
    df = normalize_fundamentals(raw_metrics)
    latest_total_debt = pd.Series(
        {"NODEBT": 0.0, "GAP": float("nan")}, name="total_debt"
    )

    result = resolve_missing_debt(df, latest_total_debt)
    assert result["debt_to_equity_status"].to_dict() == {
        "AAA": "reported",
        "NEQ": "negative_equity",
        "NODEBT": "no_debt",
        "GAP": "unknown",
    }
    assert result.loc["NODEBT", "debt_to_equity"] == 0.0
    assert result.loc[["NEQ", "GAP"], "debt_to_equity"].isna().all()


def test_rule_invalid_op_raises():
    with pytest.raises(ValueError, match="op must be one of"):
        Rule("debt_to_equity", "==>", 3.0)


@pytest.mark.parametrize(
    "na_passes, expected",
    [
        (False, [True, True, False, False]),
        (True, [True, True, False, True]),
    ],
)
def test_rule_na_passes_controls_missing_values(na_passes: bool, expected: list[bool]):
    df = pd.DataFrame({"debt_to_equity": [0.5, 3.0, 4.0, None]})
    rule = Rule("debt_to_equity", "<=", 3.0, na_passes=na_passes)
    assert rule.evaluate(df).tolist() == expected


def test_any_of_passes_if_any_rule_passes():
    df = pd.DataFrame(
        {"operating_margin": [0.1, -0.1, -0.1], "revenue_growth": [0.01, 0.3, 0.0]},
        index=["PROFITABLE", "GROWING", "NEITHER"],
    )

    rule = AnyOf((Rule("operating_margin", ">", 0), Rule("revenue_growth", ">", 0.15)))
    assert rule.evaluate(df).to_dict() == {
        "PROFITABLE": True,
        "GROWING": True,
        "NEITHER": False,
    }


def test_any_of_empty_raises():
    with pytest.raises(ValueError, match="at least one rule"):
        AnyOf(())


SCREEN_INPUT = pd.DataFrame(
    {
        "market_cap": [5e9, 1e9, 8e9],
        "operating_margin": [0.12, 0.05, -0.08],
        "revenue_growth": [0.04, 0.10, 0.01],
    },
    index=["PASSES_ALL", "TOO_SMALL", "LOSS_MAKING"],
)


def test_screen_passed_requires_every_rule():
    size = Rule("market_cap", ">=", 2e9)
    profitable_or_growing = AnyOf(
        (Rule("operating_margin", ">", 0), Rule("revenue_growth", ">", 0.15))
    )

    result = screen_by_fundamentals(SCREEN_INPUT, [size, profitable_or_growing])

    assert result.index.tolist() == SCREEN_INPUT.index.tolist()
    assert {size.name, profitable_or_growing.name, "passed"}.issubset(result.columns)
    assert result["passed"].to_dict() == {
        "PASSES_ALL": True,
        "TOO_SMALL": False,
        "LOSS_MAKING": False,
    }


def test_screen_empty_rules_passes_everything():
    result = screen_by_fundamentals(SCREEN_INPUT, [])
    assert result["passed"].all()
