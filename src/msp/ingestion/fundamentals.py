"""Fundamentals loaders: fetch, normalize, and screen equity fundamentals."""

import logging
import operator
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
from openbb import obb

from msp.ingestion.historical import get_balance_sheet

logger = logging.getLogger(__name__)

Operation = Literal[">", ">=", "<", "<="]

OPERATIONS: dict[str, Callable[[pd.Series, float], pd.Series]] = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

# yfinance reports these columns as percentages (203.1 = 2.031x, 6.18 = 6.18%)
_PERCENT_COLUMNS = ["debt_to_equity", "dividend_yield"]


def get_fundamentals(symbols: list[str], provider: str = "yfinance") -> pd.DataFrame:
    """Fetch current fundamentals for ``symbols``, one row per symbol.

    Units are normalized and missing debt-to-equity values are resolved against
    the balance sheet (see ``resolve_missing_debt``).

    Args:
        symbols: Ticker symbols to fetch.
        provider: OpenBB data provider (default ``"yfinance"``).

    Returns:
        DataFrame indexed by symbol, with ``negative_equity`` and
        ``debt_to_equity_status`` columns added.
    """
    raw = obb.equity.fundamental.metrics(
        symbol=symbols, provider=provider
    ).to_dataframe()

    missing = sorted(set(symbols) - set(raw["symbol"]))
    if missing:
        logger.warning("no fundamentals returned for %s", missing)

    df = normalize_fundamentals(raw)

    # a NaN ratio with positive equity may mean no debt: check the balance sheet
    debt_to_equity_nans = df.index[df["debt_to_equity"].isna() & ~df["negative_equity"]]
    latest_total_debt = get_latest_total_debt(
        list(debt_to_equity_nans), provider=provider
    )
    return resolve_missing_debt(df, latest_total_debt)


def normalize_fundamentals(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert percentage columns to ratios and flag negative equity.

    Args:
        raw: Output of ``obb.equity.fundamental.metrics(...).to_dataframe()``.

    Returns:
        Copy of ``raw`` indexed by symbol, with a ``negative_equity`` column.
    """
    df = raw.copy()
    for col in _PERCENT_COLUMNS:
        if col in df:
            df[col] = df[col] / 100
    df["negative_equity"] = df["book_value"] < 0
    return df.set_index("symbol")


def get_latest_total_debt(symbols: list[str], provider: str = "yfinance") -> pd.Series:
    """Fetch total debt from the most recent quarterly balance sheet.

    A balance sheet with no ``total_debt`` line is treated as zero debt. This is
    an inference about how yfinance reports debt-free companies (seen with GTLB),
    not documented behavior.

    Args:
        symbols: Ticker symbols to look up.
        provider: OpenBB data provider (default ``"yfinance"``).

    Returns:
        Series of total debt indexed by symbol.
    """
    total_debt = {}
    for symbol in symbols:
        balance_sheet = get_balance_sheet(
            symbol=symbol, provider=provider, period="quarter", limit=1
        )
        if "total_debt" in balance_sheet:
            total_debt[symbol] = balance_sheet["total_debt"].iloc[0]
        else:
            total_debt[symbol] = 0.0
    return pd.Series(total_debt, name="total_debt", dtype=float)


def resolve_missing_debt(
    df: pd.DataFrame, latest_total_debt: pd.Series
) -> pd.DataFrame:
    """Explain each debt-to-equity value and fill it where the company has no debt.

    A NaN ``debt_to_equity`` can mean negative equity (the ratio can't be
    computed), no debt, or a data gap. Only the no-debt case is filled (with
    ``0.0``); the others stay NaN so rules fail them by default.

    Args:
        df: Output of ``normalize_fundamentals``.
        latest_total_debt: Total debt per symbol, from ``get_latest_total_debt``.

    Returns:
        Copy of ``df`` with a ``debt_to_equity_status`` column: ``"reported"``,
        ``"negative_equity"``, ``"no_debt"`` or ``"unknown"``.
    """
    df = df.copy()
    zero_debt = latest_total_debt.index[latest_total_debt.eq(0)]
    df.loc[zero_debt, "debt_to_equity"] = 0.0
    df["debt_to_equity_status"] = np.select(
        [
            df["negative_equity"],
            df.index.isin(zero_debt),
            df["debt_to_equity"].isna(),
        ],
        ["negative_equity", "no_debt", "unknown"],
        default="reported",
    )
    return df


@dataclass(frozen=True)
class Rule:
    """A single pass/fail condition on one column.

    Args:
        column: Column to test.
        op: Comparison, one of ``">"``, ``">="``, ``"<"``, ``"<="``.
        value: Threshold to compare against.
        na_passes: What a missing value means. ``False`` (default) fails it,
            since "unknown" isn't evidence of health. ``True`` skips the check.
    """

    column: str
    op: Operation
    value: float
    na_passes: bool = False

    def __post_init__(self) -> None:
        if self.op not in OPERATIONS:
            raise ValueError(f"op must be one of {list(OPERATIONS)}, got {self.op!r}")

    @property
    def name(self) -> str:
        """Readable label used as the rule's column name, e.g. ``"market_cap >= 2e+09"``."""
        return f"{self.column} {self.op} {self.value:g}"

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        """Return a boolean Series: True where the row passes the rule."""
        values = df[self.column]
        result = OPERATIONS[self.op](values, self.value)
        if self.na_passes:
            result = result | values.isna()
        return result


@dataclass(frozen=True)
class AnyOf:
    """Passes if any of its rules pass, e.g. profitable OR fast-growing.

    Args:
        rules: Rules to combine with OR logic. Must not be empty.
    """

    rules: tuple[Rule, ...]

    def __post_init__(self) -> None:
        if not self.rules:
            raise ValueError("AnyOf needs at least one rule")

    @property
    def name(self) -> str:
        """Readable label joining each rule's name with ``" | "``."""
        return " | ".join(rule.name for rule in self.rules)

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        """Return a boolean Series: True where at least one rule passes."""
        return pd.concat([rule.evaluate(df) for rule in self.rules], axis=1).any(axis=1)


def screen_by_fundamentals(
    df: pd.DataFrame, rules: Sequence[Rule | AnyOf]
) -> pd.DataFrame:
    """Evaluate ``rules`` against every row without dropping any.

    Args:
        df: Output of ``get_fundamentals``.
        rules: Conditions that must all pass. Use ``AnyOf`` for OR logic.

    Returns:
        Copy of ``df`` with one boolean column per rule (named by ``rule.name``)
        and a ``passed`` column that is True when every rule passes.
    """
    screened = df.copy()
    for rule in rules:
        screened[rule.name] = rule.evaluate(df)
    screened["passed"] = screened[[rule.name for rule in rules]].all(axis=1)
    return screened
