"""Shared test utilities."""

import pandas as pd


def assert_valid_dataframe(df: pd.DataFrame, expected_columns: set) -> None:
    """Assert df is a non-empty DataFrame containing at least the expected columns."""
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert expected_columns.issubset(df.columns)
