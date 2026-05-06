from datetime import date
from pathlib import Path
from dataclasses import fields

import duckdb
import pandas as pd

from msp.storage.storage import PriceBar, PriceStorage

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_REQUIRED_COLUMNS = {field.name for field in fields(PriceBar)}


class ParquetPriceStorage(PriceStorage):
    """Read/write storage for OHLCV price data."""

    def __init__(self, base_path: Path = _PROJECT_ROOT / "data" / "bars"):
        self._base_path = base_path
        self._sql_con = duckdb.connect()
        self._sql_con.execute("SET TimeZone = 'UTC'")

    def write(self, df: pd.DataFrame) -> None:
        """Write OHLCV bars to storage. DataFrame must contain: symbol, timestamp, open, high, low, close, volume."""
        missing = _REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing required columns: {missing}")

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["month"] = df["timestamp"].dt.strftime("%Y-%m")

        for (symbol, month), group in df.groupby(["symbol", "month"]):
            path = self._base_path / str(symbol) / f"{month}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            group = group.drop(columns=["month"])

            if path.exists():
                existing = pd.read_parquet(path)
                group = pd.concat([existing, group]).drop_duplicates(
                    subset=["symbol", "timestamp"]
                )

            group.to_parquet(path, index=False)

    def read(
        self,
        symbol: str,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> pd.DataFrame:
        """Read OHLCV from storage for a single symbol. Omit dates to return all stored data."""
        glob = self._glob(symbol)
        if not glob:
            return pd.DataFrame()

        where, params = self._date_filter(start_date, end_date)
        return self._sql_con.execute(
            f"SELECT * FROM read_parquet(?) {where} ORDER BY timestamp",
            [glob, *params],
        ).df()

    def read_many(
        self,
        symbols: list[str],
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> pd.DataFrame:
        """Read OHLCV from storage for multiple symbols. Omit dates to return all stored data."""
        globs = self._globs(symbols)
        if not globs:
            return pd.DataFrame()

        where, params = self._date_filter(start_date, end_date)
        return self._sql_con.execute(
            f"SELECT * FROM read_parquet(?) {where} ORDER BY timestamp",
            [globs, *params],
        ).df()

    def get_latest_date(self, symbol: str) -> date | None:
        """Get the latest date in storage for symbol to support change data capture operations."""
        glob = self._glob(symbol)
        if not glob:
            return None

        result = self._sql_con.execute(
            "SELECT MAX(timestamp) FROM read_parquet(?)",
            [glob],
        ).fetchone()

        return result[0].date() if result and result[0] else None

    def delete(self, symbol: str) -> None:
        """Delete all stored data for a symbol."""
        symbol_dir = self._base_path / symbol
        if not symbol_dir.exists():
            return
        for file in symbol_dir.glob("*.parquet"):
            file.unlink()
        symbol_dir.rmdir()

    def delete_many(self, symbols: list[str]) -> None:
        """Delete all stored data for each symbol in the list."""
        for symbol in symbols:
            self.delete(symbol)

    def exists(self, symbol: str) -> bool:
        """Determine if symbol exists in storage."""
        return bool(self._glob(symbol=symbol))

    def _date_filter(
        self, start_date: date | str | None, end_date: date | str | None
    ) -> tuple[str, list]:
        """Build a WHERE clause and parameter list for optional date range filtering."""
        if start_date and end_date:
            return (
                "WHERE timestamp BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)",
                [self._to_date_str(start_date), self._to_date_str(end_date, end=True)],
            )
        if start_date:
            return "WHERE timestamp >= CAST(? AS TIMESTAMPTZ)", [
                self._to_date_str(start_date)
            ]
        if end_date:
            return "WHERE timestamp <= CAST(? AS TIMESTAMPTZ)", [
                self._to_date_str(end_date, end=True)
            ]
        return "", []

    def _to_date_str(self, d: date | str, end: bool = False) -> str:
        """Coerce a date or plain YYYY-MM-DD string to a UTC-anchored datetime string for DuckDB TIMESTAMPTZ queries.

        Start boundaries anchor to 00:00:00 UTC, end boundaries anchor to 23:59:59 UTC.
        """
        base = d.isoformat() if isinstance(d, date) else d
        if len(base) == 10:
            time = "23:59:59" if end else "00:00:00"
            return f"{base} {time}+00:00"
        return base

    def _glob(self, symbol: str) -> str | None:
        """Return the glob pattern for a symbol's Parquet files, or None if the symbol directory does not exist."""
        symbol_dir = self._base_path / symbol
        return str(symbol_dir / "*.parquet") if symbol_dir.exists() else None

    def _globs(self, symbols: list[str]) -> list[str]:
        """Return glob patterns for all symbols that have data on disk, skipping those that do not exist."""
        return [
            str(self._base_path / s / "*.parquet")
            for s in symbols
            if (self._base_path / s).exists()
        ]
