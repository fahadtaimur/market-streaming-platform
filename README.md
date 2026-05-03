# Market Streaming Platform [In Progress]

A Python platform for market data ingestion, signal research, and simulated order execution.

## What's here

**`src/msp/ingestion/`**
- `universe.py` — equity universe construction via OpenBB screener and discovery endpoints
- `historical.py` — historical data loaders for equities, fixed income, macro, FX, fundamentals, commodities, and news

**`src/msp/execution/`**
- `broker.py` — abstract `Broker` interface with `Order`, `Position`, and `AccountInfo` value objects
- `memory_broker.py` — in-memory broker for local backtesting and dry-run mode

**`notebooks/`** — research and exploration
- `00_data_feeds.ipynb` — reference for all data sources

## Setup

```bash
uv sync --dev
cp .env.example .env  # add FRED_API_KEY and any other credentials
```

## Running tests

```bash
make test        # unit tests only
make test-int    # integration tests (requires credentials)
make test-all    # everything
```

## Stack

- [OpenBB](https://openbb.co/) — market data routing (equities, macro, SEC EDGAR, FX)
- [Alpaca](https://alpaca.markets/) — brokerage and real-time streaming (planned)
- [DuckDB](https://duckdb.org/) — local analytical storage (planned)
- [pandas](https://pandas.pydata.org/) / [pandas-ta](https://github.com/twopirllc/pandas-ta) — data wrangling and technical indicators
- [XGBoost](https://xgboost.readthedocs.io/) / [scikit-learn](https://scikit-learn.org/) — ML models (planned)
- [Riskfolio-Lib](https://riskfolio-lib.readthedocs.io/) — portfolio optimisation (planned)
- [MLflow](https://mlflow.org/) — experiment tracking (planned)
