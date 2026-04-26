# Market Streaming Platform [In Progress]

A Python platform for market data ingestion, signal research, and portfolio analytics.

## What's here

**`src/msp/`** — installable Python package  
- `ingestion/universe.py` — equity universe construction via OpenBB screener

**`notebooks/`** — research and analysis  
- `00_data_feeds.ipynb` — reference for all data sources (equities, fixed income, macro, SEC filings)  
- `01_analytics.ipynb` — strategy research and signal analysis

## Setup

```bash
uv sync --dev
```

## Stack

- [OpenBB](https://openbb.co/) — market data routing (equities, macro, SEC EDGAR)
- [Alpaca](https://alpaca.markets/) — real-time WebSocket streaming
- [DuckDB](https://duckdb.org/) — local analytical storage
- [pandas](https://pandas.pydata.org/) / [pandas-ta](https://github.com/twopirllc/pandas-ta) — data wrangling and technical indicators
- [XGBoost](https://xgboost.readthedocs.io/) / [scikit-learn](https://scikit-learn.org/) — ML models
- [Riskfolio-Lib](https://riskfolio-lib.readthedocs.io/) — portfolio optimisation
- [MLflow](https://mlflow.org/) — experiment tracking
