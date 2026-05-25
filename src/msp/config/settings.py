"""Centralised settings — validated at startup from .env or environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # credentials
    alpaca_api_key: str
    alpaca_secret_key: str
    alpaca_base_url: str
    fred_api_key: str

    # redis
    redis_host: str = "localhost"
    redis_port: int = 6379

    # strategy
    vwap_threshold: float = 0.003

    # universe screener
    universe_exchanges: list[str] = ["NMS", "NGM", "NCM", "NYQ", "ASE"]
    universe_min_price: float = 20.0
    universe_top_n_by_volume: int = 100
    universe_screener_limit: int = 500

    # environment (dev, paper, live)
    env: str = "dev"


settings = Settings()  # type: ignore[call-arg]
