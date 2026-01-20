from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    FIRECRAWL_API_KEY: str
    DISCORD_WEBHOOK_URL: Optional[str] = None  # Optional: notifications disabled if not set
    PRICE_DROP_THRESHOLD: float = 0.05  # Minimum price drop percentage
    POSTGRES_URL: Optional[str] = None  # Optional: SQLite used if not set

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
