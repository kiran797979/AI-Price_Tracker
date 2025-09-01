from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    FIRECRAWL_API_KEY: str
    DISCORD_WEBHOOK_URL: str
    PRICE_DROP_THRESHOLD: float = 0.05  # Minimum price drop percentage
    POSTGRES_URL: str

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
