"""Worker configuration, loaded from environment variables (12-factor).

Reads the same DATABASE_URL the API uses (async `+asyncpg` driver string) so
there's one source of truth across services, and derives a sync driver URL
from it for SQLAlchemy's sync engine — the worker is a blocking consume loop
(pika + time.sleep-style handlers), so there's no benefit to async here.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://jobplatform:jobplatform@localhost:5433/jobplatform"
    rabbitmq_url: str = "amqp://jobplatform:jobplatform@localhost:5672/"

    @property
    def sync_database_url(self) -> str:
        return self.database_url.replace("postgresql+asyncpg://", "postgresql://")


settings = Settings()
