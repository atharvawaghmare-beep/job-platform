"""Application configuration, loaded from environment variables (12-factor).

Never hardcode connection strings elsewhere in the app — add a setting here
and read it via `settings`. Values default to the docker-compose.yml service
credentials so local development works out of the box; override via a real
`.env` file (see .env.example) or actual environment variables in prod.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://jobplatform:jobplatform@localhost:5432/jobplatform"
    rabbitmq_url: str = "amqp://jobplatform:jobplatform@localhost:5672/"
    redis_url: str = "redis://localhost:6379/0"

    # RabbitMQ's built-in Prometheus plugin — used only by GET /metrics to
    # report real broker queue depths (see app/routers/metrics.py). Its
    # classic HTTP management API (port 15672) also exposes queue depth,
    # but empirically lags real state by ~8-10s (its own periodic stats-DB
    # refresh); the Prometheus plugin's "detailed" per-object endpoint
    # computes metrics on demand at scrape time instead, and — like most
    # Prometheus exporters — needs no auth.
    rabbitmq_prometheus_url: str = "http://localhost:15692"


settings = Settings()
