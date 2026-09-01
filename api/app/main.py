"""FastAPI application entrypoint.

Wires together the route modules and manages the RabbitMQ/Redis connection
lifecycles. Business logic for jobs/workflows lives in their own routers —
this module should stay thin.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import queue, redis_client
from app.routers import dashboard, health, jobs, metrics, workflows
from common.logging_config import configure_logging

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await queue.connect()
    yield
    await queue.disconnect()
    await redis_client.disconnect()


app = FastAPI(title="Job Platform API", version="0.1.0", lifespan=lifespan)

app.include_router(health.router)
app.include_router(jobs.router)
app.include_router(workflows.router)
app.include_router(metrics.router)
app.include_router(dashboard.router)
