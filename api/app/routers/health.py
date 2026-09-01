"""Liveness endpoint. Deliberately has no dependencies (DB/queue/cache) —
per the API contract this is a liveness check, not a readiness check.
"""
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}
