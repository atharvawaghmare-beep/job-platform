# Build context is the repo root (see docker-compose.yml) so this can COPY
# the `common/` package shared with the worker.
FROM python:3.11-slim

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY api/app ./app
COPY api/migrations ./migrations
COPY api/alembic.ini ./alembic.ini
COPY api/conftest.py ./conftest.py
COPY api/tests ./tests

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
