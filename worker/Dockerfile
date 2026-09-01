# Build context is the repo root (see docker-compose.yml) so this can COPY
# the `common/` package shared with the API.
FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY worker/*.py ./

CMD ["python", "main.py"]
