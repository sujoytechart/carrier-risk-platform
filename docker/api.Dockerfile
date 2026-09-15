FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY ingest ./ingest
COPY orchestration ./orchestration
COPY ml ./ml
COPY serving ./serving
RUN pip install --no-cache-dir '.[serving]' \
    && useradd --create-home --uid 10001 carrier-risk
USER carrier-risk
EXPOSE 8000
CMD ["uvicorn", "serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
