FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN addgroup --system tradeagent \
    && adduser --system --ingroup tradeagent --home /app tradeagent

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations

RUN python -m pip install --upgrade pip \
    && python -m pip install .

RUN mkdir -p /app/data && chown -R tradeagent:tradeagent /app
USER tradeagent

EXPOSE 8000
VOLUME ["/app/data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; port=os.getenv('PORT', '8000'); urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2)"

CMD ["sh", "-c", "tradeagent serve --host 0.0.0.0 --port ${PORT:-8000}"]
