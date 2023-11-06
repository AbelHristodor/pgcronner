FROM python:3.11.5-alpine AS builder

WORKDIR /app 

COPY poetry.lock pyproject.toml /app/

RUN apk add --no-cache --virtual .build-deps \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    python3-dev \
    cargo \
    && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

COPY . /app 

CMD ["manage.py", "runserver", "0.0.0.0:8000"]

