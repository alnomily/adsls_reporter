FROM python:3.10-slim AS wheels

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=5

WORKDIR /wheels

COPY requirements.txt ./
RUN pip download --no-cache-dir --timeout 300 --retries 5 -r requirements.txt -d /wheels

FROM python:3.10-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libxml2-dev \
        libxslt1-dev \
        libjpeg62-turbo-dev \
        zlib1g-dev \
        fonts-dejavu-core \
        fonts-hosny-amiri \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
COPY --from=wheels /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels -r requirements.txt

COPY . .

CMD ["python", "-u", "-m", "bot.bot"]
