FROM python:3.10-slim AS wheels

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=5

WORKDIR /wheels

COPY ai_model/requirements.txt ./
RUN pip download --no-cache-dir --timeout 300 --retries 5 -r requirements.txt -d /wheels

FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY ai_model/requirements.txt ./
COPY --from=wheels /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels -r requirements.txt

# Copy service code + model file from the repo
COPY ai_model/ /app/ai_model/
COPY ai_model/ocr_crnn_model.keras /models/ocr_crnn_model.keras

ENV MODEL_PATH=/models/ocr_crnn_model.keras

EXPOSE 8000

CMD ["uvicorn", "ai_model.app:app", "--host", "0.0.0.0", "--port", "8000"]
