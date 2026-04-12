FROM python:3.11-slim

WORKDIR /app

COPY rca-engine/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY shared /app/shared
COPY rca-engine/ /app/

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
